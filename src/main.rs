mod config;
mod storage;

use clap::Parser;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;

#[derive(Parser)]
#[command(name = "slimhub", version)]
struct Cli {
    #[arg(short, long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    let cfg = config::Config::load(cli.config).expect("failed to load config");
    let hub_id = cfg.hub_id.clone();

    // 1. 打开 sled
    let store = Arc::new(storage::Storage::open(&cfg.db_path).expect("failed to open storage"));
    tracing::info!("storage ready, pending items: {}", store.pending_len());

    // 2. 建立 Zenoh 会话
    let session = match zenoh::open(zenoh::Config::default()).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            tracing::error!("zenoh: failed to connect: {}", e);
            return;
        }
    };

    // 3. 订阅 Chunk
    {
        let store = store.clone();
        let session = session.clone();
        let sub_topic = format!("{}/**", slim_common::topics::CHUNK_PREFIX);
        tokio::spawn(async move {
            match session.declare_subscriber(&sub_topic).await {
                Ok(subscriber) => {
                    tracing::info!("subscribed: {}", sub_topic);
                    while let Ok(sample) = subscriber.recv_async().await {
                        let key_expr = sample.key_expr().to_string();
                        let blind_id_hex = match key_expr.rsplit('/').next() {
                            Some(seg) => seg,
                            None => {
                                continue;
                            }
                        };
                        let blind_id = match hex::decode(blind_id_hex) {
                            Ok(bytes) if bytes.len() == 16 => {
                                let mut arr = [0u8; 16];
                                arr.copy_from_slice(&bytes);
                                arr
                            }
                            _ => {
                                continue;
                            }
                        };
                        let payload: Vec<u8> = sample.payload().to_bytes().into();
                        match store.insert_pending(&blind_id, &payload) {
                            Ok((_is_new, _key)) => {
                                if _is_new {
                                    tracing::debug!("inserted pending: {}", blind_id_hex);
                                }
                                let ack_topic =
                                    format!("{}/{}", slim_common::topics::ACK_PREFIX, blind_id_hex);
                                if let Err(e) = session.put(&ack_topic, "OK").await {
                                    tracing::error!("ack send failed: {:?}", e);
                                }
                            }
                            Err(e) => {
                                tracing::error!("insert_pending error: {:?}", e);
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("failed to declare subscriber: {:?}", e);
                }
            }
        });
    }

    // 4. 水位监控
    {
        let store = store.clone();
        let session = session.clone();
        let hub_id = hub_id.clone();
        let disk_cap = cfg.disk_capacity_gb * 1024 * 1024 * 1024;
        let high = cfg.high_watermark_pct;
        let low = cfg.low_watermark_pct;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                match store.trim_to_watermark(disk_cap, high, low) {
                    Ok(true) => {
                        let topic =
                            format!("{}/{}", slim_common::topics::BACKPRESSURE_PREFIX, hub_id);
                        let frame = serde_json::json!({
                            "hub_id": hub_id,
                            "level": "CRITICAL",
                            "disk_usage_pct": high,
                        });
                        if let Err(e) = session.put(&topic, frame.to_string()).await {
                            tracing::error!("backpressure broadcast failed: {:?}", e);
                        }
                        tracing::warn!("backpressure broadcast: disk at {}%", high);
                    }
                    Ok(false) => {}
                    Err(e) => tracing::error!("watermark error: {:?}", e),
                }
            }
        });
    }

    // 5. slimRagSvr 拉取响应
    {
        let store = store.clone();
        let session = session.clone();
        let hub_id = hub_id.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if store.pending_is_empty() {
                    continue;
                }
                let batch = store.pop_pending_batch(64);
                for (key, value) in &batch {
                    let topic = format!("slim/hub/{}/data", hub_id);
                    if let Err(e) = session.put(&topic, value.clone()).await {
                        tracing::warn!("pull push error: {:?}", e);
                        continue;
                    }
                    if let Err(e) = store.confirm_consumed(key) {
                        tracing::error!("confirm consumed error: {:?}", e);
                    }
                }
            }
        });
    }

    // 6. 等待退出
    signal::ctrl_c().await.expect("failed to listen for ctrl+c");
    tracing::info!("shutting down...");
}
