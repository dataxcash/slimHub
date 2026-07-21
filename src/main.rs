mod config;
mod storage;

use std::sync::Arc;
use std::time::Duration;
use tokio::signal;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let cfg = config::Config::load().expect("failed to load config");
    let hub_id = cfg.hub_id.clone();

    // 1. 打开 sled
    let store = Arc::new(
        storage::Storage::open(&cfg.db_path)
            .expect("failed to open storage")
    );
    tracing::info!("storage ready, pending items: {}", store.pending_len());

    // 2. 建立 Zenoh 会话
    let session = match zenoh::open(zenoh::Config::default()).await {
        Ok(s) => Arc::new(s),
        Err(e) => {
            tracing::error!("zenoh: failed to connect: {}", e);
            return;
        }
    };

    // 3. 订阅 Chunk：插入 pending → fsync → ACK（幂等：已存在则跳过 IO 重发 ACK）
    {
        let store = store.clone();
        let session = session.clone();
        let sub_topic = format!("{}/**", slim_common::topics::CHUNK_PREFIX);
        tokio::spawn(async move {
            // TODO Phase 2: 替换为真实的 zenoh::pubsub::Subscriber
            //   let sub = session.declare_subscriber(&sub_topic).res().await.unwrap();
            tracing::info!("subscribed (stub): {}", sub_topic);
            // 当前 stub，等待信号退出
            let _ = store;
            let _ = session;
            let _ = sub_topic;
        });
    }

    // 4. 水位监控：每 30 秒检查，需要背压时广播
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
                        // Tier 3 触发：广播背压信号
                        let topic = format!("{}/{}", slim_common::topics::BACKPRESSURE_PREFIX, hub_id);
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

    // 5. 响应 slimRagSvr 拉取消费
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
