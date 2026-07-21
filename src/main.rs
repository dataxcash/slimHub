mod config;
mod storage;

use std::sync::Arc;
use std::time::Duration;
use tokio::signal;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // 1. 加载配置
    let cfg = config::Config::load().expect("failed to load config");

    // 2. 打开 sled 存储引擎
    let store = Arc::new(
        storage::Storage::open(&cfg.db_path)
            .expect("failed to open storage")
    );
    tracing::info!("storage ready, pending items: {}", store.pending_len());

    // 3. 建立 Zenoh 会话
    let session = match zenoh::open(zenoh::Config::default()).await {
        Ok(s) => {
            tracing::info!("zenoh: connected");
            Arc::new(s)
        }
        Err(e) => {
            tracing::error!("zenoh: failed to connect: {}", e);
            return;
        }
    };

    // 4. 订阅 slimSync 发来的 Chunk：写入 pending → fsync → 异步 ACK
    let store_sub = store.clone();
    let session_sub = session.clone();
    let sub_topic = format!("{}/**", slim_common::topics::CHUNK_PREFIX);
    tokio::spawn(async move {
        if let Ok(sub) = session_sub.declare_subscriber(&sub_topic).await {
            tracing::info!("subscribed to: {}", sub_topic);
            loop {
                match sub.recv_async().await {
                    Ok(sample) => {
                        let blind_id_hex = sample.key_expr().to_string();
                        let payload: Vec<u8> = sample.payload().to_bytes().to_vec();
                        // 从 key 中提取 blind_id
                        let blind_id = match hex::decode(
                            blind_id_hex.rsplit('/').next().unwrap_or("")
                        ) {
                            Ok(b) => {
                                let mut id = [0u8; 16];
                                let len = b.len().min(16);
                                id[..len].copy_from_slice(&b[..len]);
                                id
                            }
                            Err(_) => continue,
                        };

                        // 写入 pending → fsync
                        if let Err(e) = store_sub.insert_pending(&blind_id, &payload) {
                            tracing::error!("sled insert failed: {:?}", e);
                            continue;
                        }

                        // 立刻发送 ACK（不等待 slimRagSvr 消费）
                        let ack_topic = format!("{}/{}",
                            slim_common::topics::ACK_PREFIX,
                            blind_id_hex.rsplit('/').next().unwrap_or(""),
                        );
                        if let Err(e) = session_sub.put(&ack_topic, "1").await {
                            tracing::warn!("ack publish failed: {:?}", e);
                        }
                    }
                    Err(e) => {
                        tracing::error!("sub recv error: {:?}", e);
                        break;
                    }
                }
            }
        }
    });

    // 5. 水位监控线程：每 30 秒检查一次
    {
        let store_wm = store.clone();
        let disk_cap = cfg.disk_capacity_gb * 1024 * 1024 * 1024;
        let high = cfg.high_watermark_pct;
        let low = cfg.low_watermark_pct;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                if let Err(e) = store_wm.trim_to_watermark(disk_cap, high, low) {
                    tracing::error!("watermark trim error: {:?}", e);
                }
            }
        });
    }

    // 6. 响应 slimRagSvr 的拉取消费
    {
        let store_pull = store.clone();
        let session_pull = session.clone();
        let hub_id = cfg.hub_id.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if store_pull.pending_is_empty() {
                    continue;
                }
                // 每次拉取 64 条最旧的 pending 数据
                let batch = store_pull.pop_pending_batch(64);
                for (key, value) in &batch {
                    // 推送给所有订阅了 /slim/hub/{hub_id}/data 的 slimRagSvr
                    let topic = format!("slim/hub/{}/data", hub_id);
                    if let Err(e) = session_pull.put(&topic, value.clone()).await {
                        tracing::warn!("pull push error: {:?}", e);
                        continue;
                    }
                    // 收到 slimRagSvr 的消费确认后移除
                    // 简化版：推出去后即移除（实际应由 slimRagSvr 发 ACK Topic 来确认）
                    if let Err(e) = store_pull.confirm_consumed(key) {
                        tracing::error!("confirm consumed error: {:?}", e);
                    }
                }
            }
        });
    }

    // 7. 等待 Ctrl+C
    signal::ctrl_c().await.expect("failed to listen for ctrl+c");
    tracing::info!("shutting down...");
}
