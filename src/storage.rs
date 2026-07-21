use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use sled::Db;

/// 存储引擎：sled 双树结构
pub struct Storage {
    db: Db,
    db_path: String,
    /// 待消费密文树
    /// Key:   [u64 大端序时间戳] + [16B blind_id]
    /// Value: 密文 Payload
    pending: sled::Tree,
    /// 已消费去重缓存
    /// Key:   [16B blind_id]
    /// Value: [u64 时间戳]（用于 TTL 淘汰）
    acknowledged: sled::Tree,
}

impl Storage {
    pub fn open(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let db: Db = sled::open(path)?;
        let pending = db.open_tree("pending")?;
        let acknowledged = db.open_tree("acknowledged")?;
        tracing::info!("sled opened: {}, size: {} items",
            path,
            pending.len() + acknowledged.len(),
        );
        Ok(Storage { db, db_path: path.to_string(), pending, acknowledged })
    }

    /// 写入 pending + 同步落盘 → 返回该条目的 key（用于 ACK）
    pub fn insert_pending(
        &self,
        blind_id: &[u8; 16],
        payload: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        // Key: 大端序时间戳 + blind_id → 天然按时间物理排序
        let mut key = Vec::with_capacity(16 + 8);
        key.extend_from_slice(&now.to_be_bytes());
        key.extend_from_slice(blind_id);

        self.pending.insert(&key, payload)?;
        // 同步落盘后立即 ACK，不等待 slimRagSvr 消费
        self.db.flush()?;
        Ok(key)
    }

    /// 批量拉取 pending 中最旧的 n 条
    pub fn pop_pending_batch(&self, n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut batch = Vec::with_capacity(n);
        for result in self.pending.iter().take(n) {
            match result {
                Ok((key, value)) => {
                    batch.push((key.to_vec(), value.to_vec()));
                }
                Err(e) => {
                    tracing::error!("pending iter error: {:?}", e);
                    break;
                }
            }
        }
        batch
    }

    /// 消费确认：从 pending 移除，写入 acknowledged
    pub fn confirm_consumed(&self, key: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(value) = self.pending.remove(key)? {
            // acknowledged 只存 blind_id（key 末尾 16B）和时间戳
            let blind_id = &key[key.len() - 16..];
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            self.acknowledged.insert(blind_id, &now.to_be_bytes())?;
        }
        Ok(())
    }

    /// 检查 blind_id 是否已被消费（去重回查）
    pub fn is_acknowledged(&self, blind_id: &[u8; 16]) -> Result<bool, Box<dyn std::error::Error>> {
        Ok(self.acknowledged.contains_key(blind_id)?)
    }

    /// pending 当前条目数
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// pending 是否为空
    pub fn pending_is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    // ============================================================
    // 双水位截断
    // ============================================================

    /// 获取 sled 文件大小（字节）
    pub fn db_size(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let path = std::path::Path::new(&self.db_path);
        let meta = std::fs::metadata(path)?;
        Ok(meta.len())
    }

    /// 从 pending 中最旧的数据开始删除，直到低于 low_watermark 比例
    pub fn trim_to_watermark(&self, disk_capacity: u64, high_pct: u8, low_pct: u8) -> Result<(), Box<dyn std::error::Error>> {
        let current = self.db_size()?;
        let high_bytes = (disk_capacity as f64 * high_pct as f64 / 100.0) as u64;
        let low_bytes = (disk_capacity as f64 * low_pct as f64 / 100.0) as u64;

        if current < high_bytes {
            return Ok(()); // 未达到高水位
        }

        tracing::warn!(
            "watermark: {}/{} bytes ({}%), trimming to {}%...",
            current, disk_capacity,
            (current as f64 / disk_capacity as f64 * 100.0) as u8,
            low_pct,
        );

        // 从最旧的条目开始删除
        let mut removed = 0u64;
        for result in self.pending.iter() {
            if self.db_size()? <= low_bytes {
                break;
            }
            match result {
                Ok((key, _)) => {
                    self.pending.remove(&key)?;
                    removed += 1;
                }
                Err(e) => {
                    tracing::error!("trim error: {:?}", e);
                    break;
                }
            }
        }

        self.db.flush()?;
        tracing::info!("trimmed {} items, size now: {} bytes", removed, self.db_size()?);
        Ok(())
    }
}
