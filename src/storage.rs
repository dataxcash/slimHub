use std::time::{SystemTime, UNIX_EPOCH};
use sled::Db;

/// 7 天 TTL（纳秒）
const TTL_7_DAYS_NS: u128 = 7 * 24 * 60 * 60 * 1_000_000_000;

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
    /// 打开 sled，显式控制并发线程以防边缘设备 CPU 抢占
    pub fn open(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let db: Db = sled::Config::new()
            .path(path)
            .cache_capacity(64 * 1024 * 1024)
            .flush_every_ms(Some(1000))
            .open()?;
        let pending = db.open_tree("pending")?;
        let acknowledged = db.open_tree("acknowledged")?;
        tracing::info!(
            "sled opened: {}, pending={}, acked={}",
            path, pending.len(), acknowledged.len(),
        );
        Ok(Storage { db, db_path: path.to_string(), pending, acknowledged })
    }

    /// 写入 pending（幂等：已存在则跳过 IO，仅重发 ACK）
    /// 返回 Ok(是否为新写入, key)
    pub fn insert_pending(
        &self,
        blind_id: &[u8; 16],
        payload: &[u8],
    ) -> Result<(bool, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
        // 幂等判定：若 blind_id 已在 pending/acknowledged 中，跳过磁盘 IO
        let pending_key = self.find_pending_by_blind_id(blind_id);
        if let Some(key) = pending_key {
            return Ok((false, key));
        }
        if self.acknowledged.contains_key(blind_id)? {
            return Ok((false, blind_id.to_vec()));
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        let mut key = Vec::with_capacity(16 + 8);
        key.extend_from_slice(&now.to_be_bytes());
        key.extend_from_slice(blind_id);

        self.pending.insert(&key, payload)?;
        self.db.flush()?;
        Ok((true, key))
    }

    /// 在 pending 树中按 blind_id 查找（扫描 key 末尾 16B）
    fn find_pending_by_blind_id(&self, blind_id: &[u8; 16]) -> Option<Vec<u8>> {
        for result in self.pending.iter() {
            match result {
                Ok((key, _)) => {
                    if key.len() >= 16 && &key[key.len() - 16..] == blind_id {
                        return Some(key.to_vec());
                    }
                }
                Err(_) => break,
            }
        }
        None
    }

    /// 批量拉取 pending 中最旧的 n 条
    pub fn pop_pending_batch(&self, n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut batch = Vec::with_capacity(n);
        for result in self.pending.iter().take(n) {
            match result {
                Ok((key, value)) => batch.push((key.to_vec(), value.to_vec())),
                Err(e) => {
                    tracing::error!("pending iter error: {:?}", e);
                    break;
                }
            }
        }
        batch
    }

    /// 消费确认：从 pending 移除，写入 acknowledged
    pub fn confirm_consumed(&self, key: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(value) = self.pending.remove(key)? {
            let blind_id = &key[key.len() - 16..];
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos();
            self.acknowledged.insert(blind_id, &now.to_be_bytes())?;
        }
        Ok(())
    }

    /// pending 条目数
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// pending 是否为空
    pub fn pending_is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    // ============================================================
    // 磁盘水位清理（按优先级三层递进）
    // ============================================================

    pub fn db_size(&self) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let meta = std::fs::metadata(&self.db_path)?;
        Ok(meta.len())
    }

    /// 三级清理：
    ///   1. acknowledged 树全部清除（安全，已被 RAG 消费）
    ///   2. pending 树中超 7 天 TTL 的过期数据
    ///   3. 若仍高于高水位 → 返回 true 表示需要背压，绝不静默丢数据
    pub fn trim_to_watermark(
        &self,
        disk_capacity: u64,
        high_pct: u8,
        low_pct: u8,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let high_bytes = (disk_capacity as f64 * high_pct as f64 / 100.0) as u64;
        let low_bytes = (disk_capacity as f64 * low_pct as f64 / 100.0) as u64;

        if self.db_size()? < high_bytes {
            return Ok(false);
        }

        tracing::warn!("watermark triggered, starting tiered cleanup...");

        // Tier 1: 清空 acknowledged 树（最安全）
        self.acknowledged.clear()?;
        self.db.flush()?;
        tracing::info!("tier1: cleared acknowledged tree");

        if self.db_size()? <= low_bytes {
            return Ok(false);
        }

        // Tier 2: 删除 pending 中超过 7 天的超期数据
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let mut removed = 0u64;
        let keys_to_remove: Vec<Vec<u8>> = self.pending.iter()
            .filter_map(|r| r.ok())
            .filter(|(key, _)| {
                if key.len() < 8 { return false; }
                let mut ts_bytes = [0u8; 8];
                ts_bytes.copy_from_slice(&key[..8]);
                let timestamp = u64::from_be_bytes(ts_bytes) as u128;
                now.saturating_sub(timestamp) > TTL_7_DAYS_NS
            })
            .map(|(key, _)| key.to_vec())
            .collect();

        for key in &keys_to_remove {
            self.pending.remove(key)?;
            removed += 1;
        }
        self.db.flush()?;
        tracing::info!("tier2: removed {} expired items (TTL >7d)", removed);

        if self.db_size()? <= low_bytes {
            return Ok(false);
        }

        // Tier 3: 清理后仍高于高水位 → 要求背压，绝不丢数据
        tracing::error!(
            "tier3: still above high watermark after cleanup, BACKPRESSURE NEEDED"
        );
        Ok(true) // caller 应广播背压信号
    }
}
