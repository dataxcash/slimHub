use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub db_path: String,
    pub disk_capacity_gb: u64,
    pub high_watermark_pct: u8,
    pub low_watermark_pct: u8,
    pub zenoh_listen: Vec<String>,
    pub hub_id: String,
}

impl Config {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let path = std::env::var("SLIMHUB_CONFIG")
            .unwrap_or_else(|_| "slimhub.toml".into());
        let content = std::fs::read_to_string(&path)?;
        let raw: toml::Value = toml::from_str(&content)?;

        Ok(Config {
            db_path: raw["storage"]["db_path"].as_str()
                .unwrap_or("/var/lib/slimhub/slimhub.db")
                .to_string(),
            disk_capacity_gb: raw["storage"]["disk_capacity_gb"]
                .as_integer().unwrap_or(10) as u64,
            high_watermark_pct: raw["storage"]["high_watermark_pct"]
                .as_integer().unwrap_or(80) as u8,
            low_watermark_pct: raw["storage"]["low_watermark_pct"]
                .as_integer().unwrap_or(60) as u8,
            zenoh_listen: raw["zenoh"]["listen"]
                .as_array()
                .map(|a| a.iter().filter_map(|v| v.as_str().map(String::from)).collect())
                .unwrap_or_else(|| vec!["tcp/0.0.0.0:7447".into()]),
            hub_id: raw["general"]["hub_id"].as_str()
                .unwrap_or("slimhub-1")
                .to_string(),
        })
    }
}
