use datafusion::common::config::ConfigExtension;
use datafusion::common::extensions_options;
use datafusion::prelude::SessionConfig;

extensions_options! {
    pub struct DescribeOwnerConfig {
        pub owner: String, default = "unknown".to_string()
    }
}

impl ConfigExtension for DescribeOwnerConfig {
    const PREFIX: &'static str = "repark.describe";
}

#[must_use]
pub fn session_owner_snapshot() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

#[must_use]
pub fn with_session_owner(config: SessionConfig, owner: String) -> SessionConfig {
    config.with_option_extension(DescribeOwnerConfig { owner })
}
