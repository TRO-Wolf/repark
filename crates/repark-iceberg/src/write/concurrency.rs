//! Write-path concurrency knobs (session conf only — never table properties).

use datafusion::common::config::ConfigExtension;
use datafusion::common::extensions_options;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{SessionConfig, SessionContext};

/// Session conf key (hyphen form — the user-facing spelling).
pub const MAX_CONCURRENT_FILES_KEY: &str = "repark.write.max-concurrent-files";

/// Alternate underscore form accepted at parse time (maps to the same extension field).
pub const MAX_CONCURRENT_FILES_KEY_ALT: &str = "repark.write.max_concurrent_files";

/// Default when the key is unset — four concurrent file writers.
pub const DEFAULT_MAX_CONCURRENT_FILES: usize = 4;

/// Validated write-path concurrency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteConcurrency {
    /// Maximum number of concurrent file-writer tasks (≥ 1).
    pub max_concurrent_files: usize,
}

impl Default for WriteConcurrency {
    fn default() -> Self {
        Self {
            max_concurrent_files: DEFAULT_MAX_CONCURRENT_FILES,
        }
    }
}

impl WriteConcurrency {
    /// Build from a validated positive count.
    /// # Errors
    /// Returns a DataFusion plan error when `max_concurrent_files < 1`.
    pub fn new(max_concurrent_files: usize) -> Result<Self> {
        if max_concurrent_files < 1 {
            return Err(DataFusionError::Plan(format!(
                "config `{MAX_CONCURRENT_FILES_KEY}` must be >= 1 (got {max_concurrent_files})"
            )));
        }
        Ok(Self {
            max_concurrent_files,
        })
    }

    /// Parse a raw conf string (`"4"`, `"1"`, …).
    /// # Errors
    /// Non-integer or `< 1` values fail loud with the key name in the message.
    pub fn parse(raw: &str) -> Result<Self> {
        let value: usize = raw.parse().map_err(|_| {
            DataFusionError::Plan(format!(
                "config `{MAX_CONCURRENT_FILES_KEY}` must be a positive integer (got {raw:?})"
            ))
        })?;
        Self::new(value)
    }
}

// DataFusion extension: SET / options key is `repark.write.max_concurrent_files` (underscore
extensions_options! {
    /// RePark write-path execution knobs (session-scoped, not table properties).
    pub struct ReparkWriteConfig {
        /// Max concurrent Iceberg file writers (`repark.write.max-concurrent-files`).
        pub max_concurrent_files: usize, default = 4_usize
    }
}

impl ConfigExtension for ReparkWriteConfig {
    const PREFIX: &'static str = "repark.write";
}

/// Attach [`ReparkWriteConfig`] to a [`SessionConfig`] (called from session build).
#[must_use]
pub fn with_write_concurrency(
    config: SessionConfig,
    concurrency: WriteConcurrency,
) -> SessionConfig {
    config.with_option_extension(ReparkWriteConfig {
        max_concurrent_files: concurrency.max_concurrent_files,
    })
}

/// Resolve write concurrency from a live [`SessionContext`] (extension or default).
#[must_use]
pub fn concurrency_from_ctx(ctx: &SessionContext) -> WriteConcurrency {
    ctx.copied_config()
        .options()
        .extensions
        .get::<ReparkWriteConfig>()
        .map(|extension| WriteConcurrency {
            max_concurrent_files: extension.max_concurrent_files.max(1),
        })
        .unwrap_or_default()
}

/// Pull `repark.write.max-concurrent-files` (or underscore alt) from a builder conf map.
/// # Errors
/// Invalid integer or value `< 1`.
pub fn concurrency_from_config_map<S>(
    config: &std::collections::HashMap<String, String, S>,
) -> Result<WriteConcurrency>
where
    S: std::hash::BuildHasher,
{
    let raw = config
        .get(MAX_CONCURRENT_FILES_KEY)
        .or_else(|| config.get(MAX_CONCURRENT_FILES_KEY_ALT));
    match raw {
        None => Ok(WriteConcurrency::default()),
        Some(value) => WriteConcurrency::parse(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_four() {
        assert_eq!(
            WriteConcurrency::default().max_concurrent_files,
            DEFAULT_MAX_CONCURRENT_FILES
        );
    }

    #[test]
    fn parse_rejects_zero_and_non_integer() {
        assert!(WriteConcurrency::parse("0").is_err());
        assert!(WriteConcurrency::parse("-1").is_err());
        assert!(WriteConcurrency::parse("nope").is_err());
        assert_eq!(
            WriteConcurrency::parse("1").unwrap().max_concurrent_files,
            1
        );
        assert_eq!(
            WriteConcurrency::parse("8").unwrap().max_concurrent_files,
            8
        );
    }

    #[test]
    fn config_map_hyphen_and_underscore() {
        let mut map = std::collections::HashMap::new();
        map.insert(MAX_CONCURRENT_FILES_KEY.to_string(), "2".to_string());
        assert_eq!(
            concurrency_from_config_map(&map)
                .unwrap()
                .max_concurrent_files,
            2
        );
        map.clear();
        map.insert(MAX_CONCURRENT_FILES_KEY_ALT.to_string(), "3".to_string());
        assert_eq!(
            concurrency_from_config_map(&map)
                .unwrap()
                .max_concurrent_files,
            3
        );
    }
}
