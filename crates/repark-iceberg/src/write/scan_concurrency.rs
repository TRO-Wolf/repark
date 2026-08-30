//! MERGE target-scan concurrency (session conf only — never a table property).

use datafusion::common::config::ConfigExtension;
use datafusion::common::extensions_options;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{SessionConfig, SessionContext};

/// Session conf key (hyphen form — the user-facing spelling).
pub const SCAN_CONCURRENCY_LIMIT_KEY: &str = "repark.scan.concurrency-limit";

/// Alternate underscore form accepted at parse time.
pub const SCAN_CONCURRENCY_LIMIT_KEY_ALT: &str = "repark.scan.concurrency_limit";

/// Validated optional scan concurrency limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ScanConcurrency {
    /// Concurrent file-open limit for the MERGE target scan, when set.
    pub concurrency_limit: Option<usize>,
}

impl ScanConcurrency {
    /// Build from a positive limit.
    /// # Errors
    /// Returns a DataFusion plan error when `limit < 1`.
    pub fn new(limit: usize) -> Result<Self> {
        if limit < 1 {
            return Err(DataFusionError::Plan(format!(
                "config `{SCAN_CONCURRENCY_LIMIT_KEY}` must be >= 1 (got {limit})"
            )));
        }
        Ok(Self {
            concurrency_limit: Some(limit),
        })
    }

    /// Parse a raw conf string (`"8"`, `"16"`, …).
    /// # Errors
    /// Non-integer or `< 1` values fail loud with the key name in the message.
    pub fn parse(raw: &str) -> Result<Self> {
        let value: usize = raw.parse().map_err(|_| {
            DataFusionError::Plan(format!(
                "config `{SCAN_CONCURRENCY_LIMIT_KEY}` must be a positive integer (got {raw:?})"
            ))
        })?;
        Self::new(value)
    }
}

// DataFusion extension: field is 0 when unset (fork default); ≥1 when the user set a limit.
extensions_options! {
    /// RePark MERGE target-scan execution knobs (session-scoped, not table properties).
    pub struct ReparkScanConfig {
        /// Concurrent Iceberg file opens for the MERGE target scan (`0` = unset / fork default).
        pub concurrency_limit: usize, default = 0_usize
    }
}

impl ConfigExtension for ReparkScanConfig {
    const PREFIX: &'static str = "repark.scan";
}

/// Attach [`ReparkScanConfig`] to a [`SessionConfig`] (called from session build).
#[must_use]
pub fn with_scan_concurrency(config: SessionConfig, scan: ScanConcurrency) -> SessionConfig {
    config.with_option_extension(ReparkScanConfig {
        concurrency_limit: scan.concurrency_limit.unwrap_or(0),
    })
}

/// Resolve scan concurrency from a live [`SessionContext`].
#[must_use]
pub fn scan_concurrency_from_ctx(ctx: &SessionContext) -> ScanConcurrency {
    ctx.copied_config()
        .options()
        .extensions
        .get::<ReparkScanConfig>()
        .map(|extension| {
            if extension.concurrency_limit >= 1 {
                ScanConcurrency {
                    concurrency_limit: Some(extension.concurrency_limit),
                }
            } else {
                ScanConcurrency::default()
            }
        })
        .unwrap_or_default()
}

/// Pull `repark.scan.concurrency-limit` (or underscore alt) from a builder conf map.
/// # Errors
/// Invalid integer or value `< 1`.
pub fn scan_concurrency_from_config_map<S>(
    config: &std::collections::HashMap<String, String, S>,
) -> Result<ScanConcurrency>
where
    S: std::hash::BuildHasher,
{
    let raw = config
        .get(SCAN_CONCURRENCY_LIMIT_KEY)
        .or_else(|| config.get(SCAN_CONCURRENCY_LIMIT_KEY_ALT));
    match raw {
        None => Ok(ScanConcurrency::default()),
        Some(value) => ScanConcurrency::parse(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unset_default_is_none() {
        assert_eq!(ScanConcurrency::default().concurrency_limit, None);
        let empty = std::collections::HashMap::<String, String>::new();
        assert_eq!(
            scan_concurrency_from_config_map(&empty)
                .unwrap()
                .concurrency_limit,
            None
        );
    }

    #[test]
    fn parse_rejects_zero_and_non_integer() {
        assert!(ScanConcurrency::parse("0").is_err());
        assert!(ScanConcurrency::parse("-1").is_err());
        assert!(ScanConcurrency::parse("nope").is_err());
        assert_eq!(
            ScanConcurrency::parse("8").unwrap().concurrency_limit,
            Some(8)
        );
    }

    #[test]
    fn config_map_hyphen_and_underscore() {
        let mut map = std::collections::HashMap::new();
        map.insert(SCAN_CONCURRENCY_LIMIT_KEY.to_string(), "16".to_string());
        assert_eq!(
            scan_concurrency_from_config_map(&map)
                .unwrap()
                .concurrency_limit,
            Some(16)
        );
        map.clear();
        map.insert(SCAN_CONCURRENCY_LIMIT_KEY_ALT.to_string(), "32".to_string());
        assert_eq!(
            scan_concurrency_from_config_map(&map)
                .unwrap()
                .concurrency_limit,
            Some(32)
        );
    }

    #[test]
    fn extension_round_trip_through_session_config() {
        let limited = ScanConcurrency::new(8).unwrap();
        let config = with_scan_concurrency(SessionConfig::new(), limited);
        let ctx = SessionContext::new_with_config(config);
        assert_eq!(scan_concurrency_from_ctx(&ctx).concurrency_limit, Some(8));

        let unset = ScanConcurrency::default();
        let config = with_scan_concurrency(SessionConfig::new(), unset);
        let ctx = SessionContext::new_with_config(config);
        assert_eq!(scan_concurrency_from_ctx(&ctx).concurrency_limit, None);
    }
}
