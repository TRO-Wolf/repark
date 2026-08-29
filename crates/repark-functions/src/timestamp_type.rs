//! Spark-door `spark.sql.timestampType` carrier for bare `TIMESTAMP` resolution.
//!
//! `TIMESTAMP_LTZ` is the default instant type; `TIMESTAMP_NTZ` opts into wall-clock values.
//! The carrier is parsed and installed at session build and is not settable through SQL.

use std::any::Any;
use std::collections::HashMap;
use std::hash::BuildHasher;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionConfig;

/// Canonical Spark `SQLConf` key. Parsed from the session builder map in `configure()`.
pub const SPARK_SQL_TIMESTAMP_TYPE_KEY: &str = "spark.sql.timestampType";

/// Spark / TZ-4 default: bare `TIMESTAMP` is an instant (LTZ).
pub const DEFAULT_SPARK_SQL_TIMESTAMP_TYPE: SparkTimestampType = SparkTimestampType::Ltz;

/// The two legal values, spelled as Spark's `SQLConf` checkValues set.
pub const TIMESTAMP_LTZ_VALUE: &str = "TIMESTAMP_LTZ";

/// Opt-in: bare `TIMESTAMP` is a wall clock (NTZ).
pub const TIMESTAMP_NTZ_VALUE: &str = "TIMESTAMP_NTZ";

/// ===========================================================================================
/// The session default for a bare SQL `TIMESTAMP`.
/// ===========================================================================================
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SparkTimestampType {
    /// Instant — Arrow `timestamp[us, tz=UTC]`, Iceberg `timestamptz`. The default.
    Ltz,
    /// Wall clock — Arrow `timestamp[us]` naive, Iceberg `timestamp`.
    Ntz,
}

impl SparkTimestampType {
    /// `true` when bare `TIMESTAMP` should resolve as NTZ.
    #[must_use]
    pub const fn is_ntz(self) -> bool {
        matches!(self, Self::Ntz)
    }

    /// The Spark conf token (`TIMESTAMP_LTZ` / `TIMESTAMP_NTZ`).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ltz => TIMESTAMP_LTZ_VALUE,
            Self::Ntz => TIMESTAMP_NTZ_VALUE,
        }
    }
}

impl Default for SparkTimestampType {
    fn default() -> Self {
        DEFAULT_SPARK_SQL_TIMESTAMP_TYPE
    }
}

/// ===========================================================================================
/// Session-scoped default the CAST / literal analyzer and Spark DDL read out of
/// [`ConfigOptions`].
/// ===========================================================================================
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparkTimestampTypeConfig {
    /// The resolved default.
    pub timestamp_type: SparkTimestampType,
}

impl Default for SparkTimestampTypeConfig {
    fn default() -> Self {
        Self {
            timestamp_type: DEFAULT_SPARK_SQL_TIMESTAMP_TYPE,
        }
    }
}

impl ConfigExtension for SparkTimestampTypeConfig {
    /// Two segments keep the carrier unreachable through `SET`.
    const PREFIX: &'static str = "repark.timestamp";
}

impl ExtensionOptions for SparkTimestampTypeConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    /// Refuse because the knob is set on the session builder.
    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: the default timestamp type is set with \
             `{SPARK_SQL_TIMESTAMP_TYPE_KEY}` on the session builder and is fixed at session build",
            Self::PREFIX
        )))
    }

    /// Keep the carrier out of `SET` listings.
    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

/// ===========================================================================================
/// Parse `spark.sql.timestampType`. Exact tokens after trim; the error names both legal values.
/// ===========================================================================================
///
/// # Errors
/// A present value that is not `TIMESTAMP_LTZ` or `TIMESTAMP_NTZ`.
pub fn parse_spark_sql_timestamp_type(raw: &str) -> Result<SparkTimestampType> {
    match raw.trim() {
        TIMESTAMP_LTZ_VALUE => Ok(SparkTimestampType::Ltz),
        TIMESTAMP_NTZ_VALUE => Ok(SparkTimestampType::Ntz),
        _ => Err(DataFusionError::Configuration(format!(
            "The value '{raw}' in the config \"{SPARK_SQL_TIMESTAMP_TYPE_KEY}\" is invalid. \
             {SPARK_SQL_TIMESTAMP_TYPE_KEY} should be one of {TIMESTAMP_LTZ_VALUE}, \
             {TIMESTAMP_NTZ_VALUE}"
        ))),
    }
}

/// ===========================================================================================
/// Read the builder conf map. Missing key → [`DEFAULT_SPARK_SQL_TIMESTAMP_TYPE`].
/// ===========================================================================================
///
/// # Errors
/// Present but unparsable value.
pub fn spark_timestamp_type_from_config_map<S>(
    config: &HashMap<String, String, S>,
) -> Result<SparkTimestampType>
where
    S: BuildHasher,
{
    match config.get(SPARK_SQL_TIMESTAMP_TYPE_KEY) {
        Some(raw) => parse_spark_sql_timestamp_type(raw),
        None => Ok(DEFAULT_SPARK_SQL_TIMESTAMP_TYPE),
    }
}

/// ===========================================================================================
/// Attach the default to a [`SessionConfig`] (Spark door `configure` hook).
/// ===========================================================================================
#[must_use]
pub fn with_spark_timestamp_type(
    config: SessionConfig,
    timestamp_type: SparkTimestampType,
) -> SessionConfig {
    config.with_option_extension(SparkTimestampTypeConfig { timestamp_type })
}

/// ===========================================================================================
/// Analyzer / DDL accessor. Missing carrier defaults to LTZ.
/// ===========================================================================================
#[must_use]
pub fn spark_timestamp_type_from_options(options: &ConfigOptions) -> SparkTimestampType {
    options
        .extensions
        .get::<SparkTimestampTypeConfig>()
        .map_or(DEFAULT_SPARK_SQL_TIMESTAMP_TYPE, |extension| {
            extension.timestamp_type
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_the_two_legal_tokens() {
        assert_eq!(
            parse_spark_sql_timestamp_type(TIMESTAMP_LTZ_VALUE).unwrap(),
            SparkTimestampType::Ltz
        );
        assert_eq!(
            parse_spark_sql_timestamp_type(TIMESTAMP_NTZ_VALUE).unwrap(),
            SparkTimestampType::Ntz
        );
        assert_eq!(
            parse_spark_sql_timestamp_type("  TIMESTAMP_NTZ  ").unwrap(),
            SparkTimestampType::Ntz
        );
    }

    #[test]
    fn parse_refuses_unknown_and_names_both_legal_values() {
        let error = parse_spark_sql_timestamp_type("TIMESTAMP")
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(SPARK_SQL_TIMESTAMP_TYPE_KEY),
            "error must name the key: {error}"
        );
        assert!(
            error.contains(TIMESTAMP_LTZ_VALUE) && error.contains(TIMESTAMP_NTZ_VALUE),
            "error must name both legal values: {error}"
        );
    }

    #[test]
    fn parse_is_case_sensitive_like_spark_check_values() {
        let error = parse_spark_sql_timestamp_type("timestamp_ntz")
            .unwrap_err()
            .to_string();
        assert!(
            error.contains(TIMESTAMP_NTZ_VALUE),
            "must name the canonical token: {error}"
        );
    }

    #[test]
    fn missing_map_key_defaults_ltz() {
        let config = HashMap::<String, String>::new();
        assert_eq!(
            spark_timestamp_type_from_config_map(&config).unwrap(),
            SparkTimestampType::Ltz
        );
    }

    #[test]
    fn missing_extension_defaults_ltz() {
        let options = ConfigOptions::new();
        assert_eq!(
            spark_timestamp_type_from_options(&options),
            SparkTimestampType::Ltz
        );
    }

    #[test]
    fn installed_ntz_is_readable() {
        let config = with_spark_timestamp_type(SessionConfig::new(), SparkTimestampType::Ntz);
        assert_eq!(
            spark_timestamp_type_from_options(config.options()),
            SparkTimestampType::Ntz
        );
    }
}
