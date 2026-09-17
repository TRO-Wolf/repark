//! Spark-door `spark.sql.caseSensitive` carrier for name resolution.

use std::any::Any;
use std::collections::HashMap;
use std::hash::BuildHasher;

use datafusion::common::Result;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionConfig;

/// Canonical Spark `SQLConf` key.
pub const SPARK_SQL_CASE_SENSITIVE_KEY: &str = "spark.sql.caseSensitive";

/// Spark default: names resolve case-insensitively.
pub const DEFAULT_SPARK_SQL_CASE_SENSITIVE: bool = false;

/// Session-scoped case-sensitivity flag name resolution reads out of [`ConfigOptions`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparkCaseSensitiveConfig {
    /// `true` → exact match; `false` → ASCII case-insensitive fold.
    pub enabled: bool,
}

impl Default for SparkCaseSensitiveConfig {
    fn default() -> Self {
        Self {
            enabled: DEFAULT_SPARK_SQL_CASE_SENSITIVE,
        }
    }
}

impl ConfigExtension for SparkCaseSensitiveConfig {
    /// Two segments keep the carrier unreachable through `SET`.
    const PREFIX: &'static str = "repark.case-sensitive";
}

impl ExtensionOptions for SparkCaseSensitiveConfig {
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
            "`{}.{key}` is not a settable option: case sensitivity is set with \
             `{SPARK_SQL_CASE_SENSITIVE_KEY}` on the session builder; change it at runtime with \
             `spark.conf.set`",
            Self::PREFIX
        )))
    }

    /// Keep the carrier out of `SET` listings.
    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

/// Parse `spark.sql.caseSensitive`.
/// # Errors
/// A present value that is not a boolean token.
pub fn parse_spark_sql_case_sensitive(raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(DataFusionError::Configuration(format!(
            "The value '{raw}' in the config \
             \"{SPARK_SQL_CASE_SENSITIVE_KEY}\" is invalid. \
             {SPARK_SQL_CASE_SENSITIVE_KEY} should be boolean, but was {raw}"
        ))),
    }
}

/// Parse `spark.sql.caseSensitive` for `spark.conf.set` (strict booleans only).
/// # Errors
/// A value that is not `true` or `false`.
pub fn parse_runtime_spark_sql_case_sensitive(raw: &str) -> Result<bool> {
    if raw.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if raw.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(DataFusionError::Configuration(format!(
            "[INVALID_CONF_VALUE.TYPE_MISMATCH] The value '{raw}' in the config \
             \"{SPARK_SQL_CASE_SENSITIVE_KEY}\" is invalid. It should be a/an 'boolean' value. \
             SQLSTATE: 22022"
        )))
    }
}

/// Read the builder conf map.
/// # Errors
/// Present but unparsable value.
pub fn spark_case_sensitive_from_config_map<S>(config: &HashMap<String, String, S>) -> Result<bool>
where
    S: BuildHasher,
{
    match config.get(SPARK_SQL_CASE_SENSITIVE_KEY) {
        Some(raw) => parse_spark_sql_case_sensitive(raw),
        None => Ok(DEFAULT_SPARK_SQL_CASE_SENSITIVE),
    }
}

/// Attach the flag to a [`SessionConfig`] (Spark door `configure` hook).
#[must_use]
pub fn with_spark_case_sensitive_config(config: SessionConfig, enabled: bool) -> SessionConfig {
    config.with_option_extension(SparkCaseSensitiveConfig { enabled })
}

/// Analyzer / resolver accessor.
#[must_use]
pub fn spark_case_sensitive_from_options(options: &ConfigOptions) -> bool {
    options
        .extensions
        .get::<SparkCaseSensitiveConfig>()
        .map_or(DEFAULT_SPARK_SQL_CASE_SENSITIVE, |extension| {
            extension.enabled
        })
}
