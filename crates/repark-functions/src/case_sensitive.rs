use std::any::Any;
use std::collections::HashMap;
use std::hash::BuildHasher;

use datafusion::common::Result;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionConfig;

pub const SPARK_SQL_CASE_SENSITIVE_KEY: &str = "spark.sql.caseSensitive";

pub const DEFAULT_SPARK_SQL_CASE_SENSITIVE: bool = false;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparkCaseSensitiveConfig {
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

    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: case sensitivity is set with \
             `{SPARK_SQL_CASE_SENSITIVE_KEY}` on the session builder; change it at runtime with \
             `spark.conf.set`",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[allow(clippy::missing_errors_doc)]
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

#[allow(clippy::missing_errors_doc)]
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

#[allow(clippy::missing_errors_doc)]
pub fn spark_case_sensitive_from_config_map<S>(config: &HashMap<String, String, S>) -> Result<bool>
where
    S: BuildHasher,
{
    match config.get(SPARK_SQL_CASE_SENSITIVE_KEY) {
        Some(raw) => parse_spark_sql_case_sensitive(raw),
        None => Ok(DEFAULT_SPARK_SQL_CASE_SENSITIVE),
    }
}

#[must_use]
pub fn with_spark_case_sensitive_config(config: SessionConfig, enabled: bool) -> SessionConfig {
    config.with_option_extension(SparkCaseSensitiveConfig { enabled })
}

#[must_use]
pub fn spark_case_sensitive_from_options(options: &ConfigOptions) -> bool {
    options
        .extensions
        .get::<SparkCaseSensitiveConfig>()
        .map_or(DEFAULT_SPARK_SQL_CASE_SENSITIVE, |extension| {
            extension.enabled
        })
}
