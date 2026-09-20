use std::any::Any;
use std::collections::HashMap;
use std::hash::BuildHasher;

use datafusion::common::Result;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionConfig;

pub const SPARK_SQL_ICEBERG_MERGE_SCHEMA_KEY: &str = "spark.sql.iceberg.merge-schema";

pub const DEFAULT_MERGE_SCHEMA: bool = false;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MergeSchemaConfig {
    pub enabled: bool,
}

impl ConfigExtension for MergeSchemaConfig {
    const PREFIX: &'static str = "repark.merge-schema";
}

impl ExtensionOptions for MergeSchemaConfig {
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
            "`{}.{key}` is not a settable option: schema merging is set with \
             `{SPARK_SQL_ICEBERG_MERGE_SCHEMA_KEY}` on the session builder; change it at runtime \
             with `spark.conf.set` or `SET {SPARK_SQL_ICEBERG_MERGE_SCHEMA_KEY}`",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_merge_schema_value(raw: &str) -> Result<bool> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        Ok(true)
    } else if trimmed.eq_ignore_ascii_case("false") {
        Ok(false)
    } else {
        Err(DataFusionError::Configuration(format!(
            "[INVALID_CONF_VALUE.TYPE_MISMATCH] The value '{raw}' in the config \
             \"{SPARK_SQL_ICEBERG_MERGE_SCHEMA_KEY}\" is invalid. It should be a/an 'boolean' \
             value. SQLSTATE: 22022"
        )))
    }
}

#[must_use]
pub fn is_merge_schema_session_key(key: &str) -> bool {
    key == SPARK_SQL_ICEBERG_MERGE_SCHEMA_KEY
}

#[allow(clippy::missing_errors_doc)]
pub fn merge_schema_from_config_map<S>(config: &HashMap<String, String, S>) -> Result<bool>
where
    S: BuildHasher,
{
    match config.get(SPARK_SQL_ICEBERG_MERGE_SCHEMA_KEY) {
        Some(raw) => parse_merge_schema_value(raw),
        None => Ok(DEFAULT_MERGE_SCHEMA),
    }
}

#[must_use]
pub fn with_merge_schema_config(config: SessionConfig, enabled: bool) -> SessionConfig {
    config.with_option_extension(MergeSchemaConfig { enabled })
}

#[must_use]
pub fn merge_schema_from_options(options: &ConfigOptions) -> bool {
    options
        .extensions
        .get::<MergeSchemaConfig>()
        .map_or(DEFAULT_MERGE_SCHEMA, |extension| extension.enabled)
}

#[cfg(test)]
mod tests;
