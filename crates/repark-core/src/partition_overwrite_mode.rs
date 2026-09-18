use std::any::Any;
use std::collections::HashMap;
use std::hash::BuildHasher;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::{SessionConfig, SessionContext};

pub const PARTITION_OVERWRITE_MODE_KEY: &str = "spark.sql.sources.partitionOverwriteMode";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitionOverwriteMode {
    #[default]
    Static,
    Dynamic,
}

impl PartitionOverwriteMode {
    #[must_use]
    pub fn is_dynamic(self) -> bool {
        self == Self::Dynamic
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_partition_overwrite_mode(raw: &str) -> Result<PartitionOverwriteMode> {
    match raw.to_ascii_lowercase().as_str() {
        "static" => Ok(PartitionOverwriteMode::Static),
        "dynamic" => Ok(PartitionOverwriteMode::Dynamic),
        _ => Err(DataFusionError::Configuration(format!(
            "[INVALID_CONF_VALUE.OUT_OF_RANGE_OF_OPTIONS] The value '{raw}' in the config \
             \"{PARTITION_OVERWRITE_MODE_KEY}\" is invalid. It should be one of 'STATIC, \
             DYNAMIC'. SQLSTATE: 22022"
        ))),
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn partition_overwrite_mode_from_config_map<S>(
    config: &HashMap<String, String, S>,
) -> Result<PartitionOverwriteMode>
where
    S: BuildHasher,
{
    match config.get(PARTITION_OVERWRITE_MODE_KEY) {
        Some(raw) => parse_partition_overwrite_mode(raw),
        None => Ok(PartitionOverwriteMode::default()),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionOverwriteModeConfig {
    pub mode: PartitionOverwriteMode,
}

impl ConfigExtension for PartitionOverwriteModeConfig {
    const PREFIX: &'static str = "repark.overwrite";
}

impl ExtensionOptions for PartitionOverwriteModeConfig {
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
            "`{}.{key}` is not a settable option: the overwrite mode is set with \
             `{PARTITION_OVERWRITE_MODE_KEY}` on the session builder; change it at runtime with \
             `SET {PARTITION_OVERWRITE_MODE_KEY}`",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[must_use]
pub fn with_partition_overwrite_mode(
    config: SessionConfig,
    mode: PartitionOverwriteMode,
) -> SessionConfig {
    config.with_option_extension(PartitionOverwriteModeConfig { mode })
}

#[must_use]
pub fn partition_overwrite_mode_from_options(options: &ConfigOptions) -> PartitionOverwriteMode {
    options
        .extensions
        .get::<PartitionOverwriteModeConfig>()
        .map_or(PartitionOverwriteMode::default(), |extension| {
            extension.mode
        })
}

#[must_use]
pub fn partition_overwrite_mode_from_ctx(ctx: &SessionContext) -> PartitionOverwriteMode {
    partition_overwrite_mode_from_options(ctx.copied_config().options())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_static_dynamic_case_insensitive() {
        for (raw, expected) in [
            ("static", PartitionOverwriteMode::Static),
            ("STATIC", PartitionOverwriteMode::Static),
            ("dynamic", PartitionOverwriteMode::Dynamic),
            ("DYNAMIC", PartitionOverwriteMode::Dynamic),
            ("DyNaMiC", PartitionOverwriteMode::Dynamic),
        ] {
            assert_eq!(parse_partition_overwrite_mode(raw).unwrap(), expected);
        }
    }

    #[test]
    fn parse_refuses_unknown_naming_the_key() {
        for raw in ["bogus", "", "dynamic ", " true"] {
            let error = parse_partition_overwrite_mode(raw)
                .expect_err("an unknown mode must not silently become static");
            assert!(
                error.to_string().contains(PARTITION_OVERWRITE_MODE_KEY),
                "refusal must name the key: {error}"
            );
        }
    }

    #[test]
    fn missing_map_key_defaults_static() {
        let config = HashMap::<String, String>::new();
        assert_eq!(
            partition_overwrite_mode_from_config_map(&config).unwrap(),
            PartitionOverwriteMode::Static
        );
    }

    #[test]
    fn map_dynamic_installs_dynamic() {
        let mut config = HashMap::new();
        config.insert(
            PARTITION_OVERWRITE_MODE_KEY.to_string(),
            "dynamic".to_string(),
        );
        assert_eq!(
            partition_overwrite_mode_from_config_map(&config).unwrap(),
            PartitionOverwriteMode::Dynamic
        );
    }

    #[test]
    fn missing_extension_defaults_static() {
        let options = ConfigOptions::new();
        assert_eq!(
            partition_overwrite_mode_from_options(&options),
            PartitionOverwriteMode::Static
        );
    }

    #[test]
    fn installed_dynamic_is_readable() {
        let config =
            with_partition_overwrite_mode(SessionConfig::new(), PartitionOverwriteMode::Dynamic);
        assert_eq!(
            partition_overwrite_mode_from_options(config.options()),
            PartitionOverwriteMode::Dynamic
        );
    }
}
