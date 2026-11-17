use std::any::Any;
use std::collections::HashMap;
use std::hash::BuildHasher;

use datafusion::common::Result;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionConfig;

pub const DECIMAL_TRUNCATE_LEGACY_KEY: &str =
    "spark.sql.legacy.decimal.retainFractionDigitsOnTruncate";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SparkDecimalTruncateLegacy {
    pub enabled: bool,
}

impl ConfigExtension for SparkDecimalTruncateLegacy {
    const PREFIX: &'static str = "repark.decimal_truncate_legacy";
}

impl ExtensionOptions for SparkDecimalTruncateLegacy {
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
            "`{}.{key}` is not a settable option; set `{DECIMAL_TRUNCATE_LEGACY_KEY}` instead",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[must_use]
pub fn parse_decimal_truncate_legacy(raw: &str) -> Option<bool> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        Some(true)
    } else if trimmed.eq_ignore_ascii_case("false") {
        Some(false)
    } else {
        None
    }
}

pub fn write_decimal_truncate_legacy(options: &mut ConfigOptions, enabled: bool) {
    options
        .extensions
        .insert(SparkDecimalTruncateLegacy { enabled });
}

#[allow(clippy::missing_errors_doc)]
pub fn install_decimal_truncate_legacy<S: BuildHasher>(
    mut config: SessionConfig,
    conf: &HashMap<String, String, S>,
) -> Result<SessionConfig> {
    let Some(raw) = conf.get(DECIMAL_TRUNCATE_LEGACY_KEY) else {
        return Ok(config);
    };
    let enabled = parse_decimal_truncate_legacy(raw).ok_or_else(|| {
        DataFusionError::Configuration(crate::merge_schema::boolean_type_mismatch_message(
            DECIMAL_TRUNCATE_LEGACY_KEY,
            &repark_common::redaction::mask_value_credentials(raw),
        ))
    })?;
    write_decimal_truncate_legacy(config.options_mut(), enabled);
    Ok(config)
}

#[must_use]
pub fn spark_decimal_rule_enabled(options: &ConfigOptions) -> bool {
    !options
        .extensions
        .get::<SparkDecimalTruncateLegacy>()
        .is_some_and(|legacy| legacy.enabled)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion::common::config::ConfigOptions;
    use datafusion::prelude::SessionConfig;

    use super::{
        DECIMAL_TRUNCATE_LEGACY_KEY, install_decimal_truncate_legacy,
        parse_decimal_truncate_legacy, spark_decimal_rule_enabled, write_decimal_truncate_legacy,
    };

    #[test]
    fn the_flag_parses_booleans_and_disables_the_rule() {
        assert_eq!(parse_decimal_truncate_legacy(" TRUE "), Some(true));
        assert_eq!(parse_decimal_truncate_legacy("false"), Some(false));
        assert_eq!(parse_decimal_truncate_legacy("yes"), None);
        let mut options = ConfigOptions::new();
        assert!(spark_decimal_rule_enabled(&options));
        write_decimal_truncate_legacy(&mut options, true);
        assert!(!spark_decimal_rule_enabled(&options));
        write_decimal_truncate_legacy(&mut options, false);
        assert!(spark_decimal_rule_enabled(&options));
    }

    #[test]
    fn the_builder_conf_installs_the_flag_and_refuses_a_non_boolean() {
        let conf = HashMap::from([(DECIMAL_TRUNCATE_LEGACY_KEY.to_string(), "true".to_string())]);
        let config = install_decimal_truncate_legacy(SessionConfig::new(), &conf).unwrap();
        assert!(!spark_decimal_rule_enabled(config.options()));
        let empty: HashMap<String, String> = HashMap::new();
        let config = install_decimal_truncate_legacy(SessionConfig::new(), &empty).unwrap();
        assert!(spark_decimal_rule_enabled(config.options()));
        let bad = HashMap::from([(DECIMAL_TRUNCATE_LEGACY_KEY.to_string(), "nope".to_string())]);
        let refusal = install_decimal_truncate_legacy(SessionConfig::new(), &bad).unwrap_err();
        assert!(
            refusal.to_string().contains(DECIMAL_TRUNCATE_LEGACY_KEY),
            "{refusal}"
        );
    }
}
