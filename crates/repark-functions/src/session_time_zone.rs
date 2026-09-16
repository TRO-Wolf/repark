//! Session-timezone carrier for calendar extractors, never a second knob.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::ScalarValue;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion::prelude::SessionConfig;

/// The zone the extractors use when no [`SessionTimeZoneConfig`] is installed.
pub const DEFAULT_EXTRACTION_TIME_ZONE: &str = "UTC";

/// The authoritative key name used in the carrier refusal message.
const AUTHORITATIVE_KEY: &str = "spark.sql.session.timeZone";

/// The resolved session zone on [`ConfigOptions`] so extractors can read it at invoke time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTimeZoneConfig {
    zone: String,
}

impl Default for SessionTimeZoneConfig {
    fn default() -> Self {
        Self {
            zone: DEFAULT_EXTRACTION_TIME_ZONE.to_string(),
        }
    }
}

impl SessionTimeZoneConfig {
    /// The zone id the extractors resolve instants in (`UTC`, `America/New_York`, `+05:30`).
    #[must_use]
    pub fn zone(&self) -> &str {
        &self.zone
    }

    pub fn set_zone(&mut self, zone: &str) {
        self.zone = zone.to_string();
    }
}

impl ConfigExtension for SessionTimeZoneConfig {
    /// Two segments keep the carrier unreachable through `SET`.
    const PREFIX: &'static str = "repark.session";
}

impl ExtensionOptions for SessionTimeZoneConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    /// Refuse because the session build owns the setting.
    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: the session timezone is set with \
             `{AUTHORITATIVE_KEY}` on the session builder and is fixed at session build",
            Self::PREFIX
        )))
    }

    /// Keep the carrier out of `SET` listings.
    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

/// Attach the resolved session zone to a [`SessionConfig`] from the Spark door's `configure` hook.
#[must_use]
pub fn with_session_time_zone(config: SessionConfig, zone: &str) -> SessionConfig {
    config.with_option_extension(SessionTimeZoneConfig {
        zone: zone.to_string(),
    })
}

/// Read the session zone back out of live config options — the extractors' one accessor.
#[must_use]
pub fn session_time_zone_from_options(options: &ConfigOptions) -> &str {
    options
        .extensions
        .get::<SessionTimeZoneConfig>()
        .map_or(DEFAULT_EXTRACTION_TIME_ZONE, SessionTimeZoneConfig::zone)
}

#[derive(Debug)]
struct CurrentTimezone {
    signature: Signature,
}

impl CurrentTimezone {
    fn new() -> Self {
        Self {
            signature: Signature::nullary(Volatility::Stable),
        }
    }
}

impl PartialEq for CurrentTimezone {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for CurrentTimezone {}

impl Hash for CurrentTimezone {
    fn hash<H: Hasher>(&self, state: &mut H) {
        "current_timezone".hash(state);
    }
}

impl ScalarUDFImpl for CurrentTimezone {
    fn name(&self) -> &'static str {
        "current_timezone"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            "current_timezone",
            DataType::Utf8,
            false,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let zone = session_time_zone_from_options(args.config_options.as_ref());
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            zone.to_string(),
        ))))
    }
}

#[must_use]
pub fn current_timezone_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(CurrentTimezone::new()))
}

#[cfg(test)]
mod tests;
