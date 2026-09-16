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
    display: String,
}

impl Default for SessionTimeZoneConfig {
    fn default() -> Self {
        Self {
            zone: DEFAULT_EXTRACTION_TIME_ZONE.to_string(),
            display: DEFAULT_EXTRACTION_TIME_ZONE.to_string(),
        }
    }
}

impl SessionTimeZoneConfig {
    #[must_use]
    pub fn zone(&self) -> &str {
        &self.zone
    }

    #[must_use]
    pub fn display(&self) -> &str {
        &self.display
    }

    pub fn set_zone(&mut self, zone: &str) {
        let trimmed = zone.trim();
        self.display = trimmed.to_string();
        self.zone = canonical_zone_id(trimmed);
    }
}

fn canonical_zone_id(trimmed: &str) -> String {
    if trimmed == "Z" {
        return DEFAULT_EXTRACTION_TIME_ZONE.to_string();
    }
    for prefix in ["GMT", "UTC", "UT"] {
        if !trimmed.starts_with(prefix) {
            continue;
        }
        if trimmed.len() == prefix.len() {
            return DEFAULT_EXTRACTION_TIME_ZONE.to_string();
        }
        let rest = &trimmed[prefix.len()..];
        if matches!(rest.as_bytes().first(), Some(b'+' | b'-')) {
            return normalize_java_offset(rest);
        }
        return trimmed.to_string();
    }
    if matches!(trimmed.as_bytes().first(), Some(b'+' | b'-')) && is_java_offset(trimmed) {
        return normalize_java_offset(trimmed);
    }
    trimmed.to_string()
}

fn is_java_offset(value: &str) -> bool {
    if value == "Z" {
        return true;
    }
    let bytes = value.as_bytes();
    if bytes.len() < 2 || (bytes[0] != b'+' && bytes[0] != b'-') {
        return false;
    }
    let body = &bytes[1..];
    let (hours, minutes, seconds) = match body.len() {
        1 | 2 if body.iter().all(u8::is_ascii_digit) => (decimal_pair(body, 0, body.len()), 0, 0),
        4 if body.iter().all(u8::is_ascii_digit) => {
            (decimal_pair(body, 0, 2), decimal_pair(body, 2, 2), 0)
        }
        5 if body[2] == b':' && is_digit_pair(body, 0) && is_digit_pair(body, 3) => {
            (decimal_pair(body, 0, 2), decimal_pair(body, 3, 2), 0)
        }
        6 if body.iter().all(u8::is_ascii_digit) => {
            let seconds = decimal_pair(body, 4, 2);
            if seconds != 0 {
                return false;
            }
            (decimal_pair(body, 0, 2), decimal_pair(body, 2, 2), 0)
        }
        8 if body[2] == b':' && body[5] == b':' => {
            if !(is_digit_pair(body, 0) && is_digit_pair(body, 3) && is_digit_pair(body, 6)) {
                return false;
            }
            let seconds = decimal_pair(body, 6, 2);
            if seconds != 0 {
                return false;
            }
            (decimal_pair(body, 0, 2), decimal_pair(body, 3, 2), 0)
        }
        _ => return false,
    };
    minutes <= 59 && seconds <= 59 && hours <= 18 && (hours < 18 || (minutes == 0 && seconds == 0))
}

fn is_digit_pair(body: &[u8], offset: usize) -> bool {
    body[offset].is_ascii_digit() && body[offset + 1].is_ascii_digit()
}

fn decimal_pair(body: &[u8], offset: usize, width: usize) -> u32 {
    let mut number = 0;
    for byte in &body[offset..offset + width] {
        number = number * 10 + u32::from(byte - b'0');
    }
    number
}

fn normalize_java_offset(signed: &str) -> String {
    let body = &signed.as_bytes()[1..];
    let (hours, minutes) = match body.len() {
        1 | 2 if body.iter().all(u8::is_ascii_digit) => (decimal_pair(body, 0, body.len()), 0),
        4 if body.iter().all(u8::is_ascii_digit) => {
            (decimal_pair(body, 0, 2), decimal_pair(body, 2, 2))
        }
        5 if body[2] == b':' && is_digit_pair(body, 0) && is_digit_pair(body, 3) => {
            (decimal_pair(body, 0, 2), decimal_pair(body, 3, 2))
        }
        6 if body.iter().all(u8::is_ascii_digit) && decimal_pair(body, 4, 2) == 0 => {
            (decimal_pair(body, 0, 2), decimal_pair(body, 2, 2))
        }
        8 if body[2] == b':'
            && body[5] == b':'
            && is_digit_pair(body, 0)
            && is_digit_pair(body, 3)
            && is_digit_pair(body, 6)
            && decimal_pair(body, 6, 2) == 0 =>
        {
            (decimal_pair(body, 0, 2), decimal_pair(body, 3, 2))
        }
        _ => return signed.to_string(),
    };
    format!("{}{hours:02}:{minutes:02}", &signed[..1])
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
             `{AUTHORITATIVE_KEY}` on the session builder; change it at runtime with \
             `SET spark.sql.session.timeZone`",
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
    let mut carrier = SessionTimeZoneConfig::default();
    carrier.set_zone(zone);
    config.with_option_extension(carrier)
}

/// Read the session zone back out of live config options — the extractors' one accessor.
#[must_use]
pub fn session_time_zone_from_options(options: &ConfigOptions) -> &str {
    options
        .extensions
        .get::<SessionTimeZoneConfig>()
        .map_or(DEFAULT_EXTRACTION_TIME_ZONE, SessionTimeZoneConfig::zone)
}

#[must_use]
pub fn session_time_zone_display_from_options(options: &ConfigOptions) -> &str {
    options
        .extensions
        .get::<SessionTimeZoneConfig>()
        .map_or(DEFAULT_EXTRACTION_TIME_ZONE, SessionTimeZoneConfig::display)
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
        let zone = session_time_zone_display_from_options(args.config_options.as_ref());
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
