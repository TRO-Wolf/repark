//! Session timezone configuration, resolved once during session construction.

use std::any::Any;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::str::FromStr;

use arrow::array::timezone::Tz;
use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use repark_common::{Error, Result, spark_error};

/// The ONE conf key the engine reads for the session timezone.
pub const SESSION_TIME_ZONE_KEY: &str = "spark.sql.session.timeZone";

/// The session timezone when [`SESSION_TIME_ZONE_KEY`] is unset.
pub const DEFAULT_SESSION_TIME_ZONE: &str = "UTC";

/// A validated session timezone: an IANA zone id (`America/New_York`) or a fixed offset (`+05:00`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTimeZone {
    /// The zone id exactly as the user wrote it, minus surrounding whitespace.
    id: String,
}

impl Default for SessionTimeZone {
    /// [`DEFAULT_SESSION_TIME_ZONE`] — the zone of a session that never set the conf key.
    fn default() -> Self {
        Self {
            id: DEFAULT_SESSION_TIME_ZONE.to_string(),
        }
    }
}

impl std::fmt::Display for SessionTimeZone {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.id)
    }
}

impl SessionTimeZone {
    /// Parse and validate one conf VALUE into a session timezone.
    /// # Errors
    /// [`Error::Config`] naming [`SESSION_TIME_ZONE_KEY`] when the value is blank or is not a zone
    pub fn parse(raw: &str) -> Result<Self> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err(Error::Config(format!(
                "`{SESSION_TIME_ZONE_KEY}` must be a timezone id (e.g. \"UTC\", \
                 \"America/New_York\") or a fixed offset (e.g. \"+05:00\"); got an empty value"
            )));
        }
        Tz::from_str(trimmed).map_err(|error| {
            Error::Config(format!(
                "`{SESSION_TIME_ZONE_KEY}` = {trimmed:?} is not a known timezone id or fixed \
                 offset ({error})"
            ))
        })?;
        Ok(Self {
            id: trimmed.to_string(),
        })
    }

    /// The validated zone id (`America/New_York`, `UTC`, `+05:00`).
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }
}

/// Resolve the session timezone from a builder conf map.
/// # Errors
/// [`Error::Config`] when the key is present with a value [`SessionTimeZone::parse`] refuses.
pub fn resolve_session_time_zone<S>(config: &HashMap<String, String, S>) -> Result<SessionTimeZone>
where
    S: BuildHasher,
{
    match config.get(SESSION_TIME_ZONE_KEY) {
        Some(raw) => SessionTimeZone::parse(raw),
        None => Ok(SessionTimeZone::default()),
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_runtime_session_zone_value(raw: &str) -> Result<SessionTimeZone> {
    if raw.is_empty() || raw.len() != raw.trim().len() {
        return Err(invalid_runtime_zone(raw));
    }
    let sign_led = raw
        .as_bytes()
        .first()
        .is_some_and(|byte| *byte == b'+' || *byte == b'-');
    let accepted = is_java_offset_zone(raw)
        || is_java_prefixed_zone(raw)
        || (!sign_led && Tz::from_str(raw).is_ok());
    if accepted {
        return Ok(SessionTimeZone {
            id: raw.to_string(),
        });
    }
    Err(invalid_runtime_zone(raw))
}

fn invalid_runtime_zone(raw: &str) -> Error {
    let shown = repark_common::redaction::mask_value_credentials(raw);
    spark_error::illegal_argument(
        spark_error::INVALID_CONF_VALUE_TIME_ZONE,
        &[
            ("value", shown.as_str()),
            ("configKey", SESSION_TIME_ZONE_KEY),
        ],
    )
}

fn is_java_offset_zone(value: &str) -> bool {
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

#[must_use]
pub fn canonical_session_zone_id(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed == "Z" {
        return DEFAULT_SESSION_TIME_ZONE.to_string();
    }
    for prefix in ["GMT", "UTC", "UT"] {
        if !trimmed.starts_with(prefix) {
            continue;
        }
        if trimmed.len() == prefix.len() {
            return DEFAULT_SESSION_TIME_ZONE.to_string();
        }
        let rest = &trimmed[prefix.len()..];
        if matches!(rest.as_bytes().first(), Some(b'+' | b'-')) {
            return normalize_java_offset(rest);
        }
        return trimmed.to_string();
    }
    if matches!(trimmed.as_bytes().first(), Some(b'+' | b'-')) && is_java_offset_zone(trimmed) {
        return normalize_java_offset(trimmed);
    }
    trimmed.to_string()
}

fn java_short_id(trimmed: &str) -> Option<&'static str> {
    match trimmed {
        "ACT" => Some("Australia/Darwin"),
        "AET" => Some("Australia/Sydney"),
        "AGT" => Some("America/Argentina/Buenos_Aires"),
        "ART" => Some("Africa/Cairo"),
        "AST" => Some("America/Anchorage"),
        "BET" => Some("America/Sao_Paulo"),
        "BST" => Some("Asia/Dhaka"),
        "CAT" => Some("Africa/Harare"),
        "CNT" => Some("America/St_Johns"),
        "CST" => Some("America/Chicago"),
        "CTT" => Some("Asia/Shanghai"),
        "EAT" => Some("Africa/Addis_Ababa"),
        "ECT" => Some("Europe/Paris"),
        "EST" => Some("-05:00"),
        "HST" => Some("-10:00"),
        "IET" => Some("America/Indiana/Indianapolis"),
        "IST" => Some("Asia/Kolkata"),
        "JST" => Some("Asia/Tokyo"),
        "MIT" => Some("Pacific/Apia"),
        "MST" => Some("-07:00"),
        "NET" => Some("Asia/Yerevan"),
        "NST" => Some("Pacific/Auckland"),
        "PLT" => Some("Asia/Karachi"),
        "PNT" => Some("America/Phoenix"),
        "PRT" => Some("America/Puerto_Rico"),
        "PST" => Some("America/Los_Angeles"),
        "SST" => Some("Pacific/Guadalcanal"),
        "VST" => Some("Asia/Ho_Chi_Minh"),
        _ => None,
    }
}

#[must_use]
pub fn java_display_zone_id(raw: &str) -> String {
    let trimmed = raw.trim();
    if matches!(trimmed, "EST5EDT" | "CST6CDT" | "MST7MDT" | "PST8PDT") {
        return trimmed.to_string();
    }
    if let Some(mapped) = java_short_id(trimmed) {
        return mapped.to_string();
    }
    if trimmed == "Z" {
        return "Z".to_string();
    }
    for prefix in ["GMT", "UTC", "UT"] {
        if !trimmed.starts_with(prefix) {
            continue;
        }
        if trimmed.len() == prefix.len() {
            return trimmed.to_string();
        }
        let rest = &trimmed[prefix.len()..];
        if matches!(rest.as_bytes().first(), Some(b'+' | b'-')) {
            let normalized = normalize_java_offset(rest);
            if normalized == "+00:00" || normalized == "-00:00" {
                return prefix.to_string();
            }
            return format!("{prefix}{normalized}");
        }
        return trimmed.to_string();
    }
    if matches!(trimmed.as_bytes().first(), Some(b'+' | b'-')) && is_java_offset_zone(trimmed) {
        let normalized = normalize_java_offset(trimmed);
        if normalized == "+00:00" || normalized == "-00:00" {
            return "Z".to_string();
        }
        return normalized;
    }
    trimmed.to_string()
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

fn is_java_prefixed_zone(value: &str) -> bool {
    for prefix in ["GMT", "UTC", "UT"] {
        if !value.starts_with(prefix) {
            continue;
        }
        if value.len() == prefix.len() {
            return true;
        }
        let rest = &value[prefix.len()..];
        if matches!(rest.as_bytes().first(), Some(b'+' | b'-')) && is_java_offset_zone(rest) {
            return true;
        }
    }
    false
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

pub const TIME_PARSER_POLICY_KEY: &str = "spark.sql.legacy.timeParserPolicy";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TimeParserPolicy {
    Legacy,
    #[default]
    Corrected,
    Exception,
}

impl TimeParserPolicy {
    #[must_use]
    pub fn is_legacy(self) -> bool {
        self == Self::Legacy
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn parse_time_parser_policy(raw: &str) -> DataFusionResult<TimeParserPolicy> {
    match raw.to_ascii_uppercase().as_str() {
        "LEGACY" => Ok(TimeParserPolicy::Legacy),
        "CORRECTED" => Ok(TimeParserPolicy::Corrected),
        "EXCEPTION" => Ok(TimeParserPolicy::Exception),
        _ => Err(DataFusionError::Configuration(format!(
            "[INVALID_CONF_VALUE.OUT_OF_RANGE_OF_OPTIONS] The value '{}' in the config \
             \"{TIME_PARSER_POLICY_KEY}\" is invalid. It should be one of 'LEGACY, CORRECTED, \
             EXCEPTION'. SQLSTATE: 22022",
            repark_common::redaction::mask_value_credentials(raw)
        ))),
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TimeParserPolicyConfig {
    pub policy: TimeParserPolicy,
}

impl ConfigExtension for TimeParserPolicyConfig {
    const PREFIX: &'static str = "repark.timeparser";
}

impl ExtensionOptions for TimeParserPolicyConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> DataFusionResult<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: the time parser policy is set with \
             `{TIME_PARSER_POLICY_KEY}` on the session builder; change it at runtime with \
             `SET {TIME_PARSER_POLICY_KEY}`",
            Self::PREFIX
        )))
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

#[must_use]
pub fn conf_dump_selects_legacy_policy(dump: &[(String, String, String)]) -> bool {
    dump.iter().any(|(key, value, _)| {
        key == TIME_PARSER_POLICY_KEY && value.eq_ignore_ascii_case("legacy")
    })
}

#[cfg(test)]
mod tests;
