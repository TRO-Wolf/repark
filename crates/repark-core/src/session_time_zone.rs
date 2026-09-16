//! Session timezone configuration, resolved once during session construction.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::str::FromStr;

use arrow::array::timezone::Tz;
use repark_common::{Error, Result};

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
    Error::IllegalArgument(format!(
        "[INVALID_CONF_VALUE.TIME_ZONE] The value '{raw}' in the config \
         \"{SESSION_TIME_ZONE_KEY}\" is invalid. Cannot resolve the given timezone. \
         SQLSTATE: 22022"
    ))
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

#[cfg(test)]
mod tests;
