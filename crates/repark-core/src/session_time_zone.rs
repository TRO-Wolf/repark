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

/// Parse and validate a RUNTIME [`SESSION_TIME_ZONE_KEY`] value (Spark `SET` / `conf.set`).
/// # Errors
/// [`Error::IllegalArgument`] with Spark's `INVALID_CONF_VALUE.TIME_ZONE` message when the
/// value is blank or unresolvable.
pub fn parse_runtime_session_zone_value(raw: &str) -> Result<SessionTimeZone> {
    let trimmed = raw.trim();
    let sign_led = trimmed
        .as_bytes()
        .first()
        .is_some_and(|byte| *byte == b'+' || *byte == b'-');
    let accepted = !trimmed.is_empty()
        && (is_java_offset_zone(trimmed)
            || is_java_prefixed_zone(trimmed)
            || (!sign_led && Tz::from_str(trimmed).is_ok()));
    if accepted {
        return Ok(SessionTimeZone {
            id: trimmed.to_string(),
        });
    }
    Err(Error::IllegalArgument(format!(
        "[INVALID_CONF_VALUE.TIME_ZONE] The value '{trimmed}' in the config \
         \"{SESSION_TIME_ZONE_KEY}\" is invalid. Cannot resolve the given timezone. \
         SQLSTATE: 22022"
    )))
}

/// Whether `value` is a Java `ZoneOffset` spelling inside ±18:00 (`Z`, `+H`, `+HH`, `+HHMM`,
/// `+HH:MM`, `+HHMMSS`, `+HH:MM:SS`). A leading-sign string that fails here must not fall
/// through to the IANA check: Arrow accepts offsets past ±18:00 that Java refuses.
fn is_java_offset_zone(value: &str) -> bool {
    if value == "Z" || value == "z" {
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
        6 if body.iter().all(u8::is_ascii_digit) => (
            decimal_pair(body, 0, 2),
            decimal_pair(body, 2, 2),
            decimal_pair(body, 4, 2),
        ),
        5 if body[2] == b':' && is_digit_pair(body, 0) && is_digit_pair(body, 3) => {
            (decimal_pair(body, 0, 2), decimal_pair(body, 3, 2), 0)
        }
        8 if body[2] == b':' && body[5] == b':' => {
            if !(is_digit_pair(body, 0) && is_digit_pair(body, 3) && is_digit_pair(body, 6)) {
                return false;
            }
            (
                decimal_pair(body, 0, 2),
                decimal_pair(body, 3, 2),
                decimal_pair(body, 6, 2),
            )
        }
        _ => return false,
    };
    minutes <= 59 && seconds <= 59 && hours <= 18 && (hours < 18 || (minutes == 0 && seconds == 0))
}

/// Whether `value` is a bare `GMT` / `UTC` / `UT` id or one carrying a signed offset.
fn is_java_prefixed_zone(value: &str) -> bool {
    for prefix in ["GMT", "UTC", "UT"] {
        if value.len() < prefix.len()
            || !value.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
        {
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

/// Whether two bytes at `offset` are ASCII digits.
fn is_digit_pair(body: &[u8], offset: usize) -> bool {
    body[offset].is_ascii_digit() && body[offset + 1].is_ascii_digit()
}

/// Parse two ASCII digits at `offset` as a decimal number.
fn decimal_pair(body: &[u8], offset: usize, width: usize) -> u32 {
    let mut number = 0;
    for byte in &body[offset..offset + width] {
        number = number * 10 + u32::from(byte - b'0');
    }
    number
}

#[cfg(test)]
mod tests;
