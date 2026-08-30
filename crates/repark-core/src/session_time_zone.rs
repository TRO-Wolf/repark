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

#[cfg(test)]
mod tests;
