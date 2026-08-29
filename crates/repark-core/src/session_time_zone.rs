//! Session timezone configuration, resolved once during session construction.
//!
//! [`SESSION_TIME_ZONE_KEY`] is the only accepted spelling. The value is validated and stored on
//! the session; queries do not read host timezone state. Repark defaults to UTC for reproducible
//! runs, a declared divergence from Spark's host-local default.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::str::FromStr;

use arrow::array::timezone::Tz;
use repark_common::{Error, Result};

/// The ONE conf key the engine reads for the session timezone (PySpark's own spelling, so a
/// migrated job's `.config(...)` line is unchanged). No alternate spelling exists — see the
/// module docs.
pub const SESSION_TIME_ZONE_KEY: &str = "spark.sql.session.timeZone";

/// The session timezone when [`SESSION_TIME_ZONE_KEY`] is unset.
///
/// `UTC`, not the host's local zone: reproducibility across hosts beats matching Spark's
/// host-dependent default, and reading the host zone would be an environment read the
/// server-prep discipline forbids. Declared as a divergence rather than silently inherited.
pub const DEFAULT_SESSION_TIME_ZONE: &str = "UTC";

/// ===========================================================================================
/// A **validated** session timezone: an IANA zone id (`America/New_York`) or a fixed offset
/// (`+05:00`), checked against the Arrow/`chrono-tz` zone database at session build.
///
/// Constructed only through [`SessionTimeZone::parse`] or [`SessionTimeZone::default`], so an
/// unparsable zone can never reach the engine — a session either builds with a real zone or
/// fails loud naming [`SESSION_TIME_ZONE_KEY`].
/// ===========================================================================================
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
    /// ===========================================================================================
    /// Parse and validate one conf VALUE into a session timezone.
    ///
    /// Surrounding whitespace is trimmed (a padded `.config(...)` value is a typo, not a
    /// different zone); everything else must parse as a zone the engine can actually resolve.
    /// ===========================================================================================
    ///
    /// # Errors
    /// [`Error::Config`] naming [`SESSION_TIME_ZONE_KEY`] when the value is blank or is not a
    /// zone id / fixed offset the engine's zone database knows.
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

/// ===========================================================================================
/// Resolve the session timezone from a builder conf map. Key absent → the default zone.
///
/// Called exactly once, from [`ReparkSessionBuilder::build`](crate::ReparkSessionBuilder::build),
/// so validation happens at session construction and never at query time.
/// ===========================================================================================
///
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
