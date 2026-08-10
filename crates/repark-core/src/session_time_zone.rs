//! The session timezone — `spark.sql.session.timeZone`, resolved ONCE at session construction.
//!
//! **One authoritative spelling.** [`SESSION_TIME_ZONE_KEY`] is the only conf key this engine
//! reads for the session zone, and this constant is the only place that string literal is
//! written in the Rust tree. There is deliberately **no** alternate spelling (no `snake_case`
//! twin, no `repark.`-namespaced alias, no case-insensitive lookalike): a second spelling of one
//! knob is how two sources of truth are born. An unrecognized lookalike therefore falls through
//! to the default exactly like any other unknown `.config(...)` key, which is PySpark's own
//! tolerance for unknown configuration.
//!
//! **Resolved once, at construction.** [`ReparkSessionBuilder::build`](crate::ReparkSessionBuilder::build)
//! parses and *validates* the value and stores the result on the session
//! ([`ReparkSession::session_time_zone`](crate::ReparkSession::session_time_zone)). Nothing reads
//! the process environment (`TZ`, the host's local zone) at query time — that is the
//! everything-through-Session discipline in `docs/adr/0004-server-prep-disciplines.md`, and it is
//! also why the default is a fixed [`DEFAULT_SESSION_TIME_ZONE`] rather than the host zone.
//!
//! **Declared divergence from Apache Spark (carried, not hidden).** Spark defaults
//! `spark.sql.session.timeZone` to the JVM's *local* zone, so the same job produces different
//! wall-clock values on two hosts. repark defaults to `UTC` so a run is reproducible on any
//! host; a job that wants host-local behavior sets the key explicitly.
//!
//! **Scope of this module (H-1a split A).** The session zone is a *carried, validated* session
//! value here. Timestamp **extraction** does not honor it yet — that fix, and its extractor
//! pins, are H-1a split B. Nothing in this module changes an evaluated result.

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
