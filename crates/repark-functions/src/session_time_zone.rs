//! The session timezone as the **extractor layer** sees it — a carrier, never a second knob.
//!
//! **Why this exists.** `spark.sql.session.timeZone` is owned, spelled and validated exactly once,
//! in `repark-core` (`repark_core::session_time_zone`). This crate is a DataFusion-native
//! capability leaf with no `repark-core` edge, and the calendar extractors in [`crate::datetime`]
//! need the resolved zone at **invoke** time. This module is the seam that carries it: a
//! [`SessionTimeZoneConfig`] rides on the session's [`ConfigOptions`], the Spark door installs it
//! from the already-resolved zone at its `configure` hook, and every extractor reads it back out
//! of `ScalarFunctionArgs::config_options`.
//!
//! **Invoke-time, not construction-time — and that is load-bearing.** The Python facade's
//! `F.year(col)` builds a *standalone* `Expr` that embeds the UDF instance directly
//! ([`crate::expr_fn`]), with no `SessionContext` in sight, so a zone baked into the UDF at
//! registration would reach the SQL doors and miss the `DataFrame` API entirely. Reading the zone
//! from the executing session's config options is the only mechanism that covers both — which is
//! why the four-entry-point matrix (native `DataFrame` / ANSI door / Spark door / facade) is
//! satisfiable at all.
//!
//! **This is not a second spelling of the knob**, and the shape enforces it rather than promising
//! it: [`ExtensionOptions::set`] refuses every key naming the one authoritative conf, and
//! [`ExtensionOptions::entries`] is empty so the carrier never surfaces as a settable option.
//! DataFusion resolves an extension namespace on the segment before the FIRST `.`, so
//! `SET repark.session.…` looks for a namespace named `repark` and finds none — exactly the way
//! the neighbouring `repark.sql` extension ([`crate::cardinality`]) is already unreachable from
//! `SET`. The builder's `.config(…)` sweep only forwards `datafusion.*` keys, so no builder key
//! reaches this struct either. There is one way to set the session zone, and it is
//! `spark.sql.session.timeZone`.

use std::any::Any;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionConfig;

/// The zone the extractors use when no [`SessionTimeZoneConfig`] is installed.
///
/// It must agree with `repark_core::DEFAULT_SESSION_TIME_ZONE`; the two cannot be one constant
/// because this crate has no `repark-core` edge, so the agreement is **pinned across the seam**
/// by `crates/repark-spark/tests/session_timezone.rs::default_session_extracts_in_the_core_default_zone`.
/// The fallback is reachable only from a bare DataFusion context that registers these UDFs
/// without the Spark door's `configure` hook (this crate's own tests and benches).
pub const DEFAULT_EXTRACTION_TIME_ZONE: &str = "UTC";

/// The conf key that DOES set the session zone, spelled here ONLY so the refusal below can name
/// it in its message.
///
/// It is a duplicate of `repark_core::SESSION_TIME_ZONE_KEY` by necessity — this crate has no
/// `repark-core` edge — so it is a **checked mirror**, not a second source of truth:
/// `crates/repark-spark/tests/session_timezone.rs::the_carrier_refusal_names_the_engines_own_key`
/// asserts the refusal text contains the engine's constant, from a crate that can see both. It is
/// never used as a lookup key; nothing is ever read or written by this string.
const AUTHORITATIVE_KEY: &str = "spark.sql.session.timeZone";

/// ===========================================================================================
/// The resolved session zone, riding on a session's [`ConfigOptions`] so the extractors can
/// read it at invoke time.
///
/// Constructed only by [`with_session_time_zone`] from a zone `repark-core` has already
/// validated — this type deliberately does no parsing and no validation of its own, because a
/// second validator is a second source of truth.
/// ===========================================================================================
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
}

impl ConfigExtension for SessionTimeZoneConfig {
    /// Two segments on purpose: DataFusion looks an extension namespace up on the text before the
    /// FIRST `.`, so nothing can address this carrier through `SET`.
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

    /// Always refuses, naming the one authoritative spelling. The carrier is written by the
    /// session build and is not a knob; a settable path here would be the second spelling this
    /// unit's acceptance gate forbids.
    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        Err(DataFusionError::Configuration(format!(
            "`{}.{key}` is not a settable option: the session timezone is set with \
             `{AUTHORITATIVE_KEY}` on the session builder and is fixed at session build",
            Self::PREFIX
        )))
    }

    /// Deliberately empty — the carrier does not advertise itself in `information_schema`
    /// settings, because listing it would read as a knob a user can turn.
    ///
    /// **Second consequence, which is not obvious and is therefore written down.**
    /// `ConfigOptions::entries()` folds every extension's entries in, and DataFusion 54.1's
    /// `ScalarFunctionExpr` equality compares `Arc::ptr_eq(config_options, …)` OR the sorted
    /// config entries. With this empty, the session zone is invisible to that comparison, so
    /// `year(ts)` built under a New York session compares EQUAL to `year(ts)` built under a Tokyo
    /// one. That is safe today only because repark never reuses a physical expression across
    /// sessions — there is no plan cache and no cross-session expression store. It stops being
    /// safe the day either exists, and the fix then is to return one non-settable descriptive
    /// entry and keep [`ExtensionOptions::set`]'s refusal as the one-spelling gate. Carried as a
    /// risk row in the unit ledger rather than as a comment alone.
    fn entries(&self) -> Vec<ConfigEntry> {
        Vec::new()
    }
}

/// ===========================================================================================
/// Attach the resolved session zone to a [`SessionConfig`] (called from the Spark door's
/// `configure` hook, with the zone `repark-core` resolved once at session build).
/// ===========================================================================================
#[must_use]
pub fn with_session_time_zone(config: SessionConfig, zone: &str) -> SessionConfig {
    config.with_option_extension(SessionTimeZoneConfig {
        zone: zone.to_string(),
    })
}

/// ===========================================================================================
/// Read the session zone back out of live config options — the extractors' one accessor.
///
/// Falls back to [`DEFAULT_EXTRACTION_TIME_ZONE`] when no carrier is installed (a bare
/// DataFusion context), so an extension-less session keeps the stored-zone behavior it has today
/// rather than failing.
/// ===========================================================================================
#[must_use]
pub fn session_time_zone_from_options(options: &ConfigOptions) -> &str {
    options
        .extensions
        .get::<SessionTimeZoneConfig>()
        .map_or(DEFAULT_EXTRACTION_TIME_ZONE, SessionTimeZoneConfig::zone)
}

#[cfg(test)]
mod tests;
