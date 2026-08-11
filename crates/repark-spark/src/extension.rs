//! `SparkExtension` — the Spark door's build-time [`SessionExtension`].
//!
//! Re-homes exactly what v1's `ReparkSessionBuilder::build()` inlined at the two hook
//! positions (the phase-cut inversion, session-api design §3):
//!
//! - [`configure`](repark_core::SessionExtension::configure) — the cardinality /
//!   `repark.sql.*` `ConfigExtension` install (v1 r24 SB1): parse the builder conf map via
//!   [`repark_functions::cardinality::repark_sql_settings_from_config_map`] and attach it with
//!   [`repark_functions::cardinality::with_repark_sql_config`]; **plus** the session-timezone
//!   carrier (H-1a split B), which is this door's whole part in making timestamp extraction
//!   honor `spark.sql.session.timeZone`.
//!
//! **Why the timezone crosses HERE and nowhere else.** `repark-core` (tier 2) owns the key, the
//! validation and the resolved value; `repark-functions` (tier-3 capability leaf) owns the
//! extractors and deliberately has no `repark-core` edge — and a core→functions edge would be the
//! forbidden upward one. This door is the only crate that depends on both, so it is the only
//! place the two can meet. It carries the value the engine already resolved; it does not re-read,
//! re-spell or re-validate it.
//! - [`register`](repark_core::SessionExtension::register) — the Spark function registry
//!   ([`repark_functions::register_all`]) plus the expression-semantics analyzer rules
//!   ([`repark_functions::analyzer_rules`], appended after DataFusion's built-ins so they see
//!   type-coerced plans).
//!
//! Deliberately NOT here:
//!
//! - The DF-54.1 uncorrelated-scalar-subquery guard — hoisted to repark-core session defaults
//!   (design G8) so extension-less native sessions keep it; the bare-Session pin lives in
//!   repark-core's session tests.
//! - The engine write knobs (`with_merge_session_knobs`, concurrency, scan pruning) — those
//!   stayed in the phase-1 core `build()` (they are engine-tier, not door-tier).
//!
//! Composed, not re-implemented:
//!
//! - The TA window UDFs — `register` delegates to [`repark_ta::TaExtension`] at v1's exact
//!   position (straight after the analyzer rules). The TA set is door-neutral (design Q11), so
//!   this door **composes** the owning crate's extension rather than calling
//!   `repark_ta::udf::register_all` itself; a native session installs `TaExtension` directly.
//!   (Restores the PR-2 rider in `task/p2b-spark-skeleton-ledger.md`.)

use datafusion::prelude::{SessionConfig, SessionContext};
use repark_core::{SessionBuildConf, SessionExtension};
use repark_ta::TaExtension;

/// ===========================================================================================
/// The Spark door's session extension: v1's inline build-time registrations as one unit.
///
/// Install with `ReparkSessionBuilder::with_extension(Arc::new(SparkExtension))` alongside the
/// `SparkDialect` — extensions are session-scoped, so a Spark-extended session has Spark
/// expression semantics through every door.
/// ===========================================================================================
#[derive(Debug, Clone, Copy, Default)]
pub struct SparkExtension;

impl SessionExtension for SparkExtension {
    /// v1 position: after the engine write knobs, before the `RuntimeEnv` is assembled —
    /// the r24 SB1 cardinality / `repark.sql.*` `ConfigExtension` install, and the
    /// session-timezone carrier the extractor layer reads at invoke time.
    ///
    /// # Errors
    /// A present-but-unparsable `repark.sql.*` conf value (v1's fail-loud contract). The zone
    /// cannot fail here — `build()` validated it before this hook runs.
    fn configure(
        &self,
        session: SessionBuildConf<'_>,
        config: SessionConfig,
    ) -> datafusion::error::Result<SessionConfig> {
        let settings =
            repark_functions::cardinality::repark_sql_settings_from_config_map(session.conf)?;
        let config = repark_functions::cardinality::with_repark_sql_config(config, settings);
        // The one crossing point (module docs): the engine's already-resolved zone becomes the
        // carrier every calendar extractor reads out of `ScalarFunctionArgs::config_options`.
        Ok(repark_functions::session_time_zone::with_session_time_zone(
            config,
            session.session_time_zone.id(),
        ))
    }

    /// v1 position: immediately after `SessionContext::new_with_config_rt` — the Spark
    /// function registry, then the expression-semantics analyzer rules (integer `/` → double,
    /// div/mod-by-zero → NULL, 0-based `[]` array subscript), then the TA window UDFs. The
    /// order is v1 `build()`'s, verbatim.
    ///
    /// # Errors
    /// Whatever the composed [`TaExtension`] returns (infallible today); the Spark-side
    /// registrations cannot fail.
    fn register(&self, ctx: &SessionContext) -> datafusion::error::Result<()> {
        repark_functions::register_all(ctx);
        for rule in repark_functions::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        TaExtension.register(ctx)
    }
}

#[cfg(test)]
mod tests;
