//! `SparkExtension` — the Spark door's build-time [`SessionExtension`].
//!
//! Re-homes exactly what v1's `ReparkSessionBuilder::build()` inlined at the two hook
//! positions (the phase-cut inversion, session-api design §3):
//!
//! - [`configure`](repark_core::SessionExtension::configure) — the cardinality /
//!   `repark.sql.*` `ConfigExtension` install (v1 r24 SB1): parse the builder conf map via
//!   [`repark_functions::cardinality::repark_sql_settings_from_config_map`] and attach it with
//!   [`repark_functions::cardinality::with_repark_sql_config`].
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
//! - The TA window UDFs (`repark_ta::udf::register_all`) — TEMPORARY OMISSION, restored in
//!   phase-2 PR-4 when the `repark-ta` crate lands and this extension composes `TaExtension`
//!   (ledger rider in `task/p2b-spark-skeleton-ledger.md`).

use std::collections::HashMap;

use datafusion::prelude::{SessionConfig, SessionContext};
use repark_core::SessionExtension;

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
    /// the r24 SB1 cardinality / `repark.sql.*` `ConfigExtension` install.
    ///
    /// # Errors
    /// A present-but-unparsable `repark.sql.*` conf value (v1's fail-loud contract).
    fn configure(
        &self,
        conf: &HashMap<String, String>,
        config: SessionConfig,
    ) -> datafusion::error::Result<SessionConfig> {
        let settings = repark_functions::cardinality::repark_sql_settings_from_config_map(conf)?;
        Ok(repark_functions::cardinality::with_repark_sql_config(
            config, settings,
        ))
    }

    /// v1 position: immediately after `SessionContext::new_with_config_rt` — the Spark
    /// function registry + the expression-semantics analyzer rules (integer `/` → double,
    /// div/mod-by-zero → NULL, 0-based `[]` array subscript).
    ///
    /// # Errors
    /// None today — registration is infallible; the seam's `Result` is kept for the PR-4
    /// TA composition.
    fn register(&self, ctx: &SessionContext) -> datafusion::error::Result<()> {
        repark_functions::register_all(ctx);
        for rule in repark_functions::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
