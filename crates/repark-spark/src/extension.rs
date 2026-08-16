//! `SparkExtension` — the Spark door's build-time [`SessionExtension`].
//!
//! Re-homes exactly what v1's `ReparkSessionBuilder::build()` inlined at the two hook
//! positions (the phase-cut inversion, session-api design §3):
//!
//! - [`configure`](repark_core::SessionExtension::configure) — the cardinality /
//!   `repark.sql.*` `ConfigExtension` install (v1 r24 SB1): parse the builder conf map via
//!   [`repark_functions::cardinality::repark_sql_settings_from_config_map`] and attach it with
//!   [`repark_functions::cardinality::with_repark_sql_config`]; **plus** the Spark-door
//!   `spark.sql.ansi.enabled` carrier (U5 / Q10=A, default **TRUE**); **plus** the Spark-door
//!   `spark.sql.timestampType` carrier (Q10, default **`TIMESTAMP_LTZ`**); **plus** the
//!   session-timezone carrier (H-1a split B), which is this door's whole part in making
//!   timestamp extraction honor `spark.sql.session.timeZone`; **plus** the Spark-door parser
//!   default `datafusion.sql_parser.parse_float_as_decimal = true` (DEC-1 / U2) so bare
//!   floating-point SQL literals (`1.23`) infer `DECIMAL`, matching Spark. The ANSI door
//!   never calls this hook.
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
//!   type-coerced plans) — preceded by [`repark_iceberg::InsertStoreAssignment`], the WI-2
//!   plain-INSERT store-assignment gate, which must speak BEFORE the CAST-legality gate inside
//!   `SparkExprSemantics` so a `DATE → INT` insert cites Spark's WRITE class.
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

use std::sync::Arc;

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

/// ===========================================================================================
/// Spark-door parser default (DEC-1 / U2): floating-point SQL literals parse as DECIMAL.
///
/// DataFusion's `sql_parser.parse_float_as_decimal` defaults to `false` (bare `1.23` is
/// `Float64`). Spark infers `DECIMAL` from the text. Every session that goes through
/// [`SparkExtension::configure`] turns the flag on. The ANSI door never calls this helper.
/// Spark-door unit fixtures that build a `SessionContext` without the extension call this so
/// they match production wiring.
/// ===========================================================================================
pub(crate) fn apply_spark_float_as_decimal(mut config: SessionConfig) -> SessionConfig {
    config.options_mut().sql_parser.parse_float_as_decimal = true;
    config
}

impl SessionExtension for SparkExtension {
    /// v1 position: after the engine write knobs, before the `RuntimeEnv` is assembled —
    /// the r24 SB1 cardinality / `repark.sql.*` `ConfigExtension` install, the Spark-door
    /// `spark.sql.ansi.enabled` carrier (default TRUE), the Spark-door
    /// `spark.sql.timestampType` carrier (default `TIMESTAMP_LTZ`), the
    /// `parse_float_as_decimal` default (DEC-1 / U2), and the session-timezone carrier
    /// the extractor layer reads at invoke time.
    ///
    /// # Errors
    /// A present-but-unparsable `repark.sql.*` conf value (v1's fail-loud contract), a
    /// present-but-unparsable `spark.sql.ansi.enabled` (Spark's `should be boolean, but was`
    /// needle), or a present-but-unparsable `spark.sql.timestampType` (must be
    /// `TIMESTAMP_LTZ` or `TIMESTAMP_NTZ`). The zone cannot fail here — `build()`
    /// validated it before this hook runs.
    fn configure(
        &self,
        session: SessionBuildConf<'_>,
        config: SessionConfig,
    ) -> datafusion::error::Result<SessionConfig> {
        let settings =
            repark_functions::cardinality::repark_sql_settings_from_config_map(session.conf)?;
        let config = repark_functions::cardinality::with_repark_sql_config(config, settings);
        let ansi_enabled = repark_functions::ansi::spark_ansi_from_config_map(session.conf)?;
        let config = repark_functions::ansi::with_spark_ansi_config(config, ansi_enabled);
        let timestamp_type =
            repark_functions::timestamp_type::spark_timestamp_type_from_config_map(session.conf)?;
        let config =
            repark_functions::timestamp_type::with_spark_timestamp_type(config, timestamp_type);
        let config = apply_spark_float_as_decimal(config);
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
        // WI-2: the plain-INSERT ANSI store-assignment gate, BEFORE the Spark expression
        // semantics. Order is semantic, not stylistic: a `DATE → INT` insert is refused by both
        // this rule (`INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST`) and the G6-3 cast-legality
        // gate inside `SparkExprSemantics` (`DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION`), and
        // Spark raises the WRITE class for that statement — so the write gate must speak first.
        ctx.add_analyzer_rule(Arc::new(repark_iceberg::InsertStoreAssignment));
        for rule in repark_functions::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        TaExtension.register(ctx)
    }
}

#[cfg(test)]
mod tests;
