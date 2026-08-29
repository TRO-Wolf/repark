//! `SparkExtension` installs Spark configuration, functions, analyzer rules, and TA window UDFs.
//!
//! `configure` carries the resolved session timezone, ANSI mode, timestamp type, decimal parsing,
//! and RePark settings. `register` installs the write gate before cast analysis, then composes
//! [`repark_ta::TaExtension`]. Core owns engine defaults; this door owns Spark semantics.

use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use repark_core::{SessionBuildConf, SessionExtension};
use repark_ta::TaExtension;

/// ===========================================================================================
/// The Spark door's session extension: build-time registrations as one unit.
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

/// ===========================================================================================
/// Spark-door parser dialect (FNP-4): use Databricks parsing for Spark higher-order functions.
/// Keep the setting per door so routing and execution use the same parser without blending doors.
/// ===========================================================================================
///
/// Not wired yet: generated SQL still uses ANSI double-quoted identifiers, which Spark parsing
/// treats as string literals and breaks internal DML. Fix that write-path contract first.
#[expect(
    dead_code,
    reason = "wired by FNP-4b once internal SQL is dialect-independent"
)]
pub(crate) fn apply_spark_parser_dialect(mut config: SessionConfig) -> SessionConfig {
    config.options_mut().sql_parser.dialect = datafusion::config::Dialect::Databricks;
    config
}

impl SessionExtension for SparkExtension {
    /// Install Spark configuration carriers, including ANSI mode, timestamp type, decimal
    /// parsing, cardinality settings, and the resolved session timezone.
    ///
    /// # Errors
    /// A present-but-unparsable `repark.sql.*` conf value (the fail-loud contract), a
    /// present-but-unparsable `spark.sql.ansi.enabled` (Spark's `should be boolean, but was`
    /// needle), or a present-but-unparsable `spark.sql.timestampType` (must be
    /// `TIMESTAMP_LTZ` or `TIMESTAMP_NTZ`). The zone cannot fail here — `build()`
    /// validated it before this hook runs.
    fn configure(
        &self,
        session: SessionBuildConf<'_>,
        config: SessionConfig,
    ) -> datafusion::error::Result<SessionConfig> {
        // pins: v3-2-create-v3-opt-in/C-009
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

    /// Register Spark functions and analyzer rules, then compose the TA window extension.
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
