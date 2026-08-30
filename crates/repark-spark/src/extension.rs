//! `SparkExtension` installs Spark configuration, functions, analyzer rules, and TA window UDFs.

use std::sync::Arc;

use datafusion::prelude::{SessionConfig, SessionContext};
use repark_core::{SessionBuildConf, SessionExtension};
use repark_ta::TaExtension;

/// The Spark door's session extension: build-time registrations as one unit.
#[derive(Debug, Clone, Copy, Default)]
pub struct SparkExtension;

/// Spark-door parser default (DEC-1 / U2): floating-point SQL literals parse as DECIMAL.
pub(crate) fn apply_spark_float_as_decimal(mut config: SessionConfig) -> SessionConfig {
    config.options_mut().sql_parser.parse_float_as_decimal = true;
    config
}

/// Spark-door parser dialect (FNP-4): use Databricks parsing for Spark higher-order functions.
#[expect(
    dead_code,
    reason = "wired by FNP-4b once internal SQL is dialect-independent"
)]
pub(crate) fn apply_spark_parser_dialect(mut config: SessionConfig) -> SessionConfig {
    config.options_mut().sql_parser.dialect = datafusion::config::Dialect::Databricks;
    config
}

impl SessionExtension for SparkExtension {
    /// Install Spark configuration carriers, including ANSI mode, timestamp type, and session zone.
    /// # Errors
    /// Returns an error when a present `repark.sql.*` or ANSI conf value cannot parse.
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
        // The one crossing point.
        Ok(repark_functions::session_time_zone::with_session_time_zone(
            config,
            session.session_time_zone.id(),
        ))
    }

    /// Register Spark functions and analyzer rules, then compose the TA window extension.
    /// # Errors
    /// # Errors Whatever the composed [`TaExtension`] returns.
    fn register(&self, ctx: &SessionContext) -> datafusion::error::Result<()> {
        repark_functions::register_all(ctx);
        // WI-2: the plain-INSERT ANSI store-assignment gate, BEFORE the Spark expression semantics.
        ctx.add_analyzer_rule(Arc::new(repark_iceberg::InsertStoreAssignment));
        for rule in repark_functions::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        TaExtension.register(ctx)
    }
}

#[cfg(test)]
mod tests;
