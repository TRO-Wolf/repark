//! `SparkExtension` installs Spark configuration, functions, analyzer rules, and TA window UDFs.

use std::sync::Arc;

use datafusion::optimizer::AnalyzerRule;
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
        let case_sensitive =
            repark_functions::case_sensitive::spark_case_sensitive_from_config_map(session.conf)?;
        let config = repark_functions::case_sensitive::with_spark_case_sensitive_config(
            config,
            case_sensitive,
        );
        let merge_schema =
            repark_functions::merge_schema::merge_schema_from_config_map(session.conf)?;
        let config = repark_functions::merge_schema::with_merge_schema_config(config, merge_schema);
        let overwrite_mode = repark_core::partition_overwrite_mode_from_config_map(session.conf)?;
        let config = repark_core::with_partition_overwrite_mode(config, overwrite_mode);
        let wap = crate::wap::wap_from_config_map(session.conf);
        let config = crate::wap::with_wap_session_config(config, wap);
        let timestamp_type =
            repark_functions::timestamp_type::spark_timestamp_type_from_config_map(session.conf)?;
        let config =
            repark_functions::timestamp_type::with_spark_timestamp_type(config, timestamp_type);
        let config = apply_spark_float_as_decimal(config);
        let config = apply_spark_parser_dialect(config);
        let verbatim =
            crate::spark_literals::escaped_string_literals_from_config_map(session.conf)?;
        let config = crate::spark_literals::with_escaped_string_literals_config(config, verbatim);
        let mut config = config;
        if !session
            .conf
            .contains_key("datafusion.catalog.default_catalog")
        {
            config.options_mut().catalog.default_catalog = "spark_catalog".to_string();
        }
        if !session
            .conf
            .contains_key("datafusion.catalog.default_schema")
        {
            config.options_mut().catalog.default_schema = "default".to_string();
        }
        Ok(repark_functions::session_time_zone::with_session_time_zone(
            config,
            session.session_time_zone.id(),
        ))
    }

    fn configure_analyzer_rules(
        &self,
        rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    ) -> datafusion::error::Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
        let rules = repark_functions::analyzer_rules_with_higher_order_preparation(rules)?;
        crate::spark_literal_typing::insert_literal_rule_before_coercion(rules)
    }

    /// Register Spark functions and analyzer rules, then compose the TA window extension.
    /// # Errors
    /// # Errors Whatever the composed [`TaExtension`] returns.
    fn register(&self, ctx: &SessionContext) -> datafusion::error::Result<()> {
        repark_functions::register_all(ctx);
        ctx.register_udf(crate::spark_typed::spark_as_udf().as_ref().clone());
        ctx.register_udf(crate::spark_typed::suffix_literal_udf().as_ref().clone());
        // WI-2: the plain-INSERT ANSI store-assignment gate, BEFORE the Spark expression semantics.
        ctx.add_analyzer_rule(Arc::new(repark_iceberg::InsertStoreAssignment));
        for rule in crate::spark_literal_typing::spark_door_post_coercion_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx.add_analyzer_rule(Arc::new(crate::spark_typed::FoldSparkNumericCasts));
        ctx.add_analyzer_rule(Arc::new(crate::spark_typed::SparkProjectionDisplay));
        ctx.add_analyzer_rule(Arc::new(repark_core::StackRewrite));
        TaExtension.register(ctx)
    }
}

#[cfg(test)]
mod tests;
