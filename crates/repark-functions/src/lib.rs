//! Spark-compatible function registry.

mod shim_macros;
/// Re-exported at the crate root so call sites keep saying `crate::shim_udf_boilerplate!`.
pub(crate) use shim_macros::shim_udf_boilerplate;

pub mod aggregate;
pub mod analyzer;
pub mod ansi;
pub mod cardinality;
pub mod collection;
pub mod datetime;
pub mod decimal_precision;
pub mod decimal_spark;
pub mod declared_refuse;
pub mod expr_fn;
pub mod format_version;
pub mod higher_order;
pub mod instant_ts;
pub mod integer_spark;
pub mod percentile_approx;
pub mod random;
pub mod session_time_zone;
pub mod spark_isnan;
pub mod spark_length;
pub mod spark_log;
pub mod spark_log1p;
pub mod spark_regexp;
pub mod spark_split_part;
pub mod string;
pub mod timestamp_cast;
pub mod timestamp_type;
pub mod try_invert;
pub mod url;
pub mod validate;

use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, ScalarUDF};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;

/// Return this crate's Spark date-function shims for inspection or registration.
#[must_use]
pub fn spark_date_shim_functions() -> Vec<Arc<ScalarUDF>> {
    datetime::functions()
}

/// Register the full Spark-compatible scalar/aggregate/window function set into `ctx`.
pub fn register_all(ctx: &SessionContext) {
    for udf in datafusion_spark::all_default_scalar_functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udaf in datafusion_spark::all_default_aggregate_functions() {
        ctx.register_udaf(udaf.as_ref().clone());
    }
    for udaf in aggregate::functions() {
        ctx.register_udaf(udaf.as_ref().clone());
    }
    ctx.register_udaf(percentile_approx::percentile_approx_udaf().as_ref().clone());
    ctx.register_udaf(
        datafusion::functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf()
            .as_ref()
            .clone(),
    );
    for udwf in datafusion_spark::all_default_window_functions() {
        ctx.register_udwf(udwf.as_ref().clone());
    }
    for udf in spark_date_shim_functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    ctx.register_udf(timestamp_cast::to_date_udf().as_ref().clone());
    for udf in instant_ts::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in string::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in collection::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in url::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in random::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in spark_log::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in spark_log1p::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in spark_isnan::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    validate::register(ctx);
    try_invert::register(ctx);
    higher_order::register(ctx);
    decimal_spark::register_spark_decimal_planner(ctx);
    integer_spark::register_spark_integer_planner(ctx);
}

/// Return analyzer rules: decimal precision, decimal rewrite, semantics, safety, then LTZ casts.
#[must_use]
pub fn analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![
        Arc::new(decimal_precision::SparkDecimalPrecision),
        Arc::new(decimal_spark::SparkDecimalRewrite),
        Arc::new(integer_spark::SparkIntegerOverflow),
        Arc::new(analyzer::SparkExprSemantics),
    ];
    rules.extend(cardinality::analyzer_rules());
    rules.push(instant_ts::ltz_timestamp_cast_rule());
    rules
}

/// Run Spark analyzer rules until schema changes reach the `TypeCoercion` fixpoint.
/// # Errors
/// Propagates analyzer-rule failures as [`datafusion::error::DataFusionError`].
pub fn analyze_eagerly(
    state: &SessionState,
    plan: LogicalPlan,
) -> datafusion::error::Result<LogicalPlan> {
    state
        .analyzer()
        .execute_and_check(plan, state.config_options(), |_, _| {})
}
