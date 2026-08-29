//! Spark-compatible function registry.
//!
//! Wires `datafusion-spark` functions, Spark semantic shims, and analyzer rules into a session.
//! The crate is DataFusion-native; conversion to `repark-core` errors happens one layer up.

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
pub mod expr_fn;
pub mod higher_order;
pub mod instant_ts;
pub mod random;
pub mod session_time_zone;
pub mod spark_length;
pub mod spark_regexp;
pub mod spark_split_part;
pub mod string;
pub mod timestamp_cast;
pub mod timestamp_type;
pub mod url;
pub mod validate;

use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, ScalarUDF};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;

/// ===========================================================================================
/// Return this crate's Spark date-function shims for inspection or registration.
/// ===========================================================================================
#[must_use]
pub fn spark_date_shim_functions() -> Vec<Arc<ScalarUDF>> {
    datetime::functions()
}

/// ===========================================================================================
/// Register the full Spark-compatible scalar/aggregate/window function set into `ctx`.
///
/// Order matters: `datafusion-spark`'s defaults are installed first, then repark shims
/// (date + string + collection + aggregate overwrite), so that on a name clash the repark
/// implementation wins (DataFusion's registry overwrites by name). D2: string `concat`
/// must follow datafusion-spark so `SparkConcat` sticks.
/// ===========================================================================================
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
    ctx.register_udaf(
        datafusion::functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf()
            .as_ref()
            .clone()
            .with_aliases(["percentile_approx", "approx_percentile"]),
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
    validate::register(ctx);
    higher_order::register(ctx);
    decimal_spark::register_spark_decimal_planner(ctx);
}

/// ===========================================================================================
/// Return analyzer rules in dependency order: decimal precision, decimal rewrite, semantics, safety, then LTZ casts.
/// ===========================================================================================
#[must_use]
pub fn analyzer_rules() -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    let mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>> = vec![
        Arc::new(decimal_precision::SparkDecimalPrecision),
        Arc::new(decimal_spark::SparkDecimalRewrite),
        Arc::new(analyzer::SparkExprSemantics),
    ];
    rules.extend(cardinality::analyzer_rules());
    rules.push(instant_ts::ltz_timestamp_cast_rule());
    rules
}

/// ===========================================================================================
/// Analyze a plan through the Spark rules; repeat until schema changes reach the TypeCoercion fixpoint.
/// ===========================================================================================
///
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
