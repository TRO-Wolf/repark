//! Spark-compatible function registry.

mod shim_macros;
/// Re-exported at the crate root so call sites keep saying `crate::shim_udf_boilerplate!`.
pub(crate) use shim_macros::shim_udf_boilerplate;

pub mod aggregate;
pub mod analyzer;
pub mod ansi;
mod avg_groups;
pub mod bitmap_agg;
pub mod bool_decimal;
mod interval_avg;
pub use bool_decimal::install_shared_analyzer_rules;
pub mod cardinality;
pub mod collection;
pub mod count_if;
pub mod datetime;
pub mod decimal_cast;
pub mod decimal_precision;
pub mod decimal_spark;
pub mod declared_refuse;
pub mod expr_fn;
pub mod format_version;
pub mod generator;
mod groups_null_state;
pub mod higher_order;
pub mod instant_ts;
mod int_to_binary;
pub mod integer_spark;
pub mod java_datetime;
pub mod java_double;
mod java_regex;
pub mod json;
pub mod lambda_rebind;
pub mod percentile_approx;
pub mod quantile_summaries;
pub mod random;
pub mod registration;
pub mod session_time_zone;
pub mod spark_base64;
pub mod spark_chr;
pub mod spark_degrees;
pub mod spark_elt;
pub mod spark_from_unixtime;
pub mod spark_hash;
pub mod spark_initcap;
pub mod spark_isnan;
pub mod spark_length;
pub mod spark_log;
pub mod spark_log1p;
pub mod spark_math;
pub mod spark_nullability;
pub mod spark_regexp;
pub mod spark_regexp_match;
pub mod spark_result_types;
pub mod spark_reverse;
pub mod spark_sequence;
pub mod spark_session_window;
pub mod spark_split;
pub mod spark_split_part;
pub mod spark_time_window;
pub mod spark_window_time;
pub mod spark_year_pad;
pub mod string;
pub mod temporal_ctor;
pub mod time_family;
pub mod timestamp_cast;
pub mod timestamp_ltz_ntz;
pub mod timestamp_type;
pub mod try_invert;
pub mod url;
pub mod validate;
pub use lambda_rebind::analyzer_rules_with_higher_order_preparation;
pub use registration::analyzer_rules;

use datafusion::execution::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;

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
    let approx_cont =
        datafusion::functions_aggregate::approx_percentile_cont::approx_percentile_cont_udaf();
    ctx.register_udaf(approx_cont.as_ref().clone());
    for udwf in datafusion_spark::all_default_window_functions() {
        ctx.register_udwf(udwf.as_ref().clone());
    }
    for udwf in spark_result_types::signed_window_functions() {
        ctx.register_udwf(udwf.as_ref().clone());
    }
    for udf in datetime::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    ctx.register_udf(timestamp_cast::to_date_udf().as_ref().clone());
    ctx.register_udf(timestamp_cast::date_udf().as_ref().clone());
    ctx.register_udf(timestamp_cast::unix_timestamp_udf().as_ref().clone());
    ctx.register_udf(spark_from_unixtime::from_unixtime_udf().as_ref().clone());
    for udf in instant_ts::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in string::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in collection::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in generator::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in json::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in url::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in random::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in spark_log::functions()
        .into_iter()
        .chain(spark_log1p::functions())
        .chain(spark_math::functions())
        .chain(spark_base64::functions())
        .chain(spark_isnan::functions())
        .chain(spark_initcap::functions())
        .chain(spark_chr::functions())
        .chain(spark_degrees::functions())
        .chain(spark_elt::functions())
        .chain(spark_hash::functions())
    {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in spark_time_window::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in spark_session_window::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    validate::register(ctx);
    try_invert::register(ctx);
    temporal_ctor::register(ctx);
    higher_order::register(ctx);
    decimal_spark::register_spark_decimal_planner(ctx);
    integer_spark::register_spark_integer_planner(ctx);
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
