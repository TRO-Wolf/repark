//! Spark-compatible function registry.
//!
//! Wires the `datafusion-spark` function set into a [`SessionContext`] and fills the gaps it does
//! not cover. Validation found `datafusion-spark` (52.x) ships the calendar *components* of dates
//! (`hour`/`minute`/`second`) and arithmetic (`date_add`/`last_day`/`next_day`) but none of the
//! bare calendar *extractors* Spark SQL exposes (`year`, `month`, `dayofweek`, ...); [`datetime`]
//! hand-implements those with Spark semantics — notably `dayofweek` (1=Sunday) and the ISO-8601
//! `weekofyear` / `yearofweek`.
//!
//! Beyond functions, [`analyzer`] carries the Spark **expression-semantics** analyzer rule
//! (integer `/` → double, divide/modulo-by-zero → NULL, 0-based `[]` array subscript) that
//! the session installs on every context (via the Spark door's `SessionExtension`) — the
//! AR-WG-SQL fidelity layer over raw DataFusion semantics. [`string`] / [`collection`] shim
//! the divergent built-ins the same way [`datetime`] shims the date gaps; [`timestamp_cast`]
//! carries TZ-5 epoch-seconds scaling under `CAST(TIMESTAMP AS <numeric>)`.
//!
//! This crate is DataFusion-native: its public surface speaks `datafusion::error::Result`, so it
//! does not depend on `repark-core`. The `DataFusionError -> repark_core::Error` conversion
//! happens one layer up, in `repark-core`.

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
/// The Spark date functions this crate contributes on top of `datafusion-spark`.
///
/// Exposed separately from [`register_all`] so callers (and tests) can inspect exactly which
/// functions the shim adds.
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
    // R-RETRACT-SHIM + DEC-5: overwrite SparkAvg with Float64 retract plus decimal
    // avg that keeps Spark's (p+4, s+4) type (sliding decimal uses decimal retract).
    for udaf in aggregate::functions() {
        ctx.register_udaf(udaf.as_ref().clone());
    }
    // Q1 R-ML-QUANTILE: Spark SQL names `percentile_approx` / `approx_percentile` as aliases
    // over DataFusion's t-digest `approx_percentile_cont` (engine already exposes the cont form;
    // Spark uses Greenwald-Khanna — accuracy arg accepted+ignored at the facade).
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
    // TZ-8: overwrite DataFusion `to_date` so a TIMESTAMP arg reads the session zone.
    ctx.register_udf(timestamp_cast::to_date_udf().as_ref().clone());
    // TZ-4 PR-1: overwrite now / current_timestamp / to_timestamp with µs+UTC.
    for udf in instant_ts::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in string::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    for udf in collection::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    // X8: parse_url / try_parse_url on java.net.URI splitting (upstream normalizes with url::Url).
    for udf in url::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    // r20 G2: Spark XORShift rand/randn (overwrites DF unseeded `random`).
    for udf in random::functions() {
        ctx.register_udf(udf.as_ref().clone());
    }
    validate::register(ctx);
    higher_order::register(ctx);
    // DEC-8: ExprPlanner must be on the session before BinaryExpr::get_type
    // refuses `(38,20)*(38,20)`. SparkExtension calls this function.
    decimal_spark::register_spark_decimal_planner(ctx);
}

/// ===========================================================================================
/// The Spark-semantics + plan-time safety analyzer rules the session installs on every
/// context (after the DataFusion built-ins, so they see type-coerced plans). See
/// [`decimal_precision`] (first — U3/U4a), [`decimal_spark`] (U4b `/` + DEC-6 wrap;
/// sees clean `decimal / decimal` before [`analyzer::SparkExprSemantics`]), [`analyzer`], and
/// [`cardinality`] (r24 SB1 / SEC-01 expansion ceilings).
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
/// Run the session's analyzer over `plan` eagerly — the ONE way to get an analyzed plan.
///
/// `SessionContext::sql` / `statement_to_plan` return **pre-analysis** plans: the Spark
/// semantics rules above have not run, so the plan's schema can disagree with the executed
/// types (int `/` is still Int64 pre-analysis but Float64 at execution). Any code that reads a
/// plan's schema or extracts its expressions for a consumer on the other side of a boundary —
/// the PyO3 Arrow export, CTAS schema derivation, `F.expr` handoff — must analyze through this
/// function first. Do not claim "analyzed" in a comment; call this and let the call site say it.
///
/// The Spark rewrite rules are idempotent, but one analyze is NOT always a whole-plan fixpoint:
/// this rule runs after `TypeCoercion`, so a rewrite that changes a leaf's type (int `/` →
/// `Float64`) under a set operation leaves the parent `UNION`'s coerced type stale until a SECOND
/// analyze re-runs `TypeCoercion`. Execution re-analyzes at physical planning and so reaches the
/// fixpoint for free; a consumer that derives a schema WITHOUT that second pass (the CTAS write
/// path) must analyze to the fixpoint itself (`repark_sql::execute_ctas`, Group L-write).
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
