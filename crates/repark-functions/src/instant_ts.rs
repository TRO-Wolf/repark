//! Spark LTZ instant producers — Arrow `Timestamp(µs, UTC)` (TZ-4 PR-1).
//!
//! DataFusion's `now()` / `current_timestamp` simplify to `Timestamp(ns, None)`, and
//! `to_timestamp` returns the same naive-ns type. Spark 4.1.2 exports both as
//! `timestamp[us, tz=UTC]`; Iceberg v2 rejects `timestamp_ns`. This module overwrites those
//! names with a type-only wrap: ticks stay epoch-relative, the annotation becomes UTC, the
//! unit becomes microseconds. Zoneless *values* are not localized (TZ-7 / PR-2).
//!
//! `CAST(<integer|NULL> AS TIMESTAMP)` is the same wire type. The rewrite lives here so
//! `analyzer.rs` (TZ-5 / other-lane file) stays untouched.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue, internal_err};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

/// Spark's default `TIMESTAMP` / LTZ Arrow type — µs with a UTC annotation.
pub(crate) fn ltz_timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC")))
}

/// ===========================================================================================
/// The instant-typed SQL producers this crate overwrites after `datafusion-spark`.
/// ===========================================================================================
#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![now_udf(), current_timestamp_udf(), to_timestamp_udf()]
}

/// ===========================================================================================
/// Analyzer rule: `CAST(<integer|NULL> AS TIMESTAMP)` yields the LTZ wire type.
/// ===========================================================================================
#[must_use]
pub fn ltz_timestamp_cast_rule() -> Arc<dyn AnalyzerRule + Send + Sync> {
    Arc::new(SparkLtzTimestampCast)
}

fn now_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNow::now()))
}

fn current_timestamp_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNow::current_timestamp()))
}

fn to_timestamp_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkToTimestamp::new()))
}

fn cast_columnar_to_ltz(value: ColumnarValue) -> Result<ColumnarValue> {
    let target = ltz_timestamp_type();
    match value {
        ColumnarValue::Array(array) => Ok(ColumnarValue::Array(cast(array.as_ref(), &target)?)),
        ColumnarValue::Scalar(scalar) => {
            let array = scalar.to_array()?;
            let casted = cast(array.as_ref(), &target)?;
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &casted, 0,
            )?))
        }
    }
}

/// ===========================================================================================
/// `now()` / `current_timestamp()` — statement-stable µs+UTC, copying DataFusion's simplify
/// contract (one timestamp per statement) and the `F.current_timestamp` binding cast.
/// ===========================================================================================
#[derive(Debug)]
struct SparkNow {
    name: &'static str,
    signature: Signature,
}

impl SparkNow {
    fn now() -> Self {
        Self {
            name: "now",
            signature: Signature::nullary(Volatility::Stable),
        }
    }

    fn current_timestamp() -> Self {
        Self {
            name: "current_timestamp",
            signature: Signature::nullary(Volatility::Stable),
        }
    }
}

impl PartialEq for SparkNow {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for SparkNow {}

impl Hash for SparkNow {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for SparkNow {
    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Field::new(self.name, ltz_timestamp_type(), false).into())
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(ltz_timestamp_type())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("invoke should not be called on a simplified now() function")
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let Some(now_ts) = info.query_execution_start_time() else {
            return Ok(ExprSimplifyResult::Original(args));
        };
        let micros = now_ts.timestamp_nanos_opt().map(|nanos| nanos / 1_000);
        Ok(ExprSimplifyResult::Simplified(Expr::Literal(
            ScalarValue::TimestampMicrosecond(micros, Some(Arc::<str>::from("UTC"))),
            None,
        )))
    }
}

/// ===========================================================================================
/// `to_timestamp` — DataFusion's parser (no `execution.time_zone`, so no zoneless
/// localization) then a type-only cast to µs+UTC. One return type; zoneless inputs keep
/// UTC-epoch ticks (TZ-7 stays a value disclosure).
/// ===========================================================================================
#[derive(Debug)]
struct SparkToTimestamp {
    signature: Signature,
    inner: datafusion::functions::datetime::to_timestamp::ToTimestampFunc,
}

impl SparkToTimestamp {
    fn new() -> Self {
        // Default ConfigOptions leave `execution.time_zone` unset (Q9 / D-B2).
        // Call the impl directly: `ScalarUDF::invoke_with_args` asserts the inner
        // ns return against our promised µs+UTC field.
        let inner = datafusion::functions::datetime::to_timestamp::ToTimestampFunc::new_with_config(
            &ConfigOptions::default(),
        );
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
            inner,
        }
    }
}

impl PartialEq for SparkToTimestamp {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkToTimestamp {}

impl Hash for SparkToTimestamp {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkToTimestamp {
    crate::shim_udf_boilerplate!("to_timestamp");

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(ltz_timestamp_type())
    }

    fn with_updated_config(&self, _config: &ConfigOptions) -> Option<ScalarUDF> {
        // Do not pick up `datafusion.execution.time_zone` (Q9).
        Some(ScalarUDF::from(Self::new()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let produced = self.inner.invoke_with_args(args)?;
        cast_columnar_to_ltz(produced)
    }
}

/// `CAST(<integer|NULL> AS TIMESTAMP)` → `Timestamp(µs, UTC)`. Idempotent: an already-LTZ
/// target is left alone. String / timestamp sources are PR-2 (zoneless localization).
#[derive(Debug)]
struct SparkLtzTimestampCast;

impl AnalyzerRule for SparkLtzTimestampCast {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)] // `AnalyzerRule::name` ties the lifetime to &self
    fn name(&self) -> &str {
        "spark_ltz_timestamp_cast"
    }
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| Ok(rewrite_cast(node, &schema)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    // DataFusion plans `CAST(<int> AS TIMESTAMP)` as
    // `CAST(CAST(int AS Timestamp(s)) AS Timestamp(ns))`. Wrap the seconds
    // hop (not a retarget onto µs — that would read the integer as micros),
    // then elide the ns hop so it cannot strip the UTC annotation.
    if let Expr::Cast(cast) = &expr {
        let targeting_ns = matches!(
            cast.field.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, _)
        );
        let targeting_seconds = matches!(
            cast.field.data_type(),
            DataType::Timestamp(TimeUnit::Second, _)
        );
        if let Ok(source) = cast.expr.get_type(schema) {
            if targeting_ns && is_ltz_timestamp(&source) {
                return Transformed::yes(*cast.expr.clone());
            }
            let source_is_int_or_null = source.is_integer() || matches!(source, DataType::Null);
            let source_is_seconds = matches!(source, DataType::Timestamp(TimeUnit::Second, _));
            if (targeting_seconds && source_is_int_or_null)
                || (targeting_ns && (source_is_int_or_null || source_is_seconds))
            {
                return wrap_as_ltz(expr, schema);
            }
        }
    }
    wrap_ns_literal(expr, schema)
}

fn is_ltz_timestamp(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone))
            if zone.as_ref().eq_ignore_ascii_case("UTC") || zone.as_ref() == "+00:00"
    )
}

fn wrap_as_ltz(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let nullable = expr.nullable(schema).unwrap_or(true);
    let field = Arc::new(Field::new("ts", ltz_timestamp_type(), nullable));
    Transformed::yes(Expr::Cast(Cast::new_from_field(Box::new(expr), field)))
}

/// DataFusion folds `CAST(<int> AS TIMESTAMP)` and `TIMESTAMP '…'` to
/// `Timestamp(ns, _)` **literals**. Wrap those only — do not rewrite column
/// references or `CAST(str AS TIMESTAMP)` (zoneless input / ingest; PR-2).
fn wrap_ns_literal(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::Literal(scalar, _) = &expr else {
        return Transformed::no(expr);
    };
    let ScalarValue::TimestampNanosecond(_, _) = scalar else {
        return Transformed::no(expr);
    };
    wrap_as_ltz(expr, schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::TimestampMicrosecondType;
    use datafusion::prelude::SessionContext;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    #[tokio::test]
    async fn now_and_current_timestamp_are_microsecond_utc_non_null() {
        let ctx = ctx();
        for sql in ["SELECT now() AS ts", "SELECT current_timestamp() AS ts"] {
            let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
            let schema = batches[0].schema();
            let field = schema.field(0);
            assert_eq!(field.data_type(), &ltz_timestamp_type(), "{sql}: type");
            assert!(!field.is_nullable(), "{sql}: Spark marks now() not-null");
        }
    }

    #[tokio::test]
    async fn to_timestamp_of_a_zone_suffixed_string_is_microsecond_utc() {
        let ctx = ctx();
        let sql = "SELECT to_timestamp('2024-03-10T01:30:00-05:00') AS ts";
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).data_type(), &ltz_timestamp_type());
        let ticks = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0);
        // 2024-03-10 01:30-05:00 = 06:30Z
        assert_eq!(ticks, 1_710_052_200_000_000);
    }

    #[tokio::test]
    async fn zoneless_to_timestamp_keeps_utc_ticks() {
        let ctx = ctx();
        let sql = "SELECT to_timestamp('2024-06-15 12:00:00') AS ts";
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ltz_timestamp_type()
        );
        let ticks = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0);
        // Digits stored as UTC — PR-1 does not localize (TZ-7).
        assert_eq!(ticks, 1_718_452_800_000_000);
    }

    #[tokio::test]
    async fn integer_cast_to_timestamp_is_microsecond_utc_seconds() {
        let ctx = ctx();
        let sql = "SELECT CAST(CAST(-1800 AS BIGINT) AS TIMESTAMP) AS ts";
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        let schema = batches[0].schema();
        assert_eq!(schema.field(0).data_type(), &ltz_timestamp_type());
        let ticks = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0);
        assert_eq!(ticks, -1_800_000_000);
    }
}
