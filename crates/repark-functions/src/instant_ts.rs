//! Spark LTZ instant producers — Arrow `Timestamp(µs, UTC)`.
//!
//! `now`, `current_timestamp`, and LTZ `to_timestamp` produce microsecond UTC timestamps.
//! Zoneless inputs localize in the session zone; zone-suffixed inputs and NTZ remain unchanged.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{Array, AsArray};
use arrow::datatypes::TimestampMicrosecondType;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err, internal_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

use crate::datetime::localize_wall_micros_in_zone;
use crate::session_time_zone::session_time_zone_from_options;
use crate::timestamp_type::{SparkTimestampType, spark_timestamp_type_from_options};

/// Spark's default `TIMESTAMP` / LTZ Arrow type — µs with a UTC annotation.
pub(crate) fn ltz_timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC")))
}

/// Spark `TIMESTAMP_NTZ` Arrow type — naive µs.
pub(crate) fn ntz_timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
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

/// Return the instant-typed `to_timestamp` kernel used by SQL and facade dispatch.
#[must_use]
pub fn to_timestamp_udf() -> Arc<ScalarUDF> {
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
/// `to_timestamp` parses strings, localizes zoneless values in the session zone, and emits µs+UTC.
/// ===========================================================================================
#[derive(Debug)]
struct SparkToTimestamp {
    signature: Signature,
    inner: datafusion::functions::datetime::to_timestamp::ToTimestampFunc,
}

impl SparkToTimestamp {
    fn new() -> Self {
        let inner = datafusion::functions::datetime::to_timestamp::ToTimestampFunc::new_with_config(
            &ConfigOptions::default(),
        );
        Self {
            signature: Signature::variadic_any(Volatility::Volatile),
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

    fn return_field_from_args(&self, _args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(Field::new(self.name(), ltz_timestamp_type(), true).into())
    }

    fn with_updated_config(&self, _config: &ConfigOptions) -> Option<ScalarUDF> {
        Some(ScalarUDF::from(Self::new()))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let zone_id = session_time_zone_from_options(args.config_options.as_ref());
        let zone = parse_extraction_zone(zone_id)?;
        if args.args.len() == 1
            && let Some(localized) = try_localize_date_or_ntz(&args.args[0], zone)?
        {
            return Ok(localized);
        }
        let strings = args.args.first().and_then(columnar_utf8_strings);
        let produced = self.inner.invoke_with_args(args)?;
        let ltz = cast_columnar_to_ltz(produced)?;
        match strings {
            Some(texts) => localize_zoneless_string_ticks(ltz, &texts, zone),
            None => Ok(ltz),
        }
    }
}

fn parse_extraction_zone(zone_id: &str) -> Result<Tz> {
    zone_id.parse::<Tz>().map_err(|error| {
        datafusion::common::DataFusionError::Execution(format!(
            "session timezone {zone_id:?} could not be resolved at query time ({error})"
        ))
    })
}

/// `true` when Spark would treat the string as already carrying a zone (instant, not wall).
fn string_carries_timezone(text: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return false;
    }
    let last = trimmed.as_bytes()[trimmed.len() - 1];
    if last == b'Z' || last == b'z' {
        return true;
    }
    if last == b']' && trimmed.contains('[') {
        return true;
    }
    ends_with_numeric_offset(trimmed)
}

fn ends_with_numeric_offset(text: &str) -> bool {
    let bytes = text.as_bytes();
    let length = bytes.len();
    for suffix_len in [6_usize, 5, 3] {
        if length < suffix_len {
            continue;
        }
        let suffix = &bytes[length - suffix_len..];
        if suffix[0] != b'+' && suffix[0] != b'-' {
            continue;
        }
        if !suffix[1].is_ascii_digit() || !suffix[2].is_ascii_digit() {
            continue;
        }
        match suffix_len {
            3 => return true,
            5 if suffix[3].is_ascii_digit() && suffix[4].is_ascii_digit() => return true,
            6 if suffix[3] == b':' && suffix[4].is_ascii_digit() && suffix[5].is_ascii_digit() => {
                return true;
            }
            _ => {}
        }
    }
    false
}

fn columnar_utf8_strings(value: &ColumnarValue) -> Option<Vec<Option<String>>> {
    let array = match value {
        ColumnarValue::Array(array) => Arc::clone(array),
        ColumnarValue::Scalar(scalar) => scalar.to_array().ok()?,
    };
    if !matches!(
        array.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return None;
    }
    let utf8 = cast(array.as_ref(), &DataType::Utf8).ok()?;
    let strings = utf8.as_string::<i32>();
    Some(
        (0..strings.len())
            .map(|row| {
                if strings.is_null(row) {
                    None
                } else {
                    Some(strings.value(row).to_string())
                }
            })
            .collect(),
    )
}

fn try_localize_date_or_ntz(value: &ColumnarValue, zone: Tz) -> Result<Option<ColumnarValue>> {
    let array = match value {
        ColumnarValue::Array(array) => Arc::clone(array),
        ColumnarValue::Scalar(scalar) => scalar.to_array()?,
    };
    match array.data_type() {
        DataType::Date32 | DataType::Date64 => {
            let days = cast(array.as_ref(), &DataType::Date32)?;
            let days = days.as_primitive::<datafusion::arrow::datatypes::Date32Type>();
            let mut builder = arrow::array::TimestampMicrosecondArray::builder(days.len());
            for row in 0..days.len() {
                if days.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let wall_micros = i64::from(days.value(row)) * 86_400 * 1_000_000;
                match localize_wall_micros_in_zone(wall_micros, zone) {
                    Some(micros) => builder.append_value(micros),
                    None => builder.append_null(),
                }
            }
            Ok(Some(ColumnarValue::Array(Arc::new(
                builder.finish().with_timezone("UTC"),
            ))))
        }
        DataType::Timestamp(TimeUnit::Microsecond, None) => {
            let micros = cast(
                array.as_ref(),
                &DataType::Timestamp(TimeUnit::Microsecond, None),
            )?;
            let micros = micros.as_primitive::<TimestampMicrosecondType>();
            let mut builder = arrow::array::TimestampMicrosecondArray::builder(micros.len());
            for row in 0..micros.len() {
                if micros.is_null(row) {
                    builder.append_null();
                    continue;
                }
                match localize_wall_micros_in_zone(micros.value(row), zone) {
                    Some(localized) => builder.append_value(localized),
                    None => builder.append_null(),
                }
            }
            Ok(Some(ColumnarValue::Array(Arc::new(
                builder.finish().with_timezone("UTC"),
            ))))
        }
        _ => Ok(None),
    }
}

fn localize_zoneless_string_ticks(
    value: ColumnarValue,
    texts: &[Option<String>],
    zone: Tz,
) -> Result<ColumnarValue> {
    let array = match value {
        ColumnarValue::Array(array) => array,
        ColumnarValue::Scalar(scalar) => Arc::new(scalar.to_array()?),
    };
    let micros = cast(
        array.as_ref(),
        &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC"))),
    )?;
    let micros = micros.as_primitive::<TimestampMicrosecondType>();
    let mut builder = arrow::array::TimestampMicrosecondArray::builder(micros.len());
    for row in 0..micros.len() {
        if micros.is_null(row) {
            builder.append_null();
            continue;
        }
        let ticks = micros.value(row);
        let zoneless = texts
            .get(row)
            .and_then(Option::as_deref)
            .is_none_or(|text| !string_carries_timezone(text));
        if zoneless {
            match localize_wall_micros_in_zone(ticks, zone) {
                Some(localized) => builder.append_value(localized),
                None => {
                    return exec_err!(
                        "cannot localize zoneless timestamp into session timezone: out of range"
                    );
                }
            }
        } else {
            builder.append_value(ticks);
        }
    }
    Ok(ColumnarValue::Array(Arc::new(
        builder.finish().with_timezone("UTC"),
    )))
}

/// `CAST(<integer|NULL> AS TIMESTAMP)` → `Timestamp(µs, UTC)`. String / date / NTZ
/// sources rewrite onto [`SparkToTimestamp`] so zoneless walls localize in the session
/// zone. Idempotent: an already-LTZ target is left alone.
#[derive(Debug)]
struct SparkLtzTimestampCast;

impl AnalyzerRule for SparkLtzTimestampCast {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let zone = session_time_zone_from_options(config).to_string();
        let timestamp_type = spark_timestamp_type_from_options(config);
        plan.transform_up_with_subqueries(|node| rewrite_plan(node, &zone, timestamp_type))
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)] // `AnalyzerRule::name` ties the lifetime to &self
    fn name(&self) -> &str {
        "spark_ltz_timestamp_cast"
    }
}

fn rewrite_plan(
    plan: LogicalPlan,
    zone: &str,
    timestamp_type: SparkTimestampType,
) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten =
            expr.transform_up(|node| Ok(rewrite_cast(node, &schema, zone, timestamp_type)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_cast(
    expr: Expr,
    schema: &DFSchema,
    zone: &str,
    timestamp_type: SparkTimestampType,
) -> Transformed<Expr> {
    if timestamp_type.is_ntz() {
        return rewrite_cast_as_ntz(expr, schema);
    }
    if let Expr::Cast(cast) = &expr {
        let targeting_naive_us = matches!(
            cast.field.data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let child_is_to_timestamp = matches!(
            cast.expr.as_ref(),
            Expr::ScalarFunction(function) if function.func.name() == "to_timestamp"
        );
        if targeting_naive_us && child_is_to_timestamp {
            return Transformed::yes(*cast.expr.clone());
        }
        let targeting_timestamp = matches!(cast.field.data_type(), DataType::Timestamp(_, _));
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
            if targeting_timestamp && is_wall_clock_cast_source(&source) {
                if let Some(literal) = localized_zoneless_utf8_literal(&cast.expr, zone) {
                    return Transformed::yes(literal);
                }
                return Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                    to_timestamp_udf(),
                    vec![*cast.expr.clone()],
                )));
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
    let rewritten = wrap_ns_literal(expr, schema, zone);
    peel_naive_cast_of_ltz_producer(rewritten.data, schema)
}

/// `TypeCoercion` may wrap a `TIMESTAMP '…'` literal in `CAST(… AS Timestamp(µs))`
/// (naive) *before* this rule rewrites the literal to `to_timestamp`. That CAST
/// then strips the UTC annotation. Peel it when the child is already LTZ.
fn peel_naive_cast_of_ltz_producer(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::Cast(cast) = &expr else {
        return Transformed::no(expr);
    };
    if !matches!(cast.field.data_type(), DataType::Timestamp(_, None)) {
        return Transformed::no(expr);
    }
    let is_to_timestamp = matches!(
        cast.expr.as_ref(),
        Expr::ScalarFunction(function) if function.func.name() == "to_timestamp"
    );
    let child_is_ltz = cast
        .expr
        .get_type(schema)
        .is_ok_and(|data_type| is_ltz_timestamp(&data_type));
    if is_to_timestamp || child_is_ltz {
        return Transformed::yes(*cast.expr.clone());
    }
    Transformed::no(expr)
}

fn is_wall_clock_cast_source(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(TimeUnit::Microsecond, None)
    )
}

fn is_ltz_timestamp(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone))
            if zone.as_ref().eq_ignore_ascii_case("UTC") || zone.as_ref() == "+00:00"
    )
}

/// A zoneless UTF-8 timestamp literal, localized in `zone` as a non-null LTZ literal.
/// `None` when the expr is not a zoneless string literal (columns stay on `to_timestamp`).
fn localized_zoneless_utf8_literal(expr: &Expr, zone: &str) -> Option<Expr> {
    let Expr::Literal(scalar, _) = expr else {
        return None;
    };
    let text = match scalar {
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => text.as_str(),
        _ => return None,
    };
    if string_carries_timezone(text) {
        return None;
    }
    let parsed_zone = zone.parse::<Tz>().ok()?;
    let naive = arrow::compute::cast(
        &arrow::array::StringArray::from(vec![text]),
        &DataType::Timestamp(TimeUnit::Microsecond, None),
    )
    .ok()?;
    let wall = naive.as_primitive::<TimestampMicrosecondType>().value(0);
    let localized = localize_wall_micros_in_zone(wall, parsed_zone)?;
    Some(Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(localized), Some(Arc::<str>::from("UTC"))),
        None,
    ))
}

fn wrap_as_ltz(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let nullable = expr.nullable(schema).unwrap_or(true);
    let field = Arc::new(Field::new("ts", ltz_timestamp_type(), nullable));
    Transformed::yes(Expr::Cast(Cast::new_from_field(Box::new(expr), field)))
}

/// Localize zoneless nanosecond timestamp literals in the session zone; annotated literals are already instants.
fn wrap_ns_literal(expr: Expr, schema: &DFSchema, zone: &str) -> Transformed<Expr> {
    let Expr::Literal(scalar, _) = &expr else {
        return Transformed::no(expr);
    };
    let ScalarValue::TimestampNanosecond(ticks, literal_zone) = scalar else {
        return Transformed::no(expr);
    };
    if literal_zone.is_some() {
        return wrap_as_ltz(expr, schema);
    }
    let Some(nanos) = ticks else {
        return wrap_as_ltz(expr, schema);
    };
    let Ok(parsed_zone) = zone.parse::<Tz>() else {
        return wrap_as_ltz(expr, schema);
    };
    let wall_micros = nanos.div_euclid(1_000);
    let Some(localized) = localize_wall_micros_in_zone(wall_micros, parsed_zone) else {
        return wrap_as_ltz(expr, schema);
    };
    Transformed::yes(Expr::Literal(
        ScalarValue::TimestampMicrosecond(Some(localized), Some(Arc::<str>::from("UTC"))),
        None,
    ))
}

/// `spark.sql.timestampType=TIMESTAMP_NTZ`: bare `TIMESTAMP` literals / casts become naive µs.
/// `to_timestamp` / `now` / `current_timestamp` stay LTZ (they are not the SQL type name).
fn rewrite_cast_as_ntz(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    if let Expr::Cast(cast) = &expr {
        let targeting_naive_us = matches!(
            cast.field.data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let targeting_timestamp = matches!(cast.field.data_type(), DataType::Timestamp(_, _));
        let targeting_ns = matches!(
            cast.field.data_type(),
            DataType::Timestamp(TimeUnit::Nanosecond, _)
        );
        let targeting_seconds = matches!(
            cast.field.data_type(),
            DataType::Timestamp(TimeUnit::Second, _)
        );
        if targeting_naive_us {
            let child_is_to_timestamp = matches!(
                cast.expr.as_ref(),
                Expr::ScalarFunction(function) if function.func.name() == "to_timestamp"
            );
            let child_is_ltz = cast
                .expr
                .get_type(schema)
                .is_ok_and(|data_type| is_ltz_timestamp(&data_type));
            if child_is_to_timestamp || child_is_ltz {
                return wrap_as_ntz(*cast.expr.clone(), schema);
            }
            return Transformed::no(expr);
        }
        if let Ok(source) = cast.expr.get_type(schema) {
            if targeting_timestamp && is_ltz_timestamp(&source) {
                return wrap_as_ntz(*cast.expr.clone(), schema);
            }
            if targeting_timestamp && is_wall_clock_cast_source(&source) {
                return wrap_as_ntz(*cast.expr.clone(), schema);
            }
            let source_is_int_or_null = source.is_integer() || matches!(source, DataType::Null);
            let source_is_seconds = matches!(source, DataType::Timestamp(TimeUnit::Second, _));
            if (targeting_seconds && source_is_int_or_null)
                || (targeting_ns && (source_is_int_or_null || source_is_seconds))
            {
                return wrap_as_ntz(expr, schema);
            }
            if targeting_ns && is_ltz_timestamp(&source) {
                return wrap_as_ntz(*cast.expr.clone(), schema);
            }
        }
    }
    wrap_ns_literal_as_ntz(expr)
}

fn wrap_as_ntz(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let nullable = expr.nullable(schema).unwrap_or(true);
    let field = Arc::new(Field::new("ts", ntz_timestamp_type(), nullable));
    Transformed::yes(Expr::Cast(Cast::new_from_field(Box::new(expr), field)))
}

/// DataFusion folds `TIMESTAMP '…'` to `Timestamp(ns, None)` literals. Under NTZ keep the
/// spelled wall as naive µs — do **not** localize in the session zone.
fn wrap_ns_literal_as_ntz(expr: Expr) -> Transformed<Expr> {
    let Expr::Literal(scalar, _) = &expr else {
        return Transformed::no(expr);
    };
    let ScalarValue::TimestampNanosecond(ticks, _literal_zone) = scalar else {
        return Transformed::no(expr);
    };
    let micros = ticks.map(|nanos| nanos.div_euclid(1_000));
    Transformed::yes(Expr::Literal(
        ScalarValue::TimestampMicrosecond(micros, None),
        None,
    ))
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
        assert_eq!(ticks, 1_710_052_200_000_000);
    }

    #[tokio::test]
    async fn zoneless_to_timestamp_under_utc_session_keeps_wall_as_utc() {
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
        assert_eq!(ticks, 1_718_452_800_000_000);
    }

    fn ctx_at(zone: &str) -> SessionContext {
        use datafusion::prelude::SessionConfig;
        let config = crate::session_time_zone::with_session_time_zone(SessionConfig::new(), zone);
        let ctx = SessionContext::new_with_config(config);
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    #[tokio::test]
    async fn zoneless_inputs_localize_in_the_session_zone() {
        let ctx = ctx_at("America/New_York");
        let expected = 1_718_467_200_000_000_i64;
        for sql in [
            "SELECT to_timestamp('2024-06-15 12:00:00') AS ts",
            "SELECT CAST('2024-06-15 12:00:00' AS TIMESTAMP) AS ts",
            "SELECT TIMESTAMP '2024-06-15 12:00:00' AS ts",
        ] {
            let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
            assert_eq!(
                batches[0].schema().field(0).data_type(),
                &ltz_timestamp_type(),
                "{sql}"
            );
            let ticks = batches[0]
                .column(0)
                .as_primitive::<TimestampMicrosecondType>()
                .value(0);
            assert_eq!(ticks, expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn zone_suffixed_to_timestamp_is_not_localized_again() {
        let ctx = ctx_at("America/New_York");
        let sql = "SELECT to_timestamp('2024-06-15T12:00:00Z') AS ts";
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        let ticks = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0);
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

    fn ctx_ntz_at(zone: &str) -> SessionContext {
        use datafusion::prelude::SessionConfig;
        let config = crate::session_time_zone::with_session_time_zone(SessionConfig::new(), zone);
        let config = crate::timestamp_type::with_spark_timestamp_type(
            config,
            crate::timestamp_type::SparkTimestampType::Ntz,
        );
        let ctx = SessionContext::new_with_config(config);
        crate::register_all(&ctx);
        for rule in crate::analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    #[tokio::test]
    async fn ntz_opt_in_bare_timestamp_is_naive_microseconds() {
        let ctx = ctx_ntz_at("America/New_York");
        let expected = 1_718_452_800_000_000_i64;
        for sql in [
            "SELECT TIMESTAMP '2024-06-15 12:00:00' AS ts",
            "SELECT CAST('2024-06-15 12:00:00' AS TIMESTAMP) AS ts",
        ] {
            let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
            assert_eq!(
                batches[0].schema().field(0).data_type(),
                &ntz_timestamp_type(),
                "{sql}"
            );
            let ticks = batches[0]
                .column(0)
                .as_primitive::<TimestampMicrosecondType>()
                .value(0);
            assert_eq!(ticks, expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn ntz_opt_in_does_not_retarget_to_timestamp_or_now() {
        let ctx = ctx_ntz_at("UTC");
        let batches = ctx
            .sql("SELECT to_timestamp('2024-06-15 12:00:00') AS ts")
            .await
            .expect("to_timestamp")
            .collect()
            .await
            .expect("collect");
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ltz_timestamp_type(),
            "to_timestamp stays LTZ; the knob is the SQL type name TIMESTAMP"
        );
        let batches = ctx
            .sql("SELECT current_timestamp() AS ts")
            .await
            .expect("current_timestamp")
            .collect()
            .await
            .expect("collect");
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ltz_timestamp_type(),
            "current_timestamp is an instant, not the SQL type name"
        );
    }

    #[tokio::test]
    async fn ntz_opt_in_integer_cast_is_naive_microseconds_seconds() {
        let ctx = ctx_ntz_at("UTC");
        let sql = "SELECT CAST(CAST(-1800 AS BIGINT) AS TIMESTAMP) AS ts";
        let batches = ctx.sql(sql).await.expect(sql).collect().await.expect(sql);
        assert_eq!(
            batches[0].schema().field(0).data_type(),
            &ntz_timestamp_type()
        );
        let ticks = batches[0]
            .column(0)
            .as_primitive::<TimestampMicrosecondType>()
            .value(0);
        assert_eq!(ticks, -1_800_000_000);
    }
}
