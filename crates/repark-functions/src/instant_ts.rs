//! Spark LTZ instant producers — Arrow `Timestamp(µs, UTC)`.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::timezone::Tz;
use arrow::array::{Array, ArrayRef, AsArray, StringArray, TimestampMicrosecondArray};
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
    ScalarUDF, ScalarUDFImpl, Signature, TryCast, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

use crate::datetime::localize_wall_micros_in_zone;
use crate::decimal_cast::DECIMAL_CAST_NULLABLE_NAME;
use crate::session_time_zone::{
    current_timezone_udf, is_session_time_zone_deferred, session_time_zone_from_options,
};
use crate::spark_string_timestamp::{
    StringCastFailure, cast_columnar_strings_to_ltz, is_string_type, rewrite_string_try_cast,
    spark_string_literal,
};
use crate::spark_type_planner::planned_instant_cast;
use crate::timestamp_type::{is_spark_timestamp_type_deferred, spark_timestamp_type_from_options};

pub(crate) mod ltz_cast;
mod ntz_cast;

pub use ltz_cast::{TIMESTAMP_LTZ_CAST_NAME, TRY_TIMESTAMP_LTZ_CAST_NAME, timestamp_ltz_cast_udf};
pub use ntz_cast::{NTZ_TIMESTAMP_CAST_RULE_NAME, insert_ntz_timestamp_cast_before_coercion};

/// Spark's default `TIMESTAMP` / LTZ Arrow type — µs with a UTC annotation.
pub(crate) fn ltz_timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::<str>::from("UTC")))
}

/// Spark `TIMESTAMP_NTZ` Arrow type — naive µs.
pub(crate) fn ntz_timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, None)
}

/// The instant-typed SQL producers this crate overwrites after `datafusion-spark`.
#[must_use]
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        now_udf(),
        current_timestamp_udf(),
        to_timestamp_udf(),
        current_timezone_udf(),
        crate::timestamp_ltz_ntz::to_timestamp_ltz_udf(),
        crate::timestamp_ltz_ntz::to_timestamp_ntz_udf(),
        crate::timestamp_ltz_ntz::try_to_timestamp_udf(),
        crate::time_family::make_time_udf(),
        crate::time_family::to_time_udf(),
        crate::time_family::time_diff_udf(),
        crate::time_family::time_trunc_udf(),
        crate::time_family::current_time_udf(),
        crate::time_family::type_of_udf(),
        crate::timestamp_ns_cast::timestamp_ns_cast_udf(false),
        crate::timestamp_ns_cast::timestamp_ns_cast_udf(true),
        crate::timestamp_ntz_cast::timestamp_ntz_cast_udf(false),
        crate::timestamp_ntz_cast::timestamp_ntz_cast_udf(true),
        crate::timestamp_ntz_cast::timestamp_ntz_literal_udf(),
        ltz_cast::timestamp_ltz_cast_udf(false),
        ltz_cast::timestamp_ltz_cast_udf(true),
    ]
}

/// Analyzer rule: `CAST(<integer|NULL> AS TIMESTAMP)` yields the LTZ wire type.
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
    Arc::new(ScalarUDF::from(SparkToTimestamp::new(StringGrammar::Spark)))
}

pub(crate) fn arrow_grammar_to_timestamp_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkToTimestamp::new(StringGrammar::Arrow)))
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

/// `now()` / `current_timestamp()` are statement-stable microseconds in UTC.
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

/// `to_timestamp` parses strings, localizes zoneless values in the session zone, and emits µs+UTC.
#[derive(Debug)]
struct SparkToTimestamp {
    signature: Signature,
    inner: datafusion::functions::datetime::to_timestamp::ToTimestampFunc,
    grammar: StringGrammar,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringGrammar {
    Spark,
    Arrow,
}

impl SparkToTimestamp {
    fn new(grammar: StringGrammar) -> Self {
        let inner = datafusion::functions::datetime::to_timestamp::ToTimestampFunc::new_with_config(
            &ConfigOptions::default(),
        );
        Self {
            signature: Signature::variadic_any(Volatility::Stable),
            inner,
            grammar,
        }
    }
}

impl PartialEq for SparkToTimestamp {
    fn eq(&self, other: &Self) -> bool {
        self.grammar == other.grammar
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
        Some(ScalarUDF::from(Self::new(self.grammar)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() >= 2 {
            return invoke_to_timestamp_with_format(self, &args);
        }
        invoke_single(self, args)
    }
}

fn invoke_single(spark: &SparkToTimestamp, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
    let zone_id = session_time_zone_from_options(args.config_options.as_ref());
    let zone = parse_extraction_zone(zone_id)?;
    if args.args.len() == 1
        && let Some(localized) = try_localize_date_or_ntz(&args.args[0], zone)?
    {
        return Ok(localized);
    }
    let ansi = crate::ansi::spark_ansi_enabled_from_options(args.config_options.as_ref());
    if spark.grammar == StringGrammar::Spark
        && let [value] = args.args.as_slice()
        && is_string_type(&value.data_type())
    {
        let failure = if ansi {
            StringCastFailure::Raise
        } else {
            StringCastFailure::Null
        };
        return cast_columnar_strings_to_ltz(value, zone, failure);
    }
    let strings = args.args.first().and_then(columnar_utf8_strings);
    match spark.inner.invoke_with_args(args) {
        Ok(produced) => {
            let ltz = cast_columnar_to_ltz(produced)?;
            match strings {
                Some(texts) => localize_zoneless_string_ticks(ltz, &texts, zone),
                None => Ok(ltz),
            }
        }
        Err(error) => {
            let Some(texts) = strings else {
                return Err(error);
            };
            translate_malformed_strings(spark, &texts, ansi)
        }
    }
}

fn translate_malformed_strings(
    spark: &SparkToTimestamp,
    texts: &[Option<String>],
    ansi: bool,
) -> Result<ColumnarValue> {
    if !ansi {
        let nulls =
            TimestampMicrosecondArray::from(vec![None::<i64>; texts.len()]).with_timezone("UTC");
        return Ok(ColumnarValue::Array(Arc::new(nulls)));
    }
    Err(datafusion::common::DataFusionError::Execution(
        malformed_timestamp_text(
            first_malformed_string(spark, texts)
                .as_deref()
                .unwrap_or("invalid"),
        ),
    ))
}

fn malformed_timestamp_text(value: &str) -> String {
    format!(
        "[CAST_INVALID_INPUT] The value '{value}' of the type \"STRING\" cannot be cast to \
         \"TIMESTAMP\" because it is malformed. Correct the value as per the syntax, or change \
         its target type. Use `try_cast` to tolerate malformed input and return NULL instead. \
         SQLSTATE: 22018"
    )
}

fn first_malformed_string(spark: &SparkToTimestamp, texts: &[Option<String>]) -> Option<String> {
    for text in texts.iter().flatten() {
        let probe: ArrayRef = Arc::new(StringArray::from(vec![Some(text.as_str())]));
        let field: FieldRef = Arc::new(Field::new("to_timestamp", ltz_timestamp_type(), true));
        let probe_args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(probe)],
            arg_fields: vec![field.clone()],
            number_rows: 1,
            return_field: field,
            config_options: Arc::new(ConfigOptions::default()),
        };
        if spark.inner.invoke_with_args(probe_args).is_err() {
            return Some(text.clone());
        }
    }
    texts.iter().flatten().next().cloned()
}

fn invoke_to_timestamp_with_format(
    spark: &SparkToTimestamp,
    args: &ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    if args.args.len() > 2 {
        return exec_err!(
            "'to_timestamp' expects 1 or 2 arguments, got {}",
            args.args.len()
        );
    }
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if !matches!(
        arrays[0].data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        let mut single = args.clone();
        single.args.truncate(1);
        single.arg_fields.truncate(1);
        return invoke_single(spark, single);
    }
    crate::java_datetime::stamps_with_format_column(&arrays[0], &arrays[1], args)
}

fn parse_extraction_zone(zone_id: &str) -> Result<Tz> {
    zone_id.parse::<Tz>().map_err(|error| {
        datafusion::common::DataFusionError::Execution(format!(
            "session timezone {zone_id:?} could not be resolved at query time ({error})"
        ))
    })
}

#[must_use]
pub fn string_carries_timezone(text: &str) -> bool {
    if let Some(parsed) = crate::spark_string_timestamp::grammar::parse_timestamp_string(text)
        && let Some(zone) = parsed.zone
        && crate::spark_string_timestamp::zone::spark_zone_id(zone).is_some()
    {
        return true;
    }
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return false;
    }
    let last = trimmed.as_bytes()[trimmed.len() - 1];
    last == b']' && trimmed.contains('[')
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

/// `CAST(<integer|NULL> AS TIMESTAMP)` → `Timestamp(µs, UTC)`.
#[derive(Debug)]
struct SparkLtzTimestampCast;

impl AnalyzerRule for SparkLtzTimestampCast {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let zone = (!is_session_time_zone_deferred(config))
            .then(|| session_time_zone_from_options(config).to_string());
        let zone_ref = zone.as_deref();
        let defer_timestamp_type = is_spark_timestamp_type_deferred(config);
        let session_ntz =
            !defer_timestamp_type && spark_timestamp_type_from_options(config).is_ntz();
        let empty = DFSchema::empty();
        let plan = crate::timestamp_ns_cast::widen_mixed_values_timestamps(
            plan,
            &|cell: Expr| {
                cell.transform_up(|node| {
                    Ok(rewrite_cast(
                        node,
                        &empty,
                        zone_ref,
                        defer_timestamp_type,
                        session_ntz,
                    ))
                })
                .data()
            },
            session_ntz,
        )?;
        let plan = if defer_timestamp_type {
            plan
        } else {
            crate::timestamp_ntz_cast::retarget_dml_store_casts(plan)?
        };
        plan.transform_up_with_subqueries(|node| {
            rewrite_plan(node, zone_ref, defer_timestamp_type, session_ntz)
        })
        .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_ltz_timestamp_cast"
    }
}

fn rewrite_plan(
    plan: LogicalPlan,
    zone: Option<&str>,
    defer_timestamp_type: bool,
    session_ntz: bool,
) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| {
            rewrite_cast(node, &schema, zone, defer_timestamp_type, session_ntz).transform_data(
                |typed| crate::timestamp_ns_cast::wall_kernel_by_target(typed, &schema),
            )
        })?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed
        .map_data(LogicalPlan::recompute_schema)?
        .map_data(crate::timestamp_ns_cast::conform_values_timestamp_columns)
}

fn rewrite_cast(
    expr: Expr,
    schema: &DFSchema,
    zone: Option<&str>,
    defer_timestamp_type: bool,
    session_ntz: bool,
) -> Transformed<Expr> {
    if defer_timestamp_type {
        if let Some((arg, false)) = ltz_cast::unwrap_ltz_cast_marker(&expr)
            && let Some(literal) = spark_string_literal(&arg, None)
        {
            return Transformed::yes(literal);
        }
        return Transformed::no(expr);
    }
    if let Some((arg, try_cast)) = ltz_cast::unwrap_ltz_cast_marker(&expr) {
        return rewrite_explicit_ltz_marker(arg, try_cast, schema, zone);
    }
    rewrite_cast_as_ltz(expr, schema, zone, session_ntz)
}

fn rewrite_explicit_ltz_marker(
    arg: Expr,
    try_cast: bool,
    schema: &DFSchema,
    zone: Option<&str>,
) -> Transformed<Expr> {
    if let Ok(source) = arg.get_type(schema) {
        if is_ltz_timestamp(&source) {
            if !try_cast {
                return Transformed::yes(arg);
            }
            return Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                crate::timestamp_ltz_ntz::try_to_timestamp_udf(),
                vec![arg],
            )));
        }
        if matches!(source, DataType::Null) {
            if matches!(&arg, Expr::Literal(scalar, _) if scalar.is_null()) {
                return Transformed::yes(Expr::Literal(
                    ScalarValue::TimestampMicrosecond(None, Some(Arc::<str>::from("UTC"))),
                    None,
                ));
            }
            let udf = if try_cast {
                crate::timestamp_ltz_ntz::try_to_timestamp_udf()
            } else {
                to_timestamp_udf()
            };
            return Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                udf,
                vec![arg],
            )));
        }
    }
    let nullable = try_cast || arg.nullable(schema).unwrap_or(true);
    let field = Arc::new(Field::new("ts", ltz_timestamp_type(), nullable));
    let unwrapped = if try_cast {
        Expr::TryCast(TryCast::new_from_field(Box::new(arg), field))
    } else {
        Expr::Cast(Cast::new_from_field(Box::new(arg), field))
    };
    let rewritten = rewrite_cast_as_ltz(unwrapped, schema, zone, false);
    Transformed::yes(rewritten.data)
}

fn unwrap_nullable_marker(expr: &Expr) -> Option<&Expr> {
    let Expr::ScalarFunction(function) = expr else {
        return None;
    };
    if function.func.name() == DECIMAL_CAST_NULLABLE_NAME && function.args.len() == 1 {
        Some(&function.args[0])
    } else {
        None
    }
}

fn rewrite_cast_as_ltz(
    expr: Expr,
    schema: &DFSchema,
    zone: Option<&str>,
    session_ntz: bool,
) -> Transformed<Expr> {
    if let Some(seamed) = crate::zone_seam::rewrite_zone_seam(&expr, schema) {
        return Transformed::yes(seamed);
    }
    if let Some(rewritten) = rewrite_string_try_cast(&expr, schema, zone) {
        return Transformed::yes(rewritten);
    }
    if let Expr::Cast(cast) = &expr {
        if session_ntz && !is_ltz_timestamp(cast.field.data_type()) {
            return Transformed::no(expr);
        }
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
            if let Some(planned) = planned_instant_cast(cast, &source, schema, false) {
                return Transformed::yes(planned);
            }
            if matches!(source, DataType::Timestamp(TimeUnit::Nanosecond, _))
                && is_ltz_timestamp(cast.field.data_type())
            {
                return Transformed::yes(crate::timestamp_ns_cast::narrow_timestamp_ns_expr(
                    *cast.expr.clone(),
                ));
            }
            if targeting_timestamp && !targeting_ns && is_wall_clock_cast_source(&source) {
                if let Some(inner) = unwrap_nullable_marker(&cast.expr) {
                    return Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                        to_timestamp_udf(),
                        vec![inner.clone()],
                    )));
                }
                if let Some(literal) = spark_string_literal(&cast.expr, zone) {
                    return Transformed::yes(literal);
                }
                return Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
                    to_timestamp_udf(),
                    vec![*cast.expr.clone()],
                )));
            }
            let source_is_int_or_null = source.is_integer() || matches!(source, DataType::Null);
            if targeting_seconds && source_is_int_or_null {
                return wrap_as_ltz(expr, schema);
            }
        }
    }
    if session_ntz {
        Transformed::no(expr)
    } else {
        peel_naive_cast_of_ltz_producer(expr, schema)
    }
}

fn peel_naive_cast_of_ltz_producer(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::Cast(cast) = &expr else {
        return Transformed::no(expr);
    };
    if !matches!(
        cast.field.data_type(),
        DataType::Timestamp(TimeUnit::Microsecond, None)
    ) {
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

pub(crate) fn is_wall_clock_cast_source(data_type: &DataType) -> bool {
    is_string_type(data_type)
        || matches!(
            data_type,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(TimeUnit::Microsecond, None)
        )
}

pub(crate) fn is_ltz_timestamp(data_type: &DataType) -> bool {
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

#[cfg(test)]
mod tests;
