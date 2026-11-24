use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Date32Array};
use arrow::compute::{DatePart, cast};
use arrow::datatypes::{DataType, Field, FieldRef, IntervalUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::types::{logical_date, logical_string};
use datafusion::common::utils::take_function_args;
use datafusion::common::{DFSchema, DataFusionError, Result, ScalarValue, internal_err, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Coercion, ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

use crate::ansi::spark_ansi_enabled_from_options;
use crate::csv::field::spark_string_to_date;
use crate::spark_string_timestamp::{StringCastFailure, cast_strings_to_ltz, is_string_type};

use super::session_wall::{extract_calendar_part, extraction_time_zone, unpack_dictionary};

#[must_use]
pub fn date_part_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(SparkDatePart::new(false)))
}

#[must_use]
pub fn facade_date_part_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(SparkDatePart::new(true)))
}

#[must_use]
pub fn extract_date_part_rule() -> Arc<dyn AnalyzerRule + Send + Sync> {
    Arc::new(SparkExtractDatePart)
}

#[allow(clippy::missing_errors_doc)]
pub fn insert_extract_date_part_before_coercion(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules.iter().position(|rule| rule.name() == "type_coercion") else {
        return plan_err!(
            "the extract date_part rule requires the default type_coercion analyzer rule"
        );
    };
    rules.insert(position, extract_date_part_rule());
    Ok(rules)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkDatePart {
    signature: Signature,
    aliases: Vec<String>,
    allow_month_day_nano_interval: bool,
}

impl SparkDatePart {
    fn new(allow_month_day_nano_interval: bool) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Timestamp),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_date())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Interval),
                    ]),
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("datepart")],
            allow_month_day_nano_interval,
        }
    }
}

#[derive(Clone, Copy)]
struct CalendarField {
    part: DatePart,
    shift: i32,
    needs_timestamp_for_string: bool,
}

impl CalendarField {
    const fn date(part: DatePart, shift: i32) -> Self {
        Self {
            part,
            shift,
            needs_timestamp_for_string: false,
        }
    }

    const fn time(part: DatePart) -> Self {
        Self {
            part,
            shift: 0,
            needs_timestamp_for_string: true,
        }
    }
}

fn normalize_part(raw: &str) -> String {
    raw.to_ascii_lowercase()
}

fn unwrap_dictionary_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => value.as_ref(),
        other => other,
    }
}

fn resolve_field(normalized: &str) -> Option<CalendarField> {
    match normalized {
        "year" | "y" | "years" | "yr" | "yrs" => Some(CalendarField::date(DatePart::Year, 0)),
        "yearofweek" => Some(CalendarField::date(DatePart::YearISO, 0)),
        "quarter" | "qtr" => Some(CalendarField::date(DatePart::Quarter, 0)),
        "month" | "mon" | "mons" | "months" => Some(CalendarField::date(DatePart::Month, 0)),
        "week" | "w" | "weeks" => Some(CalendarField::date(DatePart::WeekISO, 0)),
        "day" | "d" | "days" => Some(CalendarField::date(DatePart::Day, 0)),
        "dayofweek" | "dow" => Some(CalendarField::date(DatePart::DayOfWeekSunday0, 1)),
        "dayofweek_iso" | "dow_iso" => Some(CalendarField::date(DatePart::DayOfWeekMonday0, 1)),
        "doy" => Some(CalendarField::date(DatePart::DayOfYear, 0)),
        "hour" | "h" | "hours" | "hr" | "hrs" => Some(CalendarField::time(DatePart::Hour)),
        "minute" | "m" | "min" | "mins" | "minutes" => Some(CalendarField::time(DatePart::Minute)),
        "second" | "s" | "sec" | "secs" | "seconds" => Some(CalendarField::time(DatePart::Second)),
        _ => None,
    }
}

fn resolve_interval_field(normalized: &str, unit: IntervalUnit) -> Option<&'static str> {
    let canonical = match normalized {
        "year" | "y" | "yr" | "yrs" | "years" => "year",
        "month" | "mon" | "mons" | "months" => "month",
        "day" | "d" | "days" => "day",
        "hour" | "h" | "hr" | "hrs" | "hours" => "hour",
        "minute" | "m" | "min" | "mins" | "minutes" => "minute",
        "second" | "s" | "sec" | "secs" | "seconds" => "second",
        _ => return None,
    };
    match unit {
        IntervalUnit::YearMonth => matches!(canonical, "year" | "month").then_some(canonical),
        IntervalUnit::DayTime => {
            matches!(canonical, "day" | "hour" | "minute" | "second").then_some(canonical)
        }
        IntervalUnit::MonthDayNano => Some(canonical),
    }
}

fn invalid_extract_field_plan_err(raw: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INVALID_EXTRACT_FIELD] Cannot extract `{raw}`. SQLSTATE: 42601"
    ))
}

fn invalid_extract_field_exec_err(raw: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[INVALID_EXTRACT_FIELD] Cannot extract `{raw}`. SQLSTATE: 42601"
    ))
}

fn unsupported_month_day_nano_plan_err() -> DataFusionError {
    DataFusionError::Plan(String::from(
        "SQL `date_part` does not support Interval(MonthDayNano) because the interval qualifier \
         is not preserved; use EXTRACT or typed F.date_part",
    ))
}

fn unsupported_month_day_nano_exec_err() -> DataFusionError {
    DataFusionError::Execution(String::from(
        "SQL `date_part` does not support Interval(MonthDayNano) because the interval qualifier \
         is not preserved; use EXTRACT or typed F.date_part",
    ))
}

fn non_foldable_field_plan_err() -> DataFusionError {
    DataFusionError::Plan(String::from(
        "[NON_FOLDABLE_ARGUMENT] The function `date_part` requires the parameter `field` to be a \
         foldable expression of the type \"STRING\", but the actual argument is a non-foldable. \
         SQLSTATE: 42K08",
    ))
}

fn non_foldable_field_exec_err() -> DataFusionError {
    DataFusionError::Execution(String::from(
        "[NON_FOLDABLE_ARGUMENT] The function `date_part` requires the parameter `field` to be a \
         foldable expression of the type \"STRING\", but the actual argument is a non-foldable. \
         SQLSTATE: 42K08",
    ))
}

fn spark_sql_string_value(text: &str) -> String {
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
}

fn cast_invalid_input_date(text: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[CAST_INVALID_INPUT] The value {} of the type \"STRING\" cannot be cast to \
         \"DATE\" because it is malformed. Correct the value as per the syntax, or change \
         its target type. Use `try_cast` to tolerate malformed input and return NULL instead. \
         SQLSTATE: 22018",
        spark_sql_string_value(text)
    ))
}

fn cast_strings_to_date32(strings: &ArrayRef, failure: StringCastFailure) -> Result<ArrayRef> {
    let texts = cast(strings.as_ref(), &DataType::Utf8)?;
    let texts = texts.as_string::<i32>();
    let mut builder = Date32Array::builder(texts.len());
    for row in 0..texts.len() {
        if texts.is_null(row) {
            builder.append_null();
            continue;
        }
        let text = texts.value(row);
        match spark_string_to_date(text.as_bytes()) {
            Some(days) => builder.append_value(days),
            None if failure == StringCastFailure::Raise => {
                return Err(cast_invalid_input_date(text));
            }
            None => builder.append_null(),
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn forward_interval_to_stock(
    canonical_part: &str,
    value_arg: ColumnarValue,
    arg_fields: &[FieldRef],
    number_rows: usize,
    return_field: FieldRef,
    config_options: Arc<ConfigOptions>,
) -> Result<ColumnarValue> {
    let prepared = match value_arg {
        ColumnarValue::Array(array) => ColumnarValue::Array(unpack_dictionary(&array)?),
        ColumnarValue::Scalar(scalar) => {
            if matches!(scalar.data_type(), DataType::Dictionary(_, _)) {
                let unpacked = unpack_dictionary(&scalar.to_array()?)?;
                ColumnarValue::Scalar(ScalarValue::try_from_array(unpacked.as_ref(), 0)?)
            } else {
                ColumnarValue::Scalar(scalar)
            }
        }
    };
    let value_type = prepared.data_type();
    let value_nullable = arg_fields.get(1).is_none_or(|field| field.is_nullable());
    let forwarded = ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(canonical_part.to_owned()))),
            prepared,
        ],
        arg_fields: vec![
            Arc::new(Field::new("part", DataType::Utf8, false)),
            Arc::new(Field::new("value", value_type, value_nullable)),
        ],
        number_rows,
        return_field,
        config_options,
    };
    datafusion::functions::datetime::date_part().invoke_with_args(forwarded)
}

fn extract_from_strings(
    array: &ArrayRef,
    options: &ConfigOptions,
    field: CalendarField,
) -> Result<ArrayRef> {
    let unpacked = unpack_dictionary(array)?;
    let failure = if spark_ansi_enabled_from_options(options) {
        StringCastFailure::Raise
    } else {
        StringCastFailure::Null
    };
    let temporal = if field.needs_timestamp_for_string {
        let zone = extraction_time_zone(options)?;
        cast_strings_to_ltz(&unpacked, zone, failure)?
    } else {
        cast_strings_to_date32(&unpacked, failure)?
    };
    extract_calendar_part(&temporal, options, field.part, field.shift)
}

impl ScalarUDFImpl for SparkDatePart {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "date_part"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field_scalar, _] = take_function_args(self.name(), args.scalar_arguments)?;
        let [_, source_field] = take_function_args(self.name(), args.arg_fields)?;
        let source_type = unwrap_dictionary_type(source_field.data_type());
        if let Some(raw) = field_scalar.and_then(|scalar| scalar.try_as_str().flatten()) {
            let normalized = normalize_part(raw);
            let valid = match source_type {
                DataType::Interval(unit) => resolve_interval_field(&normalized, *unit).is_some(),
                _ => resolve_field(&normalized).is_some(),
            };
            if !valid {
                return Err(invalid_extract_field_plan_err(raw));
            }
        }
        if matches!(source_type, DataType::Interval(IntervalUnit::MonthDayNano))
            && !self.allow_month_day_nano_interval
        {
            return Err(unsupported_month_day_nano_plan_err());
        }
        let nullable =
            args.arg_fields.iter().any(|item| item.is_nullable()) || is_string_type(source_type);
        Ok(Arc::new(Field::new(self.name(), DataType::Int32, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs {
            args: call_args,
            arg_fields,
            number_rows,
            return_field,
            config_options,
        } = args;
        let [part_arg, value_arg] = take_function_args(self.name(), call_args)?;
        let ColumnarValue::Scalar(scalar) = &part_arg else {
            return Err(non_foldable_field_exec_err());
        };
        let Some(text) = scalar.try_as_str().flatten() else {
            return Err(DataFusionError::Execution(String::from(
                "First argument of `DATE_PART` must be non-null scalar Utf8",
            )));
        };
        let raw_part = text.to_owned();
        let normalized = normalize_part(&raw_part);
        let value_type = value_arg.data_type();
        let inner_type = unwrap_dictionary_type(&value_type);
        if let DataType::Interval(unit) = inner_type {
            let Some(canonical) = resolve_interval_field(&normalized, *unit) else {
                return Err(invalid_extract_field_exec_err(&raw_part));
            };
            if *unit == IntervalUnit::MonthDayNano && !self.allow_month_day_nano_interval {
                return Err(unsupported_month_day_nano_exec_err());
            }
            return forward_interval_to_stock(
                canonical,
                value_arg,
                &arg_fields,
                number_rows,
                return_field,
                config_options,
            );
        }
        let Some(field) = resolve_field(&normalized) else {
            return Err(invalid_extract_field_exec_err(&raw_part));
        };
        let is_scalar = matches!(&value_arg, ColumnarValue::Scalar(_));
        let is_str = is_string_type(inner_type);
        let array = match value_arg {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(scalar) => scalar.to_array()?,
        };
        let extracted = if is_str {
            extract_from_strings(&array, config_options.as_ref(), field)?
        } else {
            extract_calendar_part(&array, config_options.as_ref(), field.part, field.shift)?
        };
        if is_scalar {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                extracted.as_ref(),
                0,
            )?))
        } else {
            Ok(ColumnarValue::Array(extracted))
        }
    }
}

#[derive(Debug)]
struct SparkExtractDatePart;

impl AnalyzerRule for SparkExtractDatePart {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_extract_date_part"
    }
}

fn is_spark_date_part_source(data_type: &DataType) -> bool {
    matches!(
        unwrap_dictionary_type(data_type),
        DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Interval(_)
            | DataType::Null
    )
}

fn is_non_foldable_node(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_)
        | Expr::OuterReferenceColumn(_, _)
        | Expr::ScalarVariable(_, _)
        | Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_)
        | Expr::Placeholder(_)
        | Expr::AggregateFunction(_)
        | Expr::WindowFunction(_)
        | Expr::GroupingSet(_)
        | Expr::Unnest(_) => true,
        Expr::ScalarFunction(func) => func.func.signature().volatility != Volatility::Immutable,
        _ => false,
    }
}

fn is_foldable_field_expr(expr: &Expr) -> Result<bool> {
    Ok(!expr.exists(|node| Ok(is_non_foldable_node(node)))?)
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| rewrite_expr(node, &schema))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(func) = expr else {
        return Ok(Transformed::no(expr));
    };
    if func.func.name() != "date_part" || func.args.len() != 2 {
        return Ok(Transformed::no(Expr::ScalarFunction(func)));
    }
    if func.func.inner().downcast_ref::<SparkDatePart>().is_some() {
        if !is_foldable_field_expr(&func.args[0])? {
            return Err(non_foldable_field_plan_err());
        }
        return Ok(Transformed::no(Expr::ScalarFunction(func)));
    }
    let Ok(source_type) = func.args[1].get_type(schema) else {
        return Ok(Transformed::no(Expr::ScalarFunction(func)));
    };
    if !is_spark_date_part_source(&source_type) {
        return Ok(Transformed::no(Expr::ScalarFunction(func)));
    }
    if !is_foldable_field_expr(&func.args[0])? {
        return Err(non_foldable_field_plan_err());
    }
    let udf = if matches!(unwrap_dictionary_type(&source_type), DataType::Interval(_)) {
        facade_date_part_udf()
    } else {
        date_part_udf()
    };
    Ok(Transformed::yes(Expr::ScalarFunction(
        ScalarFunction::new_udf(udf, func.args),
    )))
}
