use std::sync::Arc;

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array};
use arrow::compute::DatePart;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::types::{logical_date, logical_string};
use datafusion::common::utils::take_function_args;
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err, internal_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Coercion, ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

use crate::session_time_zone::session_time_zone_from_options;

use super::session_wall::{extract_calendar_part, resolve_instant_in_zone, unpack_dictionary};

#[must_use]
pub fn date_part_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(SparkDatePart::new()))
}

#[must_use]
pub fn extract_date_part_rule() -> Arc<dyn AnalyzerRule + Send + Sync> {
    Arc::new(SparkExtractDatePart)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkDatePart {
    signature: Signature,
    aliases: Vec<String>,
}

impl SparkDatePart {
    fn new() -> Self {
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
                ],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("datepart")],
        }
    }
}

#[derive(Clone, Copy)]
enum FieldKind {
    Calendar(DatePart, i32),
    SubSecond,
    Epoch,
}

fn normalize_part(raw: &str) -> String {
    raw.trim_matches(|ch| ch == '\'' || ch == '"')
        .to_ascii_lowercase()
}

fn is_nanosecond_part(normalized: &str) -> bool {
    matches!(
        normalized,
        "nanosecond" | "nanoseconds" | "ns" | "nsec" | "nsecond" | "nseconds"
    )
}

fn resolve_field(normalized: &str) -> Option<FieldKind> {
    match normalized {
        "year" | "y" | "years" | "yr" | "yrs" => Some(FieldKind::Calendar(DatePart::Year, 0)),
        "yearofweek" | "year_iso" | "isoyear" => Some(FieldKind::Calendar(DatePart::YearISO, 0)),
        "quarter" | "qtr" | "quarters" => Some(FieldKind::Calendar(DatePart::Quarter, 0)),
        "month" | "mon" | "mons" | "months" => Some(FieldKind::Calendar(DatePart::Month, 0)),
        "week" | "w" | "weeks" | "isoweek" => Some(FieldKind::Calendar(DatePart::WeekISO, 0)),
        "day" | "d" | "days" => Some(FieldKind::Calendar(DatePart::Day, 0)),
        "dayofweek" | "dow" => Some(FieldKind::Calendar(DatePart::DayOfWeekSunday0, 1)),
        "dayofweek_iso" | "dow_iso" | "isodow" => {
            Some(FieldKind::Calendar(DatePart::DayOfWeekMonday0, 1))
        }
        "doy" | "dayofyear" => Some(FieldKind::Calendar(DatePart::DayOfYear, 0)),
        "hour" | "h" | "hours" | "hr" | "hrs" => Some(FieldKind::Calendar(DatePart::Hour, 0)),
        "minute" | "m" | "min" | "mins" | "minutes" => {
            Some(FieldKind::Calendar(DatePart::Minute, 0))
        }
        "second" | "s" | "sec" | "secs" | "seconds" => {
            Some(FieldKind::Calendar(DatePart::Second, 0))
        }
        "millisecond" | "milliseconds" | "ms" | "msec" | "msecond" | "mseconds" | "microsecond"
        | "microseconds" | "us" | "usec" | "usecond" | "useconds" | "nanosecond"
        | "nanoseconds" | "ns" | "nsec" | "nsecond" | "nseconds" => Some(FieldKind::SubSecond),
        "epoch" => Some(FieldKind::Epoch),
        _ => None,
    }
}

fn null_array_for_type(data_type: &DataType, len: usize) -> ArrayRef {
    match data_type {
        DataType::Float64 => Arc::new(Float64Array::new_null(len)),
        DataType::Int64 => Arc::new(Int64Array::new_null(len)),
        _ => Arc::new(Int32Array::new_null(len)),
    }
}

fn null_scalar_for_type(data_type: &DataType) -> ScalarValue {
    match data_type {
        DataType::Float64 => ScalarValue::Float64(None),
        DataType::Int64 => ScalarValue::Int64(None),
        _ => ScalarValue::Int32(None),
    }
}

fn forward_to_stock(
    part_arg: ColumnarValue,
    value_arg: ColumnarValue,
    resolve_zone: bool,
    args: ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    let return_type = args.return_field.data_type().clone();
    let zone = session_time_zone_from_options(args.config_options.as_ref());
    let prepared = match value_arg {
        ColumnarValue::Array(array) => {
            let unpacked = unpack_dictionary(&array)?;
            if matches!(unpacked.data_type(), DataType::Null) {
                return Ok(ColumnarValue::Array(null_array_for_type(
                    &return_type,
                    unpacked.len(),
                )));
            }
            let ready = if resolve_zone {
                resolve_instant_in_zone(&unpacked, zone)?
            } else {
                unpacked
            };
            ColumnarValue::Array(ready)
        }
        ColumnarValue::Scalar(scalar) => {
            if matches!(scalar.data_type(), DataType::Null) {
                return Ok(ColumnarValue::Scalar(null_scalar_for_type(&return_type)));
            }
            let array = scalar.to_array()?;
            let unpacked = unpack_dictionary(&array)?;
            let ready = if resolve_zone {
                resolve_instant_in_zone(&unpacked, zone)?
            } else {
                unpacked
            };
            ColumnarValue::Scalar(ScalarValue::try_from_array(ready.as_ref(), 0)?)
        }
    };
    let forwarded = ScalarFunctionArgs {
        args: vec![part_arg, prepared],
        arg_fields: args.arg_fields,
        number_rows: args.number_rows,
        return_field: args.return_field,
        config_options: args.config_options,
    };
    datafusion::functions::datetime::date_part().invoke_with_args(forwarded)
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
        let [field, _] = take_function_args(self.name(), args.scalar_arguments)?;
        let nullable = args.arg_fields.iter().any(|item| item.is_nullable());
        let data_type = match field.and_then(|scalar| scalar.try_as_str().flatten()) {
            Some(raw) => {
                let normalized = normalize_part(raw);
                if normalized == "epoch" {
                    DataType::Float64
                } else if is_nanosecond_part(&normalized) {
                    DataType::Int64
                } else {
                    DataType::Int32
                }
            }
            None => DataType::Int32,
        };
        Ok(Arc::new(Field::new(self.name(), data_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut call_args = args.args;
        let [part_arg, value_arg] =
            take_function_args(self.name(), std::mem::take(&mut call_args))?;
        let raw_part = match &part_arg {
            ColumnarValue::Scalar(scalar) => match scalar.try_as_str().flatten() {
                Some(text) => text.to_owned(),
                None => {
                    return exec_err!("First argument of `DATE_PART` must be non-null scalar Utf8");
                }
            },
            ColumnarValue::Array(_) => {
                return exec_err!("First argument of `DATE_PART` must be non-null scalar Utf8");
            }
        };
        let normalized = normalize_part(&raw_part);
        let Some(kind) = resolve_field(&normalized) else {
            return exec_err!("Date part '{raw_part}' not supported");
        };
        match kind {
            FieldKind::Calendar(part, shift) => {
                let is_scalar = matches!(&value_arg, ColumnarValue::Scalar(_));
                let array = match value_arg {
                    ColumnarValue::Array(array) => array,
                    ColumnarValue::Scalar(scalar) => scalar.to_array()?,
                };
                let extracted =
                    extract_calendar_part(&array, args.config_options.as_ref(), part, shift)?;
                if is_scalar {
                    Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                        extracted.as_ref(),
                        0,
                    )?))
                } else {
                    Ok(ColumnarValue::Array(extracted))
                }
            }
            FieldKind::SubSecond => {
                let rest = ScalarFunctionArgs {
                    args: Vec::new(),
                    arg_fields: args.arg_fields,
                    number_rows: args.number_rows,
                    return_field: args.return_field,
                    config_options: args.config_options,
                };
                forward_to_stock(part_arg, value_arg, true, rest)
            }
            FieldKind::Epoch => {
                let rest = ScalarFunctionArgs {
                    args: Vec::new(),
                    arg_fields: args.arg_fields,
                    number_rows: args.number_rows,
                    return_field: args.return_field,
                    config_options: args.config_options,
                };
                forward_to_stock(part_arg, value_arg, false, rest)
            }
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
    let inner = match data_type {
        DataType::Dictionary(_, value) => value.as_ref(),
        other => other,
    };
    matches!(
        inner,
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 | DataType::Null
    )
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_up(|node| Ok(rewrite_expr(node, &schema)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::ScalarFunction(func) = expr else {
        return Transformed::no(expr);
    };
    let should_rewrite = func.func.name() == "date_part"
        && func.args.len() == 2
        && !func.func.inner().as_any().is::<SparkDatePart>()
        && func.args[1]
            .get_type(schema)
            .is_ok_and(|data_type| is_spark_date_part_source(&data_type));
    if !should_rewrite {
        return Transformed::no(Expr::ScalarFunction(func));
    }
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        date_part_udf(),
        func.args,
    )))
}
