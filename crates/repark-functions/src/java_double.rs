use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, AsArray, Float32Array, Float64Array, StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Float32Type, Float64Type};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, DataFusionError, Result, exec_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;

pub(crate) fn java_double_text(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        };
    }
    let bits = value.to_bits();
    if bits == 1 || bits == 0x8000_0000_0000_0001 {
        return if value.is_sign_negative() {
            "-4.9E-324".to_string()
        } else {
            "4.9E-324".to_string()
        };
    }
    java_decimal_text(&format!("{value:e}"))
}

pub(crate) fn java_float_text(value: f32) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-Infinity".to_string()
        } else {
            "Infinity".to_string()
        };
    }
    java_decimal_text(&format!("{value:e}"))
}

fn java_decimal_text(shortest: &str) -> String {
    let (sign, body) = match shortest.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", shortest),
    };
    let Some((mantissa, exponent_text)) = body.split_once('e') else {
        return shortest.to_string();
    };
    let Ok(exponent) = exponent_text.parse::<i32>() else {
        return shortest.to_string();
    };
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    if (-3..=6).contains(&exponent) {
        format!("{sign}{}", plain_decimal(&digits, exponent))
    } else {
        let head = &digits[..1];
        let tail = if digits.len() > 1 { &digits[1..] } else { "0" };
        format!("{sign}{head}.{tail}E{exponent}")
    }
}

fn plain_decimal(digits: &str, exponent: i32) -> String {
    if exponent < 0 {
        let zeros = "0".repeat(usize::try_from(-exponent - 1).unwrap_or(0));
        return format!("0.{zeros}{digits}");
    }
    let point = usize::try_from(exponent + 1).unwrap_or(0);
    if point >= digits.len() {
        let zeros = "0".repeat(point - digits.len());
        format!("{digits}{zeros}.0")
    } else {
        format!("{}.{}", &digits[..point], &digits[point..])
    }
}

pub(crate) fn java_double_strings(values: &Float64Array) -> StringArray {
    let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 8);
    for index in 0..values.len() {
        if values.is_null(index) {
            builder.append_null();
        } else {
            builder.append_value(java_double_text(values.value(index)));
        }
    }
    builder.finish()
}

pub(crate) fn java_float_strings(values: &Float32Array) -> StringArray {
    let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 8);
    for index in 0..values.len() {
        if values.is_null(index) {
            builder.append_null();
        } else {
            builder.append_value(java_float_text(values.value(index)));
        }
    }
    builder.finish()
}

pub(crate) fn spark_float_to_string_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkFloatToString::new()))
}

#[derive(Debug)]
struct SparkFloatToString {
    signature: Signature,
}

impl SparkFloatToString {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkFloatToString {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkFloatToString {}

impl Hash for SparkFloatToString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkFloatToString {
    crate::shim_udf_boilerplate!("__repark_float_to_string__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(DataType::Float32 | DataType::Float64) => Ok(DataType::Utf8),
            _ => Err(DataFusionError::Plan(format!(
                "'{}' expects a FLOAT or DOUBLE argument",
                self.name()
            ))),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        let nullable = args
            .arg_fields
            .first()
            .is_none_or(|field| field.is_nullable());
        Ok(Arc::new(Field::new(self.name(), DataType::Utf8, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let Some(arg) = arrays.into_iter().next() else {
            return exec_err!("'{}' expects 1 argument", self.name());
        };
        match arg.data_type() {
            DataType::Float64 => {
                let values = arg.as_primitive::<Float64Type>();
                Ok(ColumnarValue::Array(Arc::new(java_double_strings(values))))
            }
            DataType::Float32 => {
                let values = arg.as_primitive::<Float32Type>();
                Ok(ColumnarValue::Array(Arc::new(java_float_strings(values))))
            }
            other => Err(DataFusionError::Plan(format!(
                "'{}' expects a FLOAT or DOUBLE argument, got {other}",
                self.name()
            ))),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct SparkFloatToStringCast;

impl AnalyzerRule for SparkFloatToStringCast {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_float_cast_plan)
            .data()
    }

    fn name(&self) -> &'static str {
        "spark_float_to_string_cast"
    }
}

fn rewrite_float_cast_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten =
            expr.transform_up(|node| Ok(rewrite_float_to_string_cast(node, &schema)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_float_to_string_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    let Expr::Cast(cast) = expr else {
        return Transformed::no(expr);
    };
    let Ok(source_type) = cast.expr.get_type(schema) else {
        return Transformed::no(Expr::Cast(cast));
    };
    if !matches!(source_type, DataType::Float32 | DataType::Float64) {
        return Transformed::no(Expr::Cast(cast));
    }
    if !matches!(
        cast.field.data_type(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    ) {
        return Transformed::no(Expr::Cast(cast));
    }
    Transformed::yes(Expr::ScalarFunction(ScalarFunction::new_udf(
        spark_float_to_string_udf(),
        vec![*cast.expr],
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn double_text_matches_java_thresholds() {
        assert_eq!(java_double_text(f64::INFINITY), "Infinity");
        assert_eq!(java_double_text(f64::NEG_INFINITY), "-Infinity");
        assert_eq!(java_double_text(f64::NAN), "NaN");
        assert_eq!(java_double_text(10_000_000.0), "1.0E7");
        assert_eq!(java_double_text(1_000_000.0), "1000000.0");
        assert_eq!(java_double_text(123_456_789.0), "1.23456789E8");
        assert_eq!(java_double_text(0.0001), "1.0E-4");
        assert_eq!(java_double_text(0.001), "0.001");
        assert_eq!(java_double_text(1.0e21), "1.0E21");
        assert_eq!(java_double_text(-0.0), "-0.0");
        assert_eq!(java_double_text(0.0), "0.0");
        assert_eq!(java_double_text(1.0), "1.0");
        assert_eq!(java_double_text(f64::from_bits(1)), "4.9E-324");
        assert_eq!(java_double_text(-f64::from_bits(1)), "-4.9E-324");
    }

    #[test]
    fn float_text_matches_java_thresholds() {
        assert_eq!(java_float_text(f32::INFINITY), "Infinity");
        assert_eq!(java_float_text(f32::NAN), "NaN");
        assert_eq!(java_float_text(10_000_000_000.0), "1.0E10");
        assert_eq!(java_float_text(0.1), "0.1");
        assert_eq!(java_float_text(123_456.7), "123456.7");
        assert_eq!(java_float_text(0.00001), "1.0E-5");
    }

    #[test]
    fn string_arrays_preserve_nulls() {
        let doubles: Float64Array = vec![Some(10_000_000.0), None, Some(1.0)].into();
        let rendered = java_double_strings(&doubles);
        assert_eq!(rendered.len(), 3);
        assert!(rendered.is_null(1));
        assert_eq!(rendered.value(0), "1.0E7");
        assert_eq!(rendered.value(2), "1.0");
        let floats: Float32Array = vec![None, Some(0.1)].into();
        let rendered = java_float_strings(&floats);
        assert_eq!(rendered.len(), 2);
        assert!(rendered.is_null(0));
        assert_eq!(rendered.value(1), "0.1");
    }
}
