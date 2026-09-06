use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
};

pub(crate) const DECIMAL_CAST_NULLABLE_NAME: &str = "__repark_decimal_cast_nullable__";
pub(crate) const SPARK_NONNULL_NAME: &str = "__repark_spark_nonnull__";

pub(crate) fn nullable_spark_cast(cast: &Cast, schema: &DFSchema) -> Option<Expr> {
    let (_, child) = cast.expr.to_field(schema).ok()?;
    if child.is_nullable() {
        return None;
    }
    if datafusion_nullable(&Expr::Cast(cast.clone()), schema) != Some(false) {
        return None;
    }
    if !spark_cast_nullable_from_nonnull_child(
        cast.expr.as_ref(),
        child.data_type(),
        cast.field.data_type(),
    ) {
        return None;
    }
    let operand = Expr::ScalarFunction(ScalarFunction::new_udf(
        spark_decimal_cast_nullable_udf(),
        vec![cast.expr.as_ref().clone()],
    ));
    Some(Expr::Cast(Cast::new(
        Box::new(operand),
        cast.field.data_type().clone(),
    )))
}

pub(crate) fn nonnull_spark_cast(cast: &Cast, schema: &DFSchema) -> Option<Expr> {
    let (_, child) = cast.expr.to_field(schema).ok()?;
    if child.is_nullable() {
        return None;
    }
    if !matches!(
        (child.data_type(), cast.field.data_type()),
        (DataType::Date32, DataType::Timestamp(_, _))
    ) {
        return None;
    }
    Some(Expr::ScalarFunction(ScalarFunction::new_udf(
        spark_nonnull_udf(),
        vec![Expr::Cast(cast.clone())],
    )))
}

pub(crate) fn datafusion_nullable(expr: &Expr, schema: &DFSchema) -> Option<bool> {
    expr.to_field(schema)
        .ok()
        .map(|(_, field)| field.is_nullable())
}

fn spark_cast_nullable_from_nonnull_child(
    child_expr: &Expr,
    child: &DataType,
    target: &DataType,
) -> bool {
    if let DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) = target
    {
        return decimal_cast_can_overflow(
            child_expr,
            child,
            i32::from(*precision) - i32::from(*scale),
        );
    }
    match child {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            if matches!(target, DataType::Date32 | DataType::Timestamp(_, _))
                && parses_as_target(child_expr, target)
            {
                return false;
            }
            matches!(
                target,
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Boolean
                    | DataType::Date32
                    | DataType::Timestamp(_, _)
            )
        }
        DataType::Float32 | DataType::Float64 => {
            matches!(
                target,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            )
        }
        DataType::Timestamp(_, _) => {
            matches!(target, DataType::Int8 | DataType::Int16 | DataType::Int32)
        }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            matches!(
                target,
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
            )
        }
        _ => false,
    }
}

fn parses_as_target(child_expr: &Expr, target: &DataType) -> bool {
    let Expr::Literal(scalar, _) = child_expr else {
        return false;
    };
    let text = match scalar {
        ScalarValue::Utf8(Some(text))
        | ScalarValue::LargeUtf8(Some(text))
        | ScalarValue::Utf8View(Some(text)) => text.as_str(),
        _ => return false,
    };
    let probe = arrow::array::StringArray::from(vec![text]);
    arrow::compute::cast(&probe, target).is_ok_and(|cast| cast.null_count() == 0)
}

fn decimal_cast_can_overflow(
    child_expr: &Expr,
    child: &DataType,
    target_integer_digits: i32,
) -> bool {
    match child {
        DataType::Int8 => target_integer_digits < 3,
        DataType::Int16 => target_integer_digits < 5,
        DataType::Int32 => target_integer_digits < 10,
        DataType::Int64 => target_integer_digits < effective_int64_digits(child_expr),
        DataType::Decimal128(child_precision, child_scale)
        | DataType::Decimal256(child_precision, child_scale) => {
            target_integer_digits < i32::from(*child_precision) - i32::from(*child_scale)
        }
        DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View => true,
        _ => false,
    }
}

fn effective_int64_digits(child_expr: &Expr) -> i32 {
    if let Expr::Literal(ScalarValue::Int64(Some(value)), _) = child_expr
        && i32::try_from(*value).is_ok()
    {
        10
    } else {
        20
    }
}

#[must_use]
pub fn spark_decimal_cast_nullable_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(DecimalCastNullable::new()))
}

#[must_use]
pub fn spark_nonnull_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkNonnull::new()))
}

#[derive(Debug)]
struct DecimalCastNullable {
    signature: Signature,
}

impl DecimalCastNullable {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for DecimalCastNullable {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for DecimalCastNullable {}

impl Hash for DecimalCastNullable {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for DecimalCastNullable {
    crate::shim_udf_boilerplate!("__repark_decimal_cast_nullable__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "'{DECIMAL_CAST_NULLABLE_NAME}' expects one argument"
            ))
        })
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let first = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan(format!(
                "'{DECIMAL_CAST_NULLABLE_NAME}' expects one argument"
            ))
        })?;
        Ok(Field::new(self.name(), first.data_type().clone(), true).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args.first().cloned().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "'{DECIMAL_CAST_NULLABLE_NAME}' expects one argument"
            ))
        })
    }
}

#[derive(Debug)]
struct SparkNonnull {
    signature: Signature,
}

impl SparkNonnull {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl PartialEq for SparkNonnull {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SparkNonnull {}

impl Hash for SparkNonnull {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for SparkNonnull {
    crate::shim_udf_boilerplate!("__repark_spark_nonnull__");

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            DataFusionError::Plan(format!("'{SPARK_NONNULL_NAME}' expects one argument"))
        })
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let first = args.arg_fields.first().ok_or_else(|| {
            DataFusionError::Plan(format!("'{SPARK_NONNULL_NAME}' expects one argument"))
        })?;
        Ok(Field::new(self.name(), first.data_type().clone(), false).into())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        args.args.first().cloned().ok_or_else(|| {
            DataFusionError::Execution(format!("'{SPARK_NONNULL_NAME}' expects one argument"))
        })
    }
}
