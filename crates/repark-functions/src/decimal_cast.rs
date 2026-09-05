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

pub(crate) fn nullable_decimal_cast(cast: &Cast, schema: &DFSchema) -> Option<Expr> {
    let (precision, scale) = match cast.field.data_type() {
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            (i32::from(*precision), i32::from(*scale))
        }
        _ => return None,
    };
    let (_, child) = cast.expr.to_field(schema).ok()?;
    if child.is_nullable() {
        return None;
    }
    if !decimal_cast_can_overflow(cast.expr.as_ref(), child.data_type(), precision - scale) {
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
