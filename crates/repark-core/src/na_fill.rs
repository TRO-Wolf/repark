#[cfg(test)]
mod tests;

use arrow::datatypes::{DataType as ArrowDataType, Schema};
use datafusion::common::ScalarValue;
use datafusion::functions::expr_fn::coalesce;
use datafusion::logical_expr::{Cast, Expr};

pub struct FillBuild {
    pub expr: Expr,
    pub literal: Expr,
    pub cast_to: Option<ArrowDataType>,
}

fn is_fill_numeric_type(data_type: &ArrowDataType) -> bool {
    matches!(
        data_type,
        ArrowDataType::Int8
            | ArrowDataType::Int16
            | ArrowDataType::Int32
            | ArrowDataType::Int64
            | ArrowDataType::Float32
            | ArrowDataType::Float64
    )
}

fn is_fill_numeric_scalar(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Int8(_)
            | ScalarValue::Int16(_)
            | ScalarValue::Int32(_)
            | ScalarValue::Int64(_)
            | ScalarValue::UInt8(_)
            | ScalarValue::UInt16(_)
            | ScalarValue::UInt32(_)
            | ScalarValue::UInt64(_)
            | ScalarValue::Float32(_)
            | ScalarValue::Float64(_)
    )
}

#[must_use]
pub fn na_fill_expr(
    schema: &Schema,
    bound: Expr,
    field_name: &str,
    fallback_name: &str,
    literal: ScalarValue,
) -> FillBuild {
    let cast_to = schema
        .fields()
        .iter()
        .rev()
        .find(|field| field.name() == field_name || field.name() == fallback_name)
        .map(|field| field.data_type().clone())
        .filter(|data_type| is_fill_numeric_type(data_type) && is_fill_numeric_scalar(&literal));
    let replacement = match &cast_to {
        Some(data_type) => Expr::Cast(Cast::new(
            Box::new(Expr::Literal(literal, None)),
            data_type.clone(),
        )),
        None => Expr::Literal(literal, None),
    };
    FillBuild {
        expr: coalesce(vec![bound, replacement.clone()]),
        literal: replacement,
        cast_to,
    }
}
