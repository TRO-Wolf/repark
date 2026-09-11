use std::sync::Arc;

use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::{InList, Like};
use datafusion::logical_expr::{Expr, Operator};
use datafusion::prelude::col;
#[allow(clippy::wildcard_imports)]
use datafusion_proto::bytes::*;
use iceberg::arrow::UTC_TIME_ZONE;
use iceberg::expr::{Predicate, PredicateOperator, Reference};
use iceberg::spec::{Datum, PrimitiveLiteral, PrimitiveType};
use repark_core::{Error, Result};

const MAX_PREDICATE_CONVERT_DEPTH: usize = 64;
const ICEBERG_TABLE_SCAN: &str = "IcebergTableScan";

#[allow(clippy::missing_errors_doc)]
pub fn predicate_to_expr(predicate: &Predicate) -> Result<Expr> {
    predicate_to_expr_at(predicate, 0)
}

#[allow(clippy::missing_errors_doc)]
pub fn encode_expr(expr: &Expr) -> Result<Vec<u8>> {
    expr.to_bytes()
        .map(|bytes| bytes.to_vec())
        .map_err(|error| {
            codec_err(format!(
                "{ICEBERG_TABLE_SCAN} predicate Expr could not be serialized: {error}"
            ))
        })
}

#[allow(clippy::missing_errors_doc)]
pub fn decode_expr(bytes: &[u8]) -> Result<Expr> {
    Expr::from_bytes(bytes).map_err(|error| {
        codec_err(format!(
            "{ICEBERG_TABLE_SCAN} predicate Expr could not be deserialized: {error}"
        ))
    })
}

fn codec_err(message: String) -> Error {
    Error::DataFusion(message)
}

fn refuse_shape(shape: &str) -> Result<Expr> {
    Err(codec_err(format!(
        "{ICEBERG_TABLE_SCAN} predicate shape {shape} cannot be expressed as a DataFusion Expr"
    )))
}

fn predicate_to_expr_at(predicate: &Predicate, depth: usize) -> Result<Expr> {
    if depth > MAX_PREDICATE_CONVERT_DEPTH {
        return Err(codec_err(format!(
            "{ICEBERG_TABLE_SCAN} predicate shape nested deeper than {MAX_PREDICATE_CONVERT_DEPTH}"
        )));
    }
    match predicate {
        Predicate::AlwaysTrue => refuse_shape("AlwaysTrue"),
        Predicate::AlwaysFalse => refuse_shape("AlwaysFalse"),
        Predicate::And(expression) => {
            let [left, right] = expression.inputs();
            Ok(predicate_to_expr_at(left, depth + 1)?.and(predicate_to_expr_at(right, depth + 1)?))
        }
        Predicate::Or(expression) => {
            let [left, right] = expression.inputs();
            Ok(predicate_to_expr_at(left, depth + 1)?.or(predicate_to_expr_at(right, depth + 1)?))
        }
        Predicate::Not(expression) => {
            let [inner] = expression.inputs();
            Ok(Expr::Not(Box::new(predicate_to_expr_at(inner, depth + 1)?)))
        }
        Predicate::Unary(expression) => unary_to_expr(expression.op(), expression.term()),
        Predicate::Binary(expression) => {
            binary_to_expr(expression.op(), expression.term(), expression.literal())
        }
        Predicate::Set(expression) => set_to_expr(
            expression.op(),
            expression.term(),
            expression.literals().iter(),
        ),
    }
}

fn reference_to_expr(term: &Reference) -> Expr {
    col(term.name())
}

fn unary_to_expr(operator: PredicateOperator, term: &Reference) -> Result<Expr> {
    let column = reference_to_expr(term);
    match operator {
        PredicateOperator::IsNull => Ok(column.is_null()),
        PredicateOperator::NotNull => Ok(column.is_not_null()),
        PredicateOperator::IsNan => Ok(datafusion::functions::math::expr_fn::isnan(column)),
        PredicateOperator::NotNan => Ok(Expr::Not(Box::new(
            datafusion::functions::math::expr_fn::isnan(column),
        ))),
        other => refuse_shape(&format!("unary {other}")),
    }
}

fn binary_to_expr(operator: PredicateOperator, term: &Reference, literal: &Datum) -> Result<Expr> {
    let column = reference_to_expr(term);
    let value = Expr::Literal(datum_to_scalar(literal)?, None);
    match operator {
        PredicateOperator::LessThan => Ok(Expr::BinaryExpr(
            datafusion::logical_expr::expr::BinaryExpr::new(
                Box::new(column),
                Operator::Lt,
                Box::new(value),
            ),
        )),
        PredicateOperator::LessThanOrEq => Ok(Expr::BinaryExpr(
            datafusion::logical_expr::expr::BinaryExpr::new(
                Box::new(column),
                Operator::LtEq,
                Box::new(value),
            ),
        )),
        PredicateOperator::GreaterThan => Ok(Expr::BinaryExpr(
            datafusion::logical_expr::expr::BinaryExpr::new(
                Box::new(column),
                Operator::Gt,
                Box::new(value),
            ),
        )),
        PredicateOperator::GreaterThanOrEq => Ok(Expr::BinaryExpr(
            datafusion::logical_expr::expr::BinaryExpr::new(
                Box::new(column),
                Operator::GtEq,
                Box::new(value),
            ),
        )),
        PredicateOperator::Eq => Ok(Expr::BinaryExpr(
            datafusion::logical_expr::expr::BinaryExpr::new(
                Box::new(column),
                Operator::Eq,
                Box::new(value),
            ),
        )),
        PredicateOperator::NotEq => Ok(Expr::BinaryExpr(
            datafusion::logical_expr::expr::BinaryExpr::new(
                Box::new(column),
                Operator::NotEq,
                Box::new(value),
            ),
        )),
        PredicateOperator::StartsWith => starts_with_expr(column, literal, false),
        PredicateOperator::NotStartsWith => starts_with_expr(column, literal, true),
        other => refuse_shape(&format!("binary {other}")),
    }
}

fn starts_with_expr(column: Expr, literal: &Datum, negated: bool) -> Result<Expr> {
    let prefix = match (literal.data_type(), literal.literal()) {
        (PrimitiveType::String, PrimitiveLiteral::String(text)) => text.clone(),
        _ => {
            return refuse_shape("StartsWith with a non-string datum");
        }
    };
    if prefix.contains('%') || prefix.contains('_') || prefix.contains('\\') {
        return refuse_shape("StartsWith whose prefix carries LIKE wildcards or the LIKE escape");
    }
    let pattern = format!("{prefix}%");
    Ok(Expr::Like(Like::new(
        negated,
        Box::new(column),
        Box::new(Expr::Literal(ScalarValue::Utf8(Some(pattern)), None)),
        None,
        false,
    )))
}

fn set_to_expr<'datum>(
    operator: PredicateOperator,
    term: &Reference,
    literals: impl IntoIterator<Item = &'datum Datum>,
) -> Result<Expr> {
    let negated = match operator {
        PredicateOperator::In => false,
        PredicateOperator::NotIn => true,
        other => return refuse_shape(&format!("set {other}")),
    };
    let mut items: Vec<&Datum> = literals.into_iter().collect();
    items.sort_by_key(|datum| format!("{datum:?}"));
    let mut list = Vec::with_capacity(items.len());
    for datum in items {
        list.push(Expr::Literal(datum_to_scalar(datum)?, None));
    }
    Ok(Expr::InList(InList::new(
        Box::new(reference_to_expr(term)),
        list,
        negated,
    )))
}

fn datum_to_scalar(datum: &Datum) -> Result<ScalarValue> {
    match (datum.data_type(), datum.literal()) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(value)) => {
            Ok(ScalarValue::Boolean(Some(*value)))
        }
        (PrimitiveType::Int, PrimitiveLiteral::Int(value)) => Ok(ScalarValue::Int32(Some(*value))),
        (PrimitiveType::Long, PrimitiveLiteral::Long(value)) => {
            Ok(ScalarValue::Int64(Some(*value)))
        }
        (PrimitiveType::Float, PrimitiveLiteral::Float(value)) => {
            Ok(ScalarValue::Float32(Some(value.0)))
        }
        (PrimitiveType::Double, PrimitiveLiteral::Double(value)) => {
            Ok(ScalarValue::Float64(Some(value.0)))
        }
        (PrimitiveType::Date, PrimitiveLiteral::Int(value)) => {
            Ok(ScalarValue::Date32(Some(*value)))
        }
        (PrimitiveType::Time, PrimitiveLiteral::Long(value)) => {
            Ok(ScalarValue::Time64Microsecond(Some(*value)))
        }
        (PrimitiveType::Timestamp, PrimitiveLiteral::Long(value)) => {
            Ok(ScalarValue::TimestampMicrosecond(Some(*value), None))
        }
        (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(value)) => Ok(
            ScalarValue::TimestampMicrosecond(Some(*value), Some(Arc::<str>::from(UTC_TIME_ZONE))),
        ),
        (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(value)) => {
            Ok(ScalarValue::TimestampNanosecond(Some(*value), None))
        }
        (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(value)) => Ok(
            ScalarValue::TimestampNanosecond(Some(*value), Some(Arc::<str>::from(UTC_TIME_ZONE))),
        ),
        (PrimitiveType::String, PrimitiveLiteral::String(value)) => {
            Ok(ScalarValue::Utf8(Some(value.clone())))
        }
        (PrimitiveType::Uuid, PrimitiveLiteral::UInt128(value)) => Ok(
            ScalarValue::FixedSizeBinary(16, Some(value.to_be_bytes().to_vec())),
        ),
        (PrimitiveType::Binary, PrimitiveLiteral::Binary(value)) => {
            Ok(ScalarValue::Binary(Some(value.clone())))
        }
        (PrimitiveType::Fixed(width), PrimitiveLiteral::Binary(value)) => {
            let width_i32 = i32::try_from(*width).map_err(|_| {
                codec_err(format!(
                    "{ICEBERG_TABLE_SCAN} predicate shape Fixed({width}) does not fit i32"
                ))
            })?;
            let width_usize = usize::try_from(*width).map_err(|_| {
                codec_err(format!(
                    "{ICEBERG_TABLE_SCAN} predicate shape Fixed({width}) does not fit usize"
                ))
            })?;
            if value.len() != width_usize {
                return Err(codec_err(format!(
                    "{ICEBERG_TABLE_SCAN} predicate shape Fixed({width}) datum length {} does not \
                     match",
                    value.len()
                )));
            }
            Ok(ScalarValue::FixedSizeBinary(width_i32, Some(value.clone())))
        }
        (PrimitiveType::Decimal { precision, scale }, PrimitiveLiteral::Int128(value)) => {
            let precision_u8 = u8::try_from(*precision).map_err(|_| {
                codec_err(format!(
                    "{ICEBERG_TABLE_SCAN} predicate shape Decimal precision {precision} does not \
                     fit u8"
                ))
            })?;
            let scale_i8 = i8::try_from(*scale).map_err(|_| {
                codec_err(format!(
                    "{ICEBERG_TABLE_SCAN} predicate shape Decimal scale {scale} does not fit i8"
                ))
            })?;
            Ok(ScalarValue::Decimal128(
                Some(*value),
                precision_u8,
                scale_i8,
            ))
        }
        (_, PrimitiveLiteral::AboveMax) => refuse_datum("AboveMax"),
        (_, PrimitiveLiteral::BelowMin) => refuse_datum("BelowMin"),
        (PrimitiveType::Unknown, _) => refuse_datum("Unknown"),
        (data_type, literal) => Err(codec_err(format!(
            "{ICEBERG_TABLE_SCAN} predicate shape datum {data_type:?} / {literal:?} cannot be \
             expressed as a ScalarValue"
        ))),
    }
}

fn refuse_datum(shape: &str) -> Result<ScalarValue> {
    Err(codec_err(format!(
        "{ICEBERG_TABLE_SCAN} predicate shape {shape} cannot be expressed as a ScalarValue"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::expr::Predicate;

    #[test]
    fn always_true_predicate_shape_refuses_loud() {
        let error = match predicate_to_expr(&Predicate::AlwaysTrue) {
            Ok(expr) => panic!("AlwaysTrue must refuse, got {expr}"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("AlwaysTrue") && error.contains("IcebergTableScan"),
            "refusal must name the AlwaysTrue shape, got {error}"
        );
    }

    #[test]
    fn always_false_predicate_shape_refuses_loud() {
        let error = match predicate_to_expr(&Predicate::AlwaysFalse) {
            Ok(expr) => panic!("AlwaysFalse must refuse, got {expr}"),
            Err(error) => error.to_string(),
        };
        assert!(
            error.contains("AlwaysFalse") && error.contains("IcebergTableScan"),
            "refusal must name the AlwaysFalse shape, got {error}"
        );
    }

    #[test]
    fn timestamptz_datum_converts_to_utc_timestamp_microsecond() {
        let predicate = iceberg::expr::Reference::new("ts").equal_to(Datum::timestamptz_micros(0));
        let expr = match predicate_to_expr(&predicate) {
            Ok(expr) => expr,
            Err(error) => panic!("timestamptz Eq must convert, got {error}"),
        };
        let Expr::BinaryExpr(binary) = expr else {
            panic!("timestamptz Eq must be a binary expr, got {expr}");
        };
        let Expr::Literal(scalar, _) = binary.right.as_ref() else {
            panic!(
                "timestamptz Eq right side must be a literal, got {}",
                binary.right
            );
        };
        match scalar {
            ScalarValue::TimestampMicrosecond(Some(0), Some(zone)) => {
                assert!(
                    zone.as_ref() == "UTC",
                    "timestamptz zone must be UTC, got {zone}"
                );
            }
            other => panic!(
                "timestamptz datum must become TimestampMicrosecond(Some(0), Some(\"UTC\")), got {other:?}"
            ),
        }
    }

    fn starts_with_refusal(prefix: &str) -> String {
        let predicate = iceberg::expr::Reference::new("name").starts_with(Datum::string(prefix));
        match predicate_to_expr(&predicate) {
            Ok(expr) => panic!("StartsWith prefix {prefix:?} must refuse, got {expr}"),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn starts_with_percent_prefix_refuses_loud() {
        let error = starts_with_refusal("a%b");
        assert!(
            error.contains("StartsWith") && error.contains("IcebergTableScan"),
            "percent prefix refusal must name StartsWith, got {error}"
        );
    }

    #[test]
    fn starts_with_underscore_prefix_refuses_loud() {
        let error = starts_with_refusal("a_b");
        assert!(
            error.contains("StartsWith") && error.contains("IcebergTableScan"),
            "underscore prefix refusal must name StartsWith, got {error}"
        );
    }

    #[test]
    fn starts_with_backslash_prefix_refuses_loud() {
        let error = starts_with_refusal("a\\b");
        assert!(
            error.contains("StartsWith") && error.contains("IcebergTableScan"),
            "backslash prefix refusal must name StartsWith, got {error}"
        );
    }
}
