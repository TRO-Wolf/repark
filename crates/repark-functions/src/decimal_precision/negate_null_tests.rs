use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::{Cast, Expr, TryCast};

use super::rewrite_negative_null;

fn null_decimal_cast() -> Expr {
    Expr::Negative(Box::new(Expr::Cast(Cast::new(
        Box::new(Expr::Literal(ScalarValue::Null, None)),
        DataType::Decimal128(10, 2),
    ))))
}

fn rewritten(expr: Expr) -> Transformed<Expr> {
    rewrite_negative_null(expr)
}

#[test]
fn negated_null_decimal_cast_folds_to_typed_null() {
    let out = rewritten(null_decimal_cast());
    assert!(out.transformed);
    assert_eq!(
        out.data,
        Expr::Literal(ScalarValue::Decimal128(None, 10, 2), None)
    );
}

#[test]
fn negated_null_decimal_literal_folds() {
    let expr = Expr::Negative(Box::new(Expr::Literal(
        ScalarValue::Decimal128(None, 10, 2),
        None,
    )));
    let out = rewritten(expr);
    assert!(out.transformed);
    assert_eq!(
        out.data,
        Expr::Literal(ScalarValue::Decimal128(None, 10, 2), None)
    );
}

#[test]
fn double_negative_collapses_to_typed_null() {
    let expr = Expr::Negative(Box::new(null_decimal_cast()));
    let out = rewritten(expr);
    assert!(out.transformed);
    assert_eq!(
        out.data,
        Expr::Literal(ScalarValue::Decimal128(None, 10, 2), None)
    );
}

#[test]
fn negated_null_try_cast_folds_to_typed_null() {
    let expr = Expr::Negative(Box::new(Expr::TryCast(TryCast::new(
        Box::new(Expr::Literal(ScalarValue::Null, None)),
        DataType::Decimal128(10, 2),
    ))));
    let out = rewritten(expr);
    assert!(out.transformed);
    assert_eq!(
        out.data,
        Expr::Literal(ScalarValue::Decimal128(None, 10, 2), None)
    );
}

#[test]
fn wide_cast_target_keeps_its_precision_and_scale() {
    let expr = Expr::Negative(Box::new(Expr::Cast(Cast::new(
        Box::new(Expr::Literal(ScalarValue::Null, None)),
        DataType::Decimal128(38, 10),
    ))));
    let out = rewritten(expr);
    assert!(out.transformed);
    assert_eq!(
        out.data,
        Expr::Literal(ScalarValue::Decimal128(None, 38, 10), None)
    );
}

#[test]
fn negated_valued_decimal_is_untouched() {
    let expr = Expr::Negative(Box::new(Expr::Literal(
        ScalarValue::Decimal128(Some(150), 10, 2),
        None,
    )));
    let out = rewritten(expr.clone());
    assert!(!out.transformed);
    assert_eq!(out.data, expr);
}

#[test]
fn negated_null_int_is_untouched() {
    let expr = Expr::Negative(Box::new(Expr::Literal(ScalarValue::Int32(None), None)));
    let out = rewritten(expr.clone());
    assert!(!out.transformed);
    assert_eq!(out.data, expr);
}

#[test]
fn negated_null_string_is_untouched() {
    let expr = Expr::Negative(Box::new(Expr::Literal(ScalarValue::Utf8(None), None)));
    let out = rewritten(expr.clone());
    assert!(!out.transformed);
    assert_eq!(out.data, expr);
}

#[test]
fn negated_non_decimal_cast_is_untouched() {
    let expr = Expr::Negative(Box::new(Expr::Cast(Cast::new(
        Box::new(Expr::Literal(ScalarValue::Null, None)),
        DataType::Int32,
    ))));
    let out = rewritten(expr.clone());
    assert!(!out.transformed);
    assert_eq!(out.data, expr);
}
