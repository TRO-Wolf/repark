use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::functions::expr_fn::coalesce;
use datafusion::logical_expr::{Cast, Expr};
use datafusion::prelude::col;

use super::na_fill_expr;

fn width_schema() -> Schema {
    Schema::new(vec![
        Field::new("b", DataType::Int8, true),
        Field::new("s", DataType::Int16, true),
        Field::new("i", DataType::Int32, true),
        Field::new("l", DataType::Int64, true),
        Field::new("u", DataType::UInt8, true),
        Field::new("f", DataType::Float32, true),
        Field::new("d", DataType::Float64, true),
        Field::new("t", DataType::Utf8, true),
        Field::new("e", DataType::Boolean, true),
        Field::new("m", DataType::Decimal128(10, 2), true),
    ])
}

fn cast_lit(value: ScalarValue, data_type: DataType) -> Expr {
    Expr::Cast(Cast::new(Box::new(Expr::Literal(value, None)), data_type))
}

#[test]
fn every_numeric_width_casts_to_its_own_type() {
    let schema = width_schema();
    for (name, data_type) in [
        ("b", DataType::Int8),
        ("s", DataType::Int16),
        ("i", DataType::Int32),
        ("l", DataType::Int64),
        ("f", DataType::Float32),
        ("d", DataType::Float64),
    ] {
        let literal = ScalarValue::Int32(Some(0));
        let build = na_fill_expr(&schema, col(name), name, name, literal.clone());
        let want = cast_lit(literal, data_type.clone());
        assert_eq!(build.cast_to, Some(data_type));
        assert_eq!(build.literal, want);
        assert_eq!(build.expr, coalesce(vec![col(name), want]));
    }
}

#[test]
fn float_value_into_int_column_casts_to_int() {
    let schema = width_schema();
    let literal = ScalarValue::Float64(Some(1.5));
    let build = na_fill_expr(&schema, col("i"), "i", "i", literal.clone());
    let want = cast_lit(literal, DataType::Int32);
    assert_eq!(build.cast_to, Some(DataType::Int32));
    assert_eq!(build.literal, want);
    assert_eq!(build.expr, coalesce(vec![col("i"), want]));
}

#[test]
fn int_value_into_float_column_casts_to_float() {
    let schema = width_schema();
    let literal = ScalarValue::Int32(Some(0));
    let build = na_fill_expr(&schema, col("f"), "f", "f", literal.clone());
    let want = cast_lit(literal, DataType::Float32);
    assert_eq!(build.cast_to, Some(DataType::Float32));
    assert_eq!(build.expr, coalesce(vec![col("f"), want]));
}

#[test]
fn string_column_is_untouched_by_a_numeric_value() {
    let schema = width_schema();
    let literal = ScalarValue::Int32(Some(0));
    let build = na_fill_expr(&schema, col("t"), "t", "t", literal.clone());
    assert_eq!(build.cast_to, None);
    assert_eq!(
        build.expr,
        coalesce(vec![col("t"), Expr::Literal(literal, None)])
    );
}

#[test]
fn bool_and_decimal_columns_are_untouched() {
    let schema = width_schema();
    for (name, literal) in [
        ("e", ScalarValue::Boolean(Some(true))),
        ("m", ScalarValue::Int32(Some(0))),
        ("m", ScalarValue::Float64(Some(2.5))),
        ("t", ScalarValue::Utf8(Some("z".to_string()))),
    ] {
        let build = na_fill_expr(&schema, col(name), name, name, literal.clone());
        assert_eq!(build.cast_to, None);
        assert_eq!(
            build.expr,
            coalesce(vec![col(name), Expr::Literal(literal, None)])
        );
    }
}

#[test]
fn uint_column_keeps_coercion_like_the_old_int_cast() {
    let schema = width_schema();
    let literal = ScalarValue::Int32(Some(0));
    let build = na_fill_expr(&schema, col("u"), "u", "u", literal.clone());
    assert_eq!(build.cast_to, None);
    assert_eq!(
        build.expr,
        coalesce(vec![col("u"), Expr::Literal(literal, None)])
    );
}

#[test]
fn missing_field_leaves_the_literal_uncast() {
    let schema = width_schema();
    let literal = ScalarValue::Int32(Some(0));
    let build = na_fill_expr(&schema, col("gone"), "gone", "gone", literal.clone());
    assert_eq!(build.cast_to, None);
    assert_eq!(
        build.expr,
        coalesce(vec![col("gone"), Expr::Literal(literal, None)])
    );
}

#[test]
fn fallback_name_resolves_when_the_engine_name_misses() {
    let schema = width_schema();
    let literal = ScalarValue::Int32(Some(0));
    let build = na_fill_expr(&schema, col("s"), "stale", "s", literal.clone());
    let want = cast_lit(literal, DataType::Int16);
    assert_eq!(build.cast_to, Some(DataType::Int16));
    assert_eq!(build.expr, coalesce(vec![col("s"), want]));
}
