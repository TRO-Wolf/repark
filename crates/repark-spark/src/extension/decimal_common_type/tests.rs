use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::logical_expr::{Expr, lit};

use super::{
    Mode, Shape, Target, apply, literal_digits, spark_decimal_common_type, spark_decimal_type,
    target_of, target_of_types,
};

fn decimal(precision: u8, scale: i8) -> DataType {
    DataType::Decimal128(precision, scale)
}

fn common(types: &[DataType], mode: Mode) -> Option<DataType> {
    spark_decimal_common_type(types.iter().map(|found| (found, None)), mode)
}

#[test]
fn the_fourteen_pairs_take_spark_bounded_prefer_integral_digits() {
    for (left, right, expected) in [
        (decimal(38, 0), decimal(38, 38), Some(decimal(38, 0))),
        (decimal(38, 0), decimal(10, 2), Some(decimal(38, 0))),
        (decimal(38, 0), decimal(38, 1), Some(decimal(38, 0))),
        (decimal(38, 38), decimal(10, 2), Some(decimal(38, 30))),
        (decimal(20, 0), decimal(20, 20), Some(decimal(38, 18))),
        (decimal(38, 10), decimal(38, 20), Some(decimal(38, 10))),
        (DataType::Int64, decimal(38, 20), Some(decimal(38, 18))),
        (DataType::Int32, decimal(38, 30), Some(decimal(38, 28))),
        (DataType::Int16, decimal(38, 34), Some(decimal(38, 33))),
        (DataType::Int8, decimal(38, 36), Some(decimal(38, 35))),
        (decimal(10, 0), decimal(10, 2), None),
        (DataType::Int32, decimal(10, 2), None),
        (DataType::Int64, decimal(10, 2), None),
        (decimal(28, 2), decimal(10, 4), None),
    ] {
        for mode in [Mode::Compare, Mode::Unify] {
            assert_eq!(
                common(&[left.clone(), right.clone()], mode),
                expected,
                "{left} {right}"
            );
            assert_eq!(
                common(&[right.clone(), left.clone()], mode),
                expected,
                "{right} {left}"
            );
        }
    }
}

#[test]
fn the_full_type_answers_below_thirty_eight_and_skips_integers_alone() {
    assert_eq!(
        spark_decimal_type(
            [(&DataType::Int32, None), (&decimal(10, 2), None)],
            Mode::Unify
        ),
        Some(decimal(12, 2))
    );
    assert_eq!(
        spark_decimal_type(
            [(&DataType::Int64, None), (&DataType::Int32, None)],
            Mode::Unify
        ),
        None
    );
    assert_eq!(
        common(
            &[DataType::Null, decimal(38, 0), decimal(10, 2)],
            Mode::Unify
        ),
        Some(decimal(38, 0))
    );
    assert_eq!(common(&[DataType::Utf8, decimal(38, 0)], Mode::Unify), None);
    assert_eq!(
        common(&[DataType::Decimal256(40, 2), decimal(38, 0)], Mode::Unify),
        None
    );
}

#[test]
fn an_integer_literal_keeps_its_own_digits_only_in_a_comparison() {
    let one = Some(1);
    assert_eq!(
        spark_decimal_common_type(
            [(&DataType::Int32, one), (&decimal(38, 30), None)],
            Mode::Compare
        ),
        None
    );
    assert_eq!(
        spark_decimal_common_type(
            [(&DataType::Int32, one), (&decimal(38, 30), None)],
            Mode::Unify
        ),
        Some(decimal(38, 28))
    );
    assert_eq!(
        spark_decimal_common_type(
            [(&DataType::Int32, Some(4)), (&decimal(38, 36), None)],
            Mode::Compare
        ),
        Some(decimal(38, 34))
    );
    assert_eq!(literal_digits(&lit(0_i32)), Some(1));
    assert_eq!(literal_digits(&lit(-1234_i64)), Some(4));
    assert_eq!(literal_digits(&lit(i64::MIN)), Some(19));
    assert_eq!(
        literal_digits(&Expr::Literal(ScalarValue::Int32(None), None)),
        None
    );
    assert_eq!(literal_digits(&lit(1.5_f64)), None);
}

#[test]
fn a_float_beside_a_decimal_stays_double_and_the_flag_skips_only_decimals() {
    let types = [decimal(38, 0), DataType::Float64];
    let refs = types.iter().collect::<Vec<_>>();
    assert_eq!(
        target_of_types(&refs, &[], Mode::Unify, false),
        Some(Target::Double)
    );
    let types = [decimal(38, 0), decimal(10, 2)];
    let refs = types.iter().collect::<Vec<_>>();
    assert_eq!(target_of_types(&refs, &[], Mode::Unify, false), None);
    assert_eq!(
        target_of_types(&refs, &[], Mode::Unify, true),
        Some(Target::Spark(Shape::Scalar(decimal(38, 0))))
    );
}

#[test]
fn struct_list_and_map_fields_unify_field_by_field() {
    let left = DataType::Struct(Fields::from(vec![
        Field::new("f", decimal(38, 0), true),
        Field::new("g", decimal(10, 2), true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let right = DataType::Struct(Fields::from(vec![
        Field::new("F", decimal(10, 2), true),
        Field::new("g", decimal(38, 0), true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let Some(Target::Spark(shape)) = target_of_types(&[&left, &right], &[], Mode::Unify, true)
    else {
        panic!("a struct pair past 38 takes a target");
    };
    let DataType::Struct(fields) = apply(&right, &shape) else {
        panic!("a struct stays a struct");
    };
    assert_eq!(fields[0].name(), "F");
    assert_eq!(fields[0].data_type(), &decimal(38, 0));
    assert_eq!(fields[1].data_type(), &decimal(38, 0));
    assert_eq!(fields[2].data_type(), &DataType::Utf8);
    assert_eq!(
        target_of_types(&[&left, &right], &[], Mode::Compare, true),
        None
    );
    let renamed = DataType::Struct(Fields::from(vec![
        Field::new("x", decimal(10, 2), true),
        Field::new("g", decimal(38, 0), true),
        Field::new("s", DataType::Utf8, true),
    ]));
    assert_eq!(
        target_of_types(&[&left, &renamed], &[], Mode::Unify, true),
        None
    );
    let list = |inner: DataType| DataType::List(Arc::new(Field::new("item", inner, true)));
    let Some(Target::Spark(shape)) = target_of_types(
        &[&list(decimal(38, 0)), &list(decimal(10, 2))],
        &[],
        Mode::Unify,
        true,
    ) else {
        panic!("a list pair past 38 takes a target");
    };
    assert_eq!(apply(&list(decimal(10, 2)), &shape), list(decimal(38, 0)));
    let map = |key: DataType| {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("keys", key, false),
                    Field::new("values", DataType::Utf8, true),
                ])),
                false,
            )),
            false,
        )
    };
    let Some(Target::Spark(shape)) = target_of_types(
        &[&map(decimal(38, 0)), &map(decimal(10, 2))],
        &[],
        Mode::Unify,
        true,
    ) else {
        panic!("a map pair past 38 takes a target");
    };
    assert_eq!(apply(&map(decimal(10, 2)), &shape), map(decimal(38, 0)));
}

#[test]
fn target_of_reads_literals_from_the_expressions() {
    let schema = DFSchema::empty();
    let wide = Expr::Literal(ScalarValue::Decimal128(Some(1), 38, 30), None);
    assert_eq!(
        target_of(&[&wide, &lit(1_i32)], &schema, Mode::Compare, true),
        None
    );
    assert_eq!(
        target_of(&[&wide, &lit(1_i32)], &schema, Mode::Unify, true),
        Some(Target::Spark(Shape::Scalar(decimal(38, 28))))
    );
}
