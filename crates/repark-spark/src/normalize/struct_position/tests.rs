use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, ListArray, StructArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
use datafusion::common::DFSchema;
use datafusion::logical_expr::{BinaryExpr, Expr, ExprSchemable, col, lit};

use super::udf::renamed_array;
use super::{
    PLANNED_BY_POSITION, Pairing, Seat, duplicate_field_operands_by_position, has_duplicate_names,
    pairing, positional_type, renamed_type,
};

const KINDS: [&str; 6] = ["list", "large", "fsl1", "fsl2", "view", "largeview"];

fn fields(names: [&str; 2], types: [DataType; 2]) -> DataType {
    let [first, second] = types;
    DataType::Struct(Fields::from(vec![
        Field::new(names[0], first, true),
        Field::new(names[1], second, true),
    ]))
}

fn ab() -> DataType {
    fields(["a", "b"], [DataType::Int32, DataType::Int32])
}

fn ba() -> DataType {
    fields(["b", "a"], [DataType::Int32, DataType::Int32])
}

fn list_of(kind: &str, element: DataType) -> DataType {
    let field = Arc::new(Field::new("item", element, true));
    match kind {
        "list" => DataType::List(field),
        "large" => DataType::LargeList(field),
        "fsl1" => DataType::FixedSizeList(field, 1),
        "fsl2" => DataType::FixedSizeList(field, 2),
        "view" => DataType::ListView(field),
        _ => DataType::LargeListView(field),
    }
}

fn dictionary_of(inner: DataType) -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(inner))
}

fn run_end_encoded(inner: DataType) -> DataType {
    DataType::RunEndEncoded(
        Arc::new(Field::new("run_ends", DataType::Int32, false)),
        Arc::new(Field::new("values", inner, true)),
    )
}

#[test]
fn renamed_type_keeps_the_current_kind_for_every_list_kind() {
    for target_kind in KINDS {
        for current_kind in KINDS {
            let renamed = renamed_type(&list_of(target_kind, ab()), &list_of(current_kind, ba()));
            assert_eq!(
                renamed,
                Some(list_of(current_kind, ab())),
                "{target_kind} target, {current_kind} operand"
            );
        }
    }
}

#[test]
fn renamed_type_carries_through_dictionary_and_run_end_encoded_values() {
    let encoded = [
        (
            list_of("list", ab()),
            dictionary_of(list_of("large", ba())),
            dictionary_of(list_of("large", ab())),
        ),
        (
            list_of("list", ab()),
            run_end_encoded(list_of("view", ba())),
            run_end_encoded(list_of("view", ab())),
        ),
        (
            ab(),
            dictionary_of(run_end_encoded(ba())),
            dictionary_of(run_end_encoded(ab())),
        ),
    ];
    for (target, current, expected) in encoded {
        assert_eq!(renamed_type(&target, &current), Some(expected.clone()));
        assert_eq!(
            renamed_type(&dictionary_of(target.clone()), &current),
            Some(expected)
        );
    }
}

#[test]
fn pairing_reads_kinds_and_encodings_as_their_plain_list_twin() {
    for left_kind in KINDS {
        for right_kind in KINDS {
            let left = list_of(left_kind, ab());
            let right = run_end_encoded(list_of(right_kind, ba()));
            let sizes_differ =
                matches!((left_kind, right_kind), ("fsl1", "fsl2") | ("fsl2", "fsl1"));
            let expected = if sizes_differ {
                Pairing::Clash
            } else {
                Pairing::Same
            };
            assert!(
                pairing(&left, &right, Seat::Comparison, false) == expected,
                "{left_kind} against {right_kind}"
            );
        }
    }
    let widened = fields(["b", "a"], [DataType::Int64, DataType::Int64]);
    assert!(pairing(&ab(), &widened, Seat::Comparison, false) == Pairing::Clash);
    let same_order = fields(["a", "b"], [DataType::Int64, DataType::Int64]);
    assert!(pairing(&ab(), &same_order, Seat::Comparison, false) == Pairing::Widen);
    let union = DataType::Union(
        datafusion::arrow::datatypes::UnionFields::try_new(
            vec![0],
            vec![Arc::new(Field::new("s", ab(), true))],
        )
        .expect("one union type id"),
        datafusion::arrow::datatypes::UnionMode::Dense,
    );
    assert!(pairing(&ab(), &union, Seat::Comparison, false) == Pairing::Clash);
}

#[test]
fn positional_names_reach_every_kind_and_detect_duplicates() {
    let duplicated = fields(["a", "a"], [DataType::Int32, DataType::Int32]);
    for kind in KINDS {
        let nested = dictionary_of(list_of(kind, duplicated.clone()));
        assert!(has_duplicate_names(&nested), "{kind}");
        let positional = positional_type(&nested);
        assert!(!has_duplicate_names(&positional), "{kind}");
        assert_eq!(
            positional,
            dictionary_of(list_of(
                kind,
                fields(["_1", "_2"], [DataType::Int32, DataType::Int32])
            ))
        );
    }
}

#[test]
fn renamed_array_relabels_without_moving_values() {
    let values: ArrayRef = Arc::new(StructArray::new(
        match ba() {
            DataType::Struct(fields) => fields,
            _ => unreachable!(),
        },
        vec![
            Arc::new(Int32Array::from(vec![4, 3])) as ArrayRef,
            Arc::new(Int32Array::from(vec![3, 4])) as ArrayRef,
        ],
        None,
    ));
    let list: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("item", ba(), true)),
        OffsetBuffer::from_lengths([1, 1]),
        values,
        None,
    ));
    let target = list_of("list", ab());
    let renamed = renamed_array(&list, &target).expect("relabel");
    assert_eq!(renamed.data_type(), &target);
    let element = renamed
        .as_any()
        .downcast_ref::<ListArray>()
        .expect("list")
        .value(0);
    let element = element
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("struct");
    let first = element
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("ints");
    assert_eq!(first.value(0), 4);
}

fn bound_schema() -> DFSchema {
    let duplicate = fields(["a", "a"], [DataType::Int64, DataType::Int64]);
    let clashing = fields(["a", "a"], [DataType::Boolean, DataType::Boolean]);
    DFSchema::try_from(Schema::new(vec![
        Field::new("p", ab(), true),
        Field::new("q", ba(), true),
        Field::new("d", duplicate, true),
        Field::new("c", clashing, true),
        Field::new("id", DataType::Int32, true),
    ]))
    .expect("schema")
}

fn planned_operand_type(operand: &Expr, schema: &DFSchema) -> DataType {
    let Expr::ScalarFunction(function) = operand else {
        panic!("operand is not planned: {operand}");
    };
    assert_eq!(function.func.name(), PLANNED_BY_POSITION);
    operand.get_type(schema).expect("planned type")
}

#[test]
fn bound_comparison_with_a_duplicate_named_operand_pairs_by_position() {
    let schema = bound_schema();
    let planned =
        duplicate_field_operands_by_position(col("p").eq(col("d")), &schema).expect("planned");
    let Expr::BinaryExpr(BinaryExpr { left, right, .. }) = &planned else {
        panic!("not a comparison: {planned}");
    };
    assert_eq!(planned_operand_type(left, &schema), positional_type(&ab()));
    assert_eq!(
        planned_operand_type(right, &schema),
        fields(["_1", "_2"], [DataType::Int64, DataType::Int64])
    );
    assert_eq!(planned.get_type(&schema).expect("typed"), DataType::Boolean);
}

#[test]
fn bound_comparison_nested_under_a_conjunction_pairs_by_position() {
    let schema = bound_schema();
    let expr = col("d").gt_eq(col("p")).and(col("id").eq(lit(1)));
    let planned = duplicate_field_operands_by_position(expr, &schema).expect("planned");
    let Expr::BinaryExpr(BinaryExpr { left, right, .. }) = &planned else {
        panic!("not a conjunction: {planned}");
    };
    let Expr::BinaryExpr(BinaryExpr {
        left: first,
        right: second,
        ..
    }) = left.as_ref()
    else {
        panic!("not a comparison: {left}");
    };
    planned_operand_type(first, &schema);
    planned_operand_type(second, &schema);
    assert_eq!(right.as_ref(), &col("id").eq(lit(1)));
}

#[test]
fn bound_comparison_without_duplicates_or_with_an_unknown_side_is_unchanged() {
    let schema = bound_schema();
    for expr in [
        col("p").eq(col("q")),
        col("p").eq(col("p")),
        col("d").eq(col("missing")),
    ] {
        let kept = duplicate_field_operands_by_position(expr.clone(), &schema).expect("kept");
        assert_eq!(kept, expr);
    }
}

#[test]
fn bound_comparison_whose_positional_types_clash_refuses_with_spark_class() {
    let schema = bound_schema();
    let error = duplicate_field_operands_by_position(col("p").eq(col("c")), &schema)
        .expect_err("clash refuses");
    assert!(
        error
            .to_string()
            .contains("[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES]"),
        "{error}"
    );
}
