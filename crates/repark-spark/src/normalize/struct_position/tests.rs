use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int32Array, ListArray, StructArray};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Fields};

use super::udf::renamed_array;
use super::{Pairing, Seat, has_duplicate_names, pairing, positional_type, renamed_type};

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
