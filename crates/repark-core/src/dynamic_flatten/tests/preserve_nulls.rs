use std::sync::Arc;

use arrow::array::{ArrayRef, DictionaryArray, FixedSizeListArray, Int32Array, LargeListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;

use super::{
    DynamicFlattenOptions, assert_int64, collect_one, column_names, dynamic_flatten, flatten,
    i64_array, i64_cells, list_of, options, read_batch,
};

fn large_list_of(
    offsets: impl IntoIterator<Item = i64>,
    values: ArrayRef,
    valid: Option<Vec<bool>>,
) -> ArrayRef {
    let field = Arc::new(Field::new("item", DataType::Int64, true));
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets.into_iter().collect::<Vec<_>>()));
    let nulls = valid.map(NullBuffer::from);
    Arc::new(LargeListArray::try_new(field, offsets, values, nulls).expect("large list"))
}

fn fixed_size_list_of(values: ArrayRef, valid: Vec<bool>) -> ArrayRef {
    let field = Arc::new(Field::new("item", DataType::Int64, true));
    Arc::new(
        FixedSizeListArray::try_new(field, 2, values, Some(NullBuffer::from(valid)))
            .expect("fixed-size list"),
    )
}

fn batch_with_ids(name: &str, values: ArrayRef, ids: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(name, values.data_type().clone(), true),
        ])),
        vec![i64_array(ids), values],
    )
    .expect("batch")
}

fn flatten_without_empty_rewrite(batch: RecordBatch) -> datafusion::prelude::DataFrame {
    flatten(
        batch,
        DynamicFlattenOptions {
            empty_as_null: false,
            ..options()
        },
    )
}

#[test]
fn ordinary_lists_without_empty_rewrite_have_no_case_projection() {
    let first = list_of(DataType::Int64, [0, 1], i64_array(vec![Some(1)]), None);
    let second = list_of(DataType::Int64, [0, 1], i64_array(vec![Some(2)]), None);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("first", first.data_type().clone(), true),
            Field::new("second", second.data_type().clone(), true),
        ])),
        vec![first, second],
    )
    .expect("batch");

    let frame = flatten_without_empty_rewrite(batch);
    let plan = format!("{}", frame.logical_plan().display_indent());
    assert_eq!(plan.matches("CASE WHEN").count(), 0, "{plan}");
    assert_eq!(plan.matches("Unnest:").count(), 2, "{plan}");
}

#[test]
fn empty_as_null_rewrite_only_checks_length() {
    let values = list_of(DataType::Int64, [0, 1], i64_array(vec![Some(1)]), None);
    let frame = flatten(batch_with_ids("xs", values, vec![Some(1)]), options());
    let plan = format!("{}", frame.logical_plan().display_indent());
    assert!(plan.contains("array_length"), "{plan}");
    assert!(!plan.contains("IS NULL"), "{plan}");
}

#[tokio::test]
async fn large_list_preserves_null_and_controls_empty_rows() {
    let values = large_list_of(
        [0, 0, 0, 2],
        i64_array(vec![Some(7), Some(8)]),
        Some(vec![false, true, true]),
    );
    let batch = batch_with_ids("xs", values, vec![Some(1), Some(2), Some(3)]);

    let keep_empty = collect_one(flatten(batch.clone(), options())).await;
    assert_eq!(
        i64_cells(&keep_empty, "id"),
        [Some(1), Some(2), Some(3), Some(3)]
    );
    assert_eq!(i64_cells(&keep_empty, "xs"), [None, None, Some(7), Some(8)]);
    assert_int64(&keep_empty, "xs");

    let drop_empty = collect_one(flatten_without_empty_rewrite(batch)).await;
    assert_eq!(i64_cells(&drop_empty, "id"), [Some(1), Some(3), Some(3)]);
    assert_eq!(i64_cells(&drop_empty, "xs"), [None, Some(7), Some(8)]);
    assert_int64(&drop_empty, "xs");
}

#[tokio::test]
async fn fixed_size_list_preserves_null_rows_in_both_modes() {
    let values = fixed_size_list_of(
        i64_array(vec![Some(99), Some(100), Some(7), Some(8)]),
        vec![false, true],
    );
    let batch = batch_with_ids("xs", values, vec![Some(1), Some(2)]);

    for empty_as_null in [true, false] {
        let frame = flatten(
            batch.clone(),
            DynamicFlattenOptions {
                empty_as_null,
                ..options()
            },
        );
        let plan = format!("{}", frame.logical_plan().display_indent());
        assert_eq!(plan.matches("CASE WHEN").count(), 0, "{plan}");
        let table = collect_one(frame).await;
        assert_eq!(i64_cells(&table, "id"), [Some(1), Some(2), Some(2)]);
        assert_eq!(i64_cells(&table, "xs"), [None, Some(7), Some(8)]);
        assert_int64(&table, "xs");
    }
}

#[tokio::test]
async fn dictionary_list_preserves_null_and_controls_empty_rows() {
    let dictionary_values = list_of(
        DataType::Int64,
        [0, 0, 0, 2],
        i64_array(vec![Some(7), Some(8)]),
        Some(vec![false, true, true]),
    );
    let keys = Int32Array::from(vec![Some(0), Some(1), Some(2)]);
    let dictionary = DictionaryArray::<Int32Type>::try_new(keys, dictionary_values).expect("dict");
    let batch = batch_with_ids("xs", Arc::new(dictionary), vec![Some(1), Some(2), Some(3)]);

    let keep_empty = collect_one(flatten(batch.clone(), options())).await;
    assert_eq!(
        i64_cells(&keep_empty, "id"),
        [Some(1), Some(2), Some(3), Some(3)]
    );
    assert_eq!(i64_cells(&keep_empty, "xs"), [None, None, Some(7), Some(8)]);
    assert_int64(&keep_empty, "xs");

    let drop_empty = collect_one(flatten_without_empty_rewrite(batch)).await;
    assert_eq!(i64_cells(&drop_empty, "id"), [Some(1), Some(3), Some(3)]);
    assert_eq!(i64_cells(&drop_empty, "xs"), [None, Some(7), Some(8)]);
    assert_int64(&drop_empty, "xs");
}

#[tokio::test]
async fn two_lists_keep_cartesian_schema_order() {
    let first = list_of(
        DataType::Int64,
        [0, 2],
        i64_array(vec![Some(1), Some(2)]),
        None,
    );
    let second = list_of(
        DataType::Int64,
        [0, 2],
        i64_array(vec![Some(10), Some(20)]),
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("first", first.data_type().clone(), true),
            Field::new("second", second.data_type().clone(), true),
        ])),
        vec![first, second],
    )
    .expect("batch");

    let frame = dynamic_flatten(read_batch(batch), options()).expect("flatten");
    assert_eq!(column_names(&frame), ["first", "second"]);
    let table = collect_one(frame).await;
    assert_eq!(
        i64_cells(&table, "first"),
        [Some(1), Some(1), Some(2), Some(2)]
    );
    assert_eq!(
        i64_cells(&table, "second"),
        [Some(10), Some(20), Some(10), Some(20)]
    );
}
