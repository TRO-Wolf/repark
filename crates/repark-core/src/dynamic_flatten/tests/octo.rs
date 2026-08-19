//! Octo cycle-2/3 kernel pins (file split so `tests.rs` stays under the 1500-line ceiling).

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, FixedSizeListArray, LargeListArray, LargeListViewArray, ListViewArray,
    StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use repark_common::Error;

use super::{
    DynamicFlattenOptions, analysis_token, assert_int64, collect_one, column_names,
    dynamic_flatten, flatten, i64_array, i64_cells, options, read_batch,
};

fn large_list_of(
    element: DataType,
    offsets: impl IntoIterator<Item = i64>,
    values: ArrayRef,
    valid: Option<Vec<bool>>,
) -> ArrayRef {
    let field = Arc::new(Field::new("item", element, true));
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets.into_iter().collect::<Vec<_>>()));
    let nulls = valid.map(NullBuffer::from);
    Arc::new(LargeListArray::try_new(field, offsets, values, nulls).expect("large_list"))
}

fn fixed_size_list_of(
    element: DataType,
    size: i32,
    values: ArrayRef,
    valid: Option<Vec<bool>>,
) -> ArrayRef {
    let field = Arc::new(Field::new("item", element, true));
    let nulls = valid.map(NullBuffer::from);
    Arc::new(FixedSizeListArray::try_new(field, size, values, nulls).expect("fixed_size_list"))
}

/// C2-Q-003: `list_element_type` `LargeList` arm. Deleting it must red this.
#[tokio::test]
async fn large_list_explodes() {
    let xs = large_list_of(
        DataType::Int64,
        [0_i64, 2],
        i64_array(vec![Some(1), Some(2)]),
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("xs", xs.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1)]), xs],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(column_names(&frame), ["id", "xs"]);
    let table = collect_one(frame).await;
    assert_eq!(i64_cells(&table, "id"), [Some(1), Some(1)]);
    assert_eq!(i64_cells(&table, "xs"), [Some(1), Some(2)]);
    assert_int64(&table, "xs");
}

/// C2-Q-003: `list_element_type` `FixedSizeList` arm. Deleting it must red this.
#[tokio::test]
async fn fixed_size_list_explodes() {
    let xs = fixed_size_list_of(DataType::Int64, 2, i64_array(vec![Some(3), Some(4)]), None);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("xs", xs.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1)]), xs],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(column_names(&frame), ["id", "xs"]);
    let table = collect_one(frame).await;
    assert_eq!(i64_cells(&table, "id"), [Some(1), Some(1)]);
    assert_eq!(i64_cells(&table, "xs"), [Some(3), Some(4)]);
    assert_int64(&table, "xs");
}

/// C2-L-004: `ListView` is not a leave-nested fail-open; refuse LOUD.
#[test]
fn list_view_refuses_loud() {
    let values = i64_array(vec![Some(1), Some(2)]);
    let field = Arc::new(Field::new("item", DataType::Int64, true));
    let offsets = ScalarBuffer::from(vec![0_i32]);
    let sizes = ScalarBuffer::from(vec![2_i32]);
    let view = ListViewArray::try_new(field, offsets, sizes, values, None).expect("list_view");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "xs",
            view.data_type().clone(),
            true,
        )])),
        vec![Arc::new(view)],
    )
    .expect("batch");

    analysis_token(
        dynamic_flatten(read_batch(batch), options()),
        "[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]",
    );
}

/// C3-Q-001 / C3-L-001: `LargeListView` is not a leave-nested fail-open; refuse LOUD.
/// Deleting the `LargeListView` arm of `is_list_view_type` must red this.
#[test]
fn large_list_view_refuses_loud() {
    let values = i64_array(vec![Some(1), Some(2)]);
    let field = Arc::new(Field::new("item", DataType::Int64, true));
    let offsets = ScalarBuffer::from(vec![0_i64]);
    let sizes = ScalarBuffer::from(vec![2_i64]);
    let view =
        LargeListViewArray::try_new(field, offsets, sizes, values, None).expect("large_list_view");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "xs",
            view.data_type().clone(),
            true,
        )])),
        vec![Arc::new(view)],
    )
    .expect("batch");

    analysis_token(
        dynamic_flatten(read_batch(batch), options()),
        "[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]",
    );
}

/// C2-SAF-002: max-depth remaining-schema Debug is truncated, not unbounded.
///
/// This pin kills unbounded output (token / "truncated" / message length).
/// It does not pin the allocation path: join-then-truncate of a full dump
/// would still pass. C3-SAF-001's streaming writer is a code-path choice,
/// not a property this pin can kill.
#[test]
fn max_depth_remaining_schema_is_truncated() {
    let names: Vec<String> = (0..30).map(|index| format!("f{index:02}")).collect();
    let fields: Fields = names
        .iter()
        .map(|name| Field::new(name, DataType::Int64, true))
        .collect();
    let arrays: Vec<ArrayRef> = (0..30).map(|_| i64_array(vec![Some(1)])).collect();
    let nested = StructArray::try_new(fields, arrays, None).expect("wide struct");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "wide",
            nested.data_type().clone(),
            true,
        )])),
        vec![Arc::new(nested)],
    )
    .expect("batch");

    match dynamic_flatten(
        read_batch(batch),
        DynamicFlattenOptions {
            max_depth: 0,
            ..options()
        },
    ) {
        Err(Error::Analysis(message)) => {
            assert!(
                message.contains("[DYNAMIC_FLATTEN_MAX_DEPTH]"),
                "expected MAX_DEPTH token in {message:?}"
            );
            assert!(
                message.contains("truncated"),
                "remaining-schema dump must truncate, got {message:?}"
            );
            assert!(
                message.len() < 800,
                "remaining-schema dump grew unbounded: {} chars",
                message.len()
            );
        }
        other => panic!("expected Analysis(MAX_DEPTH), got {other:?}"),
    }
}
