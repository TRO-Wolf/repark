//! Kernel pins (file split so `tests.rs` stays under the 1500-line ceiling).

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
    dynamic_flatten, flatten, i64_array, i64_cells, list_of, options, read_batch, struct_array,
    utf8_array,
};
use crate::dynamic_flatten::{PLAN_WALKS, dynamic_flatten_with_stats};

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

fn list_view_i64() -> ArrayRef {
    let values = i64_array(vec![Some(1), Some(2)]);
    let field = Arc::new(Field::new("item", DataType::Int64, true));
    let offsets = ScalarBuffer::from(vec![0_i32]);
    let sizes = ScalarBuffer::from(vec![2_i32]);
    Arc::new(ListViewArray::try_new(field, offsets, sizes, values, None).expect("list_view"))
}

fn large_list_view_i64() -> ArrayRef {
    let values = i64_array(vec![Some(1), Some(2)]);
    let field = Arc::new(Field::new("item", DataType::Int64, true));
    let offsets = ScalarBuffer::from(vec![0_i64]);
    let sizes = ScalarBuffer::from(vec![2_i64]);
    Arc::new(
        LargeListViewArray::try_new(field, offsets, sizes, values, None).expect("large_list_view"),
    )
}

fn named_column_batch(name: &str, array: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            name,
            array.data_type().clone(),
            true,
        )])),
        vec![array],
    )
    .expect("batch")
}

fn struct_wrap_batch(parent: &str, child: &str, array: ArrayRef) -> RecordBatch {
    let wrap = StructArray::try_new(
        Fields::from(vec![Field::new(child, array.data_type().clone(), true)]),
        vec![array],
        None,
    )
    .expect("wrap");
    named_column_batch(parent, Arc::new(wrap))
}

fn refuse_unsupported_element(batch: RecordBatch, options: DynamicFlattenOptions) {
    analysis_token(
        dynamic_flatten(read_batch(batch), options),
        "[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]",
    );
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
/// Default `max_depth`, top-level column only. Nested / `max_depth=0` are
/// `nested_list_view_max_depth_one_refuses_loud` and
/// `top_level_list_view_max_depth_zero_refuses_loud`.
#[test]
fn list_view_refuses_loud() {
    refuse_unsupported_element(named_column_batch("xs", list_view_i64()), options());
}

/// C3-Q-001 / C3-L-001: `LargeListView` is not a leave-nested fail-open; refuse LOUD.
/// Deleting the `LargeListView` arm of `is_list_view_type` must red this.
#[test]
fn large_list_view_refuses_loud() {
    refuse_unsupported_element(named_column_batch("xs", large_list_view_i64()), options());
}

/// R-S1-003: one struct-expand pass surfaces a nested `ListView`; refuse
/// `[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]`, not `[DYNAMIC_FLATTEN_MAX_DEPTH]`
/// and not leave-nested. Deleting the post-loop `ListView` check greens the
/// default-depth top-level pin and reds this (`max_depth=1` never re-enters
/// the in-loop check).
#[test]
fn nested_list_view_max_depth_one_refuses_loud() {
    refuse_unsupported_element(
        struct_wrap_batch("wrap", "xs", list_view_i64()),
        DynamicFlattenOptions {
            max_depth: 1,
            ..options()
        },
    );
}

/// R-S1-003: same post-loop refuse after one expand for `LargeListView`.
#[test]
fn nested_large_list_view_max_depth_one_refuses_loud() {
    refuse_unsupported_element(
        struct_wrap_batch("wrap", "xs", large_list_view_i64()),
        DynamicFlattenOptions {
            max_depth: 1,
            ..options()
        },
    );
}

/// R-S1-003: `max_depth=0` skips the in-loop `ListView` check; top-level
/// `ListView` must still refuse `[DYNAMIC_FLATTEN_UNSUPPORTED_ELEMENT]`
/// (not `MAX_DEPTH`, not Ok).
#[test]
fn top_level_list_view_max_depth_zero_refuses_loud() {
    refuse_unsupported_element(
        named_column_batch("xs", list_view_i64()),
        DynamicFlattenOptions {
            max_depth: 0,
            ..options()
        },
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

#[tokio::test]
async fn product_dynamic_flatten_does_no_plan_walk() {
    let inner = struct_array(
        vec![("Val", DataType::Int64, i64_array(vec![Some(1)]))],
        None,
    );
    let mid = struct_array(
        vec![("L2", inner.data_type().clone(), Arc::new(inner) as ArrayRef)],
        None,
    );
    let outer = struct_array(
        vec![("L1", mid.data_type().clone(), Arc::new(mid) as ArrayRef)],
        None,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("Payload", outer.data_type().clone(), true),
    ]));
    let outer: ArrayRef = Arc::new(outer);
    let product_batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![i64_array(vec![Some(1)]), Arc::clone(&outer)],
    )
    .expect("batch");
    let stats_batch =
        RecordBatch::try_new(schema, vec![i64_array(vec![Some(1)]), outer]).expect("batch");

    PLAN_WALKS.with(|cell| cell.set(0));
    let _frame = dynamic_flatten(read_batch(product_batch), options()).expect("product flatten");
    assert_eq!(
        PLAN_WALKS.with(std::cell::Cell::get),
        0,
        "product dynamic_flatten must not walk the logical plan"
    );

    let (_frame, stats) =
        dynamic_flatten_with_stats(read_batch(stats_batch), options()).expect("flatten stats");
    assert_eq!(PLAN_WALKS.with(std::cell::Cell::get), 1);
    assert!(stats.plan_nodes >= 4);
}

#[tokio::test]
async fn flatten_stats_depth_three_struct_counts_repeated_schema_walks() {
    let inner = struct_array(
        vec![("Val", DataType::Int64, i64_array(vec![Some(1)]))],
        None,
    );
    let mid = struct_array(
        vec![("L2", inner.data_type().clone(), Arc::new(inner) as ArrayRef)],
        None,
    );
    let outer = struct_array(
        vec![("L1", mid.data_type().clone(), Arc::new(mid) as ArrayRef)],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("Payload", outer.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1)]), Arc::new(outer)],
    )
    .expect("batch");
    let (_frame, stats) =
        dynamic_flatten_with_stats(read_batch(batch), options()).expect("flatten stats");
    assert_eq!(stats.struct_expansions, 3);
    assert_eq!(stats.list_explodes, 0);
    assert_eq!(stats.unnest_nodes, 0);
    assert_eq!(stats.rewrite_passes, 4);
    assert_eq!(stats.schema_walks, 10);
    assert!(stats.plan_nodes >= 4);
    assert!(stats.projection_nodes >= 3);
    assert_eq!(stats.fields_visited, 20);
}

#[tokio::test]
async fn flatten_stats_two_sibling_lists_are_sequential_unnests() {
    let left = list_of(
        DataType::Int64,
        [0, 2],
        i64_array(vec![Some(1), Some(2)]),
        None,
    );
    let right = list_of(
        DataType::Utf8,
        [0, 2],
        utf8_array(vec![Some("a"), Some("b")]),
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("Legs", left.data_type().clone(), true),
            Field::new("Tags", right.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1)]), left, right],
    )
    .expect("batch");
    let (frame, stats) =
        dynamic_flatten_with_stats(read_batch(batch), options()).expect("flatten stats");
    assert_eq!(stats.list_explodes, 2);
    assert_eq!(stats.unnest_nodes, 2);
    assert_eq!(column_names(&frame), ["id", "Legs", "Tags"]);
    let table = collect_one(frame).await;
    assert_eq!(table.num_rows(), 4);
}
