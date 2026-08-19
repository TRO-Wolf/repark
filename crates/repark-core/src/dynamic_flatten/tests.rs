//! Engine pins for [`crate::dynamic_flatten`] — value AND Arrow type.
//!
//! The first four tests are the mutation pins that decide the design. The rest clone the
//! Python suite's ENGINE cases (not the Python-only type-gate cases).

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int32Array, Int64Array, ListArray, MapArray, NullArray,
    StringArray, StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{SessionConfig, SessionContext};
use repark_common::Error;

use super::{DynamicFlattenOptions, dynamic_flatten};

fn options() -> DynamicFlattenOptions {
    DynamicFlattenOptions::default()
}

fn test_context() -> SessionContext {
    // `SessionContext::new()` leaves DF-54.1 `push_down_leaf_projections` on, which
    // miscompiles Unnest+get_field (DEFECT-2). ReParkSession wraps that rule; this
    // kernel harness is the DataFusion DataFrame API, so the broken pass is off here
    // so collect can pin values. Schema-only assertions do not need the wrap.
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .optimizer
        .enable_leaf_expression_pushdown = false;
    SessionContext::new_with_config(config)
}

fn read_batch(batch: RecordBatch) -> datafusion::prelude::DataFrame {
    test_context().read_batch(batch).expect("read_batch")
}

fn flatten(batch: RecordBatch, options: DynamicFlattenOptions) -> datafusion::prelude::DataFrame {
    dynamic_flatten(read_batch(batch), options).expect("dynamic_flatten")
}

fn column_names(frame: &datafusion::prelude::DataFrame) -> Vec<String> {
    frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

async fn collect_one(frame: datafusion::prelude::DataFrame) -> RecordBatch {
    let schema = Arc::new(frame.schema().as_arrow().clone());
    let batches = frame.collect().await.expect("collect");
    if batches.is_empty() {
        return RecordBatch::new_empty(schema);
    }
    if batches.len() == 1 {
        return batches.into_iter().next().expect("one batch");
    }
    arrow::compute::concat_batches(batches[0].schema_ref(), &batches).expect("concat")
}

fn i64_array(values: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(values))
}

fn utf8_array(values: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(values))
}

fn struct_array(
    children: Vec<(&str, DataType, ArrayRef)>,
    valid: Option<Vec<bool>>,
) -> StructArray {
    let fields: Fields = children
        .iter()
        .map(|(name, data_type, _)| Field::new(*name, data_type.clone(), true))
        .collect();
    let arrays: Vec<ArrayRef> = children.into_iter().map(|(_, _, array)| array).collect();
    let nulls = valid.map(NullBuffer::from);
    StructArray::try_new(fields, arrays, nulls).expect("struct")
}

fn list_of(
    element: DataType,
    offsets: impl IntoIterator<Item = i32>,
    values: ArrayRef,
    valid: Option<Vec<bool>>,
) -> ArrayRef {
    let field = Arc::new(Field::new("item", element, true));
    let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets.into_iter().collect::<Vec<_>>()));
    let nulls = valid.map(NullBuffer::from);
    Arc::new(ListArray::try_new(field, offsets, values, nulls).expect("list"))
}

fn decode_i64(array: &dyn Array) -> Vec<Option<i64>> {
    if let Some(ints) = array.as_any().downcast_ref::<Int64Array>() {
        return (0..ints.len())
            .map(|index| {
                if ints.is_null(index) {
                    None
                } else {
                    Some(ints.value(index))
                }
            })
            .collect();
    }
    if let Some(dictionary) = array.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let values = decode_i64(dictionary.values().as_ref());
        return (0..dictionary.len())
            .map(|index| {
                if dictionary.is_null(index) {
                    None
                } else {
                    let key = dictionary.keys().value(index);
                    values[usize::try_from(key).expect("non-negative dict key")]
                }
            })
            .collect();
    }
    panic!("not Int64 (or dict-of-Int64), got {:?}", array.data_type());
}

fn i64_cells(batch: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    let array = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"));
    decode_i64(array.as_ref())
}

fn utf8_cells(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let array = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"));
    if let Some(utf8) = array.as_any().downcast_ref::<StringArray>() {
        return (0..utf8.len())
            .map(|index| {
                if utf8.is_null(index) {
                    None
                } else {
                    Some(utf8.value(index).to_string())
                }
            })
            .collect();
    }
    panic!("{name} is not Utf8, got {:?}", array.data_type());
}

fn field_type(batch: &RecordBatch, name: &str) -> DataType {
    batch
        .schema()
        .field_with_name(name)
        .expect(name)
        .data_type()
        .clone()
}

fn assert_int64(batch: &RecordBatch, name: &str) {
    let data_type = field_type(batch, name);
    let is_int64 = data_type == DataType::Int64
        || matches!(
            data_type,
            DataType::Dictionary(_, ref value) if **value == DataType::Int64
        );
    assert!(is_int64, "{name} Arrow type {data_type:?} is not Int64");
}

fn assert_utf8(batch: &RecordBatch, name: &str) {
    let data_type = field_type(batch, name);
    assert!(
        matches!(
            data_type,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
        ),
        "{name} Arrow type {data_type:?} is not a string"
    );
}

fn analysis_token(result: Result<datafusion::prelude::DataFrame, Error>, token: &str) {
    match result {
        Err(Error::Analysis(message)) => {
            assert!(message.contains(token), "expected {token} in {message:?}");
        }
        other => panic!("expected Analysis({token}), got {other:?}"),
    }
}

// =================================================================================================
// FIRST FOUR MUTATION PINS
// =================================================================================================

/// Pin 1: null parent struct → NULL leaves, not 0/""/false. Removing the CASE must red this.
#[tokio::test]
async fn null_parent_struct_fields_are_null_not_zero() {
    let outer = struct_array(
        vec![
            ("x", DataType::Int64, i64_array(vec![None, Some(5), None])),
            (
                "label",
                DataType::Utf8,
                utf8_array(vec![None, Some("ok"), None]),
            ),
            (
                "flag",
                DataType::Int64,
                i64_array(vec![None, Some(1), None]),
            ),
        ],
        Some(vec![false, true, true]),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("outer", outer.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1), Some(2), Some(3)]), Arc::new(outer)],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(
        column_names(&frame),
        ["id", "outer_x", "outer_label", "outer_flag"]
    );
    let table = collect_one(frame).await;
    assert_eq!(i64_cells(&table, "id"), [Some(1), Some(2), Some(3)]);
    assert_eq!(i64_cells(&table, "outer_x"), [None, Some(5), None]);
    assert_eq!(
        utf8_cells(&table, "outer_label"),
        [None, Some("ok".to_string()), None]
    );
    assert_eq!(i64_cells(&table, "outer_flag"), [None, Some(1), None]);
    assert_int64(&table, "outer_x");
    assert_int64(&table, "outer_flag");
    assert_utf8(&table, "outer_label");
}

/// Pin 2: in-place column order `z, a_x, a_y, m`. Hoisting scalars first must red this.
#[tokio::test]
async fn unnest_preserves_interleaved_column_order() {
    let nested = struct_array(
        vec![
            ("x", DataType::Int64, i64_array(vec![Some(2)])),
            ("y", DataType::Int64, i64_array(vec![Some(3)])),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("z", DataType::Int64, true),
            Field::new("a", nested.data_type().clone(), true),
            Field::new("m", DataType::Int64, true),
        ])),
        vec![
            i64_array(vec![Some(1)]),
            Arc::new(nested),
            i64_array(vec![Some(4)]),
        ],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(column_names(&frame), ["z", "a_x", "a_y", "m"]);
    let table = collect_one(frame).await;
    assert_eq!(i64_cells(&table, "z"), [Some(1)]);
    assert_eq!(i64_cells(&table, "a_x"), [Some(2)]);
    assert_eq!(i64_cells(&table, "a_y"), [Some(3)]);
    assert_eq!(i64_cells(&table, "m"), [Some(4)]);
    assert_int64(&table, "z");
    assert_int64(&table, "a_x");
    assert_int64(&table, "m");
}

/// Pin 3: list-of-struct then unnest: `legs` → `legs_leg_id`, `legs_side`.
/// Unnesting lists in the struct pass must red this.
#[tokio::test]
async fn list_of_struct_explodes_then_unnests() {
    let leg = struct_array(
        vec![
            (
                "leg_id",
                DataType::Int64,
                i64_array(vec![Some(1), Some(2), Some(9)]),
            ),
            (
                "side",
                DataType::Utf8,
                utf8_array(vec![Some("Buy"), Some("Sell"), Some("Buy")]),
            ),
        ],
        None,
    );
    let legs = list_of(leg.data_type().clone(), [0, 2, 3], Arc::new(leg), None);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("legs", legs.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1), Some(2)]), legs],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(column_names(&frame), ["id", "legs_leg_id", "legs_side"]);
    let table = collect_one(frame).await;
    assert_eq!(i64_cells(&table, "id"), [Some(1), Some(1), Some(2)]);
    assert_eq!(
        i64_cells(&table, "legs_leg_id"),
        [Some(1), Some(2), Some(9)]
    );
    assert_eq!(
        utf8_cells(&table, "legs_side"),
        [
            Some("Buy".to_string()),
            Some("Sell".to_string()),
            Some("Buy".to_string())
        ]
    );
    assert_int64(&table, "legs_leg_id");
    assert_utf8(&table, "legs_side");
}

/// Pin 4: prefixed name collision with top-level (`a_x` + `a.x`) refuses LOUD.
/// Last-write-wins must red this.
#[test]
fn prefixed_name_collision_with_top_level_refuses() {
    let nested = struct_array(vec![("x", DataType::Int64, i64_array(vec![Some(2)]))], None);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a_x", DataType::Int64, true),
            Field::new("a", nested.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1)]), Arc::new(nested)],
    )
    .expect("batch");

    analysis_token(
        dynamic_flatten(read_batch(batch), options()),
        "[DYNAMIC_FLATTEN_NAME_COLLISION]",
    );
}

// =================================================================================================
// Remaining engine cases
// =================================================================================================

#[tokio::test]
async fn nested_struct_in_struct() {
    let inner = struct_array(
        vec![
            ("x", DataType::Int64, i64_array(vec![Some(10), Some(20)])),
            (
                "y",
                DataType::Utf8,
                utf8_array(vec![Some("ten"), Some("twenty")]),
            ),
        ],
        None,
    );
    let outer = struct_array(
        vec![
            (
                "label",
                DataType::Utf8,
                utf8_array(vec![Some("L"), Some("M")]),
            ),
            ("inner", inner.data_type().clone(), Arc::new(inner)),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("outer", outer.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1), Some(2)]), Arc::new(outer)],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(
        column_names(&frame),
        ["id", "outer_label", "outer_inner_x", "outer_inner_y"]
    );
    let table = collect_one(frame).await;
    assert_eq!(i64_cells(&table, "outer_inner_x"), [Some(10), Some(20)]);
    assert_eq!(
        utf8_cells(&table, "outer_label"),
        [Some("L".to_string()), Some("M".to_string())]
    );
    assert_int64(&table, "id");
    assert_int64(&table, "outer_inner_x");
    assert_utf8(&table, "outer_label");
    assert_utf8(&table, "outer_inner_y");
}

#[tokio::test]
async fn null_mid_struct_fields_are_null_not_zero() {
    let inner = struct_array(
        vec![("x", DataType::Int64, i64_array(vec![None, Some(9), None]))],
        Some(vec![false, true, false]),
    );
    let outer = struct_array(
        vec![("inner", inner.data_type().clone(), Arc::new(inner))],
        Some(vec![true, true, false]),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "o",
            outer.data_type().clone(),
            true,
        )])),
        vec![Arc::new(outer)],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    let table = collect_one(frame).await;
    let values = i64_cells(&table, "o_inner_x");
    let nulls = values.iter().filter(|value| value.is_none()).count();
    assert_eq!(nulls, 2);
    assert!(values.contains(&Some(9)));
    assert_int64(&table, "o_inner_x");
}

#[tokio::test]
async fn multi_list_serial_explode_order() {
    let a = list_of(
        DataType::Int64,
        [0, 2],
        i64_array(vec![Some(1), Some(2)]),
        None,
    );
    let b = list_of(
        DataType::Int64,
        [0, 2],
        i64_array(vec![Some(10), Some(20)]),
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("a", a.data_type().clone(), true),
            Field::new("b", b.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1)]), a, b],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(column_names(&frame), ["id", "a", "b"]);
    let table = collect_one(frame).await;
    assert_eq!(i64_cells(&table, "a"), [Some(1), Some(1), Some(2), Some(2)]);
    assert_eq!(
        i64_cells(&table, "b"),
        [Some(10), Some(20), Some(10), Some(20)]
    );
    assert_int64(&table, "a");
    assert_int64(&table, "b");
}

#[tokio::test]
async fn list_explode_preserves_interleaved_column_order() {
    let xs = list_of(
        DataType::Int64,
        [0, 2],
        i64_array(vec![Some(2), Some(3)]),
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("z", DataType::Int64, true),
            Field::new("xs", xs.data_type().clone(), true),
            Field::new("m", DataType::Int64, true),
        ])),
        vec![i64_array(vec![Some(1)]), xs, i64_array(vec![Some(4)])],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(column_names(&frame), ["z", "xs", "m"]);
    let table = collect_one(frame).await;
    assert_eq!(i64_cells(&table, "xs"), [Some(2), Some(3)]);
    assert_eq!(i64_cells(&table, "z"), [Some(1), Some(1)]);
    assert_eq!(i64_cells(&table, "m"), [Some(4), Some(4)]);
    assert_int64(&table, "xs");
}

#[tokio::test]
async fn struct_in_list_in_struct() {
    let fill = struct_array(
        vec![
            ("qty", DataType::Int64, i64_array(vec![Some(1), Some(2)])),
            ("px", DataType::Int64, i64_array(vec![Some(100), Some(101)])),
        ],
        None,
    );
    let fills = list_of(fill.data_type().clone(), [0, 2], Arc::new(fill), None);
    let payload = struct_array(
        vec![
            ("symbol", DataType::Utf8, utf8_array(vec![Some("AAA")])),
            ("fills", fills.data_type().clone(), fills),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("payload", payload.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(7)]), Arc::new(payload)],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(
        column_names(&frame),
        [
            "order_id",
            "payload_symbol",
            "payload_fills_qty",
            "payload_fills_px"
        ]
    );
    let table = collect_one(frame).await;
    assert_eq!(i64_cells(&table, "payload_fills_qty"), [Some(1), Some(2)]);
    assert_eq!(
        i64_cells(&table, "payload_fills_px"),
        [Some(100), Some(101)]
    );
    assert_int64(&table, "payload_fills_qty");
    assert_int64(&table, "payload_fills_px");
}

#[tokio::test]
async fn drop_null_typed_list() {
    let void_list = list_of(DataType::Null, [0, 0, 0], Arc::new(NullArray::new(0)), None);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("user_properties", void_list.data_type().clone(), true),
            Field::new("keep", DataType::Utf8, true),
        ])),
        vec![
            i64_array(vec![Some(1), Some(2)]),
            void_list,
            utf8_array(vec![Some("a"), Some("b")]),
        ],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(column_names(&frame), ["id", "keep"]);
    let table = collect_one(frame).await;
    assert_eq!(i64_cells(&table, "id"), [Some(1), Some(2)]);
}

#[tokio::test]
async fn drop_null_lists_false_empty_void_keeps_or_drops_by_empty_as_null() {
    let void_list = list_of(DataType::Null, [0, 0], Arc::new(NullArray::new(0)), None);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("props", void_list.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1)]), void_list],
    )
    .expect("batch");

    let keep = DynamicFlattenOptions {
        drop_null_lists: false,
        ..options()
    };
    let kept = flatten(batch.clone(), keep);
    assert_eq!(column_names(&kept), ["id", "props"]);
    let kept_table = collect_one(kept).await;
    assert_eq!(kept_table.num_rows(), 1);
    assert_eq!(field_type(&kept_table, "props"), DataType::Null);
    assert!(
        kept_table
            .column_by_name("props")
            .expect("props")
            .as_any()
            .is::<NullArray>(),
        "empty void with empty_as_null=true is a Null-typed cell (Arrow NullArray \
         may report is_null=false / null_count=0; pylist is still None)"
    );

    let drop_empty = DynamicFlattenOptions {
        drop_null_lists: false,
        empty_as_null: false,
        ..options()
    };
    let dropped = flatten(batch, drop_empty);
    let dropped_table = collect_one(dropped).await;
    assert_eq!(
        dropped_table.num_rows(),
        0,
        "empty void drops when empty_as_null=false"
    );
}

#[tokio::test]
async fn null_and_empty_array_values() {
    let xs = list_of(
        DataType::Int64,
        [0, 0, 0, 2],
        i64_array(vec![Some(7), Some(8)]),
        Some(vec![false, true, true]),
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("xs", xs.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1), Some(2), Some(3)]), xs],
    )
    .expect("batch");

    let default = flatten(batch.clone(), options());
    let default_table = collect_one(default).await;
    assert_eq!(
        i64_cells(&default_table, "id"),
        [Some(1), Some(2), Some(3), Some(3)]
    );
    assert_eq!(
        i64_cells(&default_table, "xs"),
        [None, None, Some(7), Some(8)]
    );
    assert_int64(&default_table, "xs");

    let drop_empty = DynamicFlattenOptions {
        empty_as_null: false,
        ..options()
    };
    let dropped = flatten(batch, drop_empty);
    let dropped_table = collect_one(dropped).await;
    assert_eq!(i64_cells(&dropped_table, "id"), [Some(1), Some(3), Some(3)]);
    assert_eq!(i64_cells(&dropped_table, "xs"), [None, Some(7), Some(8)]);
    assert_int64(&dropped_table, "xs");
}

#[test]
fn max_depth_refuses_loud_never_silent_truncate() {
    let inner = struct_array(vec![("c", DataType::Int64, i64_array(vec![Some(1)]))], None);
    let mid = struct_array(
        vec![("b", inner.data_type().clone(), Arc::new(inner))],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "a",
            mid.data_type().clone(),
            true,
        )])),
        vec![Arc::new(mid)],
    )
    .expect("batch");

    analysis_token(
        dynamic_flatten(
            read_batch(batch.clone()),
            DynamicFlattenOptions {
                max_depth: 1,
                ..options()
            },
        ),
        "[DYNAMIC_FLATTEN_MAX_DEPTH]",
    );
    analysis_token(
        dynamic_flatten(
            read_batch(batch.clone()),
            DynamicFlattenOptions {
                max_depth: 0,
                ..options()
            },
        ),
        "[DYNAMIC_FLATTEN_MAX_DEPTH]",
    );
}

#[tokio::test]
async fn max_depth_ample_succeeds() {
    let inner = struct_array(vec![("c", DataType::Int64, i64_array(vec![Some(1)]))], None);
    let mid = struct_array(
        vec![("b", inner.data_type().clone(), Arc::new(inner))],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "a",
            mid.data_type().clone(),
            true,
        )])),
        vec![Arc::new(mid)],
    )
    .expect("batch");

    let frame = flatten(
        batch,
        DynamicFlattenOptions {
            max_depth: 5,
            ..options()
        },
    );
    assert_eq!(column_names(&frame), ["a_b_c"]);
    let table = collect_one(frame).await;
    assert_eq!(i64_cells(&table, "a_b_c"), [Some(1)]);
}

#[tokio::test]
async fn prefix_disambiguates_sibling_struct_fields() {
    let left = struct_array(
        vec![("score", DataType::Int64, i64_array(vec![Some(1)]))],
        None,
    );
    let right = struct_array(
        vec![("score", DataType::Int64, i64_array(vec![Some(2)]))],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("left", left.data_type().clone(), true),
            Field::new("right", right.data_type().clone(), true),
        ])),
        vec![Arc::new(left), Arc::new(right)],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(column_names(&frame), ["left_score", "right_score"]);
    let table = collect_one(frame).await;
    assert_eq!(i64_cells(&table, "left_score"), [Some(1)]);
    assert_eq!(i64_cells(&table, "right_score"), [Some(2)]);
}

#[test]
fn prefixed_name_collision_between_expansions_refuses() {
    let outer = struct_array(
        vec![("inner_x", DataType::Int64, i64_array(vec![Some(1)]))],
        None,
    );
    let outer_inner = struct_array(vec![("x", DataType::Int64, i64_array(vec![Some(2)]))], None);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("outer", outer.data_type().clone(), true),
            Field::new("outer_inner", outer_inner.data_type().clone(), true),
        ])),
        vec![Arc::new(outer), Arc::new(outer_inner)],
    )
    .expect("batch");

    analysis_token(
        dynamic_flatten(read_batch(batch), options()),
        "[DYNAMIC_FLATTEN_NAME_COLLISION]",
    );
}

#[test]
fn cross_pass_prefixed_collision_refuses() {
    let inner = struct_array(vec![("c", DataType::Int64, i64_array(vec![Some(2)]))], None);
    let a = struct_array(
        vec![("b", inner.data_type().clone(), Arc::new(inner))],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a_b_c", DataType::Int64, true),
            Field::new("a", a.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1)]), Arc::new(a)],
    )
    .expect("batch");

    analysis_token(
        dynamic_flatten(read_batch(batch), options()),
        "[DYNAMIC_FLATTEN_NAME_COLLISION]",
    );
}

#[test]
fn list_explode_then_unnest_collision_with_top_level_refuses() {
    let leg = struct_array(
        vec![("leg_id", DataType::Int64, i64_array(vec![Some(1)]))],
        None,
    );
    let legs = list_of(leg.data_type().clone(), [0, 1], Arc::new(leg), None);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("legs_leg_id", DataType::Int64, true),
            Field::new("legs", legs.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(9)]), legs],
    )
    .expect("batch");

    analysis_token(
        dynamic_flatten(read_batch(batch), options()),
        "[DYNAMIC_FLATTEN_NAME_COLLISION]",
    );
}

#[tokio::test]
async fn explode_lists_false_leaves_arrays() {
    let nums = list_of(
        DataType::Int64,
        [0, 2],
        i64_array(vec![Some(1), Some(2)]),
        None,
    );
    let wrap = struct_array(
        vec![
            ("tag", DataType::Utf8, utf8_array(vec![Some("t")])),
            ("nums", nums.data_type().clone(), nums),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "wrap",
            wrap.data_type().clone(),
            true,
        )])),
        vec![Arc::new(wrap)],
    )
    .expect("batch");

    let frame = flatten(
        batch,
        DynamicFlattenOptions {
            explode_lists: false,
            ..options()
        },
    );
    assert_eq!(column_names(&frame), ["wrap_tag", "wrap_nums"]);
    let table = collect_one(frame).await;
    assert_eq!(utf8_cells(&table, "wrap_tag"), [Some("t".to_string())]);
    let nums_type = field_type(&table, "wrap_nums");
    assert!(
        matches!(nums_type, DataType::List(_) | DataType::LargeList(_)),
        "wrap_nums stays a list, got {nums_type:?}"
    );
}

#[tokio::test]
async fn custom_separator_names_column_literally() {
    let nested = struct_array(vec![("f", DataType::Int64, i64_array(vec![Some(3)]))], None);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "s",
            nested.data_type().clone(),
            true,
        )])),
        vec![Arc::new(nested)],
    )
    .expect("batch");

    let frame = flatten(
        batch,
        DynamicFlattenOptions {
            separator: ".".to_string(),
            ..options()
        },
    );
    assert_eq!(column_names(&frame), ["s.f"]);
    let table = collect_one(frame).await;
    assert_eq!(i64_cells(&table, "s.f"), [Some(3)]);
}

#[tokio::test]
async fn already_flat_is_idempotent() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Float64, true),
            Field::new("c", DataType::Utf8, true),
        ])),
        vec![
            i64_array(vec![Some(1)]),
            Arc::new(arrow::array::Float64Array::from(vec![Some(2.5)])),
            utf8_array(vec![Some("x")]),
        ],
    )
    .expect("batch");

    let once = flatten(batch.clone(), options());
    let twice = dynamic_flatten(once, options()).expect("second flatten");
    assert_eq!(column_names(&twice), ["a", "b", "c"]);
    let table = collect_one(twice).await;
    assert_eq!(i64_cells(&table, "a"), [Some(1)]);
}

#[test]
fn plan_build_does_not_execute() {
    let nested = struct_array(
        vec![("v", DataType::Int64, i64_array(vec![Some(99)]))],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("nested", nested.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1)]), Arc::new(nested)],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(column_names(&frame), ["id", "nested_v"]);
}

#[tokio::test]
async fn capitalized_legs_and_sibling_struct() {
    let meta = struct_array(
        vec![(
            "account",
            DataType::Utf8,
            utf8_array(vec![Some("A"), Some("B")]),
        )],
        None,
    );
    let leg = struct_array(
        vec![
            (
                "leg_id",
                DataType::Int64,
                i64_array(vec![Some(1), Some(2), Some(9)]),
            ),
            (
                "side",
                DataType::Utf8,
                utf8_array(vec![Some("Buy"), Some("Sell"), Some("Buy")]),
            ),
        ],
        None,
    );
    let legs = list_of(leg.data_type().clone(), [0, 2, 3], Arc::new(leg), None);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("Meta", meta.data_type().clone(), true),
            Field::new("Legs", legs.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1), Some(2)]), Arc::new(meta), legs],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(
        column_names(&frame),
        ["id", "Meta_account", "Legs_leg_id", "Legs_side"]
    );
    let table = collect_one(frame).await;
    assert_eq!(
        i64_cells(&table, "Legs_leg_id"),
        [Some(1), Some(2), Some(9)]
    );
    assert_utf8(&table, "Meta_account");
    assert_int64(&table, "Legs_leg_id");
}

#[tokio::test]
async fn dictionary_struct_is_unwrapped_one_level() {
    let values = struct_array(vec![("x", DataType::Int64, i64_array(vec![Some(7)]))], None);
    let keys = Int32Array::from(vec![Some(0)]);
    let dictionary = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).expect("dict");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "wrapped",
            dictionary.data_type().clone(),
            true,
        )])),
        vec![Arc::new(dictionary)],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(column_names(&frame), ["wrapped_x"]);
    let table = collect_one(frame).await;
    assert_eq!(i64_cells(&table, "wrapped_x"), [Some(7)]);
}

#[tokio::test]
async fn map_column_is_not_unnested() {
    let keys = StringArray::from(vec!["k"]);
    let items = StringArray::from(vec!["v"]);
    let entries = StructArray::try_new(
        Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ]),
        vec![Arc::new(keys), Arc::new(items)],
        None,
    )
    .expect("entries");
    let map = MapArray::try_new(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(entries.fields().clone()),
            false,
        )),
        OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
        entries,
        None,
        false,
    )
    .expect("map");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("m", map.data_type().clone(), true),
        ])),
        vec![i64_array(vec![Some(1)]), Arc::new(map)],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    assert_eq!(column_names(&frame), ["id", "m"]);
    let table = collect_one(frame).await;
    assert!(
        matches!(field_type(&table, "m"), DataType::Map(_, _)),
        "map column must stay a map"
    );
}

#[test]
fn empty_struct_only_schema_refuses() {
    let empty = StructArray::new_empty_fields(1, None);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "hollow",
            empty.data_type().clone(),
            true,
        )])),
        vec![Arc::new(empty)],
    )
    .expect("batch");

    analysis_token(
        dynamic_flatten(read_batch(batch), options()),
        "[DYNAMIC_FLATTEN_EMPTY_STRUCT]",
    );
}

#[tokio::test]
async fn scalar_array_inside_array_element_struct() {
    let nums = list_of(
        DataType::Int64,
        [0, 2, 2],
        i64_array(vec![Some(1), Some(2)]),
        Some(vec![true, false]),
    );
    let element = struct_array(
        vec![
            ("x", DataType::Utf8, utf8_array(vec![Some("p"), Some("q")])),
            ("nums", nums.data_type().clone(), nums),
        ],
        None,
    );
    let outer = list_of(element.data_type().clone(), [0, 2], Arc::new(element), None);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "a",
            outer.data_type().clone(),
            true,
        )])),
        vec![outer],
    )
    .expect("batch");

    let frame = flatten(batch, options());
    let table = collect_one(frame).await;
    assert_eq!(
        utf8_cells(&table, "a_x"),
        [
            Some("p".to_string()),
            Some("p".to_string()),
            Some("q".to_string())
        ]
    );
    assert_eq!(i64_cells(&table, "a_nums"), [Some(1), Some(2), None]);
    assert_int64(&table, "a_nums");
}
