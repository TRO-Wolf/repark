use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int32Array, Int64Array, StringArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{Expr, SessionContext, col, lit};

use super::update_fields_call;

fn test_context() -> SessionContext {
    crate::ReparkSession::new()
        .expect("ReparkSession")
        .context()
        .clone()
}

fn struct_array(
    fields: &[(&str, DataType)],
    columns: Vec<ArrayRef>,
    validity: Option<NullBuffer>,
) -> StructArray {
    StructArray::try_new(
        Fields::from(
            fields
                .iter()
                .map(|(name, data_type)| {
                    Arc::new(Field::new(*name, data_type.clone(), true)) as FieldRef
                })
                .collect::<Vec<_>>(),
        ),
        columns,
        validity,
    )
    .expect("struct")
}

fn base_batch() -> RecordBatch {
    let st = struct_array(
        &[("a", DataType::Int32), ("b", DataType::Utf8)],
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("x"), None, None])) as ArrayRef,
        ],
        Some(NullBuffer::from(vec![true, false, true])),
    );
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "st",
            DataType::Struct(st.fields().clone()),
            true,
        )])),
        vec![Arc::new(st) as ArrayRef],
    )
    .expect("batch")
}

fn nested_batch() -> RecordBatch {
    let inner = struct_array(
        &[("x", DataType::Int32), ("y", DataType::Int32)],
        vec![
            Arc::new(Int32Array::from(vec![Some(2), None])) as ArrayRef,
            Arc::new(Int32Array::from(vec![Some(3), None])) as ArrayRef,
        ],
        Some(NullBuffer::from(vec![true, false])),
    );
    let st = struct_array(
        &[
            ("a", DataType::Int32),
            ("inner", DataType::Struct(inner.fields().clone())),
        ],
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
            Arc::new(inner) as ArrayRef,
        ],
        Some(NullBuffer::from(vec![true, true])),
    );
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "st",
            DataType::Struct(st.fields().clone()),
            true,
        )])),
        vec![Arc::new(st) as ArrayRef],
    )
    .expect("batch")
}

async fn run(batch: RecordBatch, args: Vec<Expr>) -> StructArray {
    let frame = test_context()
        .read_batch(batch)
        .expect("read_batch")
        .select(vec![update_fields_call(args).alias("r")])
        .expect("select");
    let batches = frame.collect().await.expect("collect");
    let column = batches[0].column(0).clone();
    let Some(out) = column.as_struct_opt() else {
        panic!("expected struct output");
    };
    out.clone()
}

async fn run_err(batch: RecordBatch, args: Vec<Expr>) -> String {
    match test_context()
        .read_batch(batch)
        .expect("read_batch")
        .select(vec![update_fields_call(args).alias("r")])
    {
        Err(error) => error.to_string(),
        Ok(frame) => match frame.collect().await {
            Err(error) => error.to_string(),
            Ok(_) => panic!("expected an error"),
        },
    }
}

fn int_column(struct_array: &StructArray, index: usize) -> Vec<Option<i64>> {
    let column = struct_array.column(index);
    if let Some(array) = column.as_any().downcast_ref::<Int64Array>() {
        return array.iter().collect();
    }
    let array = column
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("int column");
    array.iter().map(|value| value.map(i64::from)).collect()
}

fn field_names(fields: &Fields) -> Vec<String> {
    fields.iter().map(|field| field.name().clone()).collect()
}

#[tokio::test]
async fn with_appends_new_field() {
    let out = run(
        base_batch(),
        vec![col("st"), lit("with"), lit("c"), lit(9_i32)],
    )
    .await;
    assert_eq!(
        field_names(out.fields()),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    assert_eq!(int_column(&out, 2), vec![Some(9), None, Some(9)]);
    assert!(out.is_null(1));
}

#[tokio::test]
async fn with_replaces_existing_keeps_position() {
    let out = run(
        base_batch(),
        vec![col("st"), lit("with"), lit("b"), lit(7_i32)],
    )
    .await;
    assert_eq!(
        field_names(out.fields()),
        vec!["a".to_string(), "b".to_string()]
    );
    assert_eq!(out.fields()[1].data_type(), &DataType::Int32);
    assert_eq!(int_column(&out, 1), vec![Some(7), None, Some(7)]);
}

#[tokio::test]
async fn with_new_spelling_wins_case_insensitive() {
    let out = run(
        base_batch(),
        vec![col("st"), lit("with"), lit("A"), lit(5_i32)],
    )
    .await;
    assert_eq!(
        field_names(out.fields()),
        vec!["A".to_string(), "b".to_string()]
    );
}

#[tokio::test]
async fn sequential_replace_after_add_keeps_one() {
    let out = run(
        base_batch(),
        vec![
            col("st"),
            lit("with"),
            lit("c"),
            lit(1_i32),
            lit("with"),
            lit("c"),
            lit(2_i32),
        ],
    )
    .await;
    assert_eq!(
        field_names(out.fields()),
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
    assert_eq!(int_column(&out, 2), vec![Some(2), None, Some(2)]);
}

#[tokio::test]
async fn drop_after_add_removes() {
    let out = run(
        base_batch(),
        vec![
            col("st"),
            lit("with"),
            lit("c"),
            lit(9_i32),
            lit("drop"),
            lit("c"),
        ],
    )
    .await;
    assert_eq!(
        field_names(out.fields()),
        vec!["a".to_string(), "b".to_string()]
    );
}

#[tokio::test]
async fn add_after_drop_appends_at_end() {
    let out = run(
        base_batch(),
        vec![
            col("st"),
            lit("drop"),
            lit("a"),
            lit("with"),
            lit("a"),
            lit(9_i32),
        ],
    )
    .await;
    assert_eq!(
        field_names(out.fields()),
        vec!["b".to_string(), "a".to_string()]
    );
    assert_eq!(int_column(&out, 1), vec![Some(9), None, Some(9)]);
}

#[tokio::test]
async fn drop_missing_is_noop() {
    let out = run(base_batch(), vec![col("st"), lit("drop"), lit("zz")]).await;
    assert_eq!(
        field_names(out.fields()),
        vec!["a".to_string(), "b".to_string()]
    );
}

#[tokio::test]
async fn drop_case_insensitive() {
    let out = run(base_batch(), vec![col("st"), lit("drop"), lit("A")]).await;
    assert_eq!(field_names(out.fields()), vec!["b".to_string()]);
}

#[tokio::test]
async fn nested_with_appends_inside_inner() {
    let out = run(
        nested_batch(),
        vec![col("st"), lit("with"), lit("inner.z"), lit(7_i32)],
    )
    .await;
    let inner = out.column(1).as_struct_opt().expect("inner struct");
    assert_eq!(
        field_names(inner.fields()),
        vec!["x".to_string(), "y".to_string(), "z".to_string()]
    );
    assert_eq!(int_column(inner, 2), vec![Some(7), None]);
    assert!(inner.is_null(1));
}

#[tokio::test]
async fn nested_drop_removes_inside_inner() {
    let out = run(nested_batch(), vec![col("st"), lit("drop"), lit("inner.x")]).await;
    let inner = out.column(1).as_struct_opt().expect("inner struct");
    assert_eq!(field_names(inner.fields()), vec!["y".to_string()]);
}

#[tokio::test]
async fn nested_missing_parent_with_refuses() {
    let error = run_err(
        nested_batch(),
        vec![col("st"), lit("with"), lit("nope.z"), lit(7_i32)],
    )
    .await;
    assert!(error.contains("FIELD_NOT_FOUND"), "{error}");
    assert!(error.contains("`nope`"), "{error}");
}

#[tokio::test]
async fn nested_missing_parent_drop_is_noop() {
    let out = run(nested_batch(), vec![col("st"), lit("drop"), lit("nope.z")]).await;
    assert_eq!(
        field_names(out.fields()),
        vec!["a".to_string(), "inner".to_string()]
    );
}

#[tokio::test]
async fn drop_all_top_level_refuses() {
    let error = run_err(
        base_batch(),
        vec![col("st"), lit("drop"), lit("a"), lit("drop"), lit("b")],
    )
    .await;
    assert!(
        error.contains("DATATYPE_MISMATCH.CANNOT_DROP_ALL_FIELDS"),
        "{error}"
    );
    assert!(
        error.contains("update_fields(st, dropfield(), dropfield())"),
        "{error}"
    );
}

#[tokio::test]
async fn non_struct_input_refuses() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, true)])),
        vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
    )
    .expect("batch");
    let error = run_err(batch, vec![col("i"), lit("with"), lit("a"), lit(1_i32)]).await;
    assert!(
        error.contains("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"),
        "{error}"
    );
    assert!(error.contains("update_fields(i, WithField(1))"), "{error}");
    assert!(error.contains("\"i\" has the type \"INT\""), "{error}");
}

#[tokio::test]
async fn duplicate_fields_all_replaced() {
    let st = struct_array(
        &[("a", DataType::Int32), ("a", DataType::Int32)],
        vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(Int32Array::from(vec![2])) as ArrayRef,
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "st",
            DataType::Struct(st.fields().clone()),
            true,
        )])),
        vec![Arc::new(st) as ArrayRef],
    )
    .expect("batch");
    let out = run(batch, vec![col("st"), lit("with"), lit("a"), lit(3_i32)]).await;
    assert_eq!(
        field_names(out.fields()),
        vec!["a".to_string(), "a".to_string()]
    );
    assert_eq!(int_column(&out, 0), vec![Some(3)]);
    assert_eq!(int_column(&out, 1), vec![Some(3)]);
}

#[tokio::test]
async fn empty_name_field_substitutes_col_index() {
    let out = run(
        base_batch(),
        vec![col("st"), lit("with"), lit(""), lit(3_i32)],
    )
    .await;
    assert_eq!(
        field_names(out.fields()),
        vec!["a".to_string(), "b".to_string(), "col2".to_string()]
    );
}

#[tokio::test]
async fn null_top_level_struct_stays_null() {
    let out = run(
        base_batch(),
        vec![col("st"), lit("with"), lit("c"), lit(9_i32)],
    )
    .await;
    assert!(out.is_valid(0));
    assert!(out.is_null(1));
    assert!(out.is_valid(2));
}

#[tokio::test]
async fn null_parent_child_values_masked() {
    let out = run(
        base_batch(),
        vec![col("st"), lit("with"), lit("c"), lit(9_i32)],
    )
    .await;
    assert!(out.column(2).is_valid(0));
    assert!(out.column(2).is_null(1));
    assert!(out.column(2).is_valid(2));
}

#[tokio::test]
async fn null_intermediate_child_values_masked() {
    let out = run(
        nested_batch(),
        vec![col("st"), lit("with"), lit("inner.z"), lit(7_i32)],
    )
    .await;
    let inner = out.column(1).as_struct_opt().expect("inner struct");
    assert!(inner.column(2).is_valid(0));
    assert!(inner.column(2).is_null(1));
}
