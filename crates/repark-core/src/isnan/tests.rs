use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, ListArray, MapArray,
    StringArray, StructArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::{Expr, SessionContext, col, lit};

use super::repark_isnan_call;

fn test_context() -> SessionContext {
    crate::ReparkSession::new()
        .expect("ReparkSession")
        .context()
        .clone()
}

fn batch(field: Field, column: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![column]).expect("batch")
}

async fn run(batch: RecordBatch, arg: Expr) -> Vec<bool> {
    let frame = test_context()
        .read_batch(batch)
        .expect("read_batch")
        .select(vec![repark_isnan_call(arg).alias("r")])
        .expect("select");
    let batches = frame.collect().await.expect("collect");
    let column = batches[0].column(0);
    let mask = column
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("bool");
    (0..mask.len()).map(|row| mask.value(row)).collect()
}

#[tokio::test]
async fn double_nan_mask() {
    let out = run(
        batch(
            Field::new("d", DataType::Float64, true),
            Arc::new(Float64Array::from(vec![Some(1.0), Some(f64::NAN), None])),
        ),
        col("d"),
    )
    .await;
    assert_eq!(out, vec![false, true, false]);
}

#[tokio::test]
async fn int_answers_false() {
    let out = run(
        batch(
            Field::new("i", DataType::Int32, true),
            Arc::new(Int32Array::from(vec![Some(1), None])),
        ),
        col("i"),
    )
    .await;
    assert_eq!(out, vec![false, false]);
}

#[tokio::test]
async fn date_answers_false_including_null() {
    let out = run(
        batch(
            Field::new("dt", DataType::Date32, true),
            Arc::new(Date32Array::from(vec![Some(0), None])),
        ),
        col("dt"),
    )
    .await;
    assert_eq!(out, vec![false, false]);
}

#[tokio::test]
async fn utf8_casts_then_tests() {
    let out = run(
        batch(
            Field::new("s", DataType::Utf8, true),
            Arc::new(StringArray::from(vec![Some("NaN"), Some("1.5"), None])),
        ),
        col("s"),
    )
    .await;
    assert_eq!(out, vec![true, false, false]);
}

#[tokio::test]
async fn utf8_malformed_refuses() {
    let frame = test_context()
        .read_batch(batch(
            Field::new("s", DataType::Utf8, true),
            Arc::new(StringArray::from(vec![Some("a")])),
        ))
        .expect("read_batch")
        .select(vec![repark_isnan_call(col("s")).alias("r")])
        .expect("select");
    let error = frame.collect().await.expect_err("collect must fail");
    assert!(error.to_string().contains("Cannot cast"), "{error}");
}

#[tokio::test]
async fn literal_nan_answers_true() {
    let out = run(
        batch(
            Field::new("i", DataType::Int32, true),
            Arc::new(Int32Array::from(vec![1])),
        ),
        lit(f64::NAN),
    )
    .await;
    assert_eq!(out, vec![true]);
}

async fn plan_err(batch: RecordBatch, arg: Expr) -> String {
    match test_context()
        .read_batch(batch)
        .expect("read_batch")
        .select(vec![repark_isnan_call(arg).alias("r")])
    {
        Err(error) => error.to_string(),
        Ok(frame) => match frame.collect().await {
            Err(error) => error.to_string(),
            Ok(_) => panic!("expected an error"),
        },
    }
}

#[tokio::test]
async fn struct_refuses_at_plan() {
    let st = StructArray::from(vec![
        (
            Arc::new(Field::new("a", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("b", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("x"), None])) as ArrayRef,
        ),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "st",
            DataType::Struct(st.fields().clone()),
            true,
        )])),
        vec![Arc::new(st) as ArrayRef],
    )
    .expect("batch");
    let error = plan_err(batch, col("st")).await;
    assert!(
        error.contains("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"),
        "pins: column-parity-1/C-009: {error}"
    );
    assert!(error.contains("isnan(st)"), "{error}");
    assert!(error.contains("STRUCT<a: INT, b: STRING>"), "{error}");
}

#[tokio::test]
async fn array_refuses_at_plan() {
    let arr = ListArray::from_iter_primitive::<Int32Type, Vec<Option<i32>>, _>(vec![
        Some(vec![Some(1), Some(2)]),
        None,
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "arr",
            arr.data_type().clone(),
            true,
        )])),
        vec![Arc::new(arr) as ArrayRef],
    )
    .expect("batch");
    let error = plan_err(batch, col("arr")).await;
    assert!(
        error.contains("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"),
        "pins: column-parity-1/C-009: {error}"
    );
    assert!(error.contains("isnan(arr)"), "{error}");
    assert!(error.contains("ARRAY<INT>"), "{error}");
}

#[tokio::test]
async fn map_refuses_at_plan() {
    let map = MapArray::new_from_strings(
        ["a", "b"].into_iter(),
        &Int32Array::from(vec![Some(1), Some(2)]),
        &[0, 1, 2],
    )
    .expect("map");
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "m",
            map.data_type().clone(),
            true,
        )])),
        vec![Arc::new(map) as ArrayRef],
    )
    .expect("batch");
    let error = plan_err(batch, col("m")).await;
    assert!(
        error.contains("DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE"),
        "{error}"
    );
    assert!(error.contains("isnan(m)"), "{error}");
    assert!(error.contains("MAP<STRING, INT>"), "{error}");
}
