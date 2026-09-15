use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
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
