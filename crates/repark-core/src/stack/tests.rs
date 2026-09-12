use arrow::array::{Array, Int64Array, StringArray};
use datafusion::prelude::SessionContext;

use super::{apply_stack, parse_stack_n};
use crate::ReparkSession;

#[test]
fn parse_stack_n_rejects_zero_and_negative() {
    assert!(parse_stack_n(0).is_err());
    assert!(parse_stack_n(-1).is_err());
    assert_eq!(parse_stack_n(2).expect("n=2"), 2);
}

#[tokio::test]
async fn stack_reshapes_four_columns_into_two_rows() {
    let session = ReparkSession::builder().build().expect("session");
    let frame = session
        .sql("SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d")
        .await
        .expect("values");
    let stacked = apply_stack(frame, 2, 0, None).expect("stack");
    let batches = stacked.collect().await.expect("collect");
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "col0");
    assert_eq!(batch.schema().field(1).name(), "col1");
    let col0 = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("col0");
    let col1 = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("col1");
    assert_eq!(col0.values(), &[1, 3]);
    assert_eq!(col1.values(), &[2, 4]);
}

#[tokio::test]
async fn stack_pads_a_short_last_row() {
    let session = ReparkSession::builder().build().expect("session");
    let frame = session
        .sql("SELECT 1 AS a, 2 AS b, 3 AS c")
        .await
        .expect("values");
    let stacked = apply_stack(frame, 2, 0, None).expect("stack");
    let batches = stacked.collect().await.expect("collect");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    let col0 = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("col0");
    let col1 = batch
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("col1");
    assert_eq!(col0.value(0), 1);
    assert_eq!(col1.value(0), 2);
    assert_eq!(col0.value(1), 3);
    assert!(col1.is_null(1));
}

#[tokio::test]
async fn stack_passthrough_repeats_leading_columns() {
    let session = ReparkSession::builder().build().expect("session");
    let frame = session
        .sql("SELECT 9 AS id, 10 AS a, 20 AS b, 30 AS c, 40 AS d")
        .await
        .expect("values");
    let stacked = apply_stack(frame, 2, 1, None).expect("stack");
    let batches = stacked.collect().await.expect("collect");
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(batch.schema().field(0).name(), "id");
    let id = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id");
    assert_eq!(id.values(), &[9, 9]);
}

#[tokio::test]
async fn stack_sql_rewrite_is_unsupported_until_the_spark_door_analyzer() {
    let context = SessionContext::new();
    crate::stack::register_stack(&context);
    let error = context
        .sql("SELECT stack(2, 1, 2, 3, 4)")
        .await
        .expect("logical")
        .collect()
        .await
        .expect_err("udf invoke");
    assert!(
        error.to_string().contains("stack") || error.to_string().contains("Unpivot"),
        "{error}"
    );
}

#[tokio::test]
async fn stack_mixed_independent_column_types() {
    let session = ReparkSession::builder().build().expect("session");
    let frame = session
        .sql("SELECT 1 AS a, 'x' AS b, 2 AS c, 'y' AS d")
        .await
        .expect("values");
    let stacked = apply_stack(frame, 2, 0, None).expect("stack");
    let batches = stacked.collect().await.expect("collect");
    let batch = &batches[0];
    let col0 = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("col0");
    let col1 = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("col1");
    assert_eq!(col0.values(), &[1, 2]);
    assert_eq!(col1.value(0), "x");
    assert_eq!(col1.value(1), "y");
}

#[tokio::test]
async fn stack_same_output_column_type_mismatch_is_loud() {
    let session = ReparkSession::builder().build().expect("session");
    let frame = session
        .sql("SELECT 1 AS a, 2 AS b, 'x' AS c, 'y' AS d")
        .await
        .expect("values");
    let error = apply_stack(frame, 2, 0, None).expect_err("type mismatch");
    assert!(
        error.to_string().contains("STACK_COLUMN_DIFF_TYPES"),
        "{error}"
    );
}
