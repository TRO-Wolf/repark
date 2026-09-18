use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::DataType;

use crate::ReparkSession;

async fn range_values(query: &str) -> Vec<i64> {
    let session = ReparkSession::new().unwrap();
    let batches = session.sql(query).await.unwrap().collect().await.unwrap();
    let mut values = Vec::new();
    for batch in &batches {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        values.extend((0..column.len()).map(|index| column.value(index)));
    }
    values
}

async fn range_error(query: &str) -> String {
    let session = ReparkSession::new().unwrap();
    match session.sql(query).await {
        Ok(frame) => match frame.collect().await {
            Ok(_) => panic!("{query} unexpectedly answered"),
            Err(error) => error.to_string(),
        },
        Err(error) => error.to_string(),
    }
}

#[tokio::test]
async fn range_schema_names_id_int64_non_null() {
    let session = ReparkSession::new().unwrap();
    let batches = session
        .sql("SELECT * FROM range(3)")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.len(), 1);
    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(schema.field(0).data_type(), &DataType::Int64);
    assert!(!schema.field(0).is_nullable());
}

#[tokio::test]
async fn range_single_arg_counts_from_zero() {
    assert_eq!(range_values("SELECT * FROM range(3)").await, vec![0, 1, 2]);
}

#[tokio::test]
async fn range_two_arg_form_counts_between_bounds() {
    assert_eq!(
        range_values("SELECT * FROM range(1, 4)").await,
        vec![1, 2, 3]
    );
}

#[tokio::test]
async fn range_three_arg_form_steps_exclusive() {
    assert_eq!(
        range_values("SELECT * FROM range(0, 10, 3)").await,
        vec![0, 3, 6, 9]
    );
}

#[tokio::test]
async fn range_fourth_argument_leaves_rows_unchanged() {
    assert_eq!(
        range_values("SELECT * FROM range(0, 10, 3, 2)").await,
        vec![0, 3, 6, 9]
    );
}

#[tokio::test]
async fn range_negative_step_counts_down() {
    assert_eq!(
        range_values("SELECT * FROM range(5, 0, -2)").await,
        vec![5, 3, 1]
    );
}

#[tokio::test]
async fn range_empty_when_start_meets_end() {
    assert!(range_values("SELECT * FROM range(0)").await.is_empty());
}

#[tokio::test]
async fn range_string_argument_coerces_to_integer() {
    assert_eq!(
        range_values("SELECT * FROM range('3')").await,
        vec![0, 1, 2]
    );
}

#[tokio::test]
async fn range_null_argument_yields_no_rows() {
    assert!(range_values("SELECT * FROM range(null)").await.is_empty());
}

#[tokio::test]
async fn range_zero_step_refuses() {
    assert!(
        range_error("SELECT * FROM range(0, 10, 0)")
            .await
            .contains("Step cannot be zero")
    );
}

#[tokio::test]
async fn range_without_arguments_refuses() {
    assert!(
        range_error("SELECT * FROM range()")
            .await
            .contains("requires 1 to 4 arguments")
    );
}

#[tokio::test]
async fn range_with_five_arguments_refuses() {
    assert!(
        range_error("SELECT * FROM range(0, 10, 1, 2, 3)")
            .await
            .contains("requires 1 to 4 arguments")
    );
}
