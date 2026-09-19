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
async fn range_null_start_refuses_unexpected_input_type() {
    assert!(
        range_error("SELECT * FROM range(null)")
            .await
            .contains("UNEXPECTED_INPUT_TYPE")
    );
}

#[tokio::test]
async fn range_null_end_refuses_unexpected_input_type() {
    assert!(
        range_error("SELECT * FROM range(0, null)")
            .await
            .contains("UNEXPECTED_INPUT_TYPE")
    );
}

#[tokio::test]
async fn range_null_step_refuses_unexpected_input_type() {
    assert!(
        range_error("SELECT * FROM range(0, 10, null)")
            .await
            .contains("UNEXPECTED_INPUT_TYPE")
    );
}

#[tokio::test]
async fn range_cast_null_bigint_refuses_unexpected_input_type() {
    assert!(
        range_error("SELECT * FROM range(CAST(null AS BIGINT))")
            .await
            .contains("UNEXPECTED_INPUT_TYPE")
    );
}

#[tokio::test]
async fn range_cast_null_int_refuses_unexpected_input_type() {
    assert!(
        range_error("SELECT * FROM range(CAST(null AS INT))")
            .await
            .contains("UNEXPECTED_INPUT_TYPE")
    );
}

#[tokio::test]
async fn range_narrow_int_widths_answer_like_bigint() {
    assert_eq!(
        range_values("SELECT * FROM range(CAST(3 AS INT))").await,
        vec![0, 1, 2]
    );
    assert_eq!(
        range_values("SELECT * FROM range(CAST(3 AS SMALLINT))").await,
        vec![0, 1, 2]
    );
    assert_eq!(
        range_values("SELECT * FROM range(CAST(3 AS TINYINT))").await,
        vec![0, 1, 2]
    );
}

#[tokio::test]
async fn range_decimal_bound_truncates_toward_zero() {
    assert_eq!(
        range_values("SELECT * FROM range(3.0)").await,
        vec![0, 1, 2]
    );
    assert_eq!(
        range_values("SELECT * FROM range(-4, -1.5)").await,
        vec![-4, -3, -2]
    );
}

#[tokio::test]
async fn range_float_bound_truncates_toward_zero() {
    assert_eq!(
        range_values("SELECT * FROM range(CAST(3.9 AS DOUBLE))").await,
        vec![0, 1, 2]
    );
    assert_eq!(
        range_values("SELECT * FROM range(-4, CAST(-1.9 AS DOUBLE))").await,
        vec![-4, -3, -2]
    );
}

#[tokio::test]
async fn range_malformed_string_refuses_cast_invalid_input() {
    assert!(
        range_error("SELECT * FROM range('abc')")
            .await
            .contains("CAST_INVALID_INPUT")
    );
}

#[tokio::test]
async fn range_overflow_up_emits_exactly_one_row() {
    assert_eq!(
        range_values("SELECT * FROM range(9223372036854775802, 9223372036854775807, 10)").await,
        vec![9_223_372_036_854_775_802]
    );
}

#[tokio::test]
async fn range_overflow_down_emits_exactly_one_row() {
    assert_eq!(
        range_values("SELECT * FROM range(-9223372036854775807, -9223372036854775808, -2)").await,
        vec![-9_223_372_036_854_775_807]
    );
}

#[tokio::test]
async fn range_near_max_end_counts_every_row() {
    let values =
        range_values("SELECT * FROM range(9223372036854775800, 9223372036854775807)").await;
    assert_eq!(values.len(), 7);
    assert_eq!(values[0], 9_223_372_036_854_775_800);
    assert_eq!(values[6], 9_223_372_036_854_775_806);
}

#[tokio::test]
async fn range_near_min_start_counts_every_row() {
    let values =
        range_values("SELECT * FROM range(-9223372036854775808, -9223372036854775801)").await;
    assert_eq!(values.len(), 7);
    assert_eq!(values[0], -9_223_372_036_854_775_808);
    assert_eq!(values[6], -9_223_372_036_854_775_802);
}

#[tokio::test]
async fn range_limit_caps_emitted_rows() {
    assert_eq!(
        range_values("SELECT * FROM range(10) LIMIT 3").await,
        vec![0, 1, 2]
    );
}

#[tokio::test]
async fn range_projected_id_answers_values() {
    assert_eq!(range_values("SELECT id FROM range(3)").await, vec![0, 1, 2]);
}

#[tokio::test]
async fn range_zero_partitions_refuses_on_non_empty_range() {
    assert!(
        range_error("SELECT * FROM range(0, 3, 1, 0)")
            .await
            .contains("Positive number of partitions required")
    );
}

#[tokio::test]
async fn range_negative_partitions_refuses_on_non_empty_range() {
    assert!(
        range_error("SELECT * FROM range(0, 3, 1, -1)")
            .await
            .contains("Positive number of partitions required")
    );
}

#[tokio::test]
async fn range_zero_partitions_answers_empty_on_empty_range() {
    assert!(
        range_values("SELECT * FROM range(0, 0, 1, 0)")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn range_malformed_partitions_refuses_cast_invalid_input() {
    assert!(
        range_error("SELECT * FROM range(0, 3, 1, 'bogus')")
            .await
            .contains("CAST_INVALID_INPUT")
    );
}

#[tokio::test]
async fn range_null_partitions_refuses_unexpected_input_type() {
    assert!(
        range_error("SELECT * FROM range(0, 3, 1, null)")
            .await
            .contains("UNEXPECTED_INPUT_TYPE")
    );
}

#[tokio::test]
async fn range_volatile_partitions_refuses_non_literal() {
    assert!(
        range_error("SELECT * FROM range(0, 3, 1, rand())")
            .await
            .contains("Arguments must be literals")
    );
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
