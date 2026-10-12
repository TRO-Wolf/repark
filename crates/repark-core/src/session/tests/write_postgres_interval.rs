use std::sync::Arc;

use arrow::array::{Array, ArrayRef, IntervalMonthDayNanoArray, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, IntervalMonthDayNano, Schema};
use datafusion::error::DataFusionError;
use repark_connect::postgres::PostgresMapping;

use crate::session::write_postgres::{ShapedColumn, shape_batch};

fn shape_single(
    mapping: PostgresMapping,
    expected: DataType,
    array: ArrayRef,
) -> Result<RecordBatch, DataFusionError> {
    let field = Field::new("v", array.data_type().clone(), true);
    let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array])
        .expect("a single-column batch builds");
    shape_batch(
        &batch,
        &[ShapedColumn::for_test("v", mapping, expected)],
        false,
        "UTC",
        "pg",
    )
}

fn interval(values: &[IntervalMonthDayNano]) -> ArrayRef {
    Arc::new(IntervalMonthDayNanoArray::from(values.to_vec()))
}

fn stored_text(shaped: &RecordBatch) -> Vec<String> {
    shaped
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("an interval column stores text")
        .iter()
        .map(|value| value.expect("no null is shaped here").to_string())
        .collect()
}

#[test]
fn interval_values_into_an_interval_column_store_as_arrow_text() {
    let shaped = shape_single(
        PostgresMapping::ServerText,
        DataType::Utf8,
        interval(&[
            IntervalMonthDayNano::new(0, 1, 0),
            IntervalMonthDayNano::new(3, 0, 0),
            IntervalMonthDayNano::new(0, 0, -5 * 3_600_000_000_000),
            IntervalMonthDayNano::new(14, 3, 14_706_500_000_000),
        ]),
    )
    .expect("an engine interval stores into an interval column");
    assert_eq!(
        stored_text(&shaped),
        [
            "1 days",
            "3 mons",
            "-5 hours",
            "14 mons 3 days 4 hours 5 mins 6.500000000 secs",
        ]
    );
}

#[test]
fn interval_values_into_a_text_column_still_refuse() {
    let error = shape_single(
        PostgresMapping::Utf8,
        DataType::Utf8,
        interval(&[IntervalMonthDayNano::new(0, 1, 0)]),
    )
    .expect_err("an interval into a text column refuses");
    let message = error.to_string();
    assert!(message.contains("cannot store-assign"), "{message}");
    assert!(
        message.contains("INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST"),
        "{message}"
    );
    assert!(message.contains("`v`"), "{message}");
}
