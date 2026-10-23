use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Int32Type, Int64Type};
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;

fn ansi_door() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("native ansi session")
}

async fn answer(session: &ReparkSession, query: &str) -> Vec<RecordBatch> {
    let frame = session
        .sql(query)
        .await
        .unwrap_or_else(|error| panic!("the ANSI door plans {query}: {error}"));
    let joined = tokio::spawn(async move { frame.collect().await }).await;
    let collected = joined.unwrap_or_else(|_| panic!("the ANSI door panicked on {query}"));
    collected.unwrap_or_else(|error| panic!("the ANSI door answers {query}: {error}"))
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn map_entries(batch: &RecordBatch, row: usize) -> Vec<(String, i64)> {
    let entries = batch.column(0).as_map().value(row);
    let keys = entries.column(0).as_string::<i32>();
    let values = entries.column(1);
    (0..entries.len())
        .map(|index| {
            let value = match values.data_type() {
                DataType::Int32 => i64::from(values.as_primitive::<Int32Type>().value(index)),
                _ => values.as_primitive::<Int64Type>().value(index),
            };
            (keys.value(index).to_string(), value)
        })
        .collect()
}

#[tokio::test]
async fn a_map_over_a_column_answers_one_row_instead_of_panicking() {
    let batches = answer(&ansi_door(), "SELECT map('a', id) AS m FROM range(1)").await;
    assert_eq!(rows(&batches), 1);
    assert_eq!(map_entries(&batches[0], 0), vec![("a".to_string(), 0)]);
}

#[tokio::test]
async fn a_two_pair_map_over_three_rows_answers_instead_of_raising_the_length_error() {
    let batches = answer(
        &ansi_door(),
        "SELECT map('a', id, 'b', id + 1) AS m FROM range(3) ORDER BY id",
    )
    .await;
    assert_eq!(rows(&batches), 3);
    let answered: Vec<Vec<(String, i64)>> = batches
        .iter()
        .flat_map(|batch| (0..batch.num_rows()).map(|row| map_entries(batch, row)))
        .collect();
    for (id, entries) in (0_i64..).zip(&answered) {
        assert_eq!(
            entries,
            &vec![("a".to_string(), id), ("b".to_string(), id + 1)]
        );
    }
}

#[tokio::test]
async fn a_constant_map_keeps_the_datafusion_constructor() {
    let batches = answer(&ansi_door(), "SELECT map('a', 1) AS m").await;
    assert_eq!(map_entries(&batches[0], 0), vec![("a".to_string(), 1)]);
}
