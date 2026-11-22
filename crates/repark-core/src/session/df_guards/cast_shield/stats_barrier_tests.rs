use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::parquet::arrow::ArrowWriter;

use super::stats_barrier::{STATS_BARRIER_NAME, barrier_plan, keeps_order};
use crate::ReparkSession;

#[test]
fn order_keeping_casts_follow_the_value_order() {
    let utc = Some(Arc::from("UTC"));
    let cases = [
        (DataType::Utf8, DataType::Int32, false),
        (DataType::Utf8View, DataType::Int64, false),
        (DataType::Int64, DataType::Utf8, false),
        (DataType::Int64, DataType::Boolean, false),
        (DataType::Date32, DataType::Utf8, false),
        (DataType::Utf8, DataType::Date32, false),
        (
            DataType::Timestamp(TimeUnit::Microsecond, utc.clone()),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, utc.clone()),
            DataType::Date32,
            false,
        ),
        (DataType::Int32, DataType::Int64, true),
        (DataType::Int64, DataType::Int32, true),
        (DataType::Boolean, DataType::Int32, true),
        (DataType::Int32, DataType::Float64, true),
        (DataType::Decimal128(10, 2), DataType::Int32, true),
        (DataType::Utf8, DataType::Utf8View, true),
        (
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Utf8,
            true,
        ),
        (
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        (
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Date32,
            true,
        ),
        (
            DataType::Timestamp(TimeUnit::Second, utc.clone()),
            DataType::Timestamp(TimeUnit::Microsecond, utc),
            true,
        ),
    ];
    for (source, target, expected) in cases {
        assert_eq!(
            keeps_order(&source, &target),
            expected,
            "{source} -> {target}"
        );
    }
}

fn write_ids(dir: &Path, ids: &[&str]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
        Field::new("n", DataType::Int32, true),
    ]));
    let count = i64::try_from(ids.len()).expect("a small fixture");
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(ids.to_vec())),
            Arc::new(Int64Array::from_iter_values(1..=count)),
            Arc::new(Int32Array::from_iter_values(
                (1..=count).map(|value| i32::try_from(value).unwrap_or(i32::MAX)),
            )),
        ],
    )
    .expect("the id batch");
    let file = std::fs::File::create(dir.join("part-0.parquet")).expect("create the file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("open the writer");
    writer.write(&batch).expect("write the batch");
    writer.close().expect("close the writer");
}

async fn session_over(ids: &[&str]) -> (ReparkSession, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("a temp dir");
    write_ids(dir.path(), ids);
    let session = ReparkSession::new().expect("a session");
    let location = format!("{}/", dir.path().to_str().expect("a utf-8 path"));
    session
        .sql(&format!(
            "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION '{location}'"
        ))
        .await
        .expect("plan the table")
        .collect()
        .await
        .expect("create the table");
    (session, dir)
}

async fn int_rows(session: &ReparkSession, sql: &str) -> Vec<Vec<Option<i64>>> {
    let batches = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let mut rows = Vec::new();
    for batch in &batches {
        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                datafusion::arrow::compute::cast(column, &DataType::Int64).expect("an int column")
            })
            .collect::<Vec<_>>();
        for row in 0..batch.num_rows() {
            rows.push(
                columns
                    .iter()
                    .map(|column| {
                        let values = column
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("an int64 column");
                        values.is_valid(row).then(|| values.value(row))
                    })
                    .collect(),
            );
        }
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn string_ids_out_of_numeric_order_answer_every_shape() {
    let (session, _dir) = session_over(&["10", "3", "25", "7", "04"]).await;
    let cases: [(&str, Vec<Vec<Option<i64>>>); 4] = [
        (
            "SELECT max(CAST(id AS INT)), min(CAST(id AS INT)) FROM t",
            vec![vec![Some(25), Some(3)]],
        ),
        (
            "SELECT max(k), min(k) FROM (SELECT CAST(id AS INT) AS k FROM t)",
            vec![vec![Some(25), Some(3)]],
        ),
        (
            "SELECT k FROM (SELECT CAST(id AS INT) AS k FROM t) WHERE k BETWEEN 4 AND 20",
            vec![vec![Some(4)], vec![Some(7)], vec![Some(10)]],
        ),
        (
            "SELECT k FROM (SELECT CAST(id AS INT) AS k, row_number() OVER \
             (PARTITION BY CAST(id AS INT) ORDER BY v DESC) AS rn FROM t) WHERE rn = 1",
            vec![
                vec![Some(3)],
                vec![Some(4)],
                vec![Some(7)],
                vec![Some(10)],
                vec![Some(25)],
            ],
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(int_rows(&session, sql).await, expected, "{sql}");
    }
}

#[tokio::test]
async fn an_order_keeping_cast_keeps_its_statistics() {
    let (session, _dir) = session_over(&["10", "3"]).await;
    let sql = "SELECT max(k), min(k) FROM (SELECT CAST(n AS BIGINT) AS k FROM t)";
    let plan = session
        .sql(&format!("EXPLAIN {sql}"))
        .await
        .expect("explain")
        .collect()
        .await
        .expect("explain rows");
    let text = datafusion::arrow::util::pretty::pretty_format_batches(&plan)
        .expect("render")
        .to_string();
    assert!(!text.contains(STATS_BARRIER_NAME), "{text}");
    assert!(!text.contains("AggregateExec"), "{text}");
    assert_eq!(int_rows(&session, sql).await, vec![vec![Some(2), Some(1)]]);
}

#[tokio::test]
async fn the_barrier_keeps_the_select_list_names_and_is_idempotent() {
    let (session, _dir) = session_over(&["10", "3"]).await;
    let state = session.context().state();
    let sql = "SELECT CAST(id AS INT), CAST(n AS BIGINT), CAST(v AS VARCHAR) FROM t";
    let logical = state.create_logical_plan(sql).await.expect("plan");
    let optimized = state.optimize(&logical).expect("optimize");
    assert_eq!(
        optimized.schema().field_names(),
        logical.schema().field_names()
    );
    let text = optimized.display_indent().to_string();
    assert_eq!(text.matches(STATS_BARRIER_NAME).count(), 2, "{text}");
    assert!(!barrier_plan(optimized).expect("re-run").transformed);
}
