//! SE-1 PR-D1: Spark-door execution-layer pin for `tightenNulls` on the `WindowSpec`
//! serving shape (nullable keys, Spark ASC → NULLS FIRST), plus the Iceberg-CREATE refuse.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use repark_core::{ReparkSession, SqlDialect};
use repark_spark::{SparkDialect, SparkExtension};
use tempfile::TempDir;

fn spark_session() -> ReparkSession {
    let dialect: Arc<dyn SqlDialect> = Arc::new(SparkDialect);
    ReparkSession::builder()
        .target_partitions(1)
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(dialect)
        .build()
        .unwrap()
}

fn nullable_sorted_rows(per_symbol: i64) -> RecordBatch {
    let mut symbols = Vec::new();
    let mut timestamps = Vec::new();
    let mut close = Vec::new();
    for symbol in ["AAA", "BBB"] {
        for tick in 0..per_symbol {
            symbols.push(symbol);
            timestamps.push(Some(tick));
            close.push(100.0 + f64::from(u32::try_from(tick).unwrap()));
        }
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
        Field::new("close", DataType::Float64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(symbols)),
            Arc::new(Int64Array::from(timestamps)),
            Arc::new(Float64Array::from(close)),
        ],
    )
    .unwrap()
}

fn keys() -> Vec<String> {
    vec!["symbol".to_string(), "ts".to_string()]
}

/// Spark-default window: `ORDER BY ts` is NULLS FIRST. This is the cell hint-mode cannot elide
/// over nullable keys.
const SERVING_WINDOW: &str =
    "SELECT symbol, ts, sum(close) OVER (PARTITION BY symbol ORDER BY ts) AS s FROM {table}";

async fn physical_plan_text(session: &ReparkSession, sql: &str) -> String {
    let frame = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("must plan `{sql}`: {error}"));
    let plan = frame
        .create_physical_plan()
        .await
        .unwrap_or_else(|error| panic!("physical plan `{sql}`: {error}"));
    datafusion::physical_plan::displayable(plan.as_ref())
        .indent(false)
        .to_string()
}

fn sort_exec_count(plan: &str) -> usize {
    plan.matches("SortExec").count()
}

#[tokio::test]
async fn tighten_elides_spark_default_window_over_nullable_keys() {
    let session = spark_session();
    let rows = nullable_sorted_rows(20_000);
    session
        .register_record_batches_as_temp_view("hint", rows.schema(), vec![rows.clone()])
        .unwrap();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("hint", &keys(), false)
        .await
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(), true)
        .await
        .unwrap();

    let hint_plan = physical_plan_text(&session, &SERVING_WINDOW.replace("{table}", "hint")).await;
    let tight_plan =
        physical_plan_text(&session, &SERVING_WINDOW.replace("{table}", "tight")).await;
    assert!(
        sort_exec_count(&hint_plan) >= 1,
        "hint mode must keep SortExec on Spark NULLS FIRST over nullable keys:\n{hint_plan}"
    );
    assert_eq!(
        sort_exec_count(&tight_plan),
        0,
        "tighten must elide SortExec on the serving shape:\n{tight_plan}"
    );
}

#[tokio::test]
async fn iceberg_create_of_tightened_frame_refuses_insert_into_existing_allowed() {
    let warehouse_dir = TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = spark_session();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();

    let rows = nullable_sorted_rows(4);
    session
        .register_record_batches_as_temp_view("plain", rows.schema(), vec![rows.clone()])
        .unwrap();
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(), true)
        .await
        .unwrap();

    let refused = session
        .sql("CREATE TABLE ice.sales.tightened AS SELECT * FROM tight")
        .await
        .expect_err("tightened CTAS must refuse");
    let message = refused.to_string();
    assert!(
        message.contains("tightenNulls"),
        "names the flag: {message}"
    );
    assert!(message.contains("PR-D2"), "names the follow-up: {message}");

    session
        .sql("CREATE TABLE ice.sales.bars AS SELECT * FROM plain")
        .await
        .expect("untightened CTAS must succeed");
    session
        .sql("INSERT INTO ice.sales.bars SELECT * FROM tight")
        .await
        .expect("INSERT into an existing table stays allowed")
        .collect()
        .await
        .expect("collect insert");
}

#[tokio::test]
async fn iceberg_create_from_derived_expression_over_tightened_source_refuses() {
    let warehouse_dir = TempDir::new().unwrap();
    let warehouse = warehouse_dir.path().to_str().unwrap().to_string();
    let session = spark_session();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "sales",
            HashMap::from([("location".to_string(), format!("{warehouse}/sales"))]),
        )
        .await
        .unwrap();
    let rows = nullable_sorted_rows(4);
    session
        .register_record_batches_as_temp_view("tight", rows.schema(), vec![rows])
        .unwrap();
    session
        .declare_temp_view_sorted("tight", &keys(), true)
        .await
        .unwrap();
    let refused = session
        .sql("CREATE TABLE ice.sales.derived AS SELECT ts + 1 AS ts2 FROM tight")
        .await
        .expect_err("derived-expression CTAS must refuse via the source walk");
    let message = refused.to_string();
    assert!(
        message.contains("tightenNulls"),
        "names the flag: {message}"
    );
}
