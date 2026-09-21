use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, Int64Array};
use repark_core::ReparkSession;
use tempfile::TempDir;

use crate::{SparkDialect, SparkExtension};

const MOR: &str = ", 'write.delete.mode' = 'merge-on-read'";

async fn session(wh: &TempDir) -> ReparkSession {
    let warehouse = wh.path().to_str().unwrap().to_string();
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .build()
        .unwrap();
    session
        .register_memory_catalog("ice", &warehouse)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "ns",
            HashMap::from([("location".to_string(), format!("{warehouse}/ns"))]),
        )
        .await
        .unwrap();
    session
}

async fn run(session: &ReparkSession, sql: &str) {
    session.sql(sql).await.unwrap().collect().await.unwrap();
}

async fn seed(session: &ReparkSession, table: &str, part: &str, props: &str) {
    run(
        session,
        &format!(
            "CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg {part} \
             TBLPROPERTIES ('format-version' = '2'{props})"
        ),
    )
    .await;
    run(
        session,
        &format!("INSERT INTO {table} VALUES (1, 'a', 'x'), (2, 'b', 'y'), (4, 'd', 'x')"),
    )
    .await;
    run(
        session,
        &format!("INSERT INTO {table} VALUES (3, 'c', 'x')"),
    )
    .await;
    run(session, &format!("DELETE FROM {table} WHERE id = 1")).await;
}

async fn batches(
    session: &ReparkSession,
    sql: &str,
) -> Vec<datafusion::arrow::record_batch::RecordBatch> {
    session.sql(sql).await.unwrap().collect().await.unwrap()
}

async fn plan_error(session: &ReparkSession, sql: &str) -> String {
    match session.sql(sql).await {
        Ok(frame) => frame
            .collect()
            .await
            .expect_err("a refused query must fail")
            .to_string(),
        Err(error) => error.to_string(),
    }
}

fn i64s(batches: &[datafusion::arrow::record_batch::RecordBatch], col: usize) -> Vec<i64> {
    let mut out = Vec::new();
    for batch in batches {
        let array = batch
            .column(col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        out.extend((0..array.len()).map(|row| array.value(row)));
    }
    out
}

fn pairs_i64(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> Vec<(i64, i64)> {
    let mut out = Vec::new();
    for batch in batches {
        let left = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let right = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        out.extend((0..left.len()).map(|row| (left.value(row), right.value(row))));
    }
    out.sort_unstable();
    out
}

fn field_names(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> Vec<String> {
    batches[0]
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}

#[tokio::test]
async fn file_and_pos_answer_spark() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", "").await;

    let rows = batches(
        &session,
        "SELECT id, _file LIKE '%.parquet', _file LIKE '%/data/%' FROM ice.ns.t",
    )
    .await;
    let mut observed: Vec<(i64, bool, bool)> = Vec::new();
    for batch in &rows {
        let id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let parquet = batch
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let data_dir = batch
            .column(2)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        observed.extend((0..id.len()).map(|r| (id.value(r), parquet.value(r), data_dir.value(r))));
    }
    observed.sort_unstable();
    assert_eq!(
        observed,
        vec![(2, true, true), (3, true, true), (4, true, true)],
        "R-MC-FILE"
    );

    let distinct = batches(&session, "SELECT count(DISTINCT _file) FROM ice.ns.t").await;
    assert_eq!(i64s(&distinct, 0), vec![3], "R-MC-FILE-DISTINCT");

    let filtered = batches(
        &session,
        "SELECT count(*) FROM ice.ns.t WHERE _file IS NOT NULL",
    )
    .await;
    assert_eq!(i64s(&filtered, 0), vec![3], "R-MC-FILE-FILTER");

    let pos = batches(&session, "SELECT id, _pos FROM ice.ns.t").await;
    assert_eq!(pairs_i64(&pos), vec![(2, 0), (3, 0), (4, 0)], "R-MC-POS");
}

#[tokio::test]
async fn pos_is_the_file_position_after_a_merge_on_read_delete() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", MOR).await;
    let pos = batches(&session, "SELECT id, _pos FROM ice.ns.t").await;
    assert_eq!(
        pairs_i64(&pos),
        vec![(2, 0), (3, 0), (4, 1)],
        "R-MC-POS-MOR"
    );
}

#[tokio::test]
async fn select_star_excludes_every_served_metadata_column() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", "").await;
    let rows = batches(&session, "SELECT * FROM ice.ns.t").await;
    assert_eq!(
        field_names(&rows),
        vec!["id", "data", "cat"],
        "T-5 / R-MC-STAR-EXCLUDES"
    );
    let rows = batches(&session, "SELECT *, _file FROM ice.ns.t").await;
    assert_eq!(
        field_names(&rows),
        vec!["id", "data", "cat", "_file"],
        "T-5 / R-MC-STAR-EXCLUDES"
    );
    let rows = batches(&session, "SELECT *, _pos FROM ice.ns.t").await;
    assert_eq!(
        field_names(&rows),
        vec!["id", "data", "cat", "_pos"],
        "T-5 / R-MC-STAR-EXCLUDES"
    );
}

#[tokio::test]
async fn served_names_fold_and_composed_shapes_refuse() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", "").await;

    let upper = batches(&session, "SELECT id, _POS FROM ice.ns.t").await;
    assert_eq!(
        pairs_i64(&upper),
        vec![(2, 0), (3, 0), (4, 0)],
        "unquoted _POS folds to _pos"
    );

    let quoted = batches(&session, "SELECT id, `_pos` FROM ice.ns.t").await;
    assert_eq!(
        pairs_i64(&quoted),
        vec![(2, 0), (3, 0), (4, 0)],
        "backtick `_pos` resolves exact"
    );

    let aliased = batches(&session, "SELECT x._pos FROM ice.ns.t AS x").await;
    let mut ordinals = i64s(&aliased, 0);
    ordinals.sort_unstable();
    assert_eq!(ordinals, vec![0, 0, 0], "compound ident through an alias");

    let error = plan_error(&session, "SELECT `_spec_id` FROM ice.ns.t").await;
    assert!(
        error.contains("[ICE-MC-1]"),
        "backtick unserved refuses typed: {error}"
    );
    assert!(
        error.contains("_spec_id"),
        "backtick unserved names the column: {error}"
    );

    let error = plan_error(&session, "DELETE FROM ice.ns.t WHERE _file IS NOT NULL").await;
    assert!(
        error.contains("[ICE-MC-1]"),
        "metadata over a non-query refuses typed: {error}"
    );

    let error = plan_error(&session, "SELECT *, _file FROM ice.ns.t, ice.ns.t").await;
    assert!(
        error.contains("[ICE-MC-1]"),
        "star over two relations refuses typed: {error}"
    );
}

#[tokio::test]
async fn unserved_metadata_columns_refuse_with_a_typed_error() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", "").await;
    for column in ["_spec_id", "_partition", "_deleted"] {
        let error = plan_error(&session, &format!("SELECT {column} FROM ice.ns.t")).await;
        assert!(
            error.contains("[ICE-MC-1]"),
            "{column} must refuse typed, got: {error}"
        );
        assert!(
            !error.contains("No field named"),
            "{column} must not leak the raw planner error, got: {error}"
        );
        assert!(
            error.contains(column),
            "{column} refusal must name the column, got: {error}"
        );
    }
}
