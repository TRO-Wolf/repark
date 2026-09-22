use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, Int32Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
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

async fn v3_session(wh: &TempDir) -> ReparkSession {
    let warehouse = wh.path().to_str().unwrap().to_string();
    let session = ReparkSession::builder()
        .with_extension(Arc::new(SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .config("repark.sql.allowCreateFormatVersion3", "true")
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

fn pairs_i64_i32(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> Vec<(i64, i32)> {
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
            .downcast_ref::<Int32Array>()
            .unwrap();
        out.extend((0..left.len()).map(|row| (left.value(row), right.value(row))));
    }
    out.sort_unstable();
    out
}

fn strings(batches: &[datafusion::arrow::record_batch::RecordBatch], col: usize) -> Vec<String> {
    use datafusion::arrow::array::StringArray;
    let mut out = Vec::new();
    for batch in batches {
        let array = batch
            .column(col)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8 column");
        out.extend((0..array.len()).map(|row| array.value(row).to_string()));
    }
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
async fn file_values_equal_the_files_metadata_table_paths() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", "").await;
    let live = batches(&session, "SELECT DISTINCT _file FROM ice.ns.t").await;
    let mut live_files = strings(&live, 0);
    live_files.sort();
    let meta = batches(&session, "SELECT file_path FROM `ice`.`ns`.`t`.`files`").await;
    let mut meta_files = strings(&meta, 0);
    meta_files.sort();
    assert_eq!(live_files, meta_files, "R-MC-FILE-IDENTITY");
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
async fn spec_id_answers_zero_on_a_single_spec_table() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", "").await;
    let rows = batches(&session, "SELECT id, _spec_id FROM ice.ns.t").await;
    assert_eq!(
        rows[0].schema().field(1).data_type(),
        &DataType::Int32,
        "R-MC-SPEC-ID type"
    );
    assert_eq!(
        pairs_i64_i32(&rows),
        vec![(2, 0), (3, 0), (4, 0)],
        "R-MC-SPEC-ID"
    );
}

#[tokio::test]
async fn spec_id_reports_each_rows_own_spec_after_evolution() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    run(
        &session,
        "CREATE TABLE ice.ns.tevo (id BIGINT, cat STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2')",
    )
    .await;
    run(&session, "INSERT INTO ice.ns.tevo VALUES (1, 'x')").await;
    run(&session, "ALTER TABLE ice.ns.tevo ADD PARTITION FIELD cat").await;
    run(&session, "INSERT INTO ice.ns.tevo VALUES (2, 'y')").await;
    let rows = batches(&session, "SELECT id, _spec_id FROM ice.ns.tevo").await;
    assert_eq!(
        pairs_i64_i32(&rows),
        vec![(1, 0), (2, 1)],
        "R-MC-SPEC-ID-EVO spec-id half"
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
    let rows = batches(&session, "SELECT *, _spec_id FROM ice.ns.t").await;
    assert_eq!(
        field_names(&rows),
        vec!["id", "data", "cat", "_spec_id"],
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

    let error = plan_error(&session, "SELECT `_partition` FROM ice.ns.t").await;
    assert!(
        error.contains("[ICE-MC-1]"),
        "backtick unserved refuses typed: {error}"
    );
    assert!(
        error.contains("_partition"),
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
async fn file_and_row_id_answer_together_on_a_format_v3_table() {
    let wh = TempDir::new().unwrap();
    let session = v3_session(&wh).await;
    run(
        &session,
        "CREATE TABLE ice.ns.tv3 (id BIGINT, data STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
    )
    .await;
    run(&session, "INSERT INTO ice.ns.tv3 VALUES (1, 'a'), (2, 'b')").await;
    let rows = batches(&session, "SELECT _file, _row_id FROM ice.ns.tv3").await;
    assert_eq!(field_names(&rows), vec!["_file", "_row_id"], "R-MC-V3-BOTH");
    let files = strings(&rows, 0);
    let mut ordinals = i64s(&rows, 1);
    ordinals.sort_unstable();
    assert_eq!(files.len(), 2, "R-MC-V3-BOTH");
    assert!(
        files.iter().all(|file| file.ends_with(".parquet")),
        "R-MC-V3-BOTH: {files:?}"
    );
    assert_eq!(ordinals, vec![0, 1], "R-MC-V3-BOTH");
}

#[tokio::test]
async fn unserved_metadata_columns_refuse_with_a_typed_error() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", "").await;
    for column in ["_partition", "_deleted"] {
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
        assert!(
            error.contains("this layer serves (_file, _pos, _spec_id)"),
            "{column} refusal must advertise the served three, got: {error}"
        );
    }
}

#[tokio::test]
async fn served_spec_id_beside_an_unserved_column_names_the_unserved_one() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", "").await;
    let error = plan_error(&session, "SELECT id, _spec_id, _partition FROM ice.ns.t").await;
    assert!(
        error.contains("[ICE-MC-1]"),
        "R-MC-SPEC-ID-EVO still refuses typed, got: {error}"
    );
    assert!(
        error.contains("metadata column _partition is not yet served"),
        "R-MC-SPEC-ID-EVO refusal must name _partition, got: {error}"
    );
    assert!(
        !error.contains("metadata column _spec_id"),
        "R-MC-SPEC-ID-EVO refusal must not blame _spec_id, got: {error}"
    );
}
