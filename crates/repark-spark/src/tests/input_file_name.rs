use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, Int64Array, StringArray};
use repark_core::{ErrorClass, ReparkSession};
use tempfile::TempDir;

use super::metadata_columns_deleted::refusal;
use crate::{SparkDialect, SparkExtension};

async fn session(wh: &TempDir) -> ReparkSession {
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

async fn seed(session: &ReparkSession, table: &str) {
    run(
        session,
        &format!(
            "CREATE TABLE {table} (id BIGINT, data STRING, cat STRING) USING iceberg \
             PARTITIONED BY (cat) TBLPROPERTIES ('format-version' = '2')"
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
    out.sort_unstable();
    out
}

fn id_bool_pairs(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> Vec<(i64, bool)> {
    let mut out = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let flags = batch
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        out.extend((0..ids.len()).map(|row| (ids.value(row), flags.value(row))));
    }
    out.sort_unstable();
    out
}

fn strings(batches: &[datafusion::arrow::record_batch::RecordBatch], col: usize) -> Vec<String> {
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
async fn input_file_name_like_parquet_answers_true_on_every_row() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;

    let rows = batches(
        &session,
        "SELECT id, input_file_name() LIKE '%.parquet' FROM ice.ns.t",
    )
    .await;
    assert_eq!(
        id_bool_pairs(&rows),
        vec![(2, true), (3, true), (4, true)],
        "R-INPUT-FILE-NAME"
    );

    let rows = batches(
        &session,
        "SELECT id, concat('p:', input_file_name()) = concat('p:', _file) FROM ice.ns.t",
    )
    .await;
    assert_eq!(
        id_bool_pairs(&rows),
        vec![(2, true), (3, true), (4, true)],
        "R-INPUT-FILE-NAME inside a function argument compares the full path"
    );

    let rewritten = strings(
        &batches(
            &session,
            "SELECT concat('p:', input_file_name()) FROM ice.ns.t ORDER BY id",
        )
        .await,
        0,
    );
    let prefixed = strings(
        &batches(
            &session,
            "SELECT concat('p:', _file) FROM ice.ns.t ORDER BY id",
        )
        .await,
        0,
    );
    assert_eq!(
        rewritten, prefixed,
        "a scalar argument preserves the full _file value row by row"
    );
}

#[tokio::test]
async fn input_file_name_equals_file_on_every_row() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;

    let rows = batches(
        &session,
        "SELECT id, input_file_name() = _file FROM ice.ns.t",
    )
    .await;
    assert_eq!(
        id_bool_pairs(&rows),
        vec![(2, true), (3, true), (4, true)],
        "input_file_name() equals _file per row"
    );

    let rows = batches(
        &session,
        "SELECT id, input_file_name() = _file FROM ice.ns.t AS x",
    )
    .await;
    assert_eq!(
        id_bool_pairs(&rows),
        vec![(2, true), (3, true), (4, true)],
        "input_file_name() qualifies through the user's own alias"
    );

    let rows = batches(
        &session,
        "SELECT id, input_file_name() = _file FROM ice.ns.t AS t",
    )
    .await;
    assert_eq!(
        id_bool_pairs(&rows),
        vec![(2, true), (3, true), (4, true)],
        "a user alias equal to the table's own name still qualifies"
    );
}

#[tokio::test]
async fn input_file_name_upper_case_folds() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;

    let rows = batches(
        &session,
        "SELECT id, INPUT_FILE_NAME() = _file FROM ice.ns.t",
    )
    .await;
    assert_eq!(
        id_bool_pairs(&rows),
        vec![(2, true), (3, true), (4, true)],
        "INPUT_FILE_NAME() folds like the lower-case call"
    );
}

#[tokio::test]
async fn input_file_name_in_where_keeps_every_row() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;

    let rows = batches(
        &session,
        "SELECT id FROM ice.ns.t WHERE input_file_name() LIKE '%.parquet'",
    )
    .await;
    assert_eq!(i64s(&rows, 0), vec![2, 3, 4], "R-INPUT-FILE-NAME in WHERE");

    let rows = batches(
        &session,
        "SELECT id FROM ice.ns.t WHERE input_file_name() = _file",
    )
    .await;
    assert_eq!(
        i64s(&rows, 0),
        vec![2, 3, 4],
        "WHERE compares input_file_name() to _file per row"
    );
}

#[tokio::test]
async fn input_file_name_in_a_derived_table_equals_file() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;

    let rows = batches(
        &session,
        "SELECT id, f = _f FROM \
         (SELECT id, input_file_name() f, _file _f FROM ice.ns.t) dt",
    )
    .await;
    assert_eq!(
        id_bool_pairs(&rows),
        vec![(2, true), (3, true), (4, true)],
        "the inner single-relation SELECT rewrites input_file_name()"
    );
}

#[tokio::test]
async fn bare_input_file_name_projection_names_the_column() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;

    let rows = batches(&session, "SELECT input_file_name() FROM ice.ns.t").await;
    assert_eq!(
        field_names(&rows),
        vec!["input_file_name()"],
        "the bare projection column is named input_file_name()"
    );

    let rewritten = strings(
        &batches(
            &session,
            "SELECT input_file_name() FROM ice.ns.t ORDER BY id",
        )
        .await,
        0,
    );
    let file_column = strings(
        &batches(&session, "SELECT _file FROM ice.ns.t ORDER BY id").await,
        0,
    );
    assert_eq!(
        rewritten, file_column,
        "the bare projection answers _file row by row"
    );
    let distinct: std::collections::HashSet<&String> = rewritten.iter().collect();
    assert!(
        distinct.len() >= 2,
        "the seed spans at least two data files: {rewritten:?}"
    );
}

#[tokio::test]
async fn input_file_name_inside_an_aggregate_arg_falls_through() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;

    let error = plan_error(
        &session,
        "SELECT count(DISTINCT input_file_name()) FROM ice.ns.t",
    )
    .await;
    assert!(
        error.contains("[UNRESOLVED_ROUTINE]") && error.contains("input_file_name"),
        "aggregate-arg shape keeps the unresolved-routine error: {error}"
    );
}

#[tokio::test]
async fn input_file_name_with_an_argument_falls_through() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;

    let error = plan_error(&session, "SELECT input_file_name(1) FROM ice.ns.t").await;
    assert!(
        error.contains("[UNRESOLVED_ROUTINE]") && error.contains("input_file_name"),
        "a call with arguments keeps the unresolved-routine error: {error}"
    );
}

#[tokio::test]
async fn input_file_name_over_a_self_join_falls_through() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;

    let error = plan_error(
        &session,
        "SELECT input_file_name() FROM ice.ns.t a JOIN ice.ns.t b ON a.id = b.id",
    )
    .await;
    assert!(
        error.contains("[UNRESOLVED_ROUTINE]") && error.contains("input_file_name"),
        "a joined SELECT keeps the unresolved-routine error: {error}"
    );
}

#[tokio::test]
async fn input_file_name_over_a_v3_comma_join_is_an_unresolved_routine() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    run(
        &session,
        "CREATE TABLE ice.ns.t3 (id BIGINT, data STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '3')",
    )
    .await;
    run(&session, "INSERT INTO ice.ns.t3 VALUES (1, 'a'), (2, 'b')").await;

    let error = refusal(
        &session,
        "SELECT input_file_name() LIKE '%.parquet' AS f FROM ice.ns.t3 a, ice.ns.t3 b",
    )
    .await;
    assert_eq!(
        (error.exception_class(), error.to_string()),
        (
            ErrorClass::Analysis,
            "[UNRESOLVED_ROUTINE] Cannot resolve routine `input_file_name` on search path \
             [`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]. \
             SQLSTATE: 42883; line 1 pos 7"
                .to_string()
        ),
    );
}

#[tokio::test]
async fn input_file_name_over_values_falls_through() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;

    let error = plan_error(&session, "SELECT input_file_name() FROM VALUES (1) AS v(a)").await;
    assert!(
        error.contains("[UNRESOLVED_ROUTINE]") && error.contains("input_file_name"),
        "a non-Iceberg relation keeps the unresolved-routine error: {error}"
    );
}

#[tokio::test]
async fn input_file_name_without_from_falls_through() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;

    let error = plan_error(&session, "SELECT input_file_name()").await;
    assert!(
        error.contains("[UNRESOLVED_ROUTINE]") && error.contains("input_file_name"),
        "a from-less SELECT keeps the unresolved-routine error: {error}"
    );
}

#[tokio::test]
async fn input_file_name_over_a_metadata_table_falls_through() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;

    let error = plan_error(
        &session,
        "SELECT input_file_name() FROM `ice`.`ns`.`t`.`snapshots`",
    )
    .await;
    assert!(
        error.contains("[UNRESOLVED_ROUTINE]") && error.contains("input_file_name"),
        "a metadata-table scan keeps the unresolved-routine error: {error}"
    );
}

#[tokio::test]
async fn input_file_name_over_a_union_all_falls_through() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;

    let error = plan_error(
        &session,
        "SELECT input_file_name() FROM \
         (SELECT id FROM ice.ns.t UNION ALL SELECT id FROM ice.ns.t) u",
    )
    .await;
    assert!(
        error.contains("[UNRESOLVED_ROUTINE]") && error.contains("input_file_name"),
        "an outer SELECT over UNION ALL keeps the unresolved-routine error: {error}"
    );
}

#[tokio::test]
async fn input_file_name_over_a_cte_sharing_the_table_alias_falls_through() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;

    let error = plan_error(
        &session,
        "WITH c AS (SELECT id, 'x' AS _file FROM ice.ns.t) \
         SELECT input_file_name() FROM c AS t",
    )
    .await;
    assert!(
        error.contains("[UNRESOLVED_ROUTINE]") && error.contains("input_file_name"),
        "a relation merely aliased like the Iceberg table keeps the unresolved-routine error: {error}"
    );

    let error = plan_error(
        &session,
        "WITH c AS (SELECT id, 'x' AS _file FROM ice.ns.t) \
         SELECT id FROM c AS t WHERE input_file_name() = 'x'",
    )
    .await;
    assert!(
        error.contains("[UNRESOLVED_ROUTINE]") && error.contains("input_file_name"),
        "the WHERE leg of the alias collision keeps the unresolved-routine error: {error}"
    );

    let error = plan_error(
        &session,
        "WITH c AS (SELECT * FROM ice.ns.t) SELECT input_file_name() FROM c",
    )
    .await;
    assert!(
        error.contains("[UNRESOLVED_ROUTINE]") && error.contains("input_file_name"),
        "a CTE without an alias keeps the unresolved-routine error: {error}"
    );

    let error = plan_error(
        &session,
        "WITH t AS (SELECT id, 'x' AS _file FROM ice.ns.t) \
         SELECT input_file_name() FROM t",
    )
    .await;
    assert!(
        error.contains("[UNRESOLVED_ROUTINE]") && error.contains("input_file_name"),
        "a CTE named like the table keeps the unresolved-routine error: {error}"
    );
}

#[tokio::test]
async fn input_file_name_after_use_over_a_cte_named_like_the_table_falls_through() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;
    run(&session, "USE ice.ns").await;

    let error = plan_error(
        &session,
        "WITH t AS (SELECT id, 'x' AS _file FROM ice.ns.t) \
         SELECT input_file_name() FROM t",
    )
    .await;
    assert!(
        error.contains("[UNRESOLVED_ROUTINE]") && error.contains("input_file_name"),
        "a CTE named like the defaulted table keeps the unresolved-routine error: {error}"
    );

    let rows = batches(
        &session,
        "WITH t AS (SELECT id, 'x' AS _file FROM ice.ns.t WHERE id = 2) SELECT _file FROM t",
    )
    .await;
    assert_eq!(
        strings(&rows, 0),
        vec!["x".to_string()],
        "the CTE's _file column wins over the physical table's"
    );
}

#[tokio::test]
async fn input_file_name_after_use_still_serves_the_table_by_name() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;
    run(&session, "USE ice.ns").await;

    let expected = strings(
        &batches(&session, "SELECT _file FROM ice.ns.t ORDER BY id").await,
        0,
    );
    for sql in [
        "SELECT input_file_name() FROM ice.ns.t ORDER BY id",
        "WITH c AS (SELECT 1 AS one) SELECT input_file_name() FROM ice.ns.t ORDER BY id",
        "WITH t AS (SELECT 1 AS one) SELECT input_file_name() FROM ice.ns.t ORDER BY id",
    ] {
        let answered = strings(&batches(&session, sql).await, 0);
        assert_eq!(
            answered, expected,
            "{sql} must still answer the physical _file values row by row"
        );
    }
}

#[tokio::test]
async fn input_file_name_over_a_temp_view_named_like_the_table_falls_through() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;
    run(&session, "USE ice.ns").await;

    run(&session, "CREATE TEMPORARY VIEW t AS SELECT 1 AS id").await;

    let error = plan_error(&session, "SELECT input_file_name() FROM t").await;
    assert!(
        error.contains("[UNRESOLVED_ROUTINE]") && error.contains("input_file_name"),
        "a temp view named like the table is not served: {error}"
    );

    let error = plan_error(&session, "SELECT _file FROM t").await;
    assert!(
        !error.is_empty(),
        "a _file read over the temp view must not silently answer: {error}"
    );
}

#[tokio::test]
async fn a_real_column_named_input_file_name_reads_unchanged() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    run(
        &session,
        "CREATE TABLE ice.ns.tcol (id INT, input_file_name STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2')",
    )
    .await;
    run(&session, "INSERT INTO ice.ns.tcol VALUES (1, 'x')").await;

    let rows = batches(&session, "SELECT id, input_file_name FROM ice.ns.tcol").await;
    assert_eq!(strings(&rows, 1), vec!["x".to_string()]);
    let ids = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int32Array>()
        .expect("int32 column");
    assert_eq!(ids.value(0), 1);
}

#[tokio::test]
async fn insert_around_the_trigger_keeps_todays_answers() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t").await;
    run(
        &session,
        "CREATE TABLE ice.ns.t2 (id BIGINT, data STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2')",
    )
    .await;

    run(
        &session,
        "INSERT INTO ice.ns.t2 SELECT id, 'x' FROM ice.ns.t",
    )
    .await;
    let rows = batches(&session, "SELECT count(*) FROM ice.ns.t2").await;
    assert_eq!(i64s(&rows, 0), vec![3], "a plain INSERT still lands");

    let error = plan_error(
        &session,
        "INSERT INTO ice.ns.t2 SELECT id, input_file_name() FROM ice.ns.t",
    )
    .await;
    assert!(
        error.contains("[UNRESOLVED_ROUTINE]") && error.contains("input_file_name"),
        "a non-query statement returns Ok(None) and keeps the unresolved-routine error: {error}"
    );
    assert!(
        !error.contains("[ICE-MC-1]"),
        "a non-query statement must not take the metadata-column refusal: {error}"
    );
}
