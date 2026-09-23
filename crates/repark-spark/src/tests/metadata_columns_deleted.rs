use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, Int32Array, Int64Array, StringArray};
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

fn pairs_i64_bool(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> Vec<(i64, bool)> {
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
            .downcast_ref::<BooleanArray>()
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

fn pairs_i64_str(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> Vec<(i64, String)> {
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
            .downcast_ref::<StringArray>()
            .unwrap();
        out.extend((0..left.len()).map(|row| (left.value(row), right.value(row).to_string())));
    }
    out.sort_unstable();
    out
}

fn pairs_bool_i64(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> Vec<(bool, i64)> {
    let mut out = Vec::new();
    for batch in batches {
        let left = batch
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
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

fn triples_i64(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> Vec<(i64, String, String)> {
    let mut out = Vec::new();
    for batch in batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let data = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let cats = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        out.extend((0..ids.len()).map(|row| {
            (
                ids.value(row),
                data.value(row).to_string(),
                cats.value(row).to_string(),
            )
        }));
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
async fn deleted_column_marks_merge_on_read_deleted_row() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", MOR).await;
    let rows = batches(&session, "SELECT id, _deleted FROM ice.ns.t ORDER BY id").await;
    assert_eq!(
        pairs_i64_bool(&rows),
        vec![(1, true), (2, false), (3, false), (4, false)],
        "R-MC-DELETED"
    );
    let schema = rows[0].schema();
    let field = &schema.fields()[1];
    assert_eq!(field.name(), "_deleted", "R-MC-DELETED");
    assert_eq!(field.data_type(), &DataType::Boolean, "R-MC-DELETED");
    assert!(!field.is_nullable(), "R-MC-DELETED");
}

#[tokio::test]
async fn not_projecting_deleted_still_filters_mor_rows() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", MOR).await;
    let rows = batches(&session, "SELECT id FROM ice.ns.t ORDER BY id").await;
    assert_eq!(i64s(&rows, 0), vec![2, 3, 4], "R-MC-DELETED-NOPROJ");
    let rows = batches(&session, "SELECT count(*) FROM ice.ns.t").await;
    assert_eq!(i64s(&rows, 0), vec![3], "R-MC-DELETED-NOPROJ");
}

#[tokio::test]
async fn select_star_keeps_user_columns_on_mor_table() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", MOR).await;
    let rows = batches(&session, "SELECT * FROM ice.ns.t ORDER BY id").await;
    assert_eq!(
        field_names(&rows),
        vec!["id", "data", "cat"],
        "R-MC-DELETED-STAR"
    );
    assert_eq!(
        triples_i64(&rows),
        vec![
            (2, "b".to_string(), "y".to_string()),
            (3, "c".to_string(), "x".to_string()),
            (4, "d".to_string(), "x".to_string()),
        ],
        "R-MC-DELETED-STAR"
    );
}

#[tokio::test]
async fn deleted_predicates_reapply_above_the_scan() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", MOR).await;
    let rows = batches(
        &session,
        "SELECT id, _deleted FROM ice.ns.t WHERE NOT _deleted ORDER BY id",
    )
    .await;
    assert_eq!(
        pairs_i64_bool(&rows),
        vec![(2, false), (3, false), (4, false)],
        "R-MC-DELETED-PRED"
    );
    let rows = batches(
        &session,
        "SELECT id, _deleted FROM ice.ns.t WHERE _deleted ORDER BY id",
    )
    .await;
    assert_eq!(pairs_i64_bool(&rows), vec![(1, true)], "R-MC-DELETED-PRED");
    let rows = batches(
        &session,
        "SELECT id FROM ice.ns.t WHERE _deleted ORDER BY id",
    )
    .await;
    assert_eq!(i64s(&rows, 0), vec![1], "R-MC-DELETED-PRED-ONLY");
    let rows = batches(
        &session,
        "SELECT id FROM ice.ns.t WHERE NOT _deleted ORDER BY id",
    )
    .await;
    assert_eq!(i64s(&rows, 0), vec![2, 3, 4], "R-MC-DELETED-PRED-ONLY");
    let rows = batches(
        &session,
        "SELECT id FROM ice.ns.t WHERE _deleted OR id > 0 ORDER BY id",
    )
    .await;
    assert_eq!(i64s(&rows, 0), vec![1, 2, 3, 4], "R-MC-DELETED-PRED-ONLY");
    let rows = batches(
        &session,
        "SELECT count(*) FROM ice.ns.t WHERE _deleted IS NOT NULL",
    )
    .await;
    assert_eq!(i64s(&rows, 0), vec![3], "R-MC-DELETED-PRED-ONLY");
}

#[tokio::test]
async fn deleted_column_on_copy_on_write_marks_all_rows_false() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", "").await;
    let rows = batches(&session, "SELECT id, _deleted FROM ice.ns.t ORDER BY id").await;
    assert_eq!(
        pairs_i64_bool(&rows),
        vec![(2, false), (3, false), (4, false)],
        "R-MC-DELETED-COW"
    );
}

#[tokio::test]
async fn deleted_column_flows_through_subqueries() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", MOR).await;
    let rows = batches(
        &session,
        "SELECT id FROM (SELECT id, _deleted FROM ice.ns.t) s \
         WHERE NOT s._deleted ORDER BY id",
    )
    .await;
    assert_eq!(i64s(&rows, 0), vec![2, 3, 4], "R-MC-DELETED-SUBQ");
    let rows = batches(
        &session,
        "SELECT id FROM (SELECT id, _deleted AS d FROM ice.ns.t) s ORDER BY id",
    )
    .await;
    assert_eq!(i64s(&rows, 0), vec![2, 3, 4], "R-MC-DELETED-SUBQ");
}

#[tokio::test]
async fn deleted_column_in_expressions_order_and_group() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", MOR).await;
    let rows = batches(
        &session,
        "SELECT id, CASE WHEN _deleted THEN 'D' ELSE 'L' END FROM ice.ns.t ORDER BY id",
    )
    .await;
    assert_eq!(
        pairs_i64_str(&rows),
        vec![
            (1, "D".to_string()),
            (2, "L".to_string()),
            (3, "L".to_string()),
            (4, "L".to_string()),
        ],
        "R-MC-DELETED-EXPR"
    );
    let rows = batches(&session, "SELECT sum(CAST(_deleted AS INT)) FROM ice.ns.t").await;
    assert_eq!(i64s(&rows, 0), vec![1], "R-MC-DELETED-EXPR");
    let rows = batches(&session, "SELECT id FROM ice.ns.t ORDER BY _deleted, id").await;
    assert_eq!(i64s(&rows, 0), vec![2, 3, 4, 1], "R-MC-DELETED-EXPR");
    let rows = batches(
        &session,
        "SELECT _deleted, count(*) FROM ice.ns.t GROUP BY _deleted ORDER BY 1",
    )
    .await;
    assert_eq!(
        pairs_bool_i64(&rows),
        vec![(false, 3), (true, 1)],
        "R-MC-DELETED-EXPR"
    );
}

#[tokio::test]
async fn deleted_column_in_self_join_answers_empty() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", MOR).await;
    let rows = batches(
        &session,
        "SELECT a.id FROM ice.ns.t a JOIN ice.ns.t b ON a.id = b.id \
         WHERE b._deleted ORDER BY 1",
    )
    .await;
    assert_eq!(i64s(&rows, 0), Vec::<i64>::new(), "R-MC-DELETED-JOIN");
}

#[tokio::test]
async fn unquoted_upper_deleted_folds_to_served_name() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", MOR).await;
    let rows = batches(&session, "SELECT id, _DELETED FROM ice.ns.t ORDER BY id").await;
    assert_eq!(
        pairs_i64_bool(&rows),
        vec![(1, true), (2, false), (3, false), (4, false)],
        "R-MC-DELETED-FOLD"
    );
}

#[tokio::test]
async fn quoted_upper_deleted_known_divergence() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", MOR).await;
    let error = plan_error(&session, "SELECT id, `_DELETED` FROM ice.ns.t").await;
    assert_eq!(
        error,
        "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or \
         function parameter with name `_DELETED` cannot be resolved. Did you mean one of the \
         following? [`id`, `data`, `cat`]. SQLSTATE: 42703",
        "quoted `_DELETED` keeps today's full refusal (KNOWN DIVERGENCE: Spark resolves it)"
    );
    let error = plan_error(&session, "SELECT id, `_FILE` FROM ice.ns.t").await;
    assert_eq!(
        error,
        "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or \
         function parameter with name `_FILE` cannot be resolved. Did you mean one of the \
         following? [`id`, `data`, `cat`]. SQLSTATE: 42703",
        "quoted `_FILE` shares the same pre-existing refusal"
    );
}

#[tokio::test]
async fn metadata_column_over_time_travel_known_divergence() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", MOR).await;
    let catalogs = session.catalogs_snapshot();
    let ident = iceberg::TableIdent::new(
        iceberg::NamespaceIdent::new("ns".to_string()),
        "t".to_string(),
    );
    let snapshot_id = catalogs
        .get("ice")
        .unwrap()
        .load_table(&ident)
        .await
        .unwrap()
        .metadata()
        .current_snapshot_id()
        .unwrap();
    let error = plan_error(
        &session,
        &format!("SELECT id, _file FROM ice.ns.t VERSION AS OF {snapshot_id}"),
    )
    .await;
    assert_eq!(
        error,
        "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or \
         function parameter with name `_file` cannot be resolved. Did you mean one of the \
         following? [`id`, `data`, `cat`]. SQLSTATE: 42703",
        "KNOWN DIVERGENCE (pre-existing, all metadata columns): Spark serves _file over \
         time travel"
    );
    let error = plan_error(
        &session,
        &format!("SELECT id, _deleted FROM ice.ns.t VERSION AS OF {snapshot_id}"),
    )
    .await;
    assert_eq!(
        error,
        "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or \
         function parameter with name `_deleted` cannot be resolved. Did you mean one of the \
         following? [`id`, `data`, `cat`]. SQLSTATE: 42703",
        "KNOWN DIVERGENCE (pre-existing, all metadata columns): Spark serves _deleted over \
         time travel"
    );
}

#[tokio::test]
async fn served_spec_id_and_deleted_answer_together() {
    let wh = TempDir::new().unwrap();
    let session = session(&wh).await;
    seed(&session, "ice.ns.t", "PARTITIONED BY (cat)", "").await;
    let rows = batches(&session, "SELECT id, _spec_id FROM ice.ns.t").await;
    assert_eq!(
        pairs_i64_i32(&rows),
        vec![(2, 0), (3, 0), (4, 0)],
        "composed served columns answer together"
    );
    let rows = batches(&session, "SELECT id, _deleted FROM ice.ns.t").await;
    assert_eq!(
        pairs_i64_bool(&rows),
        vec![(2, false), (3, false), (4, false)],
        "composed served columns answer together"
    );
    let rows = batches(&session, "SELECT id, _spec_id, _deleted FROM ice.ns.t").await;
    assert_eq!(
        field_names(&rows),
        vec!["id", "_spec_id", "_deleted"],
        "composed served columns answer together"
    );
    assert_eq!(
        rows.iter()
            .map(datafusion::arrow::record_batch::RecordBatch::num_rows)
            .sum::<usize>(),
        3
    );
}
