use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use repark_core::ReparkSession;
use tempfile::TempDir;

use super::metadata_columns_deleted::plan_error;
use crate::{SparkDialect, SparkExtension};

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

async fn seeded(wh: &TempDir) -> ReparkSession {
    let session = session(wh).await;
    run(
        &session,
        "CREATE TABLE ice.ns.l (id BIGINT, s STRING) USING iceberg",
    )
    .await;
    run(
        &session,
        "CREATE TABLE ice.ns.r (k BIGINT, t STRING) USING iceberg",
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.l VALUES (1, 'a'), (2, 'b'), (3, 'c')",
    )
    .await;
    run(
        &session,
        "INSERT INTO ice.ns.r VALUES (2, 'x'), (3, 'y'), (4, 'z'), (9, 'q')",
    )
    .await;
    session
}

async fn row_count(session: &ReparkSession, sql: &str) -> usize {
    let batches = session.sql(sql).await.unwrap().collect().await.unwrap();
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[tokio::test]
async fn nondeterministic_join_conditions_refuse_with_spark_class_and_text() {
    let cases = [
        (
            "SELECT * FROM ice.ns.l INNER JOIN ice.ns.r ON rand(7) < 0.5",
            "[INVALID_NON_DETERMINISTIC_EXPRESSIONS] The operator expects a deterministic \
             expression, but the actual expression is \"(rand(7) < 0.5)\". SQLSTATE: 42K0E",
        ),
        (
            "SELECT * FROM ice.ns.l INNER JOIN ice.ns.r ON l.id = r.k AND rand(1) >= 0",
            "[INVALID_NON_DETERMINISTIC_EXPRESSIONS] The operator expects a deterministic \
             expression, but the actual expression is \"((id = k) AND (rand(1) >= 0))\". \
             SQLSTATE: 42K0E",
        ),
        (
            "SELECT * FROM ice.ns.l LEFT JOIN ice.ns.r ON rand(7) < 0.5",
            "[INVALID_NON_DETERMINISTIC_EXPRESSIONS]",
        ),
        (
            "SELECT * FROM ice.ns.l LEFT SEMI JOIN ice.ns.r ON rand(7) < 0.5",
            "[INVALID_NON_DETERMINISTIC_EXPRESSIONS]",
        ),
        (
            "SELECT * FROM ice.ns.l INNER JOIN ice.ns.r ON uuid() IS NOT NULL",
            "[INVALID_NON_DETERMINISTIC_EXPRESSIONS]",
        ),
        (
            "SELECT * FROM ice.ns.l INNER JOIN ice.ns.r ON size(shuffle(array(l.id, r.k))) = 2",
            "[INVALID_NON_DETERMINISTIC_EXPRESSIONS]",
        ),
        (
            "SELECT * FROM ice.ns.l INNER JOIN ice.ns.r ON l.id = r.k AND EXISTS (SELECT 1 WHERE rand(1) > 2)",
            "[INVALID_NON_DETERMINISTIC_EXPRESSIONS]",
        ),
        (
            "SELECT * FROM ice.ns.l WHERE id IN (SELECT l2.id FROM ice.ns.l l2 JOIN ice.ns.r r2 ON l2.id = r2.k AND rand(1) >= 0)",
            "[INVALID_NON_DETERMINISTIC_EXPRESSIONS]",
        ),
    ];
    for (sql, text) in cases {
        let wh = TempDir::new().unwrap();
        let session = seeded(&wh).await;
        let error = plan_error(&session, sql).await;
        assert!(error.contains(text), "{sql}: got {error}");
    }
}

#[tokio::test]
async fn non_boolean_join_conditions_refuse_with_spark_class_and_text() {
    let cases = [
        (
            "SELECT * FROM ice.ns.l INNER JOIN ice.ns.r ON NULL",
            "[JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE] The join condition \"NULL\" has the invalid \
             type \"VOID\", expected \"BOOLEAN\". SQLSTATE: 42K0E",
        ),
        (
            "SELECT * FROM ice.ns.l INNER JOIN ice.ns.r ON 1",
            "[JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE] The join condition \"1\" has the invalid \
             type \"INT\", expected \"BOOLEAN\". SQLSTATE: 42K0E",
        ),
        (
            "SELECT * FROM ice.ns.l INNER JOIN ice.ns.r ON 'true'",
            "[JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE] The join condition \"true\" has the invalid \
             type \"STRING\", expected \"BOOLEAN\". SQLSTATE: 42K0E",
        ),
        (
            "SELECT * FROM ice.ns.l INNER JOIN ice.ns.r ON rand(1)",
            "[JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE] The join condition \"rand(1)\" has the \
             invalid type \"DOUBLE\", expected \"BOOLEAN\". SQLSTATE: 42K0E",
        ),
        (
            "SELECT * FROM ice.ns.l LEFT JOIN ice.ns.r ON NULL",
            "[JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE]",
        ),
    ];
    for (sql, text) in cases {
        let wh = TempDir::new().unwrap();
        let session = seeded(&wh).await;
        let error = plan_error(&session, sql).await;
        assert!(error.contains(text), "{sql}: got {error}");
    }
}

#[tokio::test]
async fn deterministic_boolean_join_conditions_still_answer() {
    let wh = TempDir::new().unwrap();
    let session = seeded(&wh).await;
    let cases = [
        (
            "SELECT * FROM ice.ns.l INNER JOIN ice.ns.r ON l.id = r.k",
            2,
        ),
        ("SELECT * FROM ice.ns.l INNER JOIN ice.ns.r ON TRUE", 12),
        (
            "SELECT * FROM ice.ns.l INNER JOIN ice.ns.r ON CAST(NULL AS BOOLEAN)",
            0,
        ),
        ("SELECT * FROM ice.ns.l LEFT JOIN ice.ns.r ON l.id = r.k", 3),
        (
            "SELECT * FROM ice.ns.l INNER JOIN ice.ns.r ON l.id = r.k AND current_timestamp() > \
             TIMESTAMP '2000-01-01 00:00:00'",
            2,
        ),
    ];
    for (sql, rows) in cases {
        assert_eq!(row_count(&session, sql).await, rows, "{sql}");
    }
}
