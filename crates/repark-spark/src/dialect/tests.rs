//! `SparkDialect` seam tests: `with_sql_dialect` routes every `sql()` call through the Spark router.

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use repark_core::{ReparkSession, SqlDialect};

use super::SparkDialect;

fn spark_session() -> ReparkSession {
    ReparkSession::builder()
        .with_sql_dialect(Arc::new(SparkDialect) as Arc<dyn SqlDialect>)
        .build()
        .expect("session build")
}

/// The dialect routes through the Spark router.
#[tokio::test]
async fn dialect_execute_runs_the_spark_router() {
    let session = spark_session();
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, true)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![Some(2), None, Some(1)]))],
    )
    .expect("fixture batch");
    session
        .context()
        .register_batch("t", batch)
        .expect("register");
    let batches = session
        .sql("SELECT v FROM t ORDER BY v")
        .await
        .expect("sql")
        .collect()
        .await
        .expect("collect");
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("i32 column");
    assert!(column.is_null(0), "Spark ASC default is NULLS FIRST");
}

/// The dialect passes router refusals through the seam and preserves the session error fold.
#[tokio::test]
async fn dialect_surfaces_router_refusals() {
    let session = spark_session();
    let error = session
        .sql("TRUNCATE TABLE ice.ns.t")
        .await
        .expect_err("TRUNCATE of a missing table must fail")
        .to_string();
    assert!(error.contains("TABLE_OR_VIEW_NOT_FOUND"), "{error}");
}

async fn collect_strings(session: &ReparkSession, sql: &str) -> Vec<String> {
    let batches = session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    let mut rows = Vec::new();
    for batch in &batches {
        for row in 0..batch.num_rows() {
            let cells = batch
                .columns()
                .iter()
                .map(|column| {
                    datafusion::arrow::util::display::array_value_to_string(column, row).unwrap()
                })
                .collect::<Vec<_>>();
            rows.push(cells.join("|"));
        }
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn spark_dialect_respells_minus_all_after_spark_normalizers_and_ddl_dml_writes() {
    let warehouse = tempfile::TempDir::new().unwrap();
    let warehouse_path = warehouse.path().to_str().unwrap().to_string();
    let session = ReparkSession::builder()
        .with_extension(Arc::new(crate::SparkExtension))
        .with_sql_dialect(Arc::new(SparkDialect))
        .build()
        .unwrap();
    session
        .register_memory_catalog("ice", &warehouse_path)
        .await
        .unwrap();
    session
        .create_namespace(
            "ice",
            "ns",
            std::collections::HashMap::from([(
                "location".to_string(),
                format!("{warehouse_path}/ns"),
            )]),
        )
        .await
        .unwrap();
    for stmt in [
        "CREATE TABLE ice.ns.base_t (a INT) USING iceberg",
        "INSERT INTO ice.ns.base_t VALUES (1), (1), (2), (3)",
        "INSERT INTO ice.ns.base_t VALUES (4)",
    ] {
        session.sql(stmt).await.unwrap().collect().await.unwrap();
    }
    let snaps = collect_strings(
        &session,
        "SELECT snapshot_id FROM ice.ns.base_t.snapshots ORDER BY committed_at",
    )
    .await;
    let snap1 = &snaps[0];
    let snap2 = &snaps[snaps.len() - 1];
    assert_eq!(
        collect_strings(
            &session,
            &format!(
                "SELECT a FROM ice.ns.base_t FOR VERSION AS OF {snap2} \
                 MINUS ALL SELECT a FROM ice.ns.base_t FOR VERSION AS OF {snap1}"
            ),
        )
        .await,
        vec!["4".to_string()]
    );
    for (sql, expected) in [
        (
            r"SELECT r'a\b' AS a UNION ALL SELECT r'a\b' MINUS ALL SELECT r'a\b' AS a",
            vec![r"a\b".to_string()],
        ),
        (
            r"SELECT R'a\b' AS a UNION ALL SELECT R'a\b' MINUS ALL SELECT R'a\b' AS a",
            vec![r"a\b".to_string()],
        ),
        (
            "SELECT 1e2BD AS a UNION ALL SELECT 1e2BD MINUS ALL SELECT 100BD AS a",
            vec!["100".to_string()],
        ),
        (
            r"SELECT 'a\'b' AS a UNION ALL SELECT 'a\'b' MINUS ALL SELECT 'a\'b' AS a",
            vec!["a'b".to_string()],
        ),
        (
            "SELECT minus all FROM (SELECT 1 AS minus, 2 AS except) t",
            vec!["1".to_string()],
        ),
    ] {
        assert_eq!(collect_strings(&session, sql).await, expected, "{sql}");
    }
    for (stmt, read_sql, expected) in [
        (
            r"CREATE TABLE ice.ns.t_r1_raw USING iceberg AS SELECT r'a\b' AS a UNION ALL SELECT r'a\b' MINUS ALL SELECT r'a\b' AS a",
            "SELECT a FROM ice.ns.t_r1_raw",
            vec![r"a\b".to_string()],
        ),
        (
            r"CREATE OR REPLACE TEMPORARY VIEW v_r1_raw AS SELECT r'a\b' AS a UNION ALL SELECT r'a\b' MINUS ALL SELECT r'a\b' AS a",
            "SELECT a FROM v_r1_raw",
            vec![r"a\b".to_string()],
        ),
        (
            r"CREATE VIEW ice.ns.pv_r1_raw AS SELECT r'a\b' AS a UNION ALL SELECT r'a\b' MINUS ALL SELECT r'a\b' AS a",
            "SELECT a FROM ice.ns.pv_r1_raw",
            vec![r"a\b".to_string()],
        ),
        (
            "CREATE TABLE ice.ns.t_r2_cv USING iceberg AS VALUES (1), (1), (4) MINUS ALL VALUES (1)",
            "SELECT * FROM ice.ns.t_r2_cv",
            vec!["1".to_string(), "4".to_string()],
        ),
        (
            "CREATE OR REPLACE TABLE ice.ns.t_r2_corv USING iceberg AS VALUES (1), (1), (4) MINUS ALL VALUES (1)",
            "SELECT * FROM ice.ns.t_r2_corv",
            vec!["1".to_string(), "4".to_string()],
        ),
        (
            "CREATE TABLE ice.ns.t_r2_tpv USING iceberg TBLPROPERTIES ('comment' = 'SELECT WITH') AS VALUES (1), (1), (4) MINUS ALL VALUES (1)",
            "SELECT * FROM ice.ns.t_r2_tpv",
            vec!["1".to_string(), "4".to_string()],
        ),
    ] {
        session.sql(stmt).await.unwrap().collect().await.unwrap();
        assert_eq!(
            collect_strings(&session, read_sql).await,
            expected,
            "{stmt}"
        );
    }
    for setup in [
        "CREATE TABLE ice.ns.m_r1 (a STRING) USING iceberg",
        "INSERT INTO ice.ns.m_r1 VALUES ('z')",
        r"MERGE INTO ice.ns.m_r1 t USING (SELECT r'a\b' AS a UNION ALL SELECT r'a\b' MINUS ALL SELECT r'a\b' AS a) s ON t.a = s.a WHEN NOT MATCHED THEN INSERT (a) VALUES (s.a)",
        "CREATE TABLE ice.ns.t_r2_rv (a INT) USING iceberg",
        "REPLACE TABLE ice.ns.t_r2_rv USING iceberg AS VALUES (1), (1), (4) MINUS ALL VALUES (1)",
        "CREATE TABLE ice.ns.t_r2_ibn (a INT) USING iceberg",
        "INSERT INTO ice.ns.t_r2_ibn BY NAME (SELECT 1 AS a UNION ALL SELECT 1 UNION ALL SELECT 4) MINUS ALL (SELECT 1 AS a)",
        "CREATE TABLE ice.ns.t_r2_iobn (a INT) USING iceberg",
        "INSERT INTO ice.ns.t_r2_iobn VALUES (9)",
        "INSERT OVERWRITE ice.ns.t_r2_iobn BY NAME (SELECT 1 AS a UNION ALL SELECT 1 UNION ALL SELECT 4) MINUS ALL (SELECT 1 AS a)",
        &format!(
            "CREATE VIEW ice.ns.pv_r1_tt AS SELECT a FROM ice.ns.base_t FOR VERSION AS OF {snap2} \
             MINUS ALL SELECT a FROM ice.ns.base_t FOR VERSION AS OF {snap1}"
        ),
    ] {
        session
            .sql(setup)
            .await
            .unwrap_or_else(|error| panic!("{setup}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("{setup}: {error}"));
    }
    assert_eq!(
        collect_strings(&session, "SELECT a FROM ice.ns.m_r1").await,
        vec![r"a\b".to_string(), "z".to_string()]
    );
    assert_eq!(
        collect_strings(&session, "SELECT * FROM ice.ns.t_r2_rv").await,
        vec!["1".to_string(), "4".to_string()]
    );
    assert_eq!(
        collect_strings(&session, "SELECT a FROM ice.ns.t_r2_ibn").await,
        vec!["1".to_string(), "4".to_string()]
    );
    assert_eq!(
        collect_strings(&session, "SELECT a FROM ice.ns.t_r2_iobn").await,
        vec!["1".to_string(), "4".to_string()]
    );
    assert_eq!(
        collect_strings(&session, "SELECT a FROM ice.ns.pv_r1_tt").await,
        vec!["4".to_string()]
    );
}
