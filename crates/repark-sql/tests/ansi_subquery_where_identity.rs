use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::Int32Type;
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

const SEED_T: &str = "CREATE TABLE ice.ns.t AS SELECT CAST(id AS INT) AS id, CAST(v AS INT) AS v \
     FROM (VALUES (1, 10), (2, 20), (3, 30), (4, 40)) AS x(id, v)";

const SEED_S: &str = "CREATE TABLE ice.ns.s AS SELECT CAST(id AS INT) AS id, CAST(w AS INT) AS w \
     FROM (VALUES (2, 20), (3, 99)) AS x(id, w)";

const IDENTITY_FORMS: [(&str, &[(i32, i32)]); 5] = [
    (
        "UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT id FROM ice.ns.s)",
        &[(1, 10), (2, 5), (3, 5), (4, 40)],
    ),
    (
        "DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)",
        &[(1, 10), (4, 40)],
    ),
    (
        "DELETE FROM ice.ns.t WHERE EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = t.id)",
        &[(1, 10), (4, 40)],
    ),
    (
        "DELETE FROM ice.ns.t WHERE id NOT IN (SELECT id FROM ice.ns.s)",
        &[(2, 20), (3, 30)],
    ),
    (
        "DELETE FROM ice.ns.t WHERE EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = 100)",
        &[(1, 10), (2, 20), (3, 30), (4, 40)],
    ),
];

async fn run(session: &ReparkSession, sql: &str) {
    session
        .sql(sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} plans: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql} runs: {error}"));
}

async fn seeded_ansi_session() -> (ReparkSession, TempDir) {
    let dialect: Arc<dyn SqlDialect> = Arc::new(AnsiDialect);
    let session = ReparkSession::builder()
        .with_sql_dialect(dialect)
        .build()
        .expect("ansi session");
    let warehouse = TempDir::new().expect("warehouse");
    let root = warehouse.path().to_str().expect("path").to_string();
    session
        .register_memory_catalog("ice", &root)
        .await
        .expect("catalog");
    run(
        &session,
        &format!("CREATE SCHEMA ice.ns WITH (location = '{root}/ns')"),
    )
    .await;
    run(&session, SEED_T).await;
    run(&session, SEED_S).await;
    (session, warehouse)
}

async fn target_rows(session: &ReparkSession) -> Vec<(i32, i32)> {
    let batches = session
        .sql("SELECT id, v FROM ice.ns.t ORDER BY id")
        .await
        .expect("read")
        .collect()
        .await
        .expect("collect");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch.column(0).as_primitive::<Int32Type>();
        let values = batch.column(1).as_primitive::<Int32Type>();
        for index in 0..batch.num_rows() {
            rows.push((ids.value(index), values.value(index)));
        }
    }
    rows
}

#[tokio::test]
async fn the_ansi_door_answers_the_identity_subquery_forms_with_sparks_rows() {
    for (statement, spark_rows) in IDENTITY_FORMS {
        let (session, _warehouse) = seeded_ansi_session().await;
        run(&session, statement).await;
        assert_eq!(target_rows(&session).await, spark_rows, "{statement}");
    }
}
