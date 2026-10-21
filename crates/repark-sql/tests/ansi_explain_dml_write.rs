use std::sync::Arc;

use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::Int32Type;
use repark_core::{ReparkSession, SqlDialect};
use repark_sql::AnsiDialect;
use tempfile::TempDir;

const SEED_T: &str = "CREATE TABLE ice.ns.t AS SELECT CAST(id AS INT) AS id, CAST(v AS INT) AS v \
     FROM (VALUES (1, 10), (2, 20), (3, 30), (4, 40)) AS x(id, v)";

const SEED_S: &str = "CREATE TABLE ice.ns.s AS SELECT CAST(id AS INT) AS id, CAST(w AS INT) AS w \
     FROM (VALUES (2, 20), (3, 99)) AS x(id, w)";

const SEEDED_T: [(i32, i32); 4] = [(1, 10), (2, 20), (3, 30), (4, 40)];

const EXPLAIN_ANALYZE_REFUSAL: &str = "EXPLAIN ANALYZE is refused: its statement writes to a \
     table, and EXPLAIN ANALYZE runs the statement it explains. A plain EXPLAIN of the same \
     statement still shows its plan without running it, and EXPLAIN ANALYZE of a query still works.";

const PREPARE_REFUSAL: &str = "PREPARE `p` is refused: its statement writes to a table, and a \
     prepared write would run later, at EXECUTE. PREPARE of a query still works.";

const IN_SUBQUERY: &str = "id IN (SELECT id FROM ice.ns.s)";

async fn outcome(session: &ReparkSession, sql: &str) -> Result<Vec<RecordBatch>, String> {
    match session.sql(sql).await {
        Ok(frame) => frame.collect().await.map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

async fn plan_text(session: &ReparkSession, sql: &str) -> String {
    let batches = outcome(session, sql).await.expect(sql);
    datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .expect("render the plan")
        .to_string()
}

async fn assert_iceberg(session: &ReparkSession, table: &str) {
    let plan = plan_text(session, &format!("EXPLAIN SELECT * FROM {table}")).await;
    assert!(
        plan.contains("IcebergTableScan"),
        "{table} is not an Iceberg table:\n{plan}"
    );
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
    for sql in [
        format!("CREATE SCHEMA ice.ns WITH (location = '{root}/ns')"),
        SEED_T.to_string(),
        SEED_S.to_string(),
    ] {
        outcome(&session, &sql).await.expect(&sql);
    }
    assert_iceberg(&session, "ice.ns.t").await;
    assert_iceberg(&session, "ice.ns.s").await;
    assert_eq!(target_state(&session).await.0, SEEDED_T.to_vec());
    (session, warehouse)
}

async fn target_state(session: &ReparkSession) -> (Vec<(i32, i32)>, usize) {
    let batches = outcome(session, "SELECT id, v FROM ice.ns.t ORDER BY id")
        .await
        .expect("read");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch.column(0).as_primitive::<Int32Type>();
        let values = batch.column(1).as_primitive::<Int32Type>();
        for index in 0..batch.num_rows() {
            rows.push((ids.value(index), values.value(index)));
        }
    }
    let snapshots = session
        .testing_list_snapshots("ice.ns.t")
        .await
        .expect("snapshots")
        .len();
    (rows, snapshots)
}

fn wrapped_writes() -> Vec<String> {
    let mut writes = Vec::new();
    for predicate in ["id = 2", IN_SUBQUERY] {
        writes.push(format!("DELETE FROM ice.ns.t WHERE {predicate}"));
        writes.push(format!("UPDATE ice.ns.t SET v = 5 WHERE {predicate}"));
        writes.push(format!(
            "INSERT INTO ice.ns.t SELECT id + 10, w FROM ice.ns.s WHERE {predicate}"
        ));
        writes.push(format!(
            "MERGE INTO ice.ns.t AS target USING (SELECT id, w FROM ice.ns.s WHERE {predicate}) \
             AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET v = source.w"
        ));
    }
    writes
}

async fn refuses_and_holds(session: &ReparkSession, text: &str, statement: &str) {
    let before = target_state(session).await;
    let refusal = outcome(session, statement).await.expect_err(statement);
    assert!(refusal.contains(text), "{statement}: {refusal}");
    let missing = outcome(session, "EXECUTE p").await.expect_err(statement);
    assert!(
        missing.contains("Prepared statement 'p' does not exist"),
        "{statement}: {missing}"
    );
    assert_eq!(target_state(session).await, before, "{statement}");
}

#[tokio::test]
async fn the_ansi_door_refuses_an_explain_analyze_of_a_write() {
    let (session, _warehouse) = seeded_ansi_session().await;
    for write in wrapped_writes() {
        let statement = format!("EXPLAIN ANALYZE {write}");
        refuses_and_holds(&session, EXPLAIN_ANALYZE_REFUSAL, &statement).await;
    }
    refuses_and_holds(
        &session,
        EXPLAIN_ANALYZE_REFUSAL,
        "EXPLAIN ANALYZE VERBOSE DELETE FROM ice.ns.t WHERE id = 2",
    )
    .await;
    assert_eq!(target_state(&session).await.0, SEEDED_T.to_vec());
}

#[tokio::test]
async fn the_ansi_door_refuses_a_prepared_write_and_leaves_nothing_to_execute() {
    let (session, _warehouse) = seeded_ansi_session().await;
    for write in wrapped_writes() {
        refuses_and_holds(&session, PREPARE_REFUSAL, &format!("PREPARE p AS {write}")).await;
    }
    for prepare in [
        "PREPARE p AS EXPLAIN ANALYZE DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)",
        "PREPARE p(INT) AS DELETE FROM ice.ns.t WHERE id = $1",
    ] {
        refuses_and_holds(&session, PREPARE_REFUSAL, prepare).await;
    }
    assert_eq!(target_state(&session).await.0, SEEDED_T.to_vec());
}

#[tokio::test]
async fn the_ansi_door_still_answers_a_prepared_query_and_a_plain_explain() {
    let (session, _warehouse) = seeded_ansi_session().await;
    let before = target_state(&session).await;
    outcome(&session, "PREPARE q AS SELECT v FROM ice.ns.t WHERE id = 2")
        .await
        .expect("PREPARE q");
    let batches = outcome(&session, "EXECUTE q").await.expect("EXECUTE q");
    let answered: Vec<i32> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<Int32Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(answered, vec![20]);
    for write in wrapped_writes() {
        if write.starts_with("MERGE") {
            continue;
        }
        let plan = plan_text(&session, &format!("EXPLAIN {write}")).await;
        assert!(plan.contains("physical_plan"), "EXPLAIN {write}: {plan}");
    }
    for statement in [
        "EXPLAIN SELECT * FROM ice.ns.t WHERE id = 2",
        "EXPLAIN ANALYZE SELECT * FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)",
    ] {
        assert!(
            !plan_text(&session, statement).await.is_empty(),
            "{statement}"
        );
    }
    assert_eq!(target_state(&session).await, before);
}
