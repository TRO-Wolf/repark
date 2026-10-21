use super::super::*;
use super::common::*;

const SEEDED: [(i32, i32); 4] = [(1, 10), (2, 20), (3, 30), (4, 40)];

async fn outcome(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Result<Vec<RecordBatch>, String> {
    match execute(ctx, catalogs, sql).await {
        Ok(frame) => frame.collect().await.map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

async fn assert_iceberg(ctx: &SessionContext, catalogs: &CatalogRegistry, table: &str) {
    let statement = format!("EXPLAIN SELECT * FROM {table}");
    let batches = outcome(ctx, catalogs, &statement).await.expect(&statement);
    let plan = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .expect("render the plan")
        .to_string();
    assert!(
        plan.contains("IcebergTableScan"),
        "{table} is not an Iceberg table:\n{plan}"
    );
}

async fn state(ctx: &SessionContext, catalogs: &CatalogRegistry) -> (Vec<(i32, i32)>, usize) {
    let batches = outcome(ctx, catalogs, "SELECT id, v FROM ice.sales.t ORDER BY id")
        .await
        .expect("read ice.sales.t");
    let mut rows = Vec::new();
    for batch in &batches {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id is INT");
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("v is INT");
        for index in 0..batch.num_rows() {
            rows.push((ids.value(index), values.value(index)));
        }
    }
    let snapshots = load_sales_table(catalogs, "t")
        .await
        .metadata()
        .snapshots()
        .count();
    (rows, snapshots)
}

async fn seeded(wh: &TempDir) -> (SessionContext, CatalogRegistry) {
    let (ctx, catalogs) = setup(wh).await;
    for sql in [
        "CREATE TABLE ice.sales.t (id INT, v INT)",
        "INSERT INTO ice.sales.t VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
        "CREATE TABLE ice.sales.s (id INT, w INT)",
        "INSERT INTO ice.sales.s VALUES (2, 20), (3, 99)",
    ] {
        run(&ctx, &catalogs, sql).await;
    }
    assert_iceberg(&ctx, &catalogs, "ice.sales.t").await;
    assert_iceberg(&ctx, &catalogs, "ice.sales.s").await;
    assert_eq!(state(&ctx, &catalogs).await, (SEEDED.to_vec(), 1));
    (ctx, catalogs)
}

const EXPLAIN_ANALYZE_REFUSAL: &str = "EXPLAIN ANALYZE is refused: its statement writes to a \
     table, and EXPLAIN ANALYZE runs the statement it explains. A plain EXPLAIN of the same \
     statement still shows its plan without running it, and EXPLAIN ANALYZE of a query still works.";

const PREPARE_REFUSAL: &str = "PREPARE `p` is refused: its statement writes to a table, and a \
     prepared write would run later, at EXECUTE. PREPARE of a query still works.";

const IN_SUBQUERY: &str = "id IN (SELECT id FROM ice.sales.s)";

fn wrapped_writes() -> Vec<String> {
    let mut writes = Vec::new();
    for predicate in ["id = 2", IN_SUBQUERY] {
        writes.push(format!("DELETE FROM ice.sales.t WHERE {predicate}"));
        writes.push(format!("UPDATE ice.sales.t SET v = 5 WHERE {predicate}"));
        writes.push(format!(
            "INSERT INTO ice.sales.t SELECT id + 10, w FROM ice.sales.s WHERE {predicate}"
        ));
        writes.push(format!(
            "MERGE INTO ice.sales.t AS target USING (SELECT id, w FROM ice.sales.s WHERE \
             {predicate}) AS source ON target.id = source.id \
             WHEN MATCHED THEN UPDATE SET v = source.w"
        ));
    }
    writes
}

async fn refuses_and_holds(text: &str, statement: &str) {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh).await;
    let refusal = outcome(&ctx, &catalogs, statement)
        .await
        .expect_err(statement);
    assert!(refusal.contains(text), "{statement}: {refusal}");
    let missing = outcome(&ctx, &catalogs, "EXECUTE p")
        .await
        .expect_err(statement);
    assert!(
        missing.contains("Prepared statement 'p' does not exist"),
        "{statement}: {missing}"
    );
    assert_eq!(
        state(&ctx, &catalogs).await,
        (SEEDED.to_vec(), 1),
        "{statement}"
    );
}

#[tokio::test]
async fn a_prepared_subquery_delete_refuses_on_the_router_door() {
    refuses_and_holds(
        PREPARE_REFUSAL,
        "PREPARE p AS DELETE FROM ice.sales.t WHERE id IN (SELECT id FROM ice.sales.s)",
    )
    .await;
}

#[tokio::test]
async fn a_prepared_subquery_update_refuses_on_the_router_door() {
    refuses_and_holds(
        PREPARE_REFUSAL,
        "PREPARE p AS UPDATE ice.sales.t SET v = 5 WHERE id IN (SELECT id FROM ice.sales.s)",
    )
    .await;
}

#[tokio::test]
async fn a_prepared_explain_analyze_subquery_delete_refuses_on_the_router_door() {
    refuses_and_holds(
        PREPARE_REFUSAL,
        "PREPARE p AS EXPLAIN ANALYZE DELETE FROM ice.sales.t \
         WHERE id IN (SELECT id FROM ice.sales.s)",
    )
    .await;
}

#[tokio::test]
async fn an_explain_analyze_of_a_write_refuses_on_the_router_door() {
    for write in wrapped_writes() {
        refuses_and_holds(EXPLAIN_ANALYZE_REFUSAL, &format!("EXPLAIN ANALYZE {write}")).await;
    }
    refuses_and_holds(
        EXPLAIN_ANALYZE_REFUSAL,
        "EXPLAIN ANALYZE VERBOSE DELETE FROM ice.sales.t WHERE id = 2",
    )
    .await;
}

#[tokio::test]
async fn a_prepared_write_refuses_on_the_router_door() {
    for write in wrapped_writes() {
        refuses_and_holds(PREPARE_REFUSAL, &format!("PREPARE p AS {write}")).await;
    }
    refuses_and_holds(
        PREPARE_REFUSAL,
        "PREPARE p(INT) AS DELETE FROM ice.sales.t WHERE id = $1",
    )
    .await;
}

#[tokio::test]
async fn a_prepared_query_still_runs_on_the_router_door() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh).await;
    let prepare = "PREPARE q AS SELECT v FROM ice.sales.t WHERE id = 2";
    outcome(&ctx, &catalogs, prepare).await.expect(prepare);
    let batches = outcome(&ctx, &catalogs, "EXECUTE q")
        .await
        .expect("EXECUTE q");
    let answered: Vec<i32> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("v is INT")
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(answered, vec![20]);
    assert_eq!(state(&ctx, &catalogs).await, (SEEDED.to_vec(), 1));
}

#[tokio::test]
async fn explain_and_explain_analyze_of_a_select_answer_on_the_router_door() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh).await;
    for statement in [
        "EXPLAIN SELECT * FROM ice.sales.t WHERE id = 2",
        "EXPLAIN ANALYZE SELECT * FROM ice.sales.t WHERE id IN (SELECT id FROM ice.sales.s)",
    ] {
        let batches = outcome(&ctx, &catalogs, statement).await.expect(statement);
        assert!(!batches.is_empty(), "{statement}");
    }
    assert_eq!(state(&ctx, &catalogs).await, (SEEDED.to_vec(), 1));
}

#[tokio::test]
async fn the_bare_in_subquery_dml_changes_exactly_the_matching_rows_on_the_router_door() {
    for (statement, expected) in [
        (
            "DELETE FROM ice.sales.t WHERE id IN (SELECT id FROM ice.sales.s)",
            vec![(1, 10), (4, 40)],
        ),
        (
            "UPDATE ice.sales.t SET v = 5 WHERE id IN (SELECT id FROM ice.sales.s)",
            vec![(1, 10), (2, 5), (3, 5), (4, 40)],
        ),
    ] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = seeded(&wh).await;
        outcome(&ctx, &catalogs, statement).await.expect(statement);
        assert_eq!(state(&ctx, &catalogs).await, (expected, 2), "{statement}");
    }
}

#[tokio::test]
async fn a_plain_predicate_prepare_refuses_on_the_router_door() {
    for prepare in [
        "PREPARE p AS DELETE FROM ice.sales.t WHERE id = 2",
        "PREPARE p AS UPDATE ice.sales.t SET v = 5 WHERE id = 2",
    ] {
        refuses_and_holds(PREPARE_REFUSAL, prepare).await;
    }
}

#[tokio::test]
async fn a_plain_explain_of_the_subquery_delete_answers_its_plan_on_the_router_door() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh).await;
    for write in wrapped_writes() {
        if write.starts_with("MERGE") {
            continue;
        }
        let statement = format!("EXPLAIN {write}");
        let batches = outcome(&ctx, &catalogs, &statement)
            .await
            .expect(&statement);
        let plan = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .expect("render the plan")
            .to_string();
        assert!(
            plan.contains("logical_plan") && plan.contains("physical_plan"),
            "{statement}: {plan}"
        );
    }
    assert_eq!(state(&ctx, &catalogs).await, (SEEDED.to_vec(), 1));
}
