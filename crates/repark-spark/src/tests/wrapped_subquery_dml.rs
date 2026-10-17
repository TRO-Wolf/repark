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

fn assert_g3e8(text: &str, verb: &str, sql: &str) {
    for part in [
        format!("{verb} with a subquery in its WHERE clause is refused on `"),
        "subquery predicates are silently mis-executed".to_string(),
        "(defect G3-E8, silent data loss)".to_string(),
        "MERGE INTO".to_string(),
        "Support returns when the underlying fix lands".to_string(),
    ] {
        assert!(text.contains(&part), "{sql}: missing {part:?} in {text}");
    }
}

async fn refuses_and_holds(verb: &str, prepare: &str) {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh).await;
    let refusal = outcome(&ctx, &catalogs, prepare).await.expect_err(prepare);
    assert_g3e8(&refusal, verb, prepare);
    let missing = outcome(&ctx, &catalogs, "EXECUTE p")
        .await
        .expect_err(prepare);
    assert!(
        missing.contains("Prepared statement 'p' does not exist"),
        "{prepare}: {missing}"
    );
    assert_eq!(
        state(&ctx, &catalogs).await,
        (SEEDED.to_vec(), 1),
        "{prepare}"
    );
}

#[tokio::test]
async fn a_prepared_subquery_delete_refuses_on_the_router_door() {
    refuses_and_holds(
        "DELETE",
        "PREPARE p AS DELETE FROM ice.sales.t WHERE id IN (SELECT id FROM ice.sales.s)",
    )
    .await;
}

#[tokio::test]
async fn a_prepared_subquery_update_refuses_on_the_router_door() {
    refuses_and_holds(
        "UPDATE",
        "PREPARE p AS UPDATE ice.sales.t SET v = 5 WHERE id IN (SELECT id FROM ice.sales.s)",
    )
    .await;
}

#[tokio::test]
async fn a_prepared_explain_analyze_subquery_delete_refuses_on_the_router_door() {
    refuses_and_holds(
        "DELETE",
        "PREPARE p AS EXPLAIN ANALYZE DELETE FROM ice.sales.t \
         WHERE id IN (SELECT id FROM ice.sales.s)",
    )
    .await;
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
async fn a_plain_predicate_prepare_still_runs_on_the_router_door() {
    for (prepare, expected) in [
        (
            "PREPARE p AS DELETE FROM ice.sales.t WHERE id = 2",
            vec![(1, 10), (3, 30), (4, 40)],
        ),
        (
            "PREPARE p AS UPDATE ice.sales.t SET v = 5 WHERE id = 2",
            vec![(1, 10), (2, 5), (3, 30), (4, 40)],
        ),
    ] {
        let wh = TempDir::new().unwrap();
        let (ctx, catalogs) = seeded(&wh).await;
        outcome(&ctx, &catalogs, prepare).await.expect(prepare);
        outcome(&ctx, &catalogs, "EXECUTE p")
            .await
            .expect("EXECUTE p");
        assert_eq!(state(&ctx, &catalogs).await, (expected, 2), "{prepare}");
    }
}

#[tokio::test]
async fn a_plain_explain_of_the_subquery_delete_answers_its_plan_on_the_router_door() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs) = seeded(&wh).await;
    let statement = "EXPLAIN DELETE FROM ice.sales.t WHERE id IN (SELECT id FROM ice.sales.s)";
    let batches = outcome(&ctx, &catalogs, statement).await.expect(statement);
    let plan = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .expect("render the plan")
        .to_string();
    assert!(
        plan.contains("logical_plan") && plan.contains("physical_plan"),
        "{plan}"
    );
    assert_eq!(state(&ctx, &catalogs).await, (SEEDED.to_vec(), 1));
}
