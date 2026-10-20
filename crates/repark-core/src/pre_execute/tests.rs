//! Pre-execute pins: planning is side-effect free and execution orders plan → guard → execute.

use std::sync::Arc;

use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

use super::PreExecute;
use crate::catalog_state::CatalogRegistry;

fn ctx_with_source() -> SessionContext {
    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .expect("batch");
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("memtable");
    ctx.register_table("src", Arc::new(table))
        .expect("register source");
    ctx
}

/// Z-3 pin: `plan` does not execute a DDL sink; direct `SessionContext::sql` provides the contrast.
#[tokio::test]
async fn plan_does_not_execute_a_ddl_sink_but_sql_does() {
    let ctx = ctx_with_source();
    let catalogs = CatalogRegistry::new();
    let belt = PreExecute::new(&ctx, &catalogs);

    let plan = belt
        .plan("SELECT * INTO planned_only FROM src")
        .await
        .expect("the statement must plan");
    assert!(
        ctx.table("planned_only").await.is_err(),
        "planning must not publish the DDL sink's target"
    );

    // Contrast: the eager path publishes it. (Same session, different target name.)
    ctx.sql("SELECT * INTO executed_eagerly FROM src")
        .await
        .expect("eager sql");
    assert!(
        ctx.table("executed_eagerly").await.is_ok(),
        "`SessionContext::sql` executes the sink — the behaviour the belt exists to interpose on"
    );

    // …and executing the planned statement afterwards still works.
    belt.execute(plan).await.expect("execute the planned sink");
    assert!(
        ctx.table("planned_only").await.is_ok(),
        "execute must publish what plan deliberately did not"
    );
}

/// PIN — `run` is plan → guard → execute, and an ordinary query still returns its rows. Kills:
/// a belt that swallows results, or one whose guard refuses statements it has no business
/// refusing (an empty catalog registry means nothing resolves into an Iceberg catalog).
#[tokio::test]
async fn run_executes_an_ordinary_query_through_the_guard() {
    let ctx = ctx_with_source();
    let catalogs = CatalogRegistry::new();
    let belt = PreExecute::new(&ctx, &catalogs);
    let batches = belt
        .run("SELECT n FROM src")
        .await
        .expect("the belt must execute an ordinary query")
        .collect()
        .await
        .expect("collect");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
}

/// PIN — the guard is a no-op for a plan that is not a DDL sink into a registered Iceberg
/// catalog. Kills: the choke point growing into a blanket statement refusal.
#[tokio::test]
async fn guard_is_a_no_op_for_a_plain_select() {
    let ctx = ctx_with_source();
    let catalogs = CatalogRegistry::new();
    let belt = PreExecute::new(&ctx, &catalogs);
    let plan = belt.plan("SELECT n FROM src").await.expect("plan");
    belt.guard(&plan)
        .expect("a plain SELECT must pass the guard");
}

const UPDATE_SUBQUERY_REFUSAL: &str = "UPDATE with SET column = (SELECT ...) is not supported \
     on this statement: RePark cannot write the subquery value here, so it refuses before any \
     write.";

fn ctx_with_update_tables() -> SessionContext {
    let ctx = ctx_with_source();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("n", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![0, 3])),
            Arc::new(Int64Array::from(vec![10, 30])),
        ],
    )
    .expect("batch");
    let table = MemTable::try_new(schema, vec![vec![batch]]).expect("memtable");
    ctx.register_table("target", Arc::new(table))
        .expect("register target");
    ctx
}

#[tokio::test]
async fn guard_refuses_an_update_whose_set_value_is_a_scalar_subquery() {
    let ctx = ctx_with_update_tables();
    let catalogs = CatalogRegistry::new();
    let belt = PreExecute::new(&ctx, &catalogs);
    for sql in [
        "UPDATE target SET n = (SELECT n FROM src) WHERE true",
        "UPDATE target SET n = (SELECT n FROM src) WHERE id IN (0, 3)",
        "UPDATE target SET n = (SELECT n FROM src)",
        "UPDATE target SET n = (SELECT n FROM src WHERE n = 1) WHERE id = 0",
        "UPDATE target SET n = ((SELECT n FROM src)), id = id + 1",
        "UPDATE target SET n = (SELECT CAST(n AS INT) FROM src)",
        "UPDATE target SET n = arrow_cast(((SELECT n FROM src)), 'Int64') WHERE true",
        "UPDATE target SET n = 1 + (SELECT n FROM src) WHERE true",
        "UPDATE target SET n = (SELECT n FROM src) + 1 WHERE true",
        "UPDATE target SET n = -(SELECT n FROM src)",
        "UPDATE target SET n = abs((SELECT n FROM src))",
        "UPDATE target SET n = CASE WHEN true THEN (SELECT n FROM src) ELSE 0 END",
        "UPDATE target SET n = COALESCE((SELECT n FROM src), 0)",
        "UPDATE target SET n = named_struct('a', (SELECT n FROM src), 'b', 0)['a']",
    ] {
        let plan = belt.plan(sql).await.expect("plan");
        let refusal = belt.guard(&plan).expect_err(sql).to_string();
        assert!(
            refusal.contains(UPDATE_SUBQUERY_REFUSAL),
            "{sql}: {refusal}"
        );
    }
}

#[tokio::test]
async fn guard_passes_an_update_whose_set_value_has_no_scalar_subquery() {
    let ctx = ctx_with_update_tables();
    let catalogs = CatalogRegistry::new();
    let belt = PreExecute::new(&ctx, &catalogs);
    for sql in [
        "UPDATE target SET n = 5 WHERE true",
        "UPDATE target SET n = n + 1",
        "UPDATE target SET n = 5 WHERE id IN (SELECT n FROM src)",
        "UPDATE target SET n = arrow_cast(n, 'Int64') WHERE true",
    ] {
        let plan = belt.plan(sql).await.expect("plan");
        belt.guard(&plan)
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}
