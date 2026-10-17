//! Seam tests: the default dialect and the [`EngineContext`] construction contract.

use crate::session_time_zone::SessionTimeZone;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::Int32Type;
use datafusion::prelude::SessionContext;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{NamespaceIdent, TableCreation};

use super::{DataFusionDialect, EngineContext, SqlDialect, refuse_dml_subquery_where};
use crate::catalog_state::CatalogRegistry;

/// PIN — `DataFusionDialect` is a pure passthrough: a trivial query executes against a BARE
/// `SessionContext` (no catalogs registered, no read-only names) and returns DataFusion's own
/// result. Risk covered: the default dialect accidentally growing statement interception or
/// requiring session state the phase-1 core does not carry.
#[tokio::test]
async fn datafusion_dialect_passthrough_executes_trivial_query() {
    let ctx = SessionContext::new();
    let catalogs = CatalogRegistry::new();
    let read_only: HashSet<String> = HashSet::new();
    let frame = DataFusionDialect
        .execute(
            EngineContext {
                ctx: &ctx,
                catalogs: &catalogs,
                read_only: &read_only,
                overwrite_intent: crate::OverwriteIntent::Session,
                source_by_name: false,
                session_time_zone: SessionTimeZone::default(),
                temp_views: None,
                verbatim_override: None,
            },
            "SELECT 1 + 1 AS two",
        )
        .await
        .expect("the passthrough dialect must execute a trivial query");
    let batches = frame
        .collect()
        .await
        .expect("collect the passthrough result");
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    let column = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("SELECT 1 + 1 yields an Int64 column");
    assert_eq!(column.value(0), 2);
}

/// PIN — `EngineContext` constructs with explicit named fields inside the defining crate
/// (`#[non_exhaustive]` constrains DOWNSTREAM constructors only — the session's own construction
/// in `sql_with` must keep compiling), and the context passes through an `Arc<dyn SqlDialect>` —
/// the exact object shape `sql_with` receives.
#[tokio::test]
async fn engine_context_constructs_with_explicit_fields() {
    let ctx = SessionContext::new();
    let catalogs = CatalogRegistry::new();
    let read_only: HashSet<String> = HashSet::new();
    let cx = EngineContext {
        ctx: &ctx,
        catalogs: &catalogs,
        read_only: &read_only,
        overwrite_intent: crate::OverwriteIntent::Session,
        source_by_name: false,
        session_time_zone: SessionTimeZone::default(),
        temp_views: None,
        verbatim_override: None,
    };
    assert!(cx.read_only.is_empty());
    let dialect: Arc<dyn SqlDialect> = Arc::new(DataFusionDialect);
    let frame = dialect
        .execute(cx, "SELECT 42 AS answer")
        .await
        .expect("an Arc'd dialect executes with an explicitly constructed context");
    assert_eq!(frame.schema().fields().len(), 1);
    assert_eq!(frame.schema().field(0).name(), "answer");
}

/// `EngineContext::new` is the downstream constructor and preserves field mapping.
#[tokio::test]
async fn engine_context_new_is_the_downstream_constructor() {
    let ctx = SessionContext::new();
    let catalogs = CatalogRegistry::new();
    let read_only: HashSet<String> = HashSet::from(["ro_pg".to_string()]);
    let cx = EngineContext::new(&ctx, &catalogs, &read_only);
    assert!(cx.read_only.contains("ro_pg"));
    let frame = DataFusionDialect
        .execute(cx, "SELECT 7 AS seven")
        .await
        .expect("a `new`-constructed context executes");
    assert_eq!(frame.schema().field(0).name(), "seven");
}

const SEEDED_T: [(i32, i32); 4] = [(1, 10), (2, 20), (3, 30), (4, 40)];

async fn run(session: &crate::ReparkSession, statement: &str) -> Result<Vec<RecordBatch>, String> {
    match session.sql(statement).await {
        Ok(frame) => frame.collect().await.map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

async fn plan_text(session: &crate::ReparkSession, statement: &str) -> Vec<(String, String)> {
    let batches = run(session, statement).await.expect(statement);
    let mut plans = Vec::new();
    for batch in &batches {
        let kinds = batch.column(0).as_string::<i32>();
        let texts = batch.column(1).as_string::<i32>();
        for index in 0..batch.num_rows() {
            plans.push((
                kinds.value(index).to_string(),
                texts.value(index).to_string(),
            ));
        }
    }
    plans
}

async fn assert_iceberg(session: &crate::ReparkSession, table: &str) {
    let plans = plan_text(session, &format!("EXPLAIN SELECT * FROM {table}")).await;
    assert!(
        plans
            .iter()
            .any(|(_, text)| text.contains("IcebergTableScan")),
        "{table} is not an Iceberg table: {plans:?}"
    );
}

async fn seeded_default_session(root: &str) -> crate::ReparkSession {
    let session = crate::ReparkSession::new().expect("default session");
    session
        .register_memory_catalog("ice", root)
        .await
        .expect("memory catalog");
    session
        .create_namespace("ice", "ns", HashMap::new())
        .await
        .expect("namespace");
    let handle = session
        .catalogs_snapshot()
        .get("ice")
        .cloned()
        .expect("the ice catalog");
    let namespace = NamespaceIdent::new("ns".to_string());
    for (name, value) in [("t", "v"), ("s", "w")] {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, value, Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("schema");
        let creation = TableCreation::builder()
            .name(name.to_string())
            .location(format!("{root}/ns/{name}"))
            .schema(schema)
            .properties(HashMap::new())
            .build();
        handle
            .create_table(&namespace, creation)
            .await
            .expect("create table");
    }
    session
        .refresh_catalog_provider("ice")
        .await
        .expect("refresh");
    for statement in [
        "INSERT INTO ice.ns.t VALUES (1, 10), (2, 20), (3, 30), (4, 40)",
        "INSERT INTO ice.ns.s VALUES (2, 20), (3, 99)",
    ] {
        run(&session, statement).await.expect(statement);
    }
    assert_iceberg(&session, "ice.ns.t").await;
    assert_iceberg(&session, "ice.ns.s").await;
    assert_eq!(target_state(&session).await, (SEEDED_T.to_vec(), 1));
    session
}

async fn target_rows(session: &crate::ReparkSession) -> Vec<(i32, i32)> {
    let batches = run(session, "SELECT id, v FROM ice.ns.t ORDER BY id")
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
    rows
}

async fn target_state(session: &crate::ReparkSession) -> (Vec<(i32, i32)>, usize) {
    let snapshots = session
        .testing_list_snapshots("ice.ns.t")
        .await
        .expect("snapshots");
    (target_rows(session).await, snapshots.len())
}

async fn refuse_and_hold(session: &crate::ReparkSession, verb: &str, table: &str, statement: &str) {
    let before = target_state(session).await;
    let refusal = run(session, statement).await.expect_err(statement);
    assert!(
        refusal.contains(&format!(
            "{verb} with a subquery in its WHERE clause is refused on `{table}`"
        )),
        "{statement}: {refusal}"
    );
    assert!(
        refusal.contains("subquery predicates are silently mis-executed"),
        "{statement}: {refusal}"
    );
    assert_eq!(target_state(session).await, before, "{statement}");
}

#[tokio::test]
async fn the_default_dialect_refuses_a_subquery_in_update_or_delete_where() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let root = warehouse.path().to_str().expect("utf-8 path");
    let session = seeded_default_session(root).await;
    for (verb, statement) in [
        (
            "UPDATE",
            "UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT id FROM ice.ns.s)",
        ),
        (
            "DELETE",
            "DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)",
        ),
        (
            "UPDATE",
            "UPDATE ice.ns.t SET v = 5 WHERE EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = t.id)",
        ),
        (
            "DELETE",
            "DELETE FROM ice.ns.t WHERE EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = t.id)",
        ),
        (
            "DELETE",
            "DELETE FROM ice.ns.t WHERE id NOT IN (SELECT id FROM ice.ns.s)",
        ),
    ] {
        refuse_and_hold(&session, verb, "ice.ns.t", statement).await;
    }
}

#[tokio::test]
async fn the_default_dialect_refuses_an_explain_analyze_of_a_subquery_dml() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let root = warehouse.path().to_str().expect("utf-8 path");
    let session = seeded_default_session(root).await;
    for (verb, statement) in [
        (
            "DELETE",
            "EXPLAIN ANALYZE DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)",
        ),
        (
            "UPDATE",
            "EXPLAIN ANALYZE UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT id FROM ice.ns.s)",
        ),
        (
            "UPDATE",
            "EXPLAIN ANALYZE VERBOSE UPDATE ice.ns.t SET v = 5 \
             WHERE EXISTS (SELECT 1 FROM ice.ns.s WHERE s.id = t.id)",
        ),
    ] {
        refuse_and_hold(&session, verb, "ice.ns.t", statement).await;
    }
}

#[tokio::test]
async fn a_plain_explain_of_a_subquery_dml_answers_its_plan() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let root = warehouse.path().to_str().expect("utf-8 path");
    let session = seeded_default_session(root).await;
    for statement in [
        "EXPLAIN DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)",
        "EXPLAIN UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT id FROM ice.ns.s)",
    ] {
        let before = target_state(&session).await;
        let kinds: HashSet<String> = plan_text(&session, statement)
            .await
            .into_iter()
            .map(|(kind, _)| kind)
            .collect();
        assert!(
            kinds.contains("logical_plan") && kinds.contains("physical_plan"),
            "{statement}: {kinds:?}"
        );
        assert_eq!(target_state(&session).await, before, "{statement}");
    }
}

#[tokio::test]
async fn a_prepared_subquery_dml_refuses_and_leaves_nothing_to_execute() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let root = warehouse.path().to_str().expect("utf-8 path");
    let session = seeded_default_session(root).await;
    for (verb, prepare, execute) in [
        (
            "DELETE",
            "PREPARE p AS DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)",
            "EXECUTE p",
        ),
        (
            "UPDATE",
            "PREPARE p AS UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT id FROM ice.ns.s)",
            "EXECUTE p",
        ),
        (
            "DELETE",
            "PREPARE p AS EXPLAIN ANALYZE DELETE FROM ice.ns.t WHERE id IN (SELECT id FROM ice.ns.s)",
            "EXECUTE p",
        ),
        (
            "DELETE",
            "PREPARE p(INT) AS DELETE FROM ice.ns.t \
             WHERE id IN (SELECT id FROM ice.ns.s WHERE w > $1)",
            "EXECUTE p(0)",
        ),
    ] {
        let before = target_state(&session).await;
        refuse_and_hold(&session, verb, "ice.ns.t", prepare).await;
        run(&session, execute).await.expect_err(execute);
        assert_eq!(target_state(&session).await, before, "{execute}");
    }
}

#[tokio::test]
async fn a_non_default_parser_dialect_still_refuses() {
    let warehouse = tempfile::TempDir::new().expect("warehouse");
    let root = warehouse.path().to_str().expect("utf-8 path");
    let session = seeded_default_session(root).await;
    run(&session, "SET datafusion.sql_parser.dialect = 'MsSQL'")
        .await
        .expect("set the dialect");
    refuse_and_hold(
        &session,
        "DELETE",
        "[ice].[ns].[t]",
        "DELETE FROM [ice].[ns].[t] WHERE [id] IN (SELECT [id] FROM [ice].[ns].[s])",
    )
    .await;
}

#[test]
fn only_explain_analyze_reaches_the_statement_it_wraps() {
    let ctx = SessionContext::new();
    for statement in [
        "EXPLAIN ANALYZE DELETE FROM t WHERE id IN (SELECT id FROM s)",
        "EXPLAIN ANALYZE EXPLAIN ANALYZE DELETE FROM t WHERE id IN (SELECT id FROM s)",
        "PREPARE p AS EXPLAIN ANALYZE DELETE FROM t WHERE id IN (SELECT id FROM s)",
    ] {
        let refusal = refuse_dml_subquery_where(&ctx, statement)
            .expect_err(statement)
            .to_string();
        assert!(
            refusal.contains("DELETE with a subquery in its WHERE clause is refused on `t`"),
            "{statement}: {refusal}"
        );
    }
    for statement in [
        "EXPLAIN DELETE FROM t WHERE id IN (SELECT id FROM s)",
        "EXPLAIN ANALYZE EXPLAIN DELETE FROM t WHERE id IN (SELECT id FROM s)",
        "PREPARE p AS EXPLAIN DELETE FROM t WHERE id IN (SELECT id FROM s)",
    ] {
        refuse_dml_subquery_where(&ctx, statement).expect(statement);
    }
}
