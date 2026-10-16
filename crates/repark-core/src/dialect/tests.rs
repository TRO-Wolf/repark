//! Seam tests: the default dialect and the [`EngineContext`] construction contract.

use crate::session_time_zone::SessionTimeZone;
use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Int64Array};
use datafusion::arrow::datatypes::Int32Type;
use datafusion::prelude::SessionContext;

use super::{DataFusionDialect, EngineContext, SqlDialect};
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

async fn seeded_default_session(root: &str) -> crate::ReparkSession {
    let session = crate::ReparkSession::new().expect("default session");
    session
        .register_memory_catalog("ice", root)
        .await
        .expect("memory catalog");
    for sql in [
        format!("CREATE SCHEMA ice.ns WITH (location = '{root}/ns')"),
        "CREATE TABLE ice.ns.t AS SELECT CAST(id AS INT) AS id, CAST(v AS INT) AS v \
         FROM (VALUES (1, 10), (2, 20), (3, 30), (4, 40)) AS x(id, v)"
            .to_string(),
        "CREATE TABLE ice.ns.s AS SELECT CAST(id AS INT) AS id, CAST(w AS INT) AS w \
         FROM (VALUES (2, 20), (3, 99)) AS x(id, w)"
            .to_string(),
    ] {
        session
            .sql(&sql)
            .await
            .expect("plans")
            .collect()
            .await
            .expect("runs");
    }
    session
}

async fn target_rows(session: &crate::ReparkSession) -> Vec<(i32, i32)> {
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
        let outcome = match session.sql(statement).await {
            Ok(frame) => frame
                .collect()
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };
        let refusal = outcome.expect_err(statement);
        assert!(
            refusal.contains(&format!(
                "{verb} with a subquery in its WHERE clause is refused on `ice.ns.t`"
            )),
            "{statement}: {refusal}"
        );
        assert!(
            refusal.contains("subquery predicates are silently mis-executed"),
            "{statement}: {refusal}"
        );
        assert_eq!(target_rows(&session).await, SEEDED_T, "{statement}");
    }
}
