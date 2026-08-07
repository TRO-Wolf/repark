//! Seam tests: the phase-1 default dialect and the [`EngineContext`] construction contract.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
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
