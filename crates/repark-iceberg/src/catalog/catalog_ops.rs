//! Full catalog-provider rebuild — the session `refresh_catalog_provider` escape hatch.
//!
//! Hoisted MOVE-ONLY from v1 `repark-sql/src/catalog_ops.rs` (phase-1 PR-B; the rest of that
//! file — P11 read-only refusals, namespace resolution, O(1) reregister helpers — stays with
//! the SQL layer and ports in phase 2). Zero behavior change.

use std::sync::Arc;

use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use iceberg::Catalog;

/// Full provider rebuild — explicit session refresh / free-SQL OOB recovery (ADR-0004).
///
/// # Errors
/// Provider build / registration failures as [`DataFusionError`].
pub async fn reregister_catalog_provider(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    name: &str,
) -> Result<()> {
    // Full O(databases) path — session `refresh_catalog_provider` and test helpers only.
    crate::catalog::rebuild_catalog_provider(ctx, catalog, name).await
}
