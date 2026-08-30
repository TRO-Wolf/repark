//! Full catalog-provider rebuild — the session `refresh_catalog_provider` escape hatch.

use std::sync::Arc;

use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use iceberg::Catalog;

/// Full provider rebuild — explicit session refresh / free-SQL OOB recovery (ADR-0004).
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
