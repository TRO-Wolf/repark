//! Iceberg catalog wiring for DataFusion.
//!
//! Catalog builders validate required properties before calling the owned fork and pass remaining
//! properties to Iceberg `FileIO`. Credentials remain inside the fork's AWS SDK chain.
//!
//! CTAS must be decomposed into schema-only `CREATE` followed by `INSERT INTO`: the fork rejects
//! the data-bearing `MemTable` that DataFusion supplies to `register_table`.

use std::sync::Arc;

use datafusion::catalog::CatalogProvider;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use iceberg::{Catalog, NamespaceIdent};

// === r24 P7: incremental catalog provider =====================================================
// PERF-07 hook API (invalidate / rebuild). OBS1 may instrument later — additive only.
mod builders;
mod catalog_ops;
mod location;
mod metadata_projection;
mod provider;

// Public product surface (order: provider → projection → builders → location).
pub use provider::{
    ReparkCatalogProvider, drop_catalog_namespace_from_provider, invalidate_catalog_namespaces,
    rebuild_catalog_provider,
};
// Hoisted from v1 repark-sql/catalog_ops.rs (phase-1 PR-B, move-only): the session
// `refresh_catalog_provider` escape hatch's engine-side adapter.
pub use catalog_ops::reregister_catalog_provider;
// r25 T2 item 0: projection wrap for fork metadata-table providers (registered via provider).
pub use builders::{glue_catalog, iceberg_to_datafusion, memory_catalog, s3tables_catalog};
pub use location::{
    NAMESPACE_LOCATION_PROPERTY, NAMESPACE_LOCATION_URI_PROPERTY, file_io_for_location,
    mirror_namespace_location_keys, resolve_namespace_location, storage_factory_for_location,
};
pub use metadata_projection::{MetadataProjectionSchemaProvider, ProjectingMetadataTableProvider};

// Crate-private helpers used by listing/register still in this root and by sibling modules.

// File-backed tests.rs historically resolved these via `use super::*` when co-located in
// lib.rs. Keep them at this module root under cfg(test) so the test battery stays MOVE-ONLY.
#[cfg(test)]
pub(crate) use builders::prop_key_names;
#[cfg(test)]
pub(crate) use iceberg_catalog_glue::GLUE_CATALOG_PROP_WAREHOUSE;
#[cfg(test)]
pub(crate) use iceberg_catalog_s3tables::S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN;
#[cfg(test)]
pub(crate) use std::collections::HashMap;

// Free SQL uses a frozen provider snapshot; facade listings read live catalog names. Product DDL
// invalidates only touched namespaces, while explicit refresh rebuilds the full provider.

/// Spark Catalog listings read live names; full provider rebuilds are reserved for explicit refresh.
pub const CATALOG_LISTING_STRATEGY: &str = "list-on-access";

/// ===========================================================================================
/// Live table names in `namespace` from the Iceberg [`Catalog`] — no DataFusion snapshot.
///
/// This is the list-on-access primitive behind the Spark Catalog facade `listTables` (T6 /
/// CQ-008 / BUG-007). Out-of-band creates and drops on the same catalog handle are visible
/// immediately; the DF `IcebergCatalogProvider` name directory is intentionally not consulted.
/// ===========================================================================================
///
/// # Errors
/// Returns a plan error when the catalog cannot list the namespace (missing namespace, IO, …).
#[tracing::instrument(
    name = "catalog.list_table_names",
    skip(catalog, namespace),
    fields(namespace = %namespace)
)]
pub async fn list_table_names(catalog: &dyn Catalog, namespace: &str) -> Result<Vec<String>> {
    let namespace_ident = NamespaceIdent::new(namespace.to_string());
    let tables = catalog
        .list_tables(&namespace_ident)
        .await
        .map_err(iceberg_to_datafusion)?;
    Ok(tables
        .into_iter()
        .map(|ident| ident.name().to_string())
        .collect())
}

/// ===========================================================================================
/// Live namespace names from the Iceberg [`Catalog`] (top-level only) — no DataFusion snapshot.
///
/// Sibling of [`list_table_names`] for callers that cannot go through `SHOW NAMESPACES`
/// (already live in `repark-sql`). Same list-on-access contract.
/// ===========================================================================================
///
/// # Errors
/// Returns a plan error when the catalog cannot list namespaces.
#[tracing::instrument(name = "catalog.list_namespace_names", skip(catalog))]
pub async fn list_namespace_names(catalog: &dyn Catalog) -> Result<Vec<String>> {
    let namespaces = catalog
        .list_namespaces(None)
        .await
        .map_err(iceberg_to_datafusion)?;
    Ok(namespaces
        .into_iter()
        .flat_map(|namespace| namespace.as_ref().clone())
        .collect())
}

/// ===========================================================================================
/// Build a DataFusion [`CatalogProvider`] by snapshotting the live Iceberg catalog once
/// ([`ReparkCatalogProvider::try_new`] — full O(databases) walk, same cost class as the fork's
/// `IcebergCatalogProvider::try_new`).
///
/// Prefer [`list_table_names`] for facade listing. Prefer [`invalidate_catalog_namespaces`] after
/// product DDL (O(1) per touched namespace). Use this (or [`rebuild_catalog_provider`]) when free
/// SQL / `information_schema` must see the current name directory after out-of-band DDL.
/// ===========================================================================================
///
/// # Errors
/// Returns an error if namespaces / table-name listings cannot be loaded.
#[tracing::instrument(name = "catalog.build_iceberg_catalog_provider", skip(catalog))]
pub async fn build_iceberg_catalog_provider(
    catalog: Arc<dyn Catalog>,
) -> Result<Arc<dyn CatalogProvider>> {
    let provider = ReparkCatalogProvider::try_new(catalog).await?;
    Ok(Arc::new(provider))
}

/// ===========================================================================================
/// Register an iceberg [`Catalog`] as a DataFusion `CatalogProvider` under `name`.
///
/// After this, `name.namespace.table` resolves directly in `spark.sql` / `DataFrame` queries.
///
/// The registered provider is a [`ReparkCatalogProvider`]: name directories are snapshotted at
/// construction (fork CQ-008 / BUG-007 residual for free SQL). Product SQL mutators invalidate
/// the **touched namespace only** via [`invalidate_catalog_namespaces`] (PERF-07). The Spark
/// Catalog facade lists tables **live** via [`list_table_names`] (list-on-access —
/// [`CATALOG_LISTING_STRATEGY`]); free SQL still needs invalidation / refresh after OOB mutations
/// (ADR-0004).
/// ===========================================================================================
///
/// # Errors
/// Returns an error if the catalog's namespaces/schemas cannot be loaded.
#[tracing::instrument(
    name = "catalog.register_iceberg_catalog",
    skip(ctx, catalog, name),
    fields(catalog_name = %name)
)]
pub async fn register_iceberg_catalog(
    ctx: &SessionContext,
    name: &str,
    catalog: Arc<dyn Catalog>,
) -> Result<()> {
    let provider = build_iceberg_catalog_provider(catalog).await?;
    ctx.register_catalog(name, provider);
    Ok(())
}

#[cfg(test)]
pub(crate) use location::{
    LocationBackend, classify_location_backend, has_colon_before_first_slash,
};
#[cfg(test)]
mod namespace_scoped_tests;
#[cfg(test)]
mod tests;
