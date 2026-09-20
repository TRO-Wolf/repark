//! Iceberg catalog wiring for DataFusion.

use std::sync::Arc;

use datafusion::catalog::CatalogProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use iceberg::{Catalog, NamespaceIdent};

// === incremental catalog provider PERF-07 hook API (invalidate / rebuild).
mod builders;
mod cache_wiring;
mod caches;
mod catalog_ops;
mod counting_storage;
mod files;
mod io_stats;
mod lineage_columns;
mod location;
mod provider;

// Public product surface (order: provider → builders → location).
pub use provider::{
    ReparkCatalogProvider, drop_catalog_namespace_from_provider, invalidate_catalog_namespaces,
    rebuild_catalog_provider,
};
// Engine-side adapter for session `refresh_catalog_provider`, hoisted from v1 catalog_ops.
pub use builders::{
    glue_catalog, glue_catalog_counted, iceberg_to_datafusion, memory_catalog,
    memory_catalog_cached, s3tables_catalog, s3tables_catalog_counted,
};
pub use caches::{
    CatalogCaches, DEFAULT_FOOTER_CACHE_BYTES, DEFAULT_MANIFEST_CACHE_BYTES,
    DEFAULT_METADATA_CACHE_ENTRIES, FOOTER_CACHE_BYTES_KEY, FOOTER_CACHE_BYTES_KEY_ALT,
    IcebergCacheSettings, MANIFEST_CACHE_BYTES_KEY, MANIFEST_CACHE_BYTES_KEY_ALT,
    METADATA_CACHE_ENTRIES_KEY, METADATA_CACHE_ENTRIES_KEY_ALT, METADATA_CACHE_KEY,
    METADATA_CACHE_KEY_ALT,
};
pub use catalog_ops::reregister_catalog_provider;
pub use counting_storage::{
    CountingStorage, CountingStorageFactory, GLUE_DEFAULT_CONFIGURED_SCHEME,
    S3TABLES_DEFAULT_CONFIGURED_SCHEME, glue_default_storage_factory,
    s3tables_default_storage_factory,
};
pub use files::write_text_file;
pub use iceberg::TableMetadataCacheStats;
pub use iceberg::arrow::ParquetFooterCacheStats;
pub use io_stats::{
    IcebergFileClass, IcebergIoCount, IcebergIoCounters, IcebergIoOp, IcebergIoStats,
    PARQUET_TAIL_MAGIC, PUFFIN_TAIL_MAGIC, classify_iceberg_path, ranged_read_op,
};
pub use lineage_columns::{
    LineageColumnsTableProvider, table_serves_row_lineage, user_field_names,
};
pub use location::{
    NAMESPACE_LOCATION_PROPERTY, NAMESPACE_LOCATION_URI_PROPERTY, file_io_for_location,
    mirror_namespace_location_keys, resolve_namespace_location, storage_factory_for_location,
};
// Crate-private helpers used by listing/register still in this root and by sibling modules.

// File-backed tests.rs historically resolved these via `use super::*` when co-located in lib.rs.
#[cfg(test)]
pub(crate) use builders::prop_key_names;
#[cfg(test)]
pub(crate) use iceberg_catalog_glue::GLUE_CATALOG_PROP_WAREHOUSE;
#[cfg(test)]
pub(crate) use iceberg_catalog_s3tables::S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN;
#[cfg(test)]
pub(crate) use std::collections::HashMap;

// Free SQL uses a frozen provider snapshot; facade listings read live catalog names.

/// Spark Catalog listings read live names.
pub const CATALOG_LISTING_STRATEGY: &str = "list-on-access";

/// Live table names in `namespace` from the Iceberg [`Catalog`] — no DataFusion snapshot.
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

#[must_use]
pub fn schema_not_found_on_drop(catalog: &str, namespace: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[SCHEMA_NOT_FOUND] The schema `{catalog}`.`{namespace}` cannot be found. Verify the \
         spelling and correctness of the schema and catalog.\nIf you did not qualify the name \
         with a catalog, verify the current_schema() output, or qualify the name with the \
         correct catalog.\nTo tolerate the error on drop use DROP SCHEMA IF EXISTS. \
         SQLSTATE: 42704"
    ))
}

#[allow(clippy::missing_errors_doc)]
pub async fn refuse_non_empty_namespace_drop(
    catalog: &dyn Catalog,
    namespace: &NamespaceIdent,
    display: &str,
) -> Result<()> {
    let tables = catalog
        .list_tables(namespace)
        .await
        .map_err(builders::iceberg_to_datafusion)?;
    if !tables.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Namespace {display} is not empty. Contains {} table(s).",
            tables.len()
        )));
    }
    let children = catalog
        .list_namespaces(Some(namespace))
        .await
        .map_err(builders::iceberg_to_datafusion)?;
    if !children.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "Namespace {display} is not empty. Contains {} child namespace(s).",
            children.len()
        )));
    }
    Ok(())
}

/// Live namespace names from the Iceberg [`Catalog`] (top-level only) — no DataFusion snapshot.
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

/// Build a DataFusion [`CatalogProvider`] by snapshotting the live Iceberg catalog once.
/// # Errors
/// Returns an error if namespaces / table-name listings cannot be loaded.
#[tracing::instrument(name = "catalog.build_iceberg_catalog_provider", skip(catalog))]
pub async fn build_iceberg_catalog_provider(
    catalog: Arc<dyn Catalog>,
) -> Result<Arc<dyn CatalogProvider>> {
    let provider = ReparkCatalogProvider::try_new(catalog).await?;
    Ok(Arc::new(provider))
}

/// Register an iceberg [`Catalog`] as a DataFusion `CatalogProvider` under `name`.
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
mod tests;
