//! Catalog builders (memory / Glue / S3 Tables) + shared prop helpers.

use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_glue::{GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalogBuilder};
use iceberg_catalog_s3tables::{S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN, S3TablesCatalogBuilder};
use tracing::Instrument;

use crate::catalog::cache_wiring::wire_caches;
use crate::catalog::caches::{CatalogCaches, IcebergCacheSettings};
use crate::catalog::counting_storage::{
    CountingStorageFactory, glue_default_storage_factory, s3tables_default_storage_factory,
};
use crate::catalog::location::storage_factory_for_location;

/// Build the AWS-free in-memory catalog over `warehouse` for local development and tests.
/// # Errors
/// Returns an error when the builder rejects the warehouse configuration.
#[tracing::instrument(
    name = "catalog.memory_catalog",
    skip(warehouse),
    fields(warehouse = %warehouse)
)]
pub async fn memory_catalog(warehouse: &str) -> Result<Arc<dyn Catalog>> {
    memory_catalog_cached(
        warehouse,
        &CatalogCaches::new(IcebergCacheSettings::default()),
    )
    .await
}

/// # Errors
/// Returns an error when the builder rejects the warehouse configuration.
#[tracing::instrument(
    name = "catalog.memory_catalog_cached",
    skip(warehouse, caches),
    fields(
        warehouse = %warehouse,
        metadata_cache = caches.metadata_cache().is_some(),
        manifest_cache_bytes = caches.manifest_cache_bytes(),
        footer_cache = caches.footer_cache().is_some()
    )
)]
pub async fn memory_catalog_cached(
    warehouse: &str,
    caches: &CatalogCaches,
) -> Result<Arc<dyn Catalog>> {
    memory_catalog_wired(warehouse, caches, HashMap::new()).await
}

#[allow(clippy::missing_errors_doc)]
#[tracing::instrument(
    name = "catalog.memory_catalog_cached_with_props",
    skip(warehouse, caches, props),
    fields(
        warehouse = %warehouse,
        metadata_cache = caches.metadata_cache().is_some(),
        manifest_cache_bytes = caches.manifest_cache_bytes(),
        footer_cache = caches.footer_cache().is_some()
    )
)]
pub async fn memory_catalog_cached_with_props<S: BuildHasher>(
    warehouse: &str,
    caches: &CatalogCaches,
    props: &HashMap<String, String, S>,
) -> Result<Arc<dyn Catalog>> {
    memory_catalog_wired(warehouse, caches, clone_props(props)).await
}

pub(crate) async fn memory_catalog_wired(
    warehouse: &str,
    caches: &CatalogCaches,
    mut props: HashMap<String, String>,
) -> Result<Arc<dyn Catalog>> {
    props.insert(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.to_string());
    let builder = MemoryCatalogBuilder::default().with_storage_factory(Arc::new(
        CountingStorageFactory::new(
            storage_factory_for_location(warehouse)?,
            caches.io_counters(),
        ),
    ));
    let catalog = wire_caches(builder, caches, &props)
        .load("memory", props)
        .await
        .map_err(iceberg_to_datafusion)?;
    Ok(Arc::new(catalog))
}

/// Build the AWS Glue catalog from `props`.
/// # Errors
/// Returns an error when `warehouse` is absent, empty, or rejected by the fork builder.
pub async fn glue_catalog<S: BuildHasher>(
    props: &HashMap<String, String, S>,
) -> Result<Arc<dyn Catalog>> {
    glue_catalog_counted(props, &CatalogCaches::disabled()).await
}

#[allow(clippy::missing_errors_doc)]
pub async fn glue_catalog_counted<S: BuildHasher>(
    props: &HashMap<String, String, S>,
    caches: &CatalogCaches,
) -> Result<Arc<dyn Catalog>> {
    // Record property names only because this map can contain credentials.
    let prop_keys = prop_key_names(props);
    let has_warehouse = props
        .get(GLUE_CATALOG_PROP_WAREHOUSE)
        .is_some_and(|value| !value.trim().is_empty());
    async move {
        require_non_empty_prop(props, GLUE_CATALOG_PROP_WAREHOUSE, "Glue")?;
        let builder = GlueCatalogBuilder::default().with_storage_factory(Arc::new(
            CountingStorageFactory::new(
                Arc::new(glue_default_storage_factory()),
                caches.io_counters(),
            ),
        ));
        let catalog = wire_caches(builder, caches, props)
            .load("glue", clone_props(props))
            .await
            .map_err(iceberg_to_datafusion)?;
        Ok(Arc::new(catalog) as Arc<dyn Catalog>)
    }
    .instrument(tracing::info_span!(
        "catalog.glue_catalog",
        prop_keys = %prop_keys,
        has_warehouse = has_warehouse,
    ))
    .await
}

/// Build the AWS S3 Tables Iceberg catalog from `props` over the fork's `S3TablesCatalogBuilder`.
/// # Errors
/// Returns an error if `table_bucket_arn` is absent/empty or the fork builder rejects config.
pub async fn s3tables_catalog<S: BuildHasher>(
    props: &HashMap<String, String, S>,
) -> Result<Arc<dyn Catalog>> {
    s3tables_catalog_counted(props, &CatalogCaches::disabled()).await
}

#[allow(clippy::missing_errors_doc)]
pub async fn s3tables_catalog_counted<S: BuildHasher>(
    props: &HashMap<String, String, S>,
    caches: &CatalogCaches,
) -> Result<Arc<dyn Catalog>> {
    let prop_keys = prop_key_names(props);
    let has_table_bucket_arn = props
        .get(S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN)
        .is_some_and(|value| !value.trim().is_empty());
    async move {
        require_non_empty_prop(props, S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN, "S3 Tables")?;
        let builder = S3TablesCatalogBuilder::default().with_storage_factory(Arc::new(
            CountingStorageFactory::new(
                Arc::new(s3tables_default_storage_factory()),
                caches.io_counters(),
            ),
        ));
        let catalog = wire_caches(builder, caches, props)
            .load("s3tables", clone_props(props))
            .await
            .map_err(iceberg_to_datafusion)?;
        Ok(Arc::new(catalog) as Arc<dyn Catalog>)
    }
    .instrument(tracing::info_span!(
        "catalog.s3tables_catalog",
        prop_keys = %prop_keys,
        has_table_bucket_arn = has_table_bucket_arn,
    ))
    .await
}

/// Comma-separated sorted catalog property **key names only** for span fields (QUAL-05).
pub(crate) fn prop_key_names<S: BuildHasher>(props: &HashMap<String, String, S>) -> String {
    let mut keys: Vec<&str> = props.keys().map(String::as_str).collect();
    keys.sort_unstable();
    keys.join(",")
}

/// Reject a missing or blank required catalog property with a plan error that names the key.
pub(crate) fn require_non_empty_prop<S: BuildHasher>(
    props: &HashMap<String, String, S>,
    key: &str,
    kind: &str,
) -> Result<()> {
    match props.get(key) {
        Some(value) if !value.trim().is_empty() => Ok(()),
        _ => Err(DataFusionError::Plan(format!(
            "{kind} catalog requires a non-empty `{key}` property"
        ))),
    }
}

/// Copy a caller's property map into the default-hasher `HashMap` `CatalogBuilder::load` consumes.
pub(crate) fn clone_props<S: BuildHasher>(
    props: &HashMap<String, String, S>,
) -> HashMap<String, String> {
    props.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Fold an iceberg error into a DataFusion error so the session carries one engine error type.
pub fn iceberg_to_datafusion(err: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}
