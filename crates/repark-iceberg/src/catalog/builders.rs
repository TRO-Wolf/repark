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
    // Use the shared scheme selector so local and object-store warehouses choose the correct
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(storage_factory_for_location(warehouse)?)
        .load(
            "memory",
            HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.to_string())]),
        )
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
    // Record property names only because this map can contain credentials.
    let prop_keys = prop_key_names(props);
    let has_warehouse = props
        .get(GLUE_CATALOG_PROP_WAREHOUSE)
        .is_some_and(|value| !value.trim().is_empty());
    async move {
        require_non_empty_prop(props, GLUE_CATALOG_PROP_WAREHOUSE, "Glue")?;
        let catalog = GlueCatalogBuilder::default()
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

/// Build the AWS **S3 Tables** Iceberg catalog (the secondary product surface) from `props`, thin
/// # Errors
/// Returns an error if the required `table_bucket_arn` property is absent or empty, or if the fork
pub async fn s3tables_catalog<S: BuildHasher>(
    props: &HashMap<String, String, S>,
) -> Result<Arc<dyn Catalog>> {
    let prop_keys = prop_key_names(props);
    let has_table_bucket_arn = props
        .get(S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN)
        .is_some_and(|value| !value.trim().is_empty());
    async move {
        require_non_empty_prop(props, S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN, "S3 Tables")?;
        let catalog = S3TablesCatalogBuilder::default()
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

/// Reject a missing or blank required catalog property with a clear plan error that names the key,
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

/// Copy a caller's property map (any hasher) into the default-hasher `HashMap` the fork's
pub(crate) fn clone_props<S: BuildHasher>(
    props: &HashMap<String, String, S>,
) -> HashMap<String, String> {
    props.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Fold an iceberg error into a DataFusion error so the session layer can carry it as one engine
pub fn iceberg_to_datafusion(err: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}
