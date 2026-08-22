//! Catalog builders (memory / Glue / S3 Tables) + shared prop helpers.
//!
//! Extracted from `lib.rs` in r26 LR3 (root-logic slim). Public paths re-exported from the crate root.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt;
use std::hash::BuildHasher;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::{Catalog, CatalogBuilder, ErrorKind};
use iceberg_catalog_glue::{GLUE_CATALOG_PROP_WAREHOUSE, GlueCatalogBuilder};
use iceberg_catalog_s3tables::{S3TABLES_CATALOG_PROP_TABLE_BUCKET_ARN, S3TablesCatalogBuilder};
use tracing::Instrument;

use crate::catalog::location::storage_factory_for_location;

/// ===========================================================================================
/// Build the AWS-free in-memory Iceberg catalog over a local-filesystem `warehouse` directory —
/// the catalog for local development and tests (Spark `local` mode analogue). Table metadata
/// lives in process memory; data/metadata files are written under `warehouse`, so everything a
/// commit produces is inspectable on disk.
/// ===========================================================================================
///
/// # Errors
/// Returns an error if the catalog builder rejects the configuration (e.g. an empty warehouse
/// path).
#[tracing::instrument(
    name = "catalog.memory_catalog",
    skip(warehouse),
    fields(warehouse = %warehouse)
)]
pub async fn memory_catalog(warehouse: &str) -> Result<Arc<dyn Catalog>> {
    // Scheme-selected storage, through the same helper the CTAS create arm uses so no call site
    // hardcodes a factory: a bare/`file://` warehouse (the documented local-dev use) selects
    // `LocalFsStorageFactory` — behaviour-identical to the prior hardcode. (An object-store
    // warehouse would select that backend, but the in-memory catalog is intended for local runs.)
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

/// ===========================================================================================
/// Build the AWS **Glue** Iceberg catalog (the primary product surface) from `props`, thin over
/// the fork's `GlueCatalogBuilder`. The fork recognizes `uri` (endpoint override), `catalog_id`,
/// and `warehouse` (**required**) and forwards every other property to Iceberg `FileIO` (so
/// `s3.*` / `AWS_*`-style credentials pass straight through). Credentials otherwise resolve via the
/// AWS SDK default chain inside the fork; `RePark` never handles them.
///
/// `warehouse` is validated fail-loud here — naming the missing key — *before* the fork builder is
/// invoked, so a misconfiguration surfaces as a clear plan error rather than a downstream `FileIO`
/// failure on first use.
/// ===========================================================================================
///
/// # Errors
/// Returns an error if the required `warehouse` property is absent or empty, or if the fork builder
/// rejects the configuration.
///
/// ## Observability (QUAL-05 / OBS1)
/// Emits span `catalog.glue_catalog` with **prop key names only** (never values — credentials ride
/// in the same map). Duration is available when a subscriber records span close events (the wheel's
/// `REPARK_LOG`/`RUST_LOG` path uses `FmtSpan::CLOSE`). AWS SDK request ids / retry counts are
/// not exposed by this thin wrapper (fork-internal residual; r25 seed).
pub async fn glue_catalog<S: BuildHasher>(
    props: &HashMap<String, String, S>,
) -> Result<Arc<dyn Catalog>> {
    // Generic + async: manual span (#[instrument] on generic async is awkward for prop_keys).
    // Fields are key names + non-secret warehouse presence only — never prop values.
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

/// ===========================================================================================
/// Build the AWS **S3 Tables** Iceberg catalog (the secondary product surface) from `props`, thin
/// over the fork's `S3TablesCatalogBuilder`. The fork recognizes `table_bucket_arn` (**required**)
/// and `endpoint_url`, and forwards every other property to Iceberg `FileIO`. S3 Tables addresses
/// its virtual bucket by ARN (not an `s3://` warehouse path), so `table_bucket_arn` is the required
/// handle.
///
/// `table_bucket_arn` is validated fail-loud here — naming the missing key — *before* the fork
/// builder is invoked.
/// ===========================================================================================
///
/// # Errors
/// Returns an error if the required `table_bucket_arn` property is absent or empty, or if the fork
/// builder rejects the configuration.
///
/// ## Observability (QUAL-05 / OBS1)
/// Emits span `catalog.s3tables_catalog` with **prop key names only** (never values). See
/// [`glue_catalog`] for the residual note on SDK request ids / retries.
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

/// ===========================================================================================
/// Comma-separated sorted catalog property **key names only** for span fields (QUAL-05).
///
/// Never includes values — the same map may carry `aws_secret_access_key` / tokens. Key names
/// stay visible so operators can see which props were configured without leaking credentials
/// (mirrors the C1-SEC-002 "key names always shown" half of `prop_key_is_secret` in
/// `repark-session` — that twin is READ ONLY this round; needles live there, not duplicated here).
/// ===========================================================================================
pub(crate) fn prop_key_names<S: BuildHasher>(props: &HashMap<String, String, S>) -> String {
    let mut keys: Vec<&str> = props.keys().map(String::as_str).collect();
    keys.sort_unstable();
    keys.join(",")
}

/// Reject a missing or blank required catalog property with a clear plan error that names the key,
/// before any fork builder runs. `kind` labels the catalog surface in the message (e.g. `"Glue"`).
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
/// `CatalogBuilder::load` consumes by value.
pub(crate) fn clone_props<S: BuildHasher>(
    props: &HashMap<String, String, S>,
) -> HashMap<String, String> {
    props.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// Fold an iceberg error into a DataFusion error so the session layer can carry it as one engine
/// error type. (`iceberg::Error` is a `std::error::Error`, so it nests via `External`.)
///
/// Hadoop-catalog `vN.metadata.json` pointers register and read, but the fork cannot compute the
/// next metadata pointer from that name. The raw error names the filename; this names the
/// convention (registry `V3-ADOPT-1`). The wrap stays `External` (not `Plan`) so Iceberg
/// classification is preserved, and [`StdError::source`] keeps the inner fork error.
pub fn iceberg_to_datafusion(err: iceberg::Error) -> DataFusionError {
    if err.kind() == ErrorKind::Unexpected
        && err.message().contains("Invalid metadata file name format:")
    {
        return DataFusionError::External(Box::new(HadoopMetadataPointerError { inner: err }));
    }
    DataFusionError::External(Box::new(err))
}

/// Operator-facing wrap of a Hadoop-named metadata pointer the fork cannot write.
#[derive(Debug)]
struct HadoopMetadataPointerError {
    inner: iceberg::Error,
}

impl fmt::Display for HadoopMetadataPointerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}. This engine's commit path requires a version-uuid metadata pointer \
             (`<version>-<uuid>.metadata.json`). The Hadoop catalog convention `vN.metadata.json` \
             registers and reads, but cannot be written; copy the file to a version-uuid name, \
             or adopt from a catalog that writes that shape (Glue).",
            self.inner
        )
    }
}

impl StdError for HadoopMetadataPointerError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.inner)
    }
}
