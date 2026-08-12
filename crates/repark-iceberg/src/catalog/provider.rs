//! Incremental DataFusion catalog provider for Iceberg (PERF-07 / r24 P7).
//!
//! The fork's [`IcebergCatalogProvider::try_new`] walks **every** namespace and
//! `list_tables` each one — O(databases) Glue calls per product DDL when this
//! engine re-registers. This module keeps a mutable name-directory map and rebuilds
//! **only the touched namespace** via a single-namespace catalog view, so a
//! CREATE/DROP/ALTER pays O(1) listing cost.
//!
//! Facade list-on-access ([`super::list_table_names`]) is unchanged: it still
//! hits the live [`Catalog`] handle. Free-SQL residual after out-of-band
//! mutations (ADR-0004) remains until an explicit full rebuild / refresh.

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, RwLock};

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::SessionContext;
use iceberg::table::Table;
use iceberg::view::{View, ViewCommit};
use iceberg::{
    Catalog, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent, ViewCreation,
};
use iceberg_datafusion::IcebergCatalogProvider;

use crate::catalog::metadata_projection::MetadataProjectionSchemaProvider;

// === r24 P7: catalog-provider incremental invalidation =========================
//
// PERF-07: product DDL must not re-walk every Glue database. Sole-writer band.
// OBS1 may attach spans later — do not invent OBS1 APIs here.
// ==============================================================================

/// Boxed future returned by desugared [`Catalog`] methods (no `async-trait` dep).
type BoxedCatalogFuture<'a, T> = Pin<Box<dyn Future<Output = iceberg::Result<T>> + Send + 'a>>;

/// ===========================================================================================
/// DataFusion [`CatalogProvider`] that can refresh one namespace without re-listing the rest.
///
/// Interior mutability: product DDL invalidates in place on the `Arc` already registered with
/// the session — no full `register_catalog` swap for the incremental path.
/// ===========================================================================================
#[derive(Debug)]
pub struct ReparkCatalogProvider {
    /// Live Iceberg catalog handle (same object the session registry holds).
    catalog: Arc<dyn Catalog>,
    /// Namespace → schema provider snapshot (fork `IcebergSchemaProvider` behind the trait).
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl ReparkCatalogProvider {
    /// ===========================================================================================
    /// Full snapshot build — same cost class as `IcebergCatalogProvider::try_new` (initial register
    /// / explicit refresh). Prefer [`Self::refresh_namespace`] after product DDL.
    /// ===========================================================================================
    ///
    /// # Errors
    /// Propagates Iceberg listing failures as DataFusion plan errors.
    pub async fn try_new(catalog: Arc<dyn Catalog>) -> Result<Self> {
        let schemas = snapshot_all_schemas(catalog.clone()).await?;
        Ok(Self {
            catalog,
            schemas: RwLock::new(schemas),
        })
    }

    /// ===========================================================================================
    /// Rebuild the name directory for a single namespace — O(1) `list_tables` (plus at most one
    /// `namespace_exists`). Missing namespace removes the schema entry (DROP NAMESPACE path).
    /// ===========================================================================================
    ///
    /// # Errors
    /// Propagates Iceberg / provider-build failures.
    pub async fn refresh_namespace(&self, namespace: &str) -> Result<()> {
        let prepared = prepare_namespace_schema(self.catalog.clone(), namespace).await?;
        apply_namespace_schema(&self.schemas, namespace, prepared);
        Ok(())
    }

    /// ===========================================================================================
    /// Drop a namespace entry from the DF name directory without listing (after product
    /// `DROP NAMESPACE` already removed it from the live catalog).
    /// ===========================================================================================
    pub fn drop_namespace_entry(&self, namespace: &str) {
        let mut guard = self
            .schemas
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.remove(namespace);
    }

    /// ===========================================================================================
    /// Full rebuild of every namespace — O(databases); used by explicit
    /// `refresh_catalog_provider` / free-SQL OOB recovery (ADR-0004 escape hatch).
    /// ===========================================================================================
    ///
    /// # Errors
    /// Propagates Iceberg listing failures.
    pub async fn refresh_all(&self) -> Result<()> {
        let schemas = snapshot_all_schemas(self.catalog.clone()).await?;
        let mut guard = self
            .schemas
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *guard = schemas;
        Ok(())
    }

    /// Live Iceberg handle this provider was built from.
    #[must_use]
    pub fn catalog_handle(&self) -> Arc<dyn Catalog> {
        self.catalog.clone()
    }
}

impl CatalogProvider for ReparkCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        let guard = self
            .schemas
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let guard = self
            .schemas
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.get(name).cloned()
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let mut guard = self
            .schemas
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // r25 morning critic: schemas arriving through this path must get the same
        // metadata-table projection honor as the snapshot/namespace-refresh paths —
        // otherwise `table$meta` lookups here bypass the item-0 fix.
        Ok(guard.insert(
            name.to_string(),
            crate::catalog::metadata_projection::MetadataProjectionSchemaProvider::wrap(schema),
        ))
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        let guard_read = self
            .schemas
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(schema) = guard_read.get(name).cloned() else {
            return Ok(None);
        };
        let table_names = schema.table_names();
        drop(guard_read);
        if !table_names.is_empty() && !cascade {
            return Err(DataFusionError::Execution(format!(
                "Cannot drop schema {name} because other tables depend on it: {}",
                table_names.join(", ")
            )));
        }
        let mut guard = self
            .schemas
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(guard.remove(name))
    }
}

/// ===========================================================================================
/// Full O(databases) rebuild and `register_catalog` — initial register + explicit refresh.
/// ===========================================================================================
///
/// # Errors
/// Provider build / registration failures.
pub async fn rebuild_catalog_provider(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    name: &str,
) -> Result<()> {
    // Prefer in-place full refresh when our provider is already registered **and** still bound to
    // the same Iceberg handle (keeps the DF Arc stable). A different `catalog` Arc means rebind —
    // replace the provider so we never silently refresh from a stale handle (octo C1-Q-003).
    if let Some(existing) = ctx.catalog(name)
        && let Some(repark) = existing.as_ref().downcast_ref::<ReparkCatalogProvider>()
        && Arc::ptr_eq(&repark.catalog_handle(), &catalog)
    {
        repark.refresh_all().await?;
        return Ok(());
    }
    let provider = ReparkCatalogProvider::try_new(catalog).await?;
    ctx.register_catalog(name, Arc::new(provider));
    Ok(())
}

/// ===========================================================================================
/// Invalidate one or more namespaces in the registered provider — O(1) list work per namespace.
///
/// Empty `namespaces` is a **no-op** (not a full rebuild): callers that want every namespace
/// refreshed must call [`rebuild_catalog_provider`] explicitly so free-SQL OOB residual is not
/// silently healed by an empty invalidate (octo C1-Q-002).
///
/// Falls back to a full rebuild when the session still holds a non-[`ReparkCatalogProvider`]
/// (should not happen after register via this crate).
/// ===========================================================================================
///
/// # Errors
/// Provider build / refresh failures.
pub async fn invalidate_catalog_namespaces(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    namespaces: &[&str],
) -> Result<()> {
    if namespaces.is_empty() {
        return Ok(());
    }

    let Some(existing) = ctx.catalog(catalog_name) else {
        // Do not silently `register_catalog` under a typo name (octo C4-Q-001).
        return Err(DataFusionError::Plan(format!(
            "catalog `{catalog_name}` is not registered; cannot invalidate namespaces"
        )));
    };
    if let Some(repark) = existing.as_ref().downcast_ref::<ReparkCatalogProvider>() {
        // Prepare every namespace off the map lock first; apply under one write so a mid-loop
        // failure cannot leave a cross-ns RENAME half-updated (octo C2-L-001).
        let mut prepared: Vec<(String, Option<Arc<dyn SchemaProvider>>)> =
            Vec::with_capacity(namespaces.len());
        for namespace in namespaces {
            let schema = prepare_namespace_schema(repark.catalog_handle(), namespace).await?;
            prepared.push(((*namespace).to_string(), schema));
        }
        let mut guard = repark
            .schemas
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (namespace, schema) in prepared {
            apply_namespace_schema_locked(&mut guard, &namespace, schema);
        }
        return Ok(());
    }

    // Foreign provider type: full rebuild replaces it with ReparkCatalogProvider.
    rebuild_catalog_provider(ctx, catalog, catalog_name).await
}

/// ===========================================================================================
/// Remove a dropped namespace from the DF name directory without listing.
/// ===========================================================================================
///
/// # Errors
/// Unregistered catalog name, or full-rebuild fallback failures (foreign provider type).
pub async fn drop_catalog_namespace_from_provider(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    catalog_name: &str,
    namespace: &str,
) -> Result<()> {
    let Some(existing) = ctx.catalog(catalog_name) else {
        return Err(DataFusionError::Plan(format!(
            "catalog `{catalog_name}` is not registered; cannot drop namespace from provider"
        )));
    };
    if let Some(repark) = existing.as_ref().downcast_ref::<ReparkCatalogProvider>() {
        repark.drop_namespace_entry(namespace);
        return Ok(());
    }
    // Foreign provider type: full rebuild.
    rebuild_catalog_provider(ctx, catalog, catalog_name).await
}

/// Snapshot every namespace via the fork provider, then extract schema handles.
async fn snapshot_all_schemas(
    catalog: Arc<dyn Catalog>,
) -> Result<HashMap<String, Arc<dyn SchemaProvider>>> {
    let iceberg = IcebergCatalogProvider::try_new(catalog)
        .await
        .map_err(super::iceberg_to_datafusion)?;
    let mut schemas = HashMap::new();
    for name in iceberg.schema_names() {
        if let Some(schema) = iceberg.schema(&name) {
            // r25 T2 item 0: honor projection on fork metadata-table providers (`table$meta`).
            schemas.insert(name, MetadataProjectionSchemaProvider::wrap(schema));
        }
    }
    Ok(schemas)
}

/// Prepare one namespace schema (or `None` if the live namespace is gone) — shared by
/// [`ReparkCatalogProvider::refresh_namespace`] and multi-ns invalidate (octo C5-Q-002).
async fn prepare_namespace_schema(
    catalog: Arc<dyn Catalog>,
    namespace: &str,
) -> Result<Option<Arc<dyn SchemaProvider>>> {
    let namespace_ident = NamespaceIdent::new(namespace.to_string());
    let exists = catalog
        .namespace_exists(&namespace_ident)
        .await
        .map_err(super::iceberg_to_datafusion)?;
    if !exists {
        return Ok(None);
    }
    let schema = build_namespace_schema(catalog, &namespace_ident).await?;
    Ok(Some(schema))
}

fn apply_namespace_schema(
    schemas: &RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
    namespace: &str,
    prepared: Option<Arc<dyn SchemaProvider>>,
) {
    let mut guard = schemas
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    apply_namespace_schema_locked(&mut guard, namespace, prepared);
}

fn apply_namespace_schema_locked(
    guard: &mut HashMap<String, Arc<dyn SchemaProvider>>,
    namespace: &str,
    prepared: Option<Arc<dyn SchemaProvider>>,
) {
    match prepared {
        Some(schema) => {
            guard.insert(namespace.to_string(), schema);
        }
        None => {
            guard.remove(namespace);
        }
    }
}

/// Build one write-capable schema provider by running `IcebergCatalogProvider::try_new` against a
/// catalog view that exposes only `namespace` (avoids listing sibling databases).
async fn build_namespace_schema(
    catalog: Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
) -> Result<Arc<dyn SchemaProvider>> {
    let schema_name = namespace_schema_name(namespace);
    let scoped: Arc<dyn Catalog> =
        Arc::new(NamespaceScopedCatalog::new(catalog, namespace.clone()));
    let iceberg = IcebergCatalogProvider::try_new(scoped)
        .await
        .map_err(super::iceberg_to_datafusion)?;
    let schema = iceberg.schema(&schema_name).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "namespace `{schema_name}` exists in the Iceberg catalog but produced no DF schema \
             provider after scoped rebuild"
        ))
    })?;
    // r25 T2 item 0: same projection wrap as full snapshot (namespace refresh path).
    Ok(MetadataProjectionSchemaProvider::wrap(schema))
}

/// Leaf schema name matching `IcebergCatalogProvider::try_new`'s flat-map of namespace parts.
fn namespace_schema_name(namespace: &NamespaceIdent) -> String {
    namespace
        .as_ref()
        .last()
        .cloned()
        .unwrap_or_else(|| namespace.to_url_string())
}

/// ===========================================================================================
/// Catalog view that reports a single namespace from `list_namespaces` so
/// `IcebergCatalogProvider::try_new` only builds one `IcebergSchemaProvider`.
///
/// Required methods and every defaulted method that an inner catalog may override are
/// **explicit forwards** to `inner` (so a trait default cannot swallow a real override —
/// G17 / both-sides trait-wrapping audit). Three composition defaults are **stated omissions**
/// (see the block comment in the `impl Catalog` body): they deliberately fall through because
/// they only call methods this wrapper already forwards.
/// ===========================================================================================
#[derive(Debug)]
pub(crate) struct NamespaceScopedCatalog {
    inner: Arc<dyn Catalog>,
    only: NamespaceIdent,
}

impl NamespaceScopedCatalog {
    /// ===========================================================================================
    /// Wrap `inner` so root `list_namespaces` reports only `only` (if it exists).
    /// ===========================================================================================
    pub(crate) fn new(inner: Arc<dyn Catalog>, only: NamespaceIdent) -> Self {
        Self { inner, only }
    }
}

impl Catalog for NamespaceScopedCatalog {
    // -------------------------------------------------------------------------------------------
    // Required Catalog methods (14). `list_namespaces` is the only intentional filter; the rest
    // fully delegate so the schema provider remains write-capable.
    // -------------------------------------------------------------------------------------------

    fn list_namespaces<'life0, 'life1, 'async_trait>(
        &'life0 self,
        parent: Option<&'life1 NamespaceIdent>,
    ) -> BoxedCatalogFuture<'async_trait, Vec<NamespaceIdent>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let inner = self.inner.clone();
        let only = self.only.clone();
        Box::pin(async move {
            // Only the root listing is used by IcebergCatalogProvider::try_new.
            if parent.is_some() {
                return Ok(Vec::new());
            }
            if inner.namespace_exists(&only).await? {
                Ok(vec![only])
            } else {
                Ok(Vec::new())
            }
        })
    }

    fn create_namespace<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> BoxedCatalogFuture<'async_trait, Namespace>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.create_namespace(namespace, properties)
    }

    fn get_namespace<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
    ) -> BoxedCatalogFuture<'async_trait, Namespace>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.get_namespace(namespace)
    }

    fn namespace_exists<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
    ) -> BoxedCatalogFuture<'async_trait, bool>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.namespace_exists(namespace)
    }

    fn update_namespace<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.update_namespace(namespace, properties)
    }

    fn drop_namespace<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.drop_namespace(namespace)
    }

    fn list_tables<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
    ) -> BoxedCatalogFuture<'async_trait, Vec<TableIdent>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.list_tables(namespace)
    }

    fn create_table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
        creation: TableCreation,
    ) -> BoxedCatalogFuture<'async_trait, Table>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.create_table(namespace, creation)
    }

    fn load_table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        table: &'life1 TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, Table>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.load_table(table)
    }

    fn drop_table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        table: &'life1 TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.drop_table(table)
    }

    fn table_exists<'life0, 'life1, 'async_trait>(
        &'life0 self,
        table: &'life1 TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, bool>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.table_exists(table)
    }

    fn rename_table<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        src: &'life1 TableIdent,
        dest: &'life2 TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.rename_table(src, dest)
    }

    fn register_table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        table: &'life1 TableIdent,
        metadata_location: String,
    ) -> BoxedCatalogFuture<'async_trait, Table>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.register_table(table, metadata_location)
    }

    fn update_table<'life0, 'async_trait>(
        &'life0 self,
        commit: TableCommit,
    ) -> BoxedCatalogFuture<'async_trait, Table>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.update_table(commit)
    }

    // -------------------------------------------------------------------------------------------
    // Stated omissions (3 of 16 defaulted methods at fork pin b009ac1).
    //
    // These trait defaults compose only from methods already forwarded above — they do not
    // call an overridable default that an inner catalog might replace with real work, so leaving
    // them as the trait default is correct *and* observable (see namespace_scoped_tests).
    //
    //   update_namespace_properties — get_namespace + update_namespace (both forwarded)
    //   set_namespace_properties    — update_namespace_properties(empty removals, updates)
    //   remove_namespace_properties — update_namespace_properties(removals, empty updates)
    //
    // Never silent: if a future fork rev makes one of these a real primitive (not a composition),
    // re-audit and convert that method to an explicit forward.
    // -------------------------------------------------------------------------------------------

    // -------------------------------------------------------------------------------------------
    // Explicit forwards (13 of 16 defaulted methods). A silent fall-through would return the
    // trait default and swallow an inner catalog's override — HIGH for publish_replace_table
    // (default FeatureUnsupported masks MemoryCatalog's CAS replace) and the views family
    // (MemoryCatalog implements views; default is FeatureUnsupported).
    // -------------------------------------------------------------------------------------------

    fn publish_create_table<'life0, 'async_trait>(
        &'life0 self,
        table: Table,
    ) -> BoxedCatalogFuture<'async_trait, Table>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.publish_create_table(table)
    }

    fn publish_replace_table<'life0, 'async_trait>(
        &'life0 self,
        table: Table,
        expected_base_metadata_location: Option<String>,
    ) -> BoxedCatalogFuture<'async_trait, Table>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        // HIGH (G17): trait default is FeatureUnsupported; MemoryCatalog implements CAS replace.
        self.inner
            .publish_replace_table(table, expected_base_metadata_location)
    }

    fn list_views<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
    ) -> BoxedCatalogFuture<'async_trait, Vec<TableIdent>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.list_views(namespace)
    }

    fn create_view<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
        creation: ViewCreation,
    ) -> BoxedCatalogFuture<'async_trait, View>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.create_view(namespace, creation)
    }

    fn load_view<'life0, 'life1, 'async_trait>(
        &'life0 self,
        view: &'life1 TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, View>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.load_view(view)
    }

    fn drop_view<'life0, 'life1, 'async_trait>(
        &'life0 self,
        view: &'life1 TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.drop_view(view)
    }

    fn view_exists<'life0, 'life1, 'async_trait>(
        &'life0 self,
        view: &'life1 TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, bool>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.view_exists(view)
    }

    fn rename_view<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        src: &'life1 TableIdent,
        dest: &'life2 TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.rename_view(src, dest)
    }

    fn update_view<'life0, 'async_trait>(
        &'life0 self,
        commit: ViewCommit,
    ) -> BoxedCatalogFuture<'async_trait, View>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.update_view(commit)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &HashMap<String, String> {
        self.inner.properties()
    }

    fn invalidate_table<'life0, 'life1, 'async_trait>(
        &'life0 self,
        table: &'life1 TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.invalidate_table(table)
    }

    fn invalidate_view<'life0, 'life1, 'async_trait>(
        &'life0 self,
        view: &'life1 TableIdent,
    ) -> BoxedCatalogFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.invalidate_view(view)
    }
}
