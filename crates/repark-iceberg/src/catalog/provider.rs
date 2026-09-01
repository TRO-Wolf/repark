//! Incremental DataFusion catalog provider for Iceberg.

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

/// Boxed future returned by desugared [`Catalog`] methods (no `async-trait` dep).
type BoxedCatalogFuture<'a, T> = Pin<Box<dyn Future<Output = iceberg::Result<T>> + Send + 'a>>;

/// DataFusion [`CatalogProvider`] that can refresh one namespace without re-listing the rest.
#[derive(Debug)]
pub struct ReparkCatalogProvider {
    /// Live Iceberg catalog handle (same object the session registry holds).
    catalog: Arc<dyn Catalog>,
    /// Namespace → schema provider snapshot (fork `IcebergSchemaProvider` behind the trait).
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl ReparkCatalogProvider {
    /// Full snapshot build — same cost class as `IcebergCatalogProvider::try_new` (initial
    /// # Errors
    /// Propagates Iceberg listing failures as DataFusion plan errors.
    pub async fn try_new(catalog: Arc<dyn Catalog>) -> Result<Self> {
        let schemas = snapshot_all_schemas(catalog.clone()).await?;
        Ok(Self {
            catalog,
            schemas: RwLock::new(schemas),
        })
    }

    /// Rebuild one namespace name directory via `list_tables` (and at most one `namespace_exists`).
    /// # Errors
    /// Propagates Iceberg / provider-build failures.
    pub async fn refresh_namespace(&self, namespace: &str) -> Result<()> {
        let prepared = prepare_namespace_schema(self.catalog.clone(), namespace).await?;
        apply_namespace_schema(&self.schemas, namespace, prepared);
        Ok(())
    }

    /// Drop a namespace entry from the DF name directory without listing (after product `DROP NAMESPACE`
    pub fn drop_namespace_entry(&self, namespace: &str) {
        let mut guard = self
            .schemas
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.remove(namespace);
    }

    /// Full rebuild of every namespace — O(databases); used by explicit `refresh_catalog_provider`
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
        // Apply the metadata projection policy to schemas registered through this path too.
        Ok(guard.insert(name.to_string(), schema))
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

/// Full O(databases) rebuild and `register_catalog` — initial register + explicit refresh.
/// # Errors
/// Provider build / registration failures.
pub async fn rebuild_catalog_provider(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    name: &str,
) -> Result<()> {
    // Prefer in-place full refresh when this provider is registered and bound to the same handle.
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

/// Invalidate one or more namespaces in the registered provider — O(1) list work per namespace.
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
        // Do not silently `register_catalog` under a typo name.
        return Err(DataFusionError::Plan(format!(
            "catalog `{catalog_name}` is not registered; cannot invalidate namespaces"
        )));
    };
    if let Some(repark) = existing.as_ref().downcast_ref::<ReparkCatalogProvider>() {
        // Prepare every namespace off the map lock first.
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

/// Remove a dropped namespace from the DF name directory without listing.
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
            freeze_fork_name_directory(schema.as_ref()).await?;
            schemas.insert(name, schema);
        }
    }
    Ok(schemas)
}

/// Prepare one namespace schema.
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

/// Build a write-capable schema provider for one namespace so sibling databases are not listed.
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
    freeze_fork_name_directory(schema.as_ref()).await?;
    Ok(schema)
}

/// Name used only to drive [`SchemaProvider::table`] through the fork's `ensure_tables_listed`.
const FORK_NAME_DIRECTORY_PROBE: &str = "__repark_snapshot_probe";

/// pins: rp-1-fork-repin/C-011
/// Capture the fork schema-provider's table-name directory at snapshot time.
async fn freeze_fork_name_directory(schema: &dyn SchemaProvider) -> Result<()> {
    let _ = schema.table(FORK_NAME_DIRECTORY_PROBE).await?;
    Ok(())
}

/// Leaf schema name matching `IcebergCatalogProvider::try_new`'s flat-map of namespace parts.
fn namespace_schema_name(namespace: &NamespaceIdent) -> String {
    namespace
        .as_ref()
        .last()
        .cloned()
        .unwrap_or_else(|| namespace.to_url_string())
}

/// Catalog view that reports one namespace from `list_namespaces` for `try_new`.
#[derive(Debug)]
pub(crate) struct NamespaceScopedCatalog {
    inner: Arc<dyn Catalog>,
    only: NamespaceIdent,
}

impl NamespaceScopedCatalog {
    /// Wrap `inner` so root `list_namespaces` reports only `only` (if it exists).
    pub(crate) fn new(inner: Arc<dyn Catalog>, only: NamespaceIdent) -> Self {
        Self { inner, only }
    }
}

impl Catalog for NamespaceScopedCatalog {
    // === Catalog methods ===

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
