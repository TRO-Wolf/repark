//! G17 wrapper tests for [`super::provider::NamespaceScopedCatalog`].
//!
//! Pins the both-sides trait-wrapping audit: every defaulted `Catalog` method is either an
//! explicit forward or a stated omission; silent fall-throughs are gone.
//!
//! pins: rp-1-fork-repin/C-003

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use tempfile::TempDir;

use super::builders::memory_catalog;
use super::provider::NamespaceScopedCatalog;

// === Boxed future alias matching provider.rs (desugared Catalog methods) =====================

type BoxedCatalogFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = iceberg::Result<T>> + Send + 'a>>;

// === Spy catalog =============================================================================

/// Counts selected `Catalog` methods while fully forwarding to `inner`.
///
/// Used to prove `NamespaceScopedCatalog` reaches the real inner override (not a trait default).
#[derive(Debug)]
struct SpyCatalog {
    inner: Arc<dyn Catalog>,
    publish_replace: AtomicUsize,
    get_namespace: AtomicUsize,
    update_namespace: AtomicUsize,
    list_views: AtomicUsize,
}

impl SpyCatalog {
    fn new(inner: Arc<dyn Catalog>) -> Self {
        Self {
            inner,
            publish_replace: AtomicUsize::new(0),
            get_namespace: AtomicUsize::new(0),
            update_namespace: AtomicUsize::new(0),
            list_views: AtomicUsize::new(0),
        }
    }

    fn publish_replace_count(&self) -> usize {
        self.publish_replace.load(Ordering::SeqCst)
    }

    fn get_namespace_count(&self) -> usize {
        self.get_namespace.load(Ordering::SeqCst)
    }

    fn update_namespace_count(&self) -> usize {
        self.update_namespace.load(Ordering::SeqCst)
    }

    fn list_views_count(&self) -> usize {
        self.list_views.load(Ordering::SeqCst)
    }
}

impl Catalog for SpyCatalog {
    fn list_namespaces<'life0, 'life1, 'async_trait>(
        &'life0 self,
        parent: Option<&'life1 NamespaceIdent>,
    ) -> BoxedCatalogFuture<'async_trait, Vec<NamespaceIdent>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.list_namespaces(parent)
    }

    fn create_namespace<'life0, 'life1, 'async_trait>(
        &'life0 self,
        namespace: &'life1 NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> BoxedCatalogFuture<'async_trait, iceberg::Namespace>
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
    ) -> BoxedCatalogFuture<'async_trait, iceberg::Namespace>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.get_namespace.fetch_add(1, Ordering::SeqCst);
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
        self.update_namespace.fetch_add(1, Ordering::SeqCst);
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
    ) -> BoxedCatalogFuture<'async_trait, iceberg::table::Table>
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
    ) -> BoxedCatalogFuture<'async_trait, iceberg::table::Table>
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
    ) -> BoxedCatalogFuture<'async_trait, iceberg::table::Table>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.register_table(table, metadata_location)
    }

    fn update_table<'life0, 'async_trait>(
        &'life0 self,
        commit: iceberg::TableCommit,
    ) -> BoxedCatalogFuture<'async_trait, iceberg::table::Table>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.update_table(commit)
    }

    fn publish_replace_table<'life0, 'async_trait>(
        &'life0 self,
        table: iceberg::table::Table,
        expected_base_metadata_location: Option<String>,
    ) -> BoxedCatalogFuture<'async_trait, iceberg::table::Table>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.publish_replace.fetch_add(1, Ordering::SeqCst);
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
        self.list_views.fetch_add(1, Ordering::SeqCst);
        self.inner.list_views(namespace)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &HashMap<String, String> {
        self.inner.properties()
    }
}

// === Fixtures =================================================================================

fn sample_schema() -> Schema {
    Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .expect("sample schema builds")
}

async fn sales_namespace_on_memory(warehouse: &str) -> (Arc<dyn Catalog>, NamespaceIdent) {
    let catalog = memory_catalog(warehouse)
        .await
        .expect("memory_catalog builds");
    let namespace = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create sales namespace");
    (catalog, namespace)
}

// === Tests ====================================================================================

/// HIGH (G17): `publish_replace_table` must reach the inner catalog. The trait default is
/// `FeatureUnsupported`; a silent fall-through would never increment the spy and would error.
#[tokio::test]
async fn publish_replace_table_forwards_to_inner_spy() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let warehouse_path = warehouse.path().to_str().expect("warehouse path is utf-8");
    let (memory, namespace) = sales_namespace_on_memory(warehouse_path).await;

    let creation = TableCreation::builder()
        .name("orders".to_string())
        .schema(sample_schema())
        .build();
    let table = memory
        .create_table(&namespace, creation)
        .await
        .expect("create orders");
    let base_location = table
        .metadata_location_result()
        .expect("table has metadata location")
        .to_string();

    let spy = Arc::new(SpyCatalog::new(memory));
    let scoped = NamespaceScopedCatalog::new(spy.clone() as Arc<dyn Catalog>, namespace);

    let published = scoped
        .publish_replace_table(table, Some(base_location))
        .await
        .expect("publish_replace_table must succeed via MemoryCatalog, not trait default");

    assert_eq!(
        spy.publish_replace_count(),
        1,
        "NamespaceScopedCatalog must call inner.publish_replace_table (not FeatureUnsupported default)"
    );
    assert_eq!(published.identifier().name, "orders");
}

/// Forwarded read path: `name()` returns the inner catalog's name unchanged.
#[tokio::test]
async fn name_forwards_inner_value_unchanged() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let warehouse_path = warehouse.path().to_str().expect("warehouse path is utf-8");
    let (memory, namespace) = sales_namespace_on_memory(warehouse_path).await;
    let expected_name = memory.name().to_string();
    assert_eq!(
        expected_name, "memory",
        "memory_catalog builder loads with name \"memory\""
    );

    let scoped = NamespaceScopedCatalog::new(memory, namespace);
    assert_eq!(
        scoped.name(),
        expected_name,
        "name() must forward the inner catalog's name, not the UNNAMED_CATALOG default"
    );
}

/// Stated omission: `update_namespace_properties` composes from forwarded
/// `get_namespace` + `update_namespace` — the spy observes both; the merged property is stored.
#[tokio::test]
async fn update_namespace_properties_composes_via_forwarded_methods() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let warehouse_path = warehouse.path().to_str().expect("warehouse path is utf-8");
    let (memory, namespace) = sales_namespace_on_memory(warehouse_path).await;

    let spy = Arc::new(SpyCatalog::new(memory));
    let scoped = NamespaceScopedCatalog::new(spy.clone() as Arc<dyn Catalog>, namespace.clone());

    let updates = HashMap::from([("owner".to_string(), "repark".to_string())]);
    scoped
        .update_namespace_properties(&namespace, HashSet::new(), updates)
        .await
        .expect("stated-omission default must compose successfully");

    assert!(
        spy.get_namespace_count() >= 1,
        "trait default must call get_namespace through the wrapper forward"
    );
    assert!(
        spy.update_namespace_count() >= 1,
        "trait default must call update_namespace through the wrapper forward"
    );

    let loaded = scoped
        .get_namespace(&namespace)
        .await
        .expect("namespace still exists");
    assert_eq!(
        loaded.properties().get("owner").map(String::as_str),
        Some("repark"),
        "composed update must persist the new property on the inner catalog"
    );
}

/// Views family forward: `list_views` must hit the inner (`MemoryCatalog` supports views).
/// A silent fall-through would return `FeatureUnsupported` without calling the spy.
#[tokio::test]
async fn list_views_forwards_to_inner_spy() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let warehouse_path = warehouse.path().to_str().expect("warehouse path is utf-8");
    let (memory, namespace) = sales_namespace_on_memory(warehouse_path).await;

    let spy = Arc::new(SpyCatalog::new(memory));
    let scoped = NamespaceScopedCatalog::new(spy.clone() as Arc<dyn Catalog>, namespace.clone());

    let views = scoped
        .list_views(&namespace)
        .await
        .expect("list_views must forward to MemoryCatalog, not FeatureUnsupported default");

    assert!(views.is_empty(), "fresh namespace has no views");
    assert_eq!(
        spy.list_views_count(),
        1,
        "NamespaceScopedCatalog must call inner.list_views"
    );
}
