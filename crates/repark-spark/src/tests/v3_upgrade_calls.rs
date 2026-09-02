use super::super::*;
use super::common::*;

use std::sync::atomic::{AtomicUsize, Ordering};

use iceberg::spec::FormatVersion;

#[derive(Debug)]
struct CountingCatalog {
    inner: Arc<dyn Catalog>,
    load_table: AtomicUsize,
    list_tables: AtomicUsize,
    namespace_exists: AtomicUsize,
}

impl CountingCatalog {
    fn new(inner: Arc<dyn Catalog>) -> Self {
        Self {
            inner,
            load_table: AtomicUsize::new(0),
            list_tables: AtomicUsize::new(0),
            namespace_exists: AtomicUsize::new(0),
        }
    }

    fn reset(&self) {
        self.load_table.store(0, Ordering::SeqCst);
        self.list_tables.store(0, Ordering::SeqCst);
        self.namespace_exists.store(0, Ordering::SeqCst);
    }

    fn counts(&self) -> (usize, usize, usize) {
        (
            self.load_table.load(Ordering::SeqCst),
            self.list_tables.load(Ordering::SeqCst),
            self.namespace_exists.load(Ordering::SeqCst),
        )
    }
}

#[async_trait::async_trait]
impl Catalog for CountingCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<iceberg::Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(
        &self,
        namespace: &NamespaceIdent,
    ) -> iceberg::Result<iceberg::Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.namespace_exists.fetch_add(1, Ordering::SeqCst);
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.list_tables.fetch_add(1, Ordering::SeqCst);
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<iceberg::table::Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<iceberg::table::Table> {
        self.load_table.fetch_add(1, Ordering::SeqCst);
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<iceberg::table::Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(
        &self,
        commit: iceberg::TableCommit,
    ) -> iceberg::Result<iceberg::table::Table> {
        self.inner.update_table(commit).await
    }
}

#[tokio::test]
async fn the_upgrade_loads_the_table_once_and_relists_nothing() {
    let wh = TempDir::new().unwrap();
    let (ctx, registry) = setup_allow_create_format_version_3(&wh).await;
    let inner = registry.get("ice").expect("ice catalog").clone();
    let counting = Arc::new(CountingCatalog::new(inner));
    let handle: Arc<dyn Catalog> = counting.clone();
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "ice", handle.clone())
        .await
        .expect("the DF provider must read through the counting catalog too");
    let mut catalogs = CatalogRegistry::new();
    catalogs.insert(
        "ice".to_string(),
        handle,
        LocationPolicy::TempFallbackAllowed {
            root: wh.path().to_path_buf(),
        },
    );

    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.calls (id INT, name STRING) USING iceberg \
         TBLPROPERTIES ('format-version' = '2')",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "INSERT INTO ice.sales.calls VALUES (1, 'a')",
    )
    .await;

    counting.reset();
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.calls SET TBLPROPERTIES ('format-version' = '3')",
    )
    .await;
    assert_eq!(
        counting.counts(),
        (2, 0, 0),
        "the upgrade loads ONCE for the resolve plus the fork's commit CAS, and re-lists nothing"
    );

    counting.reset();
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.calls SET TBLPROPERTIES ('k' = 'v')",
    )
    .await;
    assert_eq!(
        counting.counts(),
        (2, 0, 0),
        "an ordinary property ALTER is unchanged"
    );

    counting.reset();
    run(
        &ctx,
        &catalogs,
        "ALTER TABLE ice.sales.calls SET TBLPROPERTIES ('format-version' = '3')",
    )
    .await;
    assert_eq!(
        counting.counts(),
        (1, 0, 0),
        "the same-version request loads once to compare and commits nothing"
    );

    let batches = execute(
        &ctx,
        &catalogs,
        "SELECT id, _row_id, _last_updated_sequence_number FROM ice.sales.calls",
    )
    .await
    .expect("the v3 lineage columns resolve with no namespace re-registration")
    .collect()
    .await
    .expect("collect lineage");
    assert_eq!(batches[0].num_rows(), 1);

    let table = counting
        .load_table(&TableIdent::from_strs(["sales", "calls"]).unwrap())
        .await
        .unwrap();
    assert_eq!(table.metadata().format_version(), FormatVersion::V3);
}
