use std::sync::atomic::{AtomicBool, Ordering};

use super::super::*;
use super::common::*;

#[derive(Debug)]
struct FlakyLoadCatalog {
    inner: Arc<dyn Catalog>,
    fail_next_load: AtomicBool,
}

#[async_trait::async_trait]
impl Catalog for FlakyLoadCatalog {
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
        if self.fail_next_load.swap(false, Ordering::SeqCst) {
            return Err(iceberg::Error::new(
                iceberg::ErrorKind::Unexpected,
                "catalog unavailable",
            ));
        }
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

async fn flaky_door(wh: &TempDir) -> (SessionContext, CatalogRegistry, Arc<FlakyLoadCatalog>) {
    let (ctx, mut catalogs) = setup(wh).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t (id BIGINT, data STRING, cat STRING) USING iceberg \
         TBLPROPERTIES ('write.spark.accept-any-schema'='true')",
    )
    .await;
    let flaky = Arc::new(FlakyLoadCatalog {
        inner: Arc::clone(&catalogs["ice"]),
        fail_next_load: AtomicBool::new(false),
    });
    let registered: Arc<dyn Catalog> = flaky.clone();
    repark_iceberg::catalog::register_iceberg_catalog(&ctx, "flaky", Arc::clone(&registered))
        .await
        .unwrap();
    catalogs.insert(
        "flaky".to_string(),
        registered,
        LocationPolicy::TempFallbackAllowed {
            root: wh.path().to_path_buf(),
        },
    );
    (ctx, catalogs, flaky)
}

#[tokio::test]
async fn a_target_that_fails_to_load_refuses_instead_of_writing_positionally() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, flaky) = flaky_door(&wh).await;
    flaky.fail_next_load.store(true, Ordering::SeqCst);
    let outcome = match execute(
        &ctx,
        &catalogs,
        "INSERT INTO flaky.sales.t VALUES (1, 'a', 'x')",
    )
    .await
    {
        Ok(frame) => frame.collect().await.map(|_| ()),
        Err(error) => Err(error),
    };
    let mapped = repark_core::engine_err(outcome.expect_err("the load failure must surface"));
    assert!(
        matches!(mapped, repark_common::Error::Iceberg(ref message) if message.contains("catalog unavailable")),
        "got {mapped:?}"
    );
    assert_eq!(rows(&ctx, &catalogs, "SELECT * FROM ice.sales.t").await, 0);
}

#[tokio::test]
async fn a_missing_target_keeps_the_positional_answer() {
    let wh = TempDir::new().unwrap();
    let (ctx, catalogs, _) = flaky_door(&wh).await;
    let error = execute(
        &ctx,
        &catalogs,
        "INSERT INTO flaky.sales.nosuch VALUES (1, 'a', 'x')",
    )
    .await
    .expect_err("a missing table refuses");
    assert!(
        error.to_string().contains("nosuch' not found"),
        "got {error}"
    );
}
