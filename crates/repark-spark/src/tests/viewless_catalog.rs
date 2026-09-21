use super::super::*;
use super::common::*;

#[derive(Debug)]
struct ViewlessCatalog {
    inner: Arc<dyn Catalog>,
}

#[async_trait::async_trait]
impl Catalog for ViewlessCatalog {
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

fn assert_view_unsupported(error: DataFusionError, catalog: &str, or_replace: bool) {
    let expected = if or_replace {
        format!("Replacing a view is not supported by catalog: {catalog}")
    } else {
        format!("Creating a view is not supported by catalog: {catalog}")
    };
    let mapped = repark_core::engine_err(error);
    assert_eq!(
        mapped.exception_class(),
        repark_core::ErrorClass::Unsupported,
        "pins: ice-views-1/C-005"
    );
    match mapped {
        repark_core::Error::NotImplemented(message) => {
            assert_eq!(message, expected, "pins: ice-views-1/C-005");
            assert!(
                !message.contains('['),
                "the refusal carries a null condition, got: {message}"
            );
            assert!(
                !message.contains("SQLSTATE"),
                "the refusal carries no SQLSTATE, got: {message}"
            );
        }
        other => panic!("expected Error::NotImplemented, got {other:?}"),
    }
}

#[tokio::test]
async fn test_views_refuse_on_glue_and_s3tables() {
    let wh = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&wh).await;
    for name in ["glue", "s3tables"] {
        let warehouse = wh.path().join(name);
        std::fs::create_dir_all(&warehouse).unwrap();
        let inner: Arc<dyn Catalog> = Arc::new(
            MemoryCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(
                    "viewless",
                    HashMap::from([(
                        MEMORY_CATALOG_WAREHOUSE.to_string(),
                        warehouse.to_str().unwrap().to_string(),
                    )]),
                )
                .await
                .unwrap(),
        );
        inner
            .create_namespace(&NamespaceIdent::new("ns".to_string()), HashMap::new())
            .await
            .unwrap();
        let stub: Arc<dyn Catalog> = Arc::new(ViewlessCatalog { inner });
        repark_iceberg::catalog::register_iceberg_catalog(&ctx, name, stub.clone())
            .await
            .unwrap();
        catalogs.insert(
            name.to_string(),
            stub,
            LocationPolicy::TempFallbackAllowed { root: warehouse },
        );
    }
    for name in ["glue", "s3tables"] {
        let error = execute(
            &ctx,
            &catalogs,
            &format!("CREATE VIEW {name}.ns.v AS SELECT 1 AS id"),
        )
        .await
        .expect_err("CREATE VIEW on a viewless catalog must refuse");
        assert_view_unsupported(error, name, false);
        let error = execute(
            &ctx,
            &catalogs,
            &format!("CREATE OR REPLACE VIEW {name}.ns.v AS SELECT 1 AS id"),
        )
        .await
        .expect_err("CREATE OR REPLACE VIEW on a viewless catalog must refuse");
        assert_view_unsupported(error, name, true);
        let shown = execute(&ctx, &catalogs, &format!("SHOW VIEWS IN {name}.ns"))
            .await
            .expect("SHOW VIEWS on a viewless catalog must answer empty")
            .collect()
            .await
            .expect("collect");
        assert_eq!(
            shown.iter().map(RecordBatch::num_rows).sum::<usize>(),
            0,
            "pins: ice-views-1/C-005"
        );
        run(
            &ctx,
            &catalogs,
            &format!("CREATE TABLE {name}.ns.t AS SELECT * FROM src"),
        )
        .await;
        let error = execute(
            &ctx,
            &catalogs,
            &format!("CREATE OR REPLACE VIEW {name}.ns.t AS SELECT 1 AS id"),
        )
        .await
        .expect_err("replace over a table on a viewless catalog must refuse");
        assert_view_unsupported(error, name, true);
        let error = execute(
            &ctx,
            &catalogs,
            &format!("CREATE VIEW IF NOT EXISTS {name}.ns.t AS SELECT 1 AS id"),
        )
        .await
        .expect_err("create over a table on a viewless catalog must refuse");
        assert_view_unsupported(error, name, false);
    }
}
