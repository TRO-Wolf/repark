use super::super::*;
use super::common::*;
use std::sync::atomic::{AtomicUsize, Ordering};

use iceberg::{Error, ErrorKind};

#[derive(Debug)]
pub(super) struct ViewlessCatalog {
    pub(super) inner: Arc<dyn Catalog>,
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

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<iceberg::table::Table> {
        self.inner.load_table(table).await
    }
}

#[derive(Debug)]
pub(super) struct FaultCatalog {
    pub(super) inner: Arc<dyn Catalog>,
    pub(super) table_failure: Option<ErrorKind>,
    pub(super) view_failure: Option<ErrorKind>,
    pub(super) view_calls: Arc<AtomicUsize>,
    pub(super) faults: ViewFaults,
}

#[derive(Debug, Default)]
pub(super) struct ViewFaults {
    pub(super) table_exists_failure: Option<ErrorKind>,
    pub(super) table_exists_calls: Option<Arc<AtomicUsize>>,
    pub(super) view_exists_failure: Option<ErrorKind>,
    pub(super) rename_view_calls: Option<Arc<AtomicUsize>>,
    pub(super) update_view_calls: Option<Arc<AtomicUsize>>,
}

#[async_trait::async_trait]
impl Catalog for FaultCatalog {
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

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
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

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<iceberg::table::Table> {
        if let Some(kind) = self.table_failure {
            return Err(Error::new(kind, "injected load_table failure"));
        }
        self.inner.load_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        if let Some(calls) = &self.faults.table_exists_calls {
            calls.fetch_add(1, Ordering::SeqCst);
        }
        if let Some(kind) = self.faults.table_exists_failure {
            return Err(Error::new(kind, "injected table_exists failure"));
        }
        self.inner.table_exists(table).await
    }

    async fn load_view(&self, view: &TableIdent) -> iceberg::Result<iceberg::view::View> {
        self.view_calls.fetch_add(1, Ordering::SeqCst);
        if let Some(kind) = self.view_failure {
            return Err(Error::new(kind, "injected load_view failure"));
        }
        self.inner.load_view(view).await
    }

    async fn view_exists(&self, view: &TableIdent) -> iceberg::Result<bool> {
        if let Some(kind) = self.faults.view_exists_failure {
            return Err(Error::new(kind, "injected view_exists failure"));
        }
        self.inner.view_exists(view).await
    }

    async fn rename_view(
        &self,
        source: &TableIdent,
        destination: &TableIdent,
    ) -> iceberg::Result<()> {
        if let Some(calls) = &self.faults.rename_view_calls {
            calls.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.rename_view(source, destination).await
    }

    async fn update_view(
        &self,
        commit: iceberg::view::ViewCommit,
    ) -> iceberg::Result<iceberg::view::View> {
        if let Some(calls) = &self.faults.update_view_calls {
            calls.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.update_view(commit).await
    }
}

pub(super) async fn register_catalog(
    ctx: &SessionContext,
    catalogs: &mut CatalogRegistry,
    catalog: Arc<dyn Catalog>,
    warehouse: &TempDir,
) {
    repark_iceberg::catalog::register_iceberg_catalog(ctx, "fault", catalog.clone())
        .await
        .unwrap_or_else(|error| panic!("register catalog: {error}"));
    catalogs.insert(
        "fault".to_string(),
        catalog,
        LocationPolicy::TempFallbackAllowed {
            root: warehouse.path().to_path_buf(),
        },
    );
}

pub(super) async fn collected_rows(
    ctx: &SessionContext,
    catalogs: &CatalogRegistry,
    sql: &str,
) -> Vec<(String, String, Option<String>)> {
    let batches = execute(ctx, catalogs, sql)
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("collect {sql}: {error}"));
    let mut rows = Vec::new();
    for batch in batches {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let types = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let comments = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                names.value(index).to_string(),
                types.value(index).to_string(),
                (!comments.is_null(index)).then(|| comments.value(index).to_string()),
            ));
        }
    }
    rows
}

#[tokio::test]
async fn describe_table_load_failure_never_probes_view() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&warehouse).await;
    let view_calls = Arc::new(AtomicUsize::new(0));
    let catalog = Arc::new(FaultCatalog {
        inner: catalogs["ice"].clone(),
        table_failure: Some(ErrorKind::Unexpected),
        view_failure: None,
        view_calls: view_calls.clone(),
        faults: ViewFaults::default(),
    });
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    let error = execute(&ctx, &catalogs, "DESCRIBE fault.sales.absent")
        .await
        .expect_err("load_table failure must propagate");
    let DataFusionError::External(inner) = &error else {
        panic!("expected an External error, got {error:?}");
    };
    let source = inner
        .downcast_ref::<iceberg::Error>()
        .expect("expected an Iceberg error");
    assert_eq!(source.kind(), ErrorKind::Unexpected);
    assert!(error.to_string().contains("injected load_table failure"));
    assert_eq!(view_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn describe_view_load_failure_keeps_original_error() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&warehouse).await;
    let view_calls = Arc::new(AtomicUsize::new(0));
    let catalog = Arc::new(FaultCatalog {
        inner: catalogs["ice"].clone(),
        table_failure: None,
        view_failure: Some(ErrorKind::Unexpected),
        view_calls: view_calls.clone(),
        faults: ViewFaults::default(),
    });
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    let error = execute(&ctx, &catalogs, "DESCRIBE fault.sales.absent")
        .await
        .expect_err("load_view failure must propagate");
    let DataFusionError::External(inner) = &error else {
        panic!("expected an External error, got {error:?}");
    };
    let source = inner
        .downcast_ref::<iceberg::Error>()
        .expect("expected an Iceberg error");
    assert_eq!(source.kind(), ErrorKind::Unexpected);
    let message = error.to_string();
    assert!(message.contains("injected load_view failure"), "{message}");
    assert!(!message.contains("TABLE_OR_VIEW_NOT_FOUND"), "{message}");
    assert_eq!(view_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn describe_viewless_catalog_matches_memory_absence_and_keeps_tables() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.t AS SELECT * FROM src",
    )
    .await;
    let catalog = Arc::new(ViewlessCatalog {
        inner: catalogs["ice"].clone(),
    });
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    let memory_error = execute(&ctx, &catalogs, "DESCRIBE ice.sales.absent")
        .await
        .expect_err("memory catalog missing name must refuse");
    let viewless_error = execute(&ctx, &catalogs, "DESCRIBE fault.sales.absent")
        .await
        .expect_err("viewless catalog missing name must refuse");
    assert!(matches!(&memory_error, DataFusionError::Plan(_)));
    assert!(matches!(&viewless_error, DataFusionError::Plan(_)));
    let memory_error = memory_error.to_string();
    let viewless_error = viewless_error.to_string();
    assert_eq!(viewless_error, memory_error.replace("`ice`", "`fault`"));
    assert!(viewless_error.contains("TABLE_OR_VIEW_NOT_FOUND"));
    assert_eq!(
        collected_rows(&ctx, &catalogs, "DESCRIBE fault.sales.t").await,
        collected_rows(&ctx, &catalogs, "DESCRIBE ice.sales.t").await,
    );
}

#[tokio::test]
async fn describe_view_uses_stored_schema_after_source_disappears() {
    let warehouse = TempDir::new().unwrap();
    let (ctx, mut catalogs) = setup(&warehouse).await;
    run(
        &ctx,
        &catalogs,
        "CREATE TABLE ice.sales.source AS SELECT * FROM src",
    )
    .await;
    run(
        &ctx,
        &catalogs,
        "CREATE VIEW ice.sales.saved AS SELECT id, name FROM ice.sales.source",
    )
    .await;
    run(&ctx, &catalogs, "DROP TABLE ice.sales.source").await;
    let catalog = Arc::new(FaultCatalog {
        inner: catalogs["ice"].clone(),
        table_failure: None,
        view_failure: None,
        view_calls: Arc::new(AtomicUsize::new(0)),
        faults: ViewFaults::default(),
    });
    register_catalog(&ctx, &mut catalogs, catalog, &warehouse).await;
    assert_eq!(
        collected_rows(&ctx, &catalogs, "DESCRIBE fault.sales.saved").await,
        vec![
            ("id".to_string(), "int".to_string(), Some(String::new())),
            (
                "name".to_string(),
                "string".to_string(),
                Some(String::new())
            ),
        ],
    );
    assert_eq!(
        collected_rows(&ctx, &catalogs, "DESCRIBE EXTENDED fault.sales.saved").await,
        collected_rows(&ctx, &catalogs, "DESCRIBE fault.sales.saved").await,
    );
}
