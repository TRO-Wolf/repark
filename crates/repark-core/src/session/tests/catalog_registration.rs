use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error as IcebergError, ErrorKind as IcebergErrorKind, Namespace, NamespaceIdent,
    TableCommit, TableCreation, TableIdent,
};
use tokio::sync::Barrier;

use super::super::*;

#[derive(Debug)]
struct ControlledCatalog {
    inner: Arc<dyn Catalog>,
    build_barrier: Option<Arc<Barrier>>,
    fail_build: bool,
}

impl ControlledCatalog {
    fn new(inner: Arc<dyn Catalog>, build_barrier: Option<Arc<Barrier>>) -> Self {
        Self {
            inner,
            build_barrier,
            fail_build: false,
        }
    }

    fn failing(inner: Arc<dyn Catalog>) -> Self {
        Self {
            inner,
            build_barrier: None,
            fail_build: true,
        }
    }
}

#[async_trait]
impl Catalog for ControlledCatalog {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        if let Some(barrier) = &self.build_barrier {
            barrier.wait().await;
        }
        if self.fail_build {
            return Err(IcebergError::new(
                IcebergErrorKind::Unexpected,
                "controlled provider-build failure",
            ));
        }
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
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
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(
        &self,
        source: &TableIdent,
        destination: &TableIdent,
    ) -> iceberg::Result<()> {
        self.inner.rename_table(source, destination).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.inner.update_table(commit).await
    }
}

async fn controlled_catalog(name: &str, barrier: Option<Arc<Barrier>>) -> Arc<dyn Catalog> {
    let warehouse = std::env::temp_dir()
        .join(name)
        .to_string_lossy()
        .into_owned();
    let inner = repark_iceberg::catalog::memory_catalog(&warehouse)
        .await
        .unwrap();
    inner
        .create_namespace(&NamespaceIdent::new(name.to_string()), HashMap::new())
        .await
        .unwrap();
    Arc::new(ControlledCatalog::new(inner, barrier))
}

#[tokio::test]
async fn register_iceberg_catalog_rejects_duplicate_name() {
    let session = ReparkSession::new().unwrap();
    let first = controlled_catalog("repark-dup-first", None).await;
    session
        .register_iceberg_catalog("dup", first)
        .await
        .unwrap();
    let inner = controlled_catalog("repark-dup-second", None).await;
    let second: Arc<dyn Catalog> = Arc::new(ControlledCatalog::failing(inner));
    let error = session
        .register_iceberg_catalog("dup", second)
        .await
        .unwrap_err();
    let expected = "catalog 'dup' is already registered";
    assert!(matches!(error, Error::DataFusion(message) if message == expected));
}

#[tokio::test]
async fn concurrent_same_name_registration_publishes_one_matching_winner() {
    let session = ReparkSession::new().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let first = controlled_catalog("repark-race-first", Some(barrier.clone())).await;
    let second = controlled_catalog("repark-race-second", Some(barrier)).await;
    let (first_result, second_result) = tokio::join!(
        session.register_iceberg_catalog("race", first.clone()),
        session.register_iceberg_catalog("race", second.clone())
    );
    assert_eq!(
        usize::from(first_result.is_ok()) + usize::from(second_result.is_ok()),
        1
    );
    let first_won = first_result.is_ok();
    let loser = first_result
        .as_ref()
        .err()
        .or_else(|| second_result.as_ref().err())
        .unwrap();
    let expected = "catalog 'race' is already registered";
    assert!(matches!(loser, Error::DataFusion(message) if message == expected));
    let (winner, winner_namespace) = if first_won {
        (first, "repark-race-first")
    } else {
        (second, "repark-race-second")
    };
    let registered_handle = session.catalog_handle("race").unwrap();
    let provider = session.context().catalog("race").unwrap();
    assert!(Arc::ptr_eq(&registered_handle, &winner));
    assert_eq!(provider.schema_names(), vec![winner_namespace]);
}

#[tokio::test]
async fn distinct_name_provider_builds_overlap() {
    let session = ReparkSession::new().unwrap();
    let barrier = Arc::new(Barrier::new(2));
    let first = controlled_catalog("repark-overlap-first", Some(barrier.clone())).await;
    let second = controlled_catalog("repark-overlap-second", Some(barrier)).await;
    let results = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            session.register_iceberg_catalog("first", first),
            session.register_iceberg_catalog("second", second)
        )
    })
    .await
    .unwrap();
    assert!(results.0.is_ok());
    assert!(results.1.is_ok());
}

#[tokio::test]
async fn provider_build_failure_publishes_neither_surface() {
    let session = ReparkSession::new().unwrap();
    let inner = controlled_catalog("repark-build-failure", None).await;
    let failing: Arc<dyn Catalog> = Arc::new(ControlledCatalog::failing(inner));
    let error = session
        .register_iceberg_catalog("failed", failing)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("controlled provider-build failure")
    );
    assert!(session.context().catalog("failed").is_none());
    assert!(session.catalog_handle("failed").is_err());
}
