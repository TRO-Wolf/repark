use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use datafusion::error::DataFusionError;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::{
    Catalog, CatalogBuilder, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation,
    TableIdent, TableUpdate,
};
use tempfile::TempDir;
use uuid::Uuid;

use super::super::{OPERATION_ID_PROP, WRITE_MERGE_ISOLATION_LEVEL, commit, commit_row_delta};
use crate::write::CommitStateUnknownError;
use crate::write::concurrency::WriteConcurrency;

type BoxedCatalogFuture<'a, T> = Pin<Box<dyn Future<Output = iceberg::Result<T>> + Send + 'a>>;

#[derive(Debug)]
struct UnknownOutcomeCatalog {
    inner: Arc<dyn Catalog>,
    update_table_attempts: AtomicUsize,
    stamped_operation_id: Mutex<Option<String>>,
}

impl UnknownOutcomeCatalog {
    fn captured_operation_id(&self) -> String {
        self.stamped_operation_id
            .lock()
            .expect("capture lock")
            .clone()
            .expect("the commit must stamp an engine.operation-id")
    }
}

impl Catalog for UnknownOutcomeCatalog {
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
        mut commit: TableCommit,
    ) -> BoxedCatalogFuture<'async_trait, Table>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            self.update_table_attempts.fetch_add(1, Ordering::SeqCst);
            let stamped = commit
                .take_updates()
                .iter()
                .find_map(|update| match update {
                    TableUpdate::AddSnapshot { snapshot } => snapshot
                        .summary()
                        .additional_properties
                        .get(OPERATION_ID_PROP)
                        .cloned(),
                    _ => None,
                });
            *self.stamped_operation_id.lock().expect("capture lock") = stamped;
            Err(iceberg::Error::new(
                ErrorKind::CommitStateUnknown,
                "lost UpdateTable response".to_string(),
            ))
        })
    }
}

async fn setup(warehouse: &TempDir) -> (Arc<UnknownOutcomeCatalog>, Arc<dyn Catalog>, TableIdent) {
    let path = warehouse
        .path()
        .to_str()
        .expect("utf-8 warehouse path")
        .to_string();
    let inner: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(iceberg::io::LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), path)]),
            )
            .await
            .expect("build memory catalog"),
    );
    let namespace = NamespaceIdent::new("sales".to_string());
    inner
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("create namespace");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("build schema");
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .properties(HashMap::from([(
            WRITE_MERGE_ISOLATION_LEVEL.to_string(),
            "serializable".to_string(),
        )]))
        .build();
    inner
        .create_table(&namespace, creation)
        .await
        .expect("create table");
    let injector = Arc::new(UnknownOutcomeCatalog {
        inner,
        update_table_attempts: AtomicUsize::new(0),
        stamped_operation_id: Mutex::new(None),
    });
    let catalog: Arc<dyn Catalog> = injector.clone();
    (
        injector,
        catalog,
        TableIdent::new(namespace, "t".to_string()),
    )
}

fn assert_unknown_commit(injector: &UnknownOutcomeCatalog, error: DataFusionError) {
    let captured = injector.captured_operation_id();
    Uuid::parse_str(&captured).expect("the stamped operation id is a UUID");
    let DataFusionError::External(inner) = error else {
        panic!("expected DataFusionError::External, got {error:?}")
    };
    let stamped = inner
        .downcast_ref::<CommitStateUnknownError>()
        .expect("the surfaced error must carry the stamped commit wrapper");
    assert_eq!(stamped.inner().kind(), ErrorKind::CommitStateUnknown);
    assert_eq!(stamped.operation_id(), captured);
    assert_eq!(
        injector.update_table_attempts.load(Ordering::SeqCst),
        1,
        "an unknown commit outcome is never retried"
    );
}

#[tokio::test]
async fn merge_overwrite_commit_unknown_surfaces_the_stamped_operation_id() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (injector, catalog, ident) = setup(&warehouse).await;
    let table = catalog.load_table(&ident).await.expect("load table");
    let error = commit(
        &catalog,
        &table,
        None,
        vec![],
        vec![super::occ::data_file("t/unknown-1.parquet")],
    )
    .await
    .expect_err("the unknown commit outcome must surface");
    assert_unknown_commit(&injector, error);
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    assert!(
        reloaded.metadata().current_snapshot().is_none(),
        "the commit did not land: no snapshot may appear"
    );
}

#[tokio::test]
async fn merge_row_delta_commit_unknown_surfaces_the_stamped_operation_id() {
    let warehouse = TempDir::new().expect("temp warehouse");
    let (injector, catalog, ident) = setup(&warehouse).await;
    let table = catalog.load_table(&ident).await.expect("load table");
    let error = commit_row_delta(
        &catalog,
        &table,
        None,
        vec![],
        vec![super::occ::data_file("t/unknown-2.parquet")],
        WriteConcurrency::new(1).expect("write concurrency"),
    )
    .await
    .expect_err("the unknown commit outcome must surface");
    assert_unknown_commit(&injector, error);
    let reloaded = catalog.load_table(&ident).await.expect("reload");
    assert!(
        reloaded.metadata().current_snapshot().is_none(),
        "the commit did not land: no snapshot may appear"
    );
}
