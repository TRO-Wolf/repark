use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::io::LocalFsStorageFactory;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{
    Catalog, CatalogBuilder, Namespace, NamespaceIdent, Result, TableCommit, TableCreation,
    TableIdent,
};
use tempfile::TempDir;

use super::*;

#[derive(Debug)]
struct Counted {
    inner: Arc<dyn Catalog>,
    loads: AtomicUsize,
}

impl Counted {
    fn loads(&self) -> usize {
        self.loads.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Catalog for Counted {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, ident: &TableIdent) -> Result<Table> {
        self.loads.fetch_add(1, Ordering::SeqCst);
        self.inner.load_table(ident).await
    }

    async fn drop_table(&self, ident: &TableIdent) -> Result<()> {
        self.inner.drop_table(ident).await
    }

    async fn table_exists(&self, ident: &TableIdent) -> Result<bool> {
        self.inner.table_exists(ident).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(&self, ident: &TableIdent, location: String) -> Result<Table> {
        self.inner.register_table(ident, location).await
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.inner.update_table(commit).await
    }
}

struct Bed {
    _warehouse: TempDir,
    counted: Arc<Counted>,
    session: Arc<dyn Catalog>,
    context: SessionContext,
}

fn ident() -> TableIdent {
    TableIdent::from_strs(["ns", "t"]).expect("ident")
}

async fn bed() -> Bed {
    let warehouse = TempDir::new().expect("warehouse");
    let root = warehouse.path().to_str().expect("utf-8 path").to_string();
    let raw: Arc<dyn Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .with_storage_factory(Arc::new(LocalFsStorageFactory))
            .load(
                "memory",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), root)]),
            )
            .await
            .expect("memory catalog"),
    );
    let counted = Arc::new(Counted {
        inner: raw,
        loads: AtomicUsize::new(0),
    });
    let namespace = NamespaceIdent::new("ns".to_string());
    counted
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::optional(1, "n", Type::Primitive(PrimitiveType::Int)).into(),
        ])
        .build()
        .expect("schema");
    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .build();
    counted
        .create_table(&namespace, creation)
        .await
        .expect("create");
    let context = SessionContext::new_with_config(with_session_tables(SessionConfig::new()));
    let counted_catalog: Arc<dyn Catalog> = Arc::clone(&counted) as Arc<dyn Catalog>;
    let (session, provider) = session_catalog_provider(&context, "c", counted_catalog)
        .await
        .expect("decorated");
    context.register_catalog("c", provider);
    Bed {
        _warehouse: warehouse,
        counted,
        session,
        context,
    }
}

fn tables(bed: &Bed) -> Arc<SessionTables> {
    bed.context
        .state_ref()
        .read()
        .config()
        .get_extension::<SessionTables>()
        .expect("session tables")
}

async fn set_property(catalog: &Arc<dyn Catalog>, value: &str) -> Table {
    let table = catalog.load_table(&ident()).await.expect("load");
    let tx = Transaction::new(&table);
    let tx = tx
        .update_table_properties()
        .set("k".to_string(), value.to_string())
        .apply(tx)
        .expect("apply");
    tx.commit(catalog.as_ref()).await.expect("commit")
}

fn location(state: Option<TableState>) -> Option<String> {
    match state {
        Some(TableState::Live(table)) => table.metadata_location().map(str::to_string),
        _ => None,
    }
}

#[tokio::test]
async fn a_commit_through_the_session_handle_moves_the_map_and_the_epoch() {
    let bed = bed().await;
    let tables = tables(&bed);
    assert_eq!(tables.epoch("c", &ident()), 0);
    let loaded = bed.session.load_table(&ident()).await.expect("load");
    let first = tables.epoch("c", &ident());
    assert!(first > 0);
    let committed = set_property(&bed.session, "v1").await;
    assert!(tables.epoch("c", &ident()) > first);
    assert_eq!(
        location(tables.current("c", &ident())),
        committed.metadata_location().map(str::to_string)
    );
    assert_ne!(loaded.metadata_location(), committed.metadata_location());
}

#[tokio::test]
async fn a_load_older_than_a_commit_does_not_put_the_older_table_back() {
    let bed = bed().await;
    let tables = tables(&bed);
    let stale = bed.session.load_table(&ident()).await.expect("load");
    let before = tables.epoch("c", &ident());
    let committed = set_property(&bed.session, "v1").await;
    tables.observe("c", &ident(), &stale, before, false);
    assert_eq!(
        location(tables.current("c", &ident())),
        committed.metadata_location().map(str::to_string)
    );
}

#[tokio::test]
async fn a_plain_reload_keeps_the_epoch_and_a_refresh_moves_it() {
    let bed = bed().await;
    let tables = tables(&bed);
    bed.session.load_table(&ident()).await.expect("load");
    let first = tables.epoch("c", &ident());
    bed.session.load_table(&ident()).await.expect("reload");
    assert_eq!(tables.epoch("c", &ident()), first);
    refresh_load(&bed.session, &ident()).await.expect("refresh");
    assert!(tables.epoch("c", &ident()) > first);
    assert_eq!(
        table_epoch(&bed.context.copied_config(), "c", &ident()),
        Some(tables.epoch("c", &ident()))
    );
}

#[tokio::test]
async fn a_drop_records_a_drop_marker() {
    let bed = bed().await;
    let tables = tables(&bed);
    bed.session.drop_table(&ident()).await.expect("drop");
    assert!(matches!(
        tables.current("c", &ident()),
        Some(TableState::Dropped)
    ));
}

#[tokio::test]
async fn a_resolved_provider_carries_its_planning_load() {
    let bed = bed().await;
    let frame = bed.context.sql("SELECT * FROM c.ns.t").await.expect("plan");
    let plan = frame.logical_plan().clone();
    let mut bound = Vec::new();
    datafusion::common::tree_node::TreeNode::apply(&plan, |node| {
        if let datafusion::logical_expr::LogicalPlan::TableScan(scan) = node
            && let Ok(provider) = datafusion::datasource::source_as_provider(&scan.source)
        {
            bound.extend(binding(&provider));
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .expect("walk");
    let [found] = bound.as_slice() else {
        panic!("one bound scan, got {}", bound.len());
    };
    assert_eq!(found.mode(), BindMode::Query);
    assert_eq!(found.ident(), &ident());
    assert_eq!(
        found.table().metadata_location().map(str::to_string),
        location(tables(&bed).current("c", &ident()))
    );
}

#[tokio::test]
async fn a_rebound_provider_scans_without_a_catalog_load() {
    let bed = bed().await;
    let table = bed.session.load_table(&ident()).await.expect("load");
    let before = bed.counted.loads();
    let provider = rebound_provider(&table).expect("rebound");
    let state = bed.context.state();
    provider.scan(&state, None, &[], None).await.expect("scan");
    assert_eq!(bed.counted.loads(), before);
}
