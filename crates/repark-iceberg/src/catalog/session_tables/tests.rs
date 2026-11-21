use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
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
use crate::write::EncryptedTableRefusal;

const KEY: &str = "SEKRETKEYVAL9f3a7";
const KEY_PROPERTY: &str = "encryption.key-id";

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
    guarded: Arc<dyn Catalog>,
    session: Arc<dyn Catalog>,
    context: SessionContext,
}

fn ident() -> TableIdent {
    TableIdent::from_strs(["ns", "t"]).expect("ident")
}

async fn bed() -> Bed {
    bed_with(&[]).await
}

async fn keyed_bed() -> Bed {
    bed_with(&[(KEY_PROPERTY, KEY)]).await
}

async fn bed_with(properties: &[(&str, &str)]) -> Bed {
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
        .properties(
            properties
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
        )
        .build();
    counted
        .create_table(&namespace, creation)
        .await
        .expect("create");
    let context = SessionContext::new_with_config(with_session_tables(SessionConfig::new()));
    let guarded = EncryptionGuardCatalog::install(Arc::clone(&counted) as Arc<dyn Catalog>);
    let (session, provider) = session_catalog_provider(&context, "c", Arc::clone(&guarded))
        .await
        .expect("decorated");
    assert!(!Arc::ptr_eq(&session, &guarded));
    context.register_catalog("c", provider);
    Bed {
        _warehouse: warehouse,
        counted,
        guarded,
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
        Some(TableState::Live(table)) => table.table().metadata_location().map(str::to_string),
        _ => None,
    }
}

fn served(bed: &Bed) -> HeldTable {
    match tables(bed).current("c", &ident()) {
        Some(TableState::Live(table)) => table,
        other => panic!("a live map entry, got {other:?}"),
    }
}

fn data_path(table: &Table) -> String {
    format!("{}/data/probe.parquet", table.metadata().location())
}

async fn file_write(table: &Table) -> std::result::Result<(), String> {
    let path = data_path(table);
    let output = table
        .file_io()
        .new_output(&path)
        .map_err(|error| error.to_string())?;
    output
        .write(Bytes::from_static(b"x"))
        .await
        .map_err(|error| error.to_string())?;
    table
        .file_io()
        .delete(&path)
        .await
        .map_err(|error| error.to_string())
}

async fn snapshot_commit(catalog: &dyn Catalog, table: &Table) -> Result<Table> {
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .set_snapshot_properties(HashMap::from([("probe".to_string(), "1".to_string())]))
        .apply(tx)?;
    tx.commit(catalog).await
}

async fn statistics_commit(catalog: &dyn Catalog, table: &Table) -> Result<Table> {
    let tx = Transaction::new(table);
    let tx = tx
        .update_statistics()
        .set_statistics(iceberg::spec::StatisticsFile {
            snapshot_id: 1,
            statistics_path: format!("{}/data/probe.stats", table.metadata().location()),
            file_size_in_bytes: 1,
            file_footer_size_in_bytes: 1,
            key_metadata: None,
            blob_metadata: Vec::new(),
        })
        .apply(tx)?;
    tx.commit(catalog).await
}

async fn set_key(catalog: &Arc<dyn Catalog>, present: bool) -> Table {
    let table = catalog.load_table(&ident()).await.expect("load");
    let tx = Transaction::new(&table);
    let properties = tx.update_table_properties();
    let properties = if present {
        properties.set(KEY_PROPERTY.to_string(), KEY.to_string())
    } else {
        properties.remove(KEY_PROPERTY.to_string())
    };
    let tx = properties.apply(tx).expect("apply");
    tx.commit(catalog.as_ref()).await.expect("key commit")
}

fn refusal_text<T: std::fmt::Debug>(result: Result<T>) -> String {
    let error = result.expect_err("a keyed table must refuse");
    assert!(EncryptedTableRefusal::find(&error).is_some(), "{error}");
    assert!(!error.to_string().contains(KEY), "{error}");
    error.to_string()
}

async fn staged_with_snapshot(staged: iceberg::transaction::StagedTableTransaction) -> Table {
    let table = staged.table().clone();
    let tx = Transaction::new(&table);
    let tx = tx
        .fast_append()
        .set_snapshot_properties(HashMap::from([("probe".to_string(), "1".to_string())]))
        .apply(tx)
        .expect("apply");
    drop(staged);
    tx.apply_locally().await.expect("staged snapshot")
}

fn keyed_creation(name: &str, source: &Table, location: Option<String>) -> TableCreation {
    let schema = source.metadata().current_schema().as_ref().clone();
    let properties = [(KEY_PROPERTY.to_string(), KEY.to_string())];
    match location {
        Some(location) => TableCreation::builder()
            .name(name.to_string())
            .location(location)
            .schema(schema)
            .properties(properties)
            .build(),
        None => TableCreation::builder()
            .name(name.to_string())
            .schema(schema)
            .properties(properties)
            .build(),
    }
}

struct Recorded {
    epoch: u64,
    local: LocalEpoch,
    location: Option<String>,
    tracked: usize,
}

fn recorded(bed: &Bed) -> Recorded {
    let tables = tables(bed);
    Recorded {
        epoch: tables.epoch("c", &ident()),
        local: tables.local_epoch("c", &ident()),
        location: location(tables.current("c", &ident())),
        tracked: tables.tracked(),
    }
}

fn assert_unmoved(bed: &Bed, before: &Recorded) {
    let after = recorded(bed);
    assert_eq!(after.epoch, before.epoch);
    assert_eq!(after.local, before.local);
    assert_eq!(after.location, before.location);
    assert_eq!(after.tracked, before.tracked);
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
    let stale = HeldTable(bed.session.load_table(&ident()).await.expect("load"));
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
async fn a_load_of_another_writers_commit_moves_the_epoch_and_not_the_local_epoch() {
    let bed = bed().await;
    let tables = tables(&bed);
    bed.session.load_table(&ident()).await.expect("load");
    let first = tables.local_epoch("c", &ident());
    assert_eq!(first.changed, tables.epoch("c", &ident()));
    assert_eq!(first.refreshed, 0);
    let other = set_property(&bed.guarded, "other").await;
    let before = tables.epoch("c", &ident());
    bed.session.load_table(&ident()).await.expect("fresh load");
    assert!(tables.epoch("c", &ident()) > before);
    assert_eq!(
        location(tables.current("c", &ident())),
        other.metadata_location().map(str::to_string)
    );
    assert_eq!(tables.local_epoch("c", &ident()), first);
    let before = tables.epoch("c", &ident());
    bed.session.load_table(&ident()).await.expect("reload");
    assert_eq!(tables.epoch("c", &ident()), before);
    assert_eq!(tables.local_epoch("c", &ident()), first);
    refresh_load(&bed.session, &ident()).await.expect("refresh");
    let refreshed = tables.local_epoch("c", &ident());
    assert!(refreshed.changed > first.changed);
    assert_eq!(refreshed.refreshed, refreshed.changed);
    set_property(&bed.session, "own").await;
    let committed = tables.local_epoch("c", &ident());
    assert!(committed.changed > refreshed.changed);
    assert_eq!(committed.refreshed, refreshed.refreshed);
    assert_eq!(committed.changed, tables.epoch("c", &ident()));
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
    bed.session.load_table(&ident()).await.expect("load");
    let table = served(&bed);
    let before = bed.counted.loads();
    let provider = rebound_provider(&table).expect("rebound");
    let state = bed.context.state();
    provider.scan(&state, None, &[], None).await.expect("scan");
    assert_eq!(bed.counted.loads(), before);
}

#[tokio::test]
async fn a_handle_the_guard_did_not_return_is_never_wrapped() {
    let bed = bed().await;
    let raw: Arc<dyn Catalog> = Arc::clone(&bed.counted) as Arc<dyn Catalog>;
    assert!(EncryptionGuardCatalog::guarded(&raw).is_none());
    assert!(EncryptionGuardCatalog::guarded(&bed.guarded).is_some());
    assert!(EncryptionGuardCatalog::guarded(&bed.session).is_none());
    let context = SessionContext::new_with_config(with_session_tables(SessionConfig::new()));
    let (handle, provider) = session_catalog_provider(&context, "raw", Arc::clone(&raw))
        .await
        .expect("registered");
    assert!(Arc::ptr_eq(&handle, &raw));
    context.register_catalog("raw", provider);
    handle.load_table(&ident()).await.expect("load");
    context
        .sql("SELECT * FROM raw.ns.t")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("read");
    let tables = context
        .state_ref()
        .read()
        .config()
        .get_extension::<SessionTables>()
        .expect("session tables");
    assert_eq!(tables.tracked(), 0);
    let (handle, _) = session_catalog_provider(&context, "nested", Arc::clone(&bed.session))
        .await
        .expect("registered");
    assert!(Arc::ptr_eq(&handle, &bed.session));
}

#[tokio::test]
async fn a_refused_snapshot_or_statistics_commit_records_nothing() {
    let bed = keyed_bed().await;
    let table = bed.session.load_table(&ident()).await.expect("load");
    let before = recorded(&bed);
    assert!(before.epoch > 0);
    refusal_text(snapshot_commit(bed.session.as_ref(), &table).await);
    assert_unmoved(&bed, &before);
    refusal_text(statistics_commit(bed.session.as_ref(), &table).await);
    assert_unmoved(&bed, &before);
}

#[tokio::test]
async fn a_refused_commit_on_a_handle_loaded_before_the_key_records_nothing() {
    let bed = bed().await;
    let stale = bed.session.load_table(&ident()).await.expect("load");
    let keyed = set_key(&bed.session, true).await;
    let before = recorded(&bed);
    assert_eq!(
        before.location,
        keyed.metadata_location().map(str::to_string)
    );
    refusal_text(snapshot_commit(bed.session.as_ref(), &stale).await);
    assert_unmoved(&bed, &before);
}

#[tokio::test]
async fn a_refused_file_write_records_nothing() {
    let bed = keyed_bed().await;
    let table = bed.session.load_table(&ident()).await.expect("load");
    let before = recorded(&bed);
    let refused = file_write(&table)
        .await
        .expect_err("a keyed handle refuses a data file");
    assert!(
        EncryptedTableRefusal::in_text(&refused).is_some(),
        "{refused}"
    );
    assert_unmoved(&bed, &before);
}

#[tokio::test]
async fn a_refused_staged_create_or_replace_publish_records_nothing() {
    let bed = bed().await;
    let source = bed.session.load_table(&ident()).await.expect("load");
    let before = recorded(&bed);
    let staged_ident = TableIdent::from_strs(["ns", "staged"]).expect("ident");
    let staged = iceberg::transaction::StagedTableTransaction::begin_create(
        source.file_io().clone(),
        staged_ident.clone(),
        keyed_creation(
            "staged",
            &source,
            Some(format!("{}-staged", source.metadata().location())),
        ),
    )
    .await
    .expect("staged create");
    let table = staged_with_snapshot(staged).await;
    refusal_text(bed.session.publish_create_table(table).await);
    assert_eq!(tables(&bed).epoch("c", &staged_ident), 0);
    assert!(tables(&bed).current("c", &staged_ident).is_none());
    assert_unmoved(&bed, &before);
    let staged = iceberg::transaction::StagedTableTransaction::begin_replace(
        &source,
        keyed_creation("t", &source, None),
    )
    .await
    .expect("staged replace");
    let table = staged_with_snapshot(staged).await;
    refusal_text(
        bed.session
            .publish_replace_table(table, source.metadata_location().map(str::to_string))
            .await,
    );
    assert_unmoved(&bed, &before);
}

#[tokio::test]
async fn a_map_served_keyed_table_refuses_as_a_fresh_guarded_load_does() {
    let bed = keyed_bed().await;
    set_property(&bed.session, "v1").await;
    let held = served(&bed);
    let fresh = bed.guarded.load_table(&ident()).await.expect("fresh load");
    assert!(same_state(held.table(), &fresh));
    let shim = OneTableCatalog {
        table: held.clone(),
    };
    let shimmed = shim.load_table(&ident()).await.expect("shim load");
    let expected = file_write(&fresh).await.expect_err("fresh refuses");
    assert_eq!(file_write(held.table()).await.err(), Some(expected.clone()));
    assert_eq!(file_write(&shimmed).await.err(), Some(expected));
    let expected = refusal_text(snapshot_commit(bed.guarded.as_ref(), &fresh).await);
    assert_eq!(
        refusal_text(snapshot_commit(bed.session.as_ref(), held.table()).await),
        expected
    );
    assert_eq!(
        refusal_text(snapshot_commit(bed.session.as_ref(), &shimmed).await),
        expected
    );
    let through_shim = snapshot_commit(&shim, &shimmed)
        .await
        .expect_err("the shim commits nothing");
    assert!(EncryptedTableRefusal::find(&through_shim).is_some());
    assert!(shim.drop_table(&ident()).await.is_err());
    assert!(shim.register_table(&ident(), String::new()).await.is_err());
}

#[tokio::test]
async fn a_key_added_or_removed_by_a_same_session_commit_reaches_the_map_as_a_fresh_load() {
    let bed = bed().await;
    bed.session.load_table(&ident()).await.expect("load");
    assert_eq!(file_write(served(&bed).table()).await, Ok(()));
    set_key(&bed.session, true).await;
    let held = served(&bed);
    let fresh = bed.guarded.load_table(&ident()).await.expect("fresh load");
    assert!(same_state(held.table(), &fresh));
    let expected = file_write(&fresh).await.expect_err("fresh refuses");
    assert_eq!(file_write(held.table()).await.err(), Some(expected));
    set_key(&bed.session, false).await;
    let held = served(&bed);
    let fresh = bed.guarded.load_table(&ident()).await.expect("fresh load");
    assert!(same_state(held.table(), &fresh));
    assert_eq!(file_write(&fresh).await, Ok(()));
    assert_eq!(file_write(held.table()).await, Ok(()));
    let shim = OneTableCatalog {
        table: held.clone(),
    };
    let through_shim = snapshot_commit(&shim, held.table())
        .await
        .expect_err("the shim commits nothing");
    assert_eq!(through_shim.message(), outside_one_table().message());
    snapshot_commit(bed.session.as_ref(), held.table())
        .await
        .expect("an unkeyed table commits");
}
