use super::common::*;
use std::collections::{BTreeSet, HashSet};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use iceberg::spec::Snapshot;
use iceberg::table::Table;
use iceberg::view::{View, ViewCommit};
use iceberg::{Error, ErrorKind, Namespace, TableCommit, TableUpdate, ViewCreation};

pub(super) const OPERATION_ID: &str = "engine.operation-id";
pub(super) const SESSION_PROPERTY: &str = "k";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Attempt {
    pub(super) snapshot_id: i64,
    pub(super) operation_id: Option<String>,
    pub(super) session_property: bool,
}

#[derive(Debug)]
pub(super) struct GateCatalog {
    inner: Arc<dyn Catalog>,
    forward: bool,
    cut_reads: bool,
    reads_cut: AtomicBool,
    commits: AtomicUsize,
    forwarded: AtomicUsize,
    failed_loads: AtomicUsize,
    attempts: Mutex<Vec<Attempt>>,
}

fn attempt_of(snapshot: &Snapshot) -> Attempt {
    let summary = &snapshot.summary().additional_properties;
    Attempt {
        snapshot_id: snapshot.snapshot_id(),
        operation_id: summary.get(OPERATION_ID).cloned(),
        session_property: summary.contains_key(SESSION_PROPERTY),
    }
}

fn snapshot_ids(table: &Table) -> BTreeSet<i64> {
    table
        .metadata()
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .collect()
}

impl GateCatalog {
    pub(super) fn new(inner: Arc<dyn Catalog>, forward: bool, cut_reads: bool) -> Self {
        Self {
            inner,
            forward,
            cut_reads,
            reads_cut: AtomicBool::new(false),
            commits: AtomicUsize::new(0),
            forwarded: AtomicUsize::new(0),
            failed_loads: AtomicUsize::new(0),
            attempts: Mutex::new(Vec::new()),
        }
    }

    pub(super) fn open_reads(&self) {
        self.reads_cut.store(false, Ordering::SeqCst);
    }

    pub(super) fn commits(&self) -> usize {
        self.commits.load(Ordering::SeqCst)
    }

    pub(super) fn forwarded(&self) -> usize {
        self.forwarded.load(Ordering::SeqCst)
    }

    pub(super) fn failed_loads(&self) -> usize {
        self.failed_loads.load(Ordering::SeqCst)
    }

    pub(super) fn attempts(&self) -> Vec<Attempt> {
        self.attempts.lock().expect("the attempts lock").clone()
    }

    fn record(&self, attempts: Vec<Attempt>) {
        self.attempts
            .lock()
            .expect("the attempts lock")
            .extend(attempts);
    }

    async fn forward_then_lose_the_reply(&self, commit: TableCommit) -> iceberg::Result<()> {
        let before = commit.base_table().map(snapshot_ids).unwrap_or_default();
        let committed = self.inner.update_table(commit).await?;
        self.forwarded.fetch_add(1, Ordering::SeqCst);
        self.record(
            committed
                .metadata()
                .snapshots()
                .filter(|snapshot| !before.contains(&snapshot.snapshot_id()))
                .map(|snapshot| attempt_of(snapshot))
                .collect(),
        );
        Ok(())
    }

    fn drop_the_request(&self, mut commit: TableCommit) {
        self.record(
            commit
                .take_updates()
                .into_iter()
                .filter_map(|update| match update {
                    TableUpdate::AddSnapshot { snapshot } => Some(attempt_of(&snapshot)),
                    _ => None,
                })
                .collect(),
        );
    }
}

#[async_trait::async_trait]
impl Catalog for GateCatalog {
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

    async fn update_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        removals: HashSet<String>,
        updates: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner
            .update_namespace_properties(namespace, removals, updates)
            .await
    }

    async fn set_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        updates: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner
            .set_namespace_properties(namespace, updates)
            .await
    }

    async fn remove_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        removals: HashSet<String>,
    ) -> iceberg::Result<()> {
        self.inner
            .remove_namespace_properties(namespace, removals)
            .await
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
        if self.reads_cut.load(Ordering::SeqCst) {
            self.failed_loads.fetch_add(1, Ordering::SeqCst);
            return Err(Error::new(
                ErrorKind::Unexpected,
                "the gate cut the catalog read",
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
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.commits.fetch_add(1, Ordering::SeqCst);
        if self.forward {
            self.forward_then_lose_the_reply(commit).await?;
        } else {
            self.drop_the_request(commit);
        }
        if self.cut_reads {
            self.reads_cut.store(true, Ordering::SeqCst);
        }
        Err(Error::new(
            ErrorKind::CommitStateUnknown,
            "the gate lost the commit reply",
        ))
    }

    async fn publish_create_table(&self, table: Table) -> iceberg::Result<Table> {
        self.inner.publish_create_table(table).await
    }

    async fn publish_replace_table(
        &self,
        table: Table,
        expected_base_metadata_location: Option<String>,
    ) -> iceberg::Result<Table> {
        self.inner
            .publish_replace_table(table, expected_base_metadata_location)
            .await
    }

    async fn list_views(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_views(namespace).await
    }

    async fn create_view(
        &self,
        namespace: &NamespaceIdent,
        creation: ViewCreation,
    ) -> iceberg::Result<View> {
        self.inner.create_view(namespace, creation).await
    }

    async fn load_view(&self, view: &TableIdent) -> iceberg::Result<View> {
        self.inner.load_view(view).await
    }

    async fn drop_view(&self, view: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_view(view).await
    }

    async fn view_exists(&self, view: &TableIdent) -> iceberg::Result<bool> {
        self.inner.view_exists(view).await
    }

    async fn rename_view(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_view(src, dest).await
    }

    async fn update_view(&self, commit: ViewCommit) -> iceberg::Result<View> {
        self.inner.update_view(commit).await
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &HashMap<String, String> {
        self.inner.properties()
    }

    async fn invalidate_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.invalidate_table(table).await
    }

    async fn invalidate_view(&self, view: &TableIdent) -> iceberg::Result<()> {
        self.inner.invalidate_view(view).await
    }
}
