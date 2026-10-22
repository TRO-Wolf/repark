use super::common::*;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use iceberg::io::FileIO;
use iceberg::spec::{DataContentType, DataFileFormat, Snapshot, TableMetadata};
use iceberg::table::Table;
use iceberg::view::{View, ViewCommit};
use iceberg::{Namespace, TableCommit, TableUpdate, ViewCreation};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HoldMode {
    Hold,
    ForwardThenHold,
    Forward,
}

#[derive(Debug)]
pub(super) struct HoldCatalog {
    pub(super) inner: Arc<dyn Catalog>,
    pub(super) mode: HoldMode,
    pub(super) marker: PathBuf,
    pub(super) parent: u32,
    pub(super) limit: Duration,
}

pub(super) const HOLD_LIMIT: Duration = Duration::from_mins(2);
pub(super) const HOLD_RELEASED_EXIT: i32 = 75;

pub(super) fn local_path(path: &str) -> String {
    path.strip_prefix("file://")
        .or_else(|| path.strip_prefix("file:"))
        .unwrap_or(path)
        .to_string()
}

pub(super) type Files = BTreeMap<String, (&'static str, Option<DataFileFormat>)>;

async fn add_snapshot_files(
    file_io: &FileIO,
    metadata: &TableMetadata,
    snapshot: &Snapshot,
    files: &mut Files,
) {
    files.insert(local_path(snapshot.manifest_list()), ("lists", None));
    let list = snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .expect("load a manifest list");
    for manifest_file in list.entries() {
        files.insert(
            local_path(&manifest_file.manifest_path),
            ("manifests", None),
        );
        let manifest = manifest_file
            .load_manifest(file_io)
            .await
            .expect("load a manifest");
        for entry in manifest.entries() {
            let kind = match entry.content_type() {
                DataContentType::Data => ("data", None),
                DataContentType::PositionDeletes | DataContentType::EqualityDeletes => {
                    ("deletes", Some(entry.file_format()))
                }
            };
            files.insert(local_path(entry.file_path()), kind);
        }
    }
}

pub(super) fn snapshot_ids(metadata: &TableMetadata) -> BTreeSet<i64> {
    metadata
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .collect()
}

pub(super) async fn reachable(table: &Table) -> Files {
    let mut files = BTreeMap::new();
    for snapshot in table.metadata().snapshots() {
        add_snapshot_files(table.file_io(), table.metadata(), snapshot, &mut files).await;
    }
    if let Some(location) = table.metadata_location() {
        files.insert(local_path(location), ("metadata", None));
    }
    for entry in table.metadata().metadata_log() {
        files.insert(local_path(&entry.metadata_file), ("metadata", None));
    }
    files
}

async fn attempted_files(
    file_io: &FileIO,
    metadata: &TableMetadata,
    attempted: &[Snapshot],
    base: Option<&Table>,
) -> Files {
    let before = match base {
        Some(base) => reachable(base).await,
        None => BTreeMap::new(),
    };
    let mut files = BTreeMap::new();
    for snapshot in attempted {
        add_snapshot_files(file_io, metadata, snapshot, &mut files).await;
    }
    files.retain(|path, _| !before.contains_key(path));
    files
}

impl HoldCatalog {
    async fn hold(&self) -> iceberg::Result<Table> {
        let deadline = Instant::now() + self.limit;
        while std::os::unix::process::parent_id() == self.parent && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        std::process::exit(HOLD_RELEASED_EXIT)
    }

    fn write_marker(&self, attempted: &[Snapshot], location: &str, files: &Files) {
        let mut lines = vec![format!("location {}", local_path(location))];
        for snapshot in attempted {
            lines.push(format!("snapshot {}", snapshot.snapshot_id()));
            for key in snapshot.summary().additional_properties.keys() {
                lines.push(format!("summary {key}"));
            }
        }
        for (path, (kind, format)) in files {
            lines.push(format!("{kind} {path}"));
            if let Some(format) = format {
                lines.push(format!("format {format:?} {path}"));
            }
        }
        let text = lines.join("\n");
        let staged = self.marker.with_extension("partial");
        std::fs::write(&staged, text).expect("write the marker");
        std::fs::rename(&staged, &self.marker).expect("publish the marker");
    }

    async fn hold_commit(&self, mut commit: TableCommit) -> iceberg::Result<Table> {
        let base = commit
            .take_base_table()
            .expect("every door's commit carries its base table");
        let attempted: Vec<Snapshot> = commit
            .take_updates()
            .into_iter()
            .filter_map(|update| match update {
                TableUpdate::AddSnapshot { snapshot } => Some(snapshot),
                _ => None,
            })
            .collect();
        let files = attempted_files(base.file_io(), base.metadata(), &attempted, Some(&base)).await;
        self.write_marker(&attempted, base.metadata().location(), &files);
        self.hold().await
    }

    async fn forward_commit(&self, commit: TableCommit) -> iceberg::Result<Table> {
        let base = commit.base_table().cloned();
        let committed = self.inner.update_table(commit).await?;
        let before = base
            .as_ref()
            .map(|table| snapshot_ids(table.metadata()))
            .unwrap_or_default();
        let attempted: Vec<Snapshot> = committed
            .metadata()
            .snapshots()
            .filter(|snapshot| !before.contains(&snapshot.snapshot_id()))
            .map(|snapshot| snapshot.as_ref().clone())
            .collect();
        let files = attempted_files(
            committed.file_io(),
            committed.metadata(),
            &attempted,
            base.as_ref(),
        )
        .await;
        self.write_marker(&attempted, committed.metadata().location(), &files);
        if self.mode == HoldMode::ForwardThenHold {
            return self.hold().await;
        }
        Ok(committed)
    }
}

#[async_trait::async_trait]
impl Catalog for HoldCatalog {
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
        match self.mode {
            HoldMode::Hold => self.hold_commit(commit).await,
            HoldMode::ForwardThenHold | HoldMode::Forward => self.forward_commit(commit).await,
        }
    }

    async fn publish_create_table(&self, table: Table) -> iceberg::Result<Table> {
        let attempted: Vec<Snapshot> = table
            .metadata()
            .snapshots()
            .map(|snapshot| snapshot.as_ref().clone())
            .collect();
        let mut files = attempted_files(table.file_io(), table.metadata(), &attempted, None).await;
        if let Some(location) = table.metadata_location() {
            files.insert(local_path(location), ("metadata", None));
        }
        self.write_marker(&attempted, table.metadata().location(), &files);
        if self.mode == HoldMode::Hold {
            return self.hold().await;
        }
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
