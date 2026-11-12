use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Mutex;

use async_trait::async_trait;
use iceberg::spec::{DataFile, SnapshotRef};
use iceberg::table::Table;
use iceberg::transaction::{StagedTableMode, StagedTableTransaction};
use iceberg::view::{View, ViewCommit};
use iceberg::{
    Catalog, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent,
    ViewCreation,
};

#[allow(clippy::missing_errors_doc)]
pub async fn commit_staged(
    staged: StagedTableTransaction,
    data_files: Vec<DataFile>,
    catalog: &dyn Catalog,
) -> Result<Table> {
    let base = staged.table().clone();
    let mode = staged.mode();
    let written = written_paths(&data_files);
    let capture = PublishCapture {
        inner: catalog,
        handed: Mutex::new(None),
    };
    let result = staged.add_data_files(data_files).commit(&capture).await;
    if let Err(error) = &result {
        let handed = capture.handed.lock().ok().and_then(|mut slot| slot.take());
        discard_rejected(catalog, mode, &base, handed.as_ref(), &written, error).await;
    }
    result
}

#[allow(clippy::missing_errors_doc)]
pub async fn publish_applied(
    catalog: &dyn Catalog,
    base: &Table,
    mode: StagedTableMode,
    table: Table,
    replace_base: Option<String>,
    written: &[String],
) -> Result<Table> {
    let handed = table.clone();
    let result = match mode {
        StagedTableMode::Create => catalog.publish_create_table(table).await,
        StagedTableMode::Replace => catalog.publish_replace_table(table, replace_base).await,
    };
    if let Err(error) = &result {
        discard_rejected(catalog, mode, base, Some(&handed), written, error).await;
    }
    result
}

#[must_use]
pub fn written_paths(data_files: &[DataFile]) -> Vec<String> {
    data_files
        .iter()
        .map(|file| file.file_path().to_string())
        .collect()
}

pub async fn discard_unpublished_create(staged: &StagedTableTransaction) {
    if staged.mode() != StagedTableMode::Create {
        return;
    }
    let Some(location) = staged.table().metadata_location() else {
        return;
    };
    if let Err(error) = staged.table().file_io().delete(location).await {
        tracing::warn!(
            ?error,
            location,
            "could not delete the begin file of a failed create"
        );
    }
}

async fn discard_rejected(
    catalog: &dyn Catalog,
    mode: StagedTableMode,
    base: &Table,
    handed: Option<&Table>,
    written: &[String],
    error: &iceberg::Error,
) {
    if error.kind() == ErrorKind::CommitStateUnknown {
        return;
    }
    let Probe::Absent(current) = publish_absent(catalog, mode, base, handed).await else {
        tracing::warn!(
            table = %base.identifier(),
            "staged publish failed but the catalog state is not known to exclude it; keeping its files"
        );
        return;
    };
    let referenced = match &current {
        Some(current) => match referenced_files(current, base).await {
            Ok(referenced) => referenced,
            Err(error) => {
                tracing::warn!(
                    ?error,
                    table = %base.identifier(),
                    "could not walk the current table's files; keeping the staged publish's files"
                );
                return;
            }
        },
        None => HashSet::new(),
    };
    let mut paths = BTreeSet::new();
    if let Some(handed) = handed {
        paths.extend(snapshot_files(handed, base).await);
        if let Some(location) = handed.metadata_location() {
            paths.insert(location.to_string());
        }
    }
    if mode == StagedTableMode::Create
        && let Some(location) = base.metadata_location()
    {
        paths.insert(location.to_string());
    }
    paths.extend(written.iter().cloned());
    for path in paths.into_iter().filter(|path| !referenced.contains(path)) {
        if let Err(error) = base.file_io().delete(&path).await {
            tracing::warn!(
                ?error,
                path,
                "could not delete a file of a rejected staged publish"
            );
        }
    }
}

enum Probe {
    Kept,
    Absent(Option<Box<Table>>),
}

async fn publish_absent(
    catalog: &dyn Catalog,
    mode: StagedTableMode,
    base: &Table,
    handed: Option<&Table>,
) -> Probe {
    let current = match catalog.load_table(base.identifier()).await {
        Ok(current) => current,
        Err(error)
            if mode == StagedTableMode::Create && error.kind() == ErrorKind::TableNotFound =>
        {
            return Probe::Absent(None);
        }
        Err(_) => return Probe::Kept,
    };
    let absent = match handed {
        Some(handed) => !publish_landed(&current, mode, base, handed),
        None => {
            mode == StagedTableMode::Replace || current.metadata().uuid() != base.metadata().uuid()
        }
    };
    if absent {
        Probe::Absent(Some(Box::new(current)))
    } else {
        Probe::Kept
    }
}

async fn referenced_files(current: &Table, base: &Table) -> Result<HashSet<String>> {
    let metadata = current.metadata();
    let known = snapshot_ids(base);
    let mut referenced: HashSet<String> = metadata
        .metadata_log()
        .iter()
        .map(|entry| entry.metadata_file.clone())
        .chain(current.metadata_location().map(str::to_string))
        .collect();
    let current_id = metadata.current_snapshot_id();
    for snapshot in metadata.snapshots() {
        referenced.insert(snapshot.manifest_list().to_string());
        if known.contains(&snapshot.snapshot_id()) && Some(snapshot.snapshot_id()) != current_id {
            continue;
        }
        let list = snapshot
            .load_manifest_list(current.file_io(), metadata)
            .await?;
        for manifest in list.entries() {
            if known.contains(&manifest.added_snapshot_id)
                || !referenced.insert(manifest.manifest_path.clone())
            {
                continue;
            }
            let loaded = manifest.load_manifest(current.file_io()).await?;
            referenced.extend(
                loaded
                    .entries()
                    .iter()
                    .map(|entry| entry.data_file().file_path().to_string()),
            );
        }
    }
    Ok(referenced)
}

fn snapshot_ids(table: &Table) -> HashSet<i64> {
    table
        .metadata()
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .collect()
}

fn publish_landed(current: &Table, mode: StagedTableMode, base: &Table, handed: &Table) -> bool {
    let metadata = current.metadata();
    let location_logged = handed.metadata_location().is_some_and(|location| {
        current.metadata_location() == Some(location)
            || metadata
                .metadata_log()
                .iter()
                .any(|entry| entry.metadata_file == location)
    });
    let snapshot_kept = new_snapshots(handed, base)
        .any(|snapshot| metadata.snapshot_by_id(snapshot.snapshot_id()).is_some());
    let uuid_kept = mode == StagedTableMode::Create && metadata.uuid() == handed.metadata().uuid();
    location_logged || snapshot_kept || uuid_kept
}

fn new_snapshots<'a>(handed: &'a Table, base: &Table) -> impl Iterator<Item = &'a SnapshotRef> {
    let known = snapshot_ids(base);
    handed
        .metadata()
        .snapshots()
        .filter(move |snapshot| !known.contains(&snapshot.snapshot_id()))
}

async fn snapshot_files(handed: &Table, base: &Table) -> Vec<String> {
    let mut paths = Vec::new();
    for snapshot in new_snapshots(handed, base) {
        paths.push(snapshot.manifest_list().to_string());
        match snapshot
            .load_manifest_list(handed.file_io(), handed.metadata())
            .await
        {
            Ok(list) => paths.extend(
                list.entries()
                    .iter()
                    .filter(|manifest| manifest.added_snapshot_id == snapshot.snapshot_id())
                    .map(|manifest| manifest.manifest_path.clone()),
            ),
            Err(error) => tracing::warn!(
                ?error,
                manifest_list = snapshot.manifest_list(),
                "could not read the manifest list of a rejected staged publish"
            ),
        }
    }
    paths
}

#[derive(Debug)]
struct PublishCapture<'a> {
    inner: &'a dyn Catalog,
    handed: Mutex<Option<Table>>,
}

impl PublishCapture<'_> {
    fn record(&self, table: &Table) {
        if let Ok(mut slot) = self.handed.lock() {
            *slot = Some(table.clone());
        }
    }
}

#[async_trait]
impl Catalog for PublishCapture<'_> {
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

    async fn update_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        removals: HashSet<String>,
        updates: HashMap<String, String>,
    ) -> Result<()> {
        self.inner
            .update_namespace_properties(namespace, removals, updates)
            .await
    }

    async fn set_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        updates: HashMap<String, String>,
    ) -> Result<()> {
        self.inner
            .set_namespace_properties(namespace, updates)
            .await
    }

    async fn remove_namespace_properties(
        &self,
        namespace: &NamespaceIdent,
        removals: HashSet<String>,
    ) -> Result<()> {
        self.inner
            .remove_namespace_properties(namespace, removals)
            .await
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

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        self.inner.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.drop_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_table(src, dest).await
    }

    async fn register_table(&self, table: &TableIdent, metadata_location: String) -> Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        self.inner.update_table(commit).await
    }

    async fn publish_create_table(&self, table: Table) -> Result<Table> {
        self.record(&table);
        self.inner.publish_create_table(table).await
    }

    async fn publish_replace_table(
        &self,
        table: Table,
        expected_base_metadata_location: Option<String>,
    ) -> Result<Table> {
        self.record(&table);
        self.inner
            .publish_replace_table(table, expected_base_metadata_location)
            .await
    }

    async fn list_views(&self, namespace: &NamespaceIdent) -> Result<Vec<TableIdent>> {
        self.inner.list_views(namespace).await
    }

    async fn create_view(
        &self,
        namespace: &NamespaceIdent,
        creation: ViewCreation,
    ) -> Result<View> {
        self.inner.create_view(namespace, creation).await
    }

    async fn load_view(&self, view: &TableIdent) -> Result<View> {
        self.inner.load_view(view).await
    }

    async fn drop_view(&self, view: &TableIdent) -> Result<()> {
        self.inner.drop_view(view).await
    }

    async fn view_exists(&self, view: &TableIdent) -> Result<bool> {
        self.inner.view_exists(view).await
    }

    async fn rename_view(&self, src: &TableIdent, dest: &TableIdent) -> Result<()> {
        self.inner.rename_view(src, dest).await
    }

    async fn update_view(&self, commit: ViewCommit) -> Result<View> {
        self.inner.update_view(commit).await
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &HashMap<String, String> {
        self.inner.properties()
    }

    async fn invalidate_table(&self, table: &TableIdent) -> Result<()> {
        self.inner.invalidate_table(table).await
    }

    async fn invalidate_view(&self, view: &TableIdent) -> Result<()> {
        self.inner.invalidate_view(view).await
    }
}
