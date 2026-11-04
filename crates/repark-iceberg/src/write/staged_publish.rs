use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Mutex;

use async_trait::async_trait;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{StagedTableMode, StagedTableTransaction};
use iceberg::{
    Catalog, ErrorKind, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent,
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
    if !publish_absent(catalog, mode, base, handed).await {
        tracing::warn!(
            table = %base.identifier(),
            "staged publish failed but the catalog state is not known to exclude it; keeping its files"
        );
        return;
    }
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
    for path in paths {
        if let Err(error) = base.file_io().delete(&path).await {
            tracing::warn!(
                ?error,
                path,
                "could not delete a file of a rejected staged publish"
            );
        }
    }
}

async fn publish_absent(
    catalog: &dyn Catalog,
    mode: StagedTableMode,
    base: &Table,
    handed: Option<&Table>,
) -> bool {
    let current = match catalog.load_table(base.identifier()).await {
        Ok(current) => current,
        Err(error) => return error.kind() == ErrorKind::TableNotFound,
    };
    let Some(handed) = handed else {
        return mode == StagedTableMode::Replace
            || current.metadata().uuid() != base.metadata().uuid();
    };
    match mode {
        StagedTableMode::Create => current.metadata().uuid() != handed.metadata().uuid(),
        StagedTableMode::Replace => {
            current.metadata_location() != handed.metadata_location()
                && (handed.metadata().current_snapshot_id().is_none()
                    || current.metadata().current_snapshot_id()
                        != handed.metadata().current_snapshot_id())
        }
    }
}

async fn snapshot_files(handed: &Table, base: &Table) -> Vec<String> {
    let known: HashSet<i64> = base
        .metadata()
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .collect();
    let mut paths = Vec::new();
    for snapshot in handed.metadata().snapshots() {
        if known.contains(&snapshot.snapshot_id()) {
            continue;
        }
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

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn properties(&self) -> &HashMap<String, String> {
        self.inner.properties()
    }
}
