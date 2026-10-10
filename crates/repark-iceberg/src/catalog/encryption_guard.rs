use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use iceberg::io::{
    FileIO, FileIOBuilder, FileInfo, FileMetadata, FileRead, FileWrite, InputFile, OutputFile,
    Storage, StorageConfig, StorageFactory,
};
use iceberg::table::Table;
use iceberg::transaction::StagedTableTransaction;
use iceberg::view::{View, ViewCommit};
use iceberg::{
    Catalog, Namespace, NamespaceIdent, Result, TableCommit, TableCreation, TableIdent,
    ViewCreation,
};
use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

use crate::write::encryption::{EncryptedTableRefusal, carries_encryption_key};

pub(crate) const METADATA_ONLY_UPDATES: [&str; 17] = [
    "UpgradeFormatVersion",
    "AddSchema",
    "SetCurrentSchema",
    "RemoveSchemas",
    "AddSpec",
    "SetDefaultSpec",
    "RemovePartitionSpecs",
    "AddSortOrder",
    "SetDefaultSortOrder",
    "SetSnapshotRef",
    "RemoveSnapshotRef",
    "RemoveSnapshots",
    "RemoveStatistics",
    "RemovePartitionStatistics",
    "SetLocation",
    "SetProperties",
    "RemoveProperties",
];
const METADATA_JSON_SUFFIXES: [&str; 2] = [".metadata.json", ".metadata.json.gz"];
const WRITE_METADATA_PATH: &str = "write.metadata.path";
const COMMIT_FIELD_INDENT: &str = "    ";
const UPDATE_ENTRY_INDENT: &str = "        ";

pub struct EncryptionGuardCatalog {
    inner: Arc<dyn Catalog>,
}

impl std::fmt::Debug for EncryptionGuardCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(formatter)
    }
}

impl EncryptionGuardCatalog {
    #[must_use]
    pub fn install(inner: Arc<dyn Catalog>) -> Arc<dyn Catalog> {
        Arc::new(Self { inner })
    }
}

fn is_keyed(table: &Table) -> bool {
    carries_encryption_key(table.metadata().properties())
}

pub(crate) fn update_variants(rendered: &str) -> Option<Vec<&str>> {
    let opened = format!("{COMMIT_FIELD_INDENT}updates: [");
    let empty = format!("{COMMIT_FIELD_INDENT}updates: [],");
    let closed = format!("{COMMIT_FIELD_INDENT}],");
    let mut lines = rendered.lines();
    loop {
        let line = lines.next()?;
        if line == empty {
            return Some(Vec::new());
        }
        if line == opened {
            break;
        }
    }
    let mut variants = Vec::new();
    for line in lines {
        if line == closed {
            return Some(variants);
        }
        if line.is_empty() {
            continue;
        }
        let entry = line.strip_prefix(UPDATE_ENTRY_INDENT)?;
        if entry.starts_with([' ', '}', ')', ']']) {
            continue;
        }
        let end = entry
            .find(|character: char| !character.is_ascii_alphanumeric())
            .unwrap_or(entry.len());
        if end == 0 {
            return None;
        }
        variants.push(&entry[..end]);
    }
    None
}

pub(crate) fn is_metadata_only(rendered: &str) -> bool {
    update_variants(rendered).is_some_and(|variants| {
        variants
            .iter()
            .all(|variant| METADATA_ONLY_UPDATES.contains(variant))
    })
}

pub(crate) async fn base_is_keyed(
    inner: &dyn Catalog,
    ident: &TableIdent,
    base: Option<&Table>,
) -> Result<bool> {
    match base {
        Some(base) => Ok(is_keyed(base)),
        None => Ok(is_keyed(&inner.load_table(ident).await?)),
    }
}

fn metadata_directories(location: &str, properties: &HashMap<String, String>) -> Vec<String> {
    match properties.get(WRITE_METADATA_PATH) {
        Some(directory) => vec![directory.trim_end_matches('/').to_string()],
        None => vec![
            format!("{location}/metadata"),
            format!("{}/metadata", location.trim_end_matches('/')),
        ],
    }
}

fn refuse_keyed_publish(staged: &Table) -> Result<()> {
    if is_keyed(staged) && staged.metadata().current_snapshot().is_some() {
        return Err(EncryptedTableRefusal::of(staged.identifier()).into_iceberg());
    }
    Ok(())
}

#[allow(clippy::missing_errors_doc)]
pub fn guard_table(table: Table) -> Result<Table> {
    if !is_keyed(&table) {
        return Ok(table);
    }
    let mut builder = Table::builder()
        .file_io(guard_file_io(
            table.file_io(),
            table.identifier(),
            metadata_directories(table.metadata().location(), table.metadata().properties()),
        ))
        .metadata(table.metadata_ref())
        .identifier(table.identifier().clone())
        .readonly(table.readonly())
        .object_cache(table.object_cache());
    if let Some(location) = table.metadata_location() {
        builder = builder.metadata_location(location);
    }
    if let Some(footer_cache) = table.footer_cache() {
        builder = builder.footer_cache(footer_cache);
    }
    builder.build()
}

#[allow(clippy::missing_errors_doc)]
pub async fn begin_staged_create(
    file_io: FileIO,
    ident: TableIdent,
    creation: TableCreation,
) -> Result<StagedTableTransaction> {
    let file_io = if carries_encryption_key(&creation.properties) {
        let directories = creation
            .location
            .as_deref()
            .map(|location| metadata_directories(location, &creation.properties))
            .unwrap_or_default();
        guard_file_io(&file_io, &ident, directories)
    } else {
        file_io
    };
    StagedTableTransaction::begin_create(file_io, ident, creation).await
}

fn guard_file_io(
    file_io: &FileIO,
    ident: &TableIdent,
    metadata_directories: Vec<String>,
) -> FileIO {
    let factory = EncryptionGuardStorageFactory {
        refusal: EncryptedTableRefusal::of(ident),
        metadata_directories,
        inner: file_io.clone(),
    };
    FileIOBuilder::new(Arc::new(factory))
        .with_props(file_io.config().props().iter())
        .build()
}

pub(crate) fn is_table_metadata_json(path: &str, metadata_directories: &[String]) -> bool {
    metadata_directories.iter().any(|directory| {
        path.strip_prefix(directory.as_str())
            .and_then(|rest| rest.strip_prefix('/'))
            .is_some_and(|name| {
                !name.contains('/')
                    && METADATA_JSON_SUFFIXES
                        .iter()
                        .any(|suffix| name.len() > suffix.len() && name.ends_with(suffix))
            })
    })
}

#[derive(Debug, Clone)]
struct EncryptionGuardStorageFactory {
    refusal: EncryptedTableRefusal,
    metadata_directories: Vec<String>,
    inner: FileIO,
}

impl Serialize for EncryptionGuardStorageFactory {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("EncryptionGuardStorageFactory", 1)?;
        state.serialize_field("table", self.refusal.table())?;
        state.end()
    }
}

impl StorageFactory for EncryptionGuardStorageFactory {
    fn build(&self, _config: &StorageConfig) -> Result<Arc<dyn Storage>> {
        Ok(Arc::new(EncryptionGuardStorage {
            refusal: self.refusal.clone(),
            metadata_directories: self.metadata_directories.clone(),
            inner: self.inner.clone(),
        }))
    }

    fn typetag_name(&self) -> &'static str {
        "EncryptionGuardStorageFactory"
    }

    fn typetag_deserialize(&self) {}
}

#[derive(Debug, Clone)]
struct EncryptionGuardStorage {
    refusal: EncryptedTableRefusal,
    metadata_directories: Vec<String>,
    inner: FileIO,
}

impl EncryptionGuardStorage {
    fn refuse_file_create(&self, path: &str) -> Result<()> {
        if is_table_metadata_json(path, &self.metadata_directories) {
            return Ok(());
        }
        Err(self.refusal.clone().into_iceberg())
    }
}

impl Serialize for EncryptionGuardStorage {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("EncryptionGuardStorage", 1)?;
        state.serialize_field("table", self.refusal.table())?;
        state.end()
    }
}

#[async_trait]
impl Storage for EncryptionGuardStorage {
    async fn exists(&self, path: &str) -> Result<bool> {
        self.inner.exists(path).await
    }

    async fn metadata(&self, path: &str) -> Result<FileMetadata> {
        self.inner.new_input(path)?.metadata().await
    }

    async fn read(&self, path: &str) -> Result<Bytes> {
        self.inner.new_input(path)?.read().await
    }

    async fn reader(&self, path: &str) -> Result<Box<dyn FileRead>> {
        self.inner.new_input(path)?.reader().await
    }

    async fn write(&self, path: &str, bs: Bytes) -> Result<()> {
        self.refuse_file_create(path)?;
        self.inner.new_output(path)?.write(bs).await
    }

    async fn write_new(&self, path: &str, bs: Bytes) -> Result<()> {
        self.refuse_file_create(path)?;
        self.inner.write_new(path, bs).await
    }

    async fn writer(&self, path: &str) -> Result<Box<dyn FileWrite>> {
        self.refuse_file_create(path)?;
        self.inner.new_output(path)?.writer().await
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.inner.delete(path).await
    }

    async fn delete_prefix(&self, path: &str) -> Result<()> {
        self.inner.delete_prefix(path).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<FileInfo>> {
        self.inner.list(prefix).await
    }

    fn new_input(&self, path: &str) -> Result<InputFile> {
        Ok(InputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn new_output(&self, path: &str) -> Result<OutputFile> {
        Ok(OutputFile::new(Arc::new(self.clone()), path.to_string()))
    }

    fn typetag_name(&self) -> &'static str {
        "EncryptionGuardStorage"
    }

    fn typetag_deserialize(&self) {}
}

#[async_trait]
impl Catalog for EncryptionGuardCatalog {
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
        guard_table(self.inner.create_table(namespace, creation).await?)
    }

    async fn load_table(&self, table: &TableIdent) -> Result<Table> {
        guard_table(self.inner.load_table(table).await?)
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
        guard_table(self.inner.register_table(table, metadata_location).await?)
    }

    async fn update_table(&self, commit: TableCommit) -> Result<Table> {
        let keyed = base_is_keyed(
            self.inner.as_ref(),
            commit.identifier(),
            commit.base_table(),
        )
        .await?;
        if keyed && !is_metadata_only(&format!("{commit:#?}")) {
            return Err(EncryptedTableRefusal::of(commit.identifier()).into_iceberg());
        }
        guard_table(self.inner.update_table(commit).await?)
    }

    async fn publish_create_table(&self, table: Table) -> Result<Table> {
        refuse_keyed_publish(&table)?;
        guard_table(self.inner.publish_create_table(table).await?)
    }

    async fn publish_replace_table(
        &self,
        table: Table,
        expected_base_metadata_location: Option<String>,
    ) -> Result<Table> {
        refuse_keyed_publish(&table)?;
        guard_table(
            self.inner
                .publish_replace_table(table, expected_base_metadata_location)
                .await?,
        )
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
