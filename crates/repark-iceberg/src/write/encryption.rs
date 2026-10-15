use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};

use super::unsupported_error;

pub const ENCRYPTION_KEY_ID_PROPERTY: &str = "encryption.key-id";

const REFUSAL_HEAD: &str = "Table ";
const REFUSAL_TAIL: &str = " carries property 'encryption.key-id': RePark has no table encryption \
                            and refuses to write plaintext into a table that asks for it (ENC-1).";
const MAX_SOURCE_DEPTH: usize = 32;

#[must_use]
pub fn refusal_text(table: &str) -> String {
    format!("{REFUSAL_HEAD}{table}{REFUSAL_TAIL}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptedTableRefusal {
    table: String,
}

impl EncryptedTableRefusal {
    #[must_use]
    pub fn of(ident: &TableIdent) -> Self {
        Self {
            table: format!("{}.{}", ident.namespace(), ident.name()),
        }
    }

    #[must_use]
    pub fn table(&self) -> &str {
        &self.table
    }

    #[must_use]
    pub fn in_error(error: &(dyn std::error::Error + 'static)) -> Option<Self> {
        let mut current = Some(error);
        for _ in 0..MAX_SOURCE_DEPTH {
            let link = current?;
            if let Some(found) = link.downcast_ref::<Self>() {
                return Some(found.clone());
            }
            current = link.source();
        }
        None
    }

    #[must_use]
    pub fn in_text(text: &str) -> Option<Self> {
        let tail = text.find(REFUSAL_TAIL)?;
        let head = text[..tail].rfind(REFUSAL_HEAD)?;
        Some(Self {
            table: text[head + REFUSAL_HEAD.len()..tail].to_string(),
        })
    }

    #[must_use]
    pub fn find(error: &(dyn std::error::Error + 'static)) -> Option<Self> {
        Self::in_error(error).or_else(|| Self::in_text(&error.to_string()))
    }

    #[must_use]
    pub fn into_iceberg(self) -> iceberg::Error {
        iceberg::Error::new(iceberg::ErrorKind::FeatureUnsupported, self.to_string())
            .with_retryable(false)
            .with_source(self)
    }

    #[must_use]
    pub fn into_datafusion(self) -> DataFusionError {
        unsupported_error(self.to_string())
    }
}

impl std::fmt::Display for EncryptedTableRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&refusal_text(&self.table))
    }
}

impl std::error::Error for EncryptedTableRefusal {}

#[must_use]
pub fn carries_encryption_key<S: std::hash::BuildHasher>(
    properties: &HashMap<String, String, S>,
) -> bool {
    properties.contains_key(ENCRYPTION_KEY_ID_PROPERTY)
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_encrypted_properties<S: std::hash::BuildHasher>(
    properties: &HashMap<String, String, S>,
    ident: &TableIdent,
) -> Result<()> {
    if carries_encryption_key(properties) {
        return Err(EncryptedTableRefusal::of(ident).into_datafusion());
    }
    Ok(())
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_encrypted_table(table: &Table) -> Result<()> {
    refuse_encrypted_properties(table.metadata().properties(), table.identifier())
}

#[allow(clippy::missing_errors_doc)]
pub async fn refuse_encrypted_write(catalog: &dyn Catalog, ident: &TableIdent) -> Result<()> {
    let Ok(table) = catalog.load_table(ident).await else {
        return Ok(());
    };
    refuse_encrypted_table(&table)
}

#[must_use]
pub fn normalize_encrypted_refusal(error: DataFusionError) -> DataFusionError {
    match EncryptedTableRefusal::find(&error) {
        Some(refusal) => refusal.into_datafusion(),
        None => error,
    }
}

#[must_use]
pub(crate) fn written_file_paths(files: &[DataFile]) -> Vec<String> {
    files
        .iter()
        .map(|file| file.file_path().to_string())
        .collect()
}

pub(crate) async fn delete_staged_files_best_effort(
    table: &Table,
    paths: &[String],
) -> Vec<(String, iceberg::Error)> {
    let mut failures = Vec::new();
    let file_io = table.file_io();
    for path in paths {
        if let Err(error) = file_io.delete(path).await {
            tracing::warn!(
                path = %path,
                error = %error,
                "failed to delete a staged file after a refused commit"
            );
            failures.push((path.clone(), error));
        }
    }
    failures
}

pub(crate) async fn sweep_staged_files_on_keyed_refusal(
    table: &Table,
    paths: &[String],
    error: iceberg::Error,
) -> iceberg::Error {
    if error.kind() == iceberg::ErrorKind::CommitStateUnknown {
        tracing::warn!(
            files = paths.len(),
            "commit state unknown; leaving staged files in place"
        );
        return error;
    }
    if EncryptedTableRefusal::find(&error).is_none() {
        return error;
    }
    let failures = delete_staged_files_best_effort(table, paths).await;
    if failures.is_empty() {
        return error;
    }
    let mut message = error.message().to_string();
    for (path, failure) in &failures {
        message.push_str(" failed to delete staged file '");
        message.push_str(path);
        message.push_str("': ");
        message.push_str(&failure.to_string());
        message.push(';');
    }
    iceberg::Error::new(error.kind(), message)
        .with_retryable(error.retryable())
        .with_source(error)
}

#[cfg(test)]
mod sweep_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use super::*;

    fn ident() -> TableIdent {
        TableIdent::new(NamespaceIdent::new("sweep".to_string()), "t".to_string())
    }

    fn refusal() -> iceberg::Error {
        EncryptedTableRefusal::of(&ident()).into_iceberg()
    }

    fn expected_text() -> String {
        refusal_text("sweep.t")
    }

    async fn plain_table(warehouse: &TempDir) -> (Arc<dyn Catalog>, iceberg::table::Table) {
        let catalog =
            crate::memory_catalog(warehouse.path().to_str().expect("warehouse path is utf8"))
                .await
                .expect("memory catalog builds");
        catalog
            .create_namespace(&NamespaceIdent::new("sweep".to_string()), HashMap::new())
            .await
            .expect("namespace creates");
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            ])
            .build()
            .expect("id schema builds");
        catalog
            .create_table(
                ident().namespace(),
                TableCreation::builder()
                    .name("t".to_string())
                    .schema(schema)
                    .properties(HashMap::new())
                    .build(),
            )
            .await
            .expect("table creates");
        let table = catalog.load_table(&ident()).await.expect("table loads");
        (catalog, table)
    }

    fn warehouse_file(warehouse: &TempDir, name: &str) -> String {
        let path = warehouse.path().join(name);
        std::fs::write(&path, b"probe").expect("probe file writes");
        path.to_str().expect("probe path is utf8").to_string()
    }

    #[tokio::test]
    async fn keyed_refusal_with_nothing_staged_returns_the_same_error() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let (_catalog, table) = plain_table(&warehouse).await;
        let error = refusal();
        let swept = sweep_staged_files_on_keyed_refusal(&table, &[], error).await;
        assert_eq!(swept.message(), expected_text().as_str());
        assert!(EncryptedTableRefusal::find(&swept).is_some());
    }

    #[tokio::test]
    async fn keyed_refusal_sweep_deletes_only_the_listed_path() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let (_catalog, table) = plain_table(&warehouse).await;
        let listed = warehouse_file(&warehouse, "listed.parquet");
        let kept = warehouse_file(&warehouse, "kept.parquet");
        let swept =
            sweep_staged_files_on_keyed_refusal(&table, std::slice::from_ref(&listed), refusal())
                .await;
        assert_eq!(swept.message(), expected_text().as_str());
        assert!(!std::path::Path::new(&listed).exists());
        assert!(std::path::Path::new(&kept).exists());
    }

    #[tokio::test]
    async fn keyed_refusal_names_a_file_it_could_not_delete() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let (_catalog, table) = plain_table(&warehouse).await;
        let directory = warehouse.path().join("sweep-probe-dir");
        std::fs::create_dir(&directory).expect("probe directory creates");
        let path = directory.to_str().expect("probe path is utf8").to_string();
        let swept =
            sweep_staged_files_on_keyed_refusal(&table, std::slice::from_ref(&path), refusal())
                .await;
        assert!(swept.message().contains(expected_text().as_str()));
        assert!(swept.message().contains(path.as_str()));
        assert!(EncryptedTableRefusal::find(&swept).is_some());
        assert_eq!(swept.kind(), iceberg::ErrorKind::FeatureUnsupported);
        assert!(!swept.retryable());
    }

    #[tokio::test]
    async fn non_keyed_commit_error_sweeps_nothing() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let (_catalog, table) = plain_table(&warehouse).await;
        let listed = warehouse_file(&warehouse, "listed.parquet");
        let error = iceberg::Error::new(iceberg::ErrorKind::DataInvalid, "conflict");
        let swept =
            sweep_staged_files_on_keyed_refusal(&table, std::slice::from_ref(&listed), error).await;
        assert_eq!(swept.message(), "conflict");
        assert!(std::path::Path::new(&listed).exists());
    }

    #[tokio::test]
    async fn unknown_commit_state_sweeps_nothing() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let (_catalog, table) = plain_table(&warehouse).await;
        let listed = warehouse_file(&warehouse, "listed.parquet");
        let error = iceberg::Error::new(iceberg::ErrorKind::CommitStateUnknown, "unknown");
        let swept =
            sweep_staged_files_on_keyed_refusal(&table, std::slice::from_ref(&listed), error).await;
        assert_eq!(swept.message(), "unknown");
        assert!(std::path::Path::new(&listed).exists());
    }
}
