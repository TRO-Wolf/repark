use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use iceberg::Catalog;
use iceberg::expr::Predicate;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use tracing::Instrument;

use super::KnownPartitions;
use super::dv_close;
use super::iceberg_err;
use crate::write::commit_error::commit_err;
use crate::write::concurrency::WriteConcurrency;
use crate::write::encryption::{
    delete_staged_files_best_effort, sweep_staged_files_on_keyed_refusal, written_file_paths,
};
use crate::write::sink_offsets::SiteStamp;

pub(crate) const WRITE_MERGE_ISOLATION_LEVEL: &str = "write.merge.isolation-level";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IsolationLevel {
    Serializable,
    Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RowDeltaKind {
    Merge,
    Delete,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CommitScope {
    pub isolation: IsolationLevel,
    pub isolation_property: &'static str,
    pub conflict_filter: Predicate,
}

impl CommitScope {
    #[cfg(test)]
    pub(crate) fn unscoped(isolation: IsolationLevel) -> Self {
        Self {
            isolation,
            isolation_property: WRITE_MERGE_ISOLATION_LEVEL,
            conflict_filter: Predicate::AlwaysTrue,
        }
    }

    pub(crate) fn scoped(isolation: IsolationLevel, conflict_filter: Predicate) -> Self {
        Self {
            isolation,
            isolation_property: WRITE_MERGE_ISOLATION_LEVEL,
            conflict_filter,
        }
    }

    pub(crate) fn governed_by(self, isolation_property: &'static str) -> Self {
        Self {
            isolation_property,
            ..self
        }
    }

    pub(crate) fn row_delta(&self, kind: RowDeltaKind) -> RowDeltaPolicy {
        RowDeltaPolicy {
            kind,
            scope: self.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct RowDeltaPolicy {
    pub kind: RowDeltaKind,
    pub scope: CommitScope,
}

async fn sweep_after_rejected_commit(
    table: &Table,
    paths: &[String],
    error: iceberg::Error,
) -> iceberg::Error {
    if error.kind() == iceberg::ErrorKind::CommitStateUnknown {
        tracing::warn!(
            files = paths.len(),
            "MERGE commit state unknown; leaving written files in place (cleanup skipped so a \
             possibly-successful commit is never corrupted; reclaim via orphan-file maintenance)"
        );
        return error;
    }
    if crate::write::encryption::EncryptedTableRefusal::find(&error).is_some() {
        return sweep_staged_files_on_keyed_refusal(table, paths, error).await;
    }
    delete_staged_files_best_effort(table, paths).await;
    error
}

pub(crate) fn resolve_merge_isolation(table: &Table) -> Result<IsolationLevel> {
    match table
        .metadata()
        .properties()
        .get(WRITE_MERGE_ISOLATION_LEVEL)
    {
        Some(name) => match name.to_ascii_lowercase().as_str() {
            "serializable" => Ok(IsolationLevel::Serializable),
            "snapshot" => Ok(IsolationLevel::Snapshot),
            _ => Err(DataFusionError::Plan(format!(
                "Invalid isolation level: {name}"
            ))),
        },
        None => Ok(IsolationLevel::Serializable),
    }
}

#[cfg(test)]
pub(crate) async fn commit(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    affected: Vec<DataFile>,
    new_files: Vec<DataFile>,
) -> Result<()> {
    commit_on_ref(
        catalog,
        table,
        snapshot_id,
        affected,
        new_files,
        &Predicate::AlwaysTrue,
        None,
        &[],
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_on_ref(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    affected: Vec<DataFile>,
    new_files: Vec<DataFile>,
    conflict_filter: &Predicate,
    branch: Option<&str>,
    summary_extra: &[(String, String)],
) -> Result<()> {
    let isolation = resolve_merge_isolation(table)?;
    commit_overwrite_on_ref(
        catalog,
        table,
        snapshot_id,
        affected,
        new_files,
        &CommitScope::scoped(isolation, conflict_filter.clone()),
        branch,
        summary_extra,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn commit_overwrite(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    affected: Vec<DataFile>,
    new_files: Vec<DataFile>,
    scope: &CommitScope,
    summary_extra: &[(String, String)],
) -> Result<()> {
    commit_overwrite_on_ref(
        catalog,
        table,
        snapshot_id,
        affected,
        new_files,
        scope,
        None,
        summary_extra,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_overwrite_on_ref(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    affected: Vec<DataFile>,
    new_files: Vec<DataFile>,
    scope: &CommitScope,
    branch: Option<&str>,
    summary_extra: &[(String, String)],
) -> Result<()> {
    if affected.is_empty() && new_files.is_empty() {
        return Ok(());
    }
    crate::write::refuse_encrypted_table(table)?;
    let new_file_paths = written_file_paths(&new_files);
    let stamp = SiteStamp::claim_isolated(table, branch, summary_extra, scope)?;
    let summary_extra = stamp.extras(summary_extra)?;
    let engine = crate::write::summary_collision::EngineSummary::for_changes(
        table, &new_files, &affected, branch,
    );
    let (operation_id, summary) =
        crate::write::write_options::summary_with_extras(&summary_extra, &engine)?;
    let tx = Transaction::new(table);
    let tx = if affected.is_empty() {
        let mut action = tx
            .overwrite_files()
            .add_files(new_files)
            .conflict_detection_filter(scope.conflict_filter.clone())
            .case_sensitive(true)
            .set_snapshot_properties(summary);
        if scope.isolation == IsolationLevel::Serializable {
            action = action.validate_no_conflicting_data();
        }
        if let Some(pin) = snapshot_id {
            action = action.validate_from_snapshot(pin);
        }
        let action =
            crate::write::commit_target::maybe_to_branch(action, branch, |action, name| {
                action.to_branch(name)
            });
        action.apply(tx).map_err(iceberg_err)?
    } else {
        let mut action = tx
            .overwrite_files()
            .delete_data_files(affected)
            .add_files(new_files)
            .conflict_detection_filter(scope.conflict_filter.clone())
            .validate_no_conflicting_deletes()
            .case_sensitive(true)
            .set_snapshot_properties(summary);
        if scope.isolation == IsolationLevel::Serializable {
            action = action.validate_no_conflicting_data();
        }
        if let Some(pin) = snapshot_id {
            action = action.validate_from_snapshot(pin);
        }
        let action =
            crate::write::commit_target::maybe_to_branch(action, branch, |action, name| {
                action.to_branch(name)
            });
        action.apply(tx).map_err(iceberg_err)?
    };
    let tx = stamp.transaction(tx)?;
    match tx.commit(catalog.as_ref()).await {
        Ok(committed) => stamp.record(&committed),
        Err(error) => {
            let error = sweep_after_rejected_commit(table, &new_file_paths, error).await;
            Err(commit_err(error, &operation_id))
        }
    }
}

#[cfg(test)]
pub(crate) async fn commit_row_delta(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    pairs: Vec<crate::write::position_delete::PositionDeletePair>,
    data_files: Vec<DataFile>,
    concurrency: WriteConcurrency,
) -> Result<()> {
    commit_row_delta_on_ref(
        catalog,
        table,
        snapshot_id,
        pairs,
        data_files,
        concurrency,
        None,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn commit_row_delta_on_ref(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    pairs: Vec<crate::write::position_delete::PositionDeletePair>,
    data_files: Vec<DataFile>,
    concurrency: WriteConcurrency,
    branch: Option<&str>,
) -> Result<()> {
    let isolation = resolve_merge_isolation(table)?;
    commit_row_delta_kind_on_ref(
        catalog,
        table,
        snapshot_id,
        pairs,
        data_files,
        concurrency,
        &CommitScope::unscoped(isolation).row_delta(RowDeltaKind::Merge),
        branch,
        KnownPartitions::new(),
        &[],
        &crate::write::write_options::WriterStagingOverrides::none(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_row_delta_on_ref_with_partitions(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    pairs: Vec<crate::write::position_delete::PositionDeletePair>,
    data_files: Vec<DataFile>,
    concurrency: WriteConcurrency,
    conflict_filter: &Predicate,
    branch: Option<&str>,
    known_partitions: KnownPartitions,
    summary_extra: &[(String, String)],
    staging: &crate::write::write_options::WriterStagingOverrides,
) -> Result<()> {
    let isolation = resolve_merge_isolation(table)?;
    let scope = CommitScope::scoped(isolation, conflict_filter.clone());
    commit_row_delta_kind_on_ref(
        catalog,
        table,
        snapshot_id,
        pairs,
        data_files,
        concurrency,
        &scope.row_delta(RowDeltaKind::Merge),
        branch,
        known_partitions,
        summary_extra,
        staging,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn commit_row_delta_kind(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    pairs: Vec<crate::write::position_delete::PositionDeletePair>,
    data_files: Vec<DataFile>,
    concurrency: WriteConcurrency,
    policy: &RowDeltaPolicy,
) -> Result<()> {
    commit_row_delta_kind_on_ref(
        catalog,
        table,
        snapshot_id,
        pairs,
        data_files,
        concurrency,
        policy,
        None,
        KnownPartitions::new(),
        &[],
        &crate::write::write_options::WriterStagingOverrides::none(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn commit_row_delta_kind_with_partitions(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    pairs: Vec<crate::write::position_delete::PositionDeletePair>,
    data_files: Vec<DataFile>,
    concurrency: WriteConcurrency,
    policy: &RowDeltaPolicy,
    known_partitions: KnownPartitions,
    summary_extra: &[(String, String)],
    staging: &crate::write::write_options::WriterStagingOverrides,
) -> Result<()> {
    commit_row_delta_kind_on_ref(
        catalog,
        table,
        snapshot_id,
        pairs,
        data_files,
        concurrency,
        policy,
        None,
        known_partitions,
        summary_extra,
        staging,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_row_delta_kind_on_ref(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    pairs: Vec<crate::write::position_delete::PositionDeletePair>,
    data_files: Vec<DataFile>,
    concurrency: WriteConcurrency,
    policy: &RowDeltaPolicy,
    branch: Option<&str>,
    known_partitions: KnownPartitions,
    summary_extra: &[(String, String)],
    staging: &crate::write::write_options::WriterStagingOverrides,
) -> Result<()> {
    if pairs.is_empty() && data_files.is_empty() {
        return Ok(());
    }
    crate::write::refuse_encrypted_table(table)?;
    let data_file_paths = written_file_paths(&data_files);
    let stamp = SiteStamp::claim_isolated(table, branch, summary_extra, &policy.scope)?;
    let pair_count = pairs.len() as u64;
    let data_file_count = data_files.len() as u64;
    let mut prepared = dv_close::prepare_row_delta_deletes(
        table,
        &pairs,
        concurrency,
        known_partitions,
        snapshot_id,
        staging,
    )
    .instrument(tracing::info_span!(
        "merge.write_deletes",
        pairs = pair_count
    ))
    .await?;
    let referenced = std::mem::take(&mut prepared.referenced);
    let delete_file_paths = std::mem::take(&mut prepared.abort_paths);
    let delete_file_count = delete_file_paths.len() as u64;
    let arm_deleted_on_delete = prepared.arm_validate_deleted_files_on_delete;

    let (added_deletes, removed_deletes) = prepared.delete_file_changes();
    let mut added_files = data_files.clone();
    added_files.extend(added_deletes.iter().cloned());
    let summary_extra = stamp.extras(summary_extra)?;
    let engine = crate::write::summary_collision::EngineSummary::for_changes(
        table,
        &added_files,
        removed_deletes,
        branch,
    );
    let (operation_id, summary) =
        crate::write::write_options::summary_with_extras(&summary_extra, &engine)?;
    let tx = Transaction::new(table);
    let mut action = tx.row_delta().add_data_files(data_files);
    action = prepared.apply(action);
    action = action
        .conflict_detection_filter(policy.scope.conflict_filter.clone())
        .validate_data_files_exist(referenced)
        .case_sensitive(true)
        .set_snapshot_properties(summary);
    if matches!(policy.kind, RowDeltaKind::Merge) {
        action = action
            .validate_deleted_files()
            .validate_no_conflicting_delete_files();
    } else if arm_deleted_on_delete {
        action = action.validate_deleted_files();
    }
    if policy.scope.isolation == IsolationLevel::Serializable {
        action = action.validate_no_conflicting_data_files();
    }
    if let Some(pin) = snapshot_id {
        action = action.validate_from_snapshot(pin);
    }
    let action = crate::write::commit_target::maybe_to_branch(action, branch, |action, name| {
        action.to_branch(name)
    });
    let tx = stamp.transaction(action.apply(tx).map_err(iceberg_err)?)?;
    match tx
        .commit(catalog.as_ref())
        .instrument(tracing::info_span!(
            "merge.commit",
            data_files = data_file_count,
            delete_files = delete_file_count
        ))
        .await
    {
        Ok(committed) => stamp.record(&committed),
        Err(error) => {
            let mut abort_paths = data_file_paths;
            abort_paths.extend(delete_file_paths);
            let error = sweep_after_rejected_commit(table, &abort_paths, error).await;
            Err(commit_err(error, &operation_id))
        }
    }
}

#[cfg(test)]
mod keyed_sweep_pins {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use iceberg::transaction::Transaction;
    use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
    use tempfile::TempDir;

    use super::*;
    use crate::write::encryption::{ENCRYPTION_KEY_ID_PROPERTY, EncryptedTableRefusal};

    const KEY_VALUE: &str = "merge-sweep-probe-key";

    fn ident() -> TableIdent {
        TableIdent::new(NamespaceIdent::new("sweep".to_string()), "t".to_string())
    }

    fn expected_text() -> String {
        crate::write::encryption::refusal_text("sweep.t")
    }

    async fn v2_table(warehouse: &TempDir) -> Arc<dyn Catalog> {
        let catalog =
            crate::memory_catalog(warehouse.path().to_str().expect("warehouse path is utf8"))
                .await
                .expect("memory catalog builds");
        catalog
            .create_namespace(ident().namespace(), HashMap::new())
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
        let created = catalog
            .load_table(&ident())
            .await
            .expect("created handle loads");
        let upgraded = Transaction::new(&created)
            .upgrade_table_version()
            .set_format_version(iceberg::spec::FormatVersion::V2)
            .apply(Transaction::new(&created))
            .expect("version action applies");
        upgraded
            .commit(catalog.as_ref())
            .await
            .expect("version commits");
        catalog
    }

    async fn add_key(catalog: &Arc<dyn Catalog>) {
        let fresh = catalog
            .load_table(&ident())
            .await
            .expect("fresh handle loads");
        let base = Transaction::new(&fresh);
        let action = base.update_table_properties().set(
            ENCRYPTION_KEY_ID_PROPERTY.to_string(),
            KEY_VALUE.to_string(),
        );
        let keyed = action.apply(base).expect("key action applies");
        keyed.commit(catalog.as_ref()).await.expect("key commits");
    }

    async fn keyed_fixture(warehouse: &TempDir) -> (Arc<dyn Catalog>, Table) {
        let catalog = v2_table(warehouse).await;
        let stale = catalog
            .load_table(&ident())
            .await
            .expect("stale handle loads");
        add_key(&catalog).await;
        (catalog, stale)
    }

    async fn keyed_fixture_with_referenced_file(
        warehouse: &TempDir,
    ) -> (Arc<dyn Catalog>, Table, Vec<DataFile>) {
        let catalog = v2_table(warehouse).await;
        let created = catalog
            .load_table(&ident())
            .await
            .expect("created handle loads");
        let referenced = stage_one_batch(&created).await;
        crate::write::commit_append(&catalog, &created, referenced.clone())
            .await
            .expect("the referenced file commits");
        let stale = catalog
            .load_table(&ident())
            .await
            .expect("stale handle loads");
        add_key(&catalog).await;
        (catalog, stale, referenced)
    }

    fn local_path(file: &DataFile) -> String {
        let path = file.file_path();
        path.strip_prefix("file://").unwrap_or(path).to_string()
    }

    fn parquet_rows(file: &DataFile) -> usize {
        let opened = std::fs::File::open(local_path(file)).expect("the referenced file opens");
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(opened)
            .expect("the referenced file is parquet")
            .build()
            .expect("the referenced reader builds")
            .map(|batch| batch.expect("a referenced batch reads").num_rows())
            .sum()
    }

    fn assert_referenced_kept_and_fresh_swept(
        error: &DataFusionError,
        referenced: &[DataFile],
        fresh: &[DataFile],
    ) {
        assert!(EncryptedTableRefusal::find(error).is_some(), "{error}");
        assert!(error.to_string().contains(expected_text().as_str()));
        for file in referenced {
            assert_eq!(parquet_rows(file), 2, "referenced {}", file.file_path());
        }
        for file in fresh {
            assert!(
                !std::path::Path::new(&local_path(file)).exists(),
                "fresh {} was left behind",
                file.file_path()
            );
        }
    }

    async fn stage_one_batch(table: &Table) -> Vec<DataFile> {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2])) as _])
            .expect("batch builds");
        crate::write::merge::write_data_files(table, vec![batch])
            .await
            .expect("staging writes")
    }

    fn staged_directory(staged: &[DataFile]) -> std::path::PathBuf {
        let first = staged.first().expect("at least one staged file");
        let path = first.file_path();
        let root = path.strip_prefix("file://").unwrap_or(path);
        std::path::Path::new(root)
            .parent()
            .expect("a staged file has a parent directory")
            .to_path_buf()
    }

    fn directory_names(directory: &std::path::Path) -> BTreeSet<String> {
        std::fs::read_dir(directory)
            .expect("the staged directory lists")
            .flatten()
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .collect()
    }

    #[tokio::test]
    async fn keyed_table_refused_merge_overwrite_sweeps_its_staged_file() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let (catalog, stale) = keyed_fixture(&warehouse).await;
        let staged = stage_one_batch(&stale).await;
        assert!(!staged.is_empty());
        let directory = staged_directory(&staged);
        let before = directory_names(&directory);
        assert_eq!(before.len(), staged.len());
        let scope = CommitScope::scoped(IsolationLevel::Serializable, Predicate::AlwaysTrue);
        let error = commit_overwrite(&catalog, &stale, None, Vec::new(), staged, &scope, &[])
            .await
            .expect_err("the keyed merge overwrite refuses");
        assert!(EncryptedTableRefusal::find(&error).is_some());
        assert!(error.to_string().contains(expected_text().as_str()));
        assert!(directory_names(&directory).is_empty());
    }

    #[tokio::test]
    async fn keyed_table_refused_row_delta_sweeps_its_staged_file() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let (catalog, stale) = keyed_fixture(&warehouse).await;
        let staged = stage_one_batch(&stale).await;
        assert!(!staged.is_empty());
        let directory = staged_directory(&staged);
        let before = directory_names(&directory);
        assert_eq!(before.len(), staged.len());
        let scope = CommitScope::scoped(IsolationLevel::Serializable, Predicate::AlwaysTrue);
        let policy = scope.row_delta(RowDeltaKind::Merge);
        let error = commit_row_delta_kind(
            &catalog,
            &stale,
            None,
            Vec::new(),
            staged,
            WriteConcurrency::default(),
            &policy,
        )
        .await
        .expect_err("the keyed row delta refuses");
        assert!(EncryptedTableRefusal::find(&error).is_some());
        assert!(error.to_string().contains(expected_text().as_str()));
        assert!(directory_names(&directory).is_empty());
    }

    #[tokio::test]
    async fn keyed_refused_merge_overwrite_keeps_a_snapshot_referenced_file() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let (catalog, stale, referenced) = keyed_fixture_with_referenced_file(&warehouse).await;
        let fresh = stage_one_batch(&stale).await;
        let mut new_files = referenced.clone();
        new_files.extend(fresh.clone());
        let scope = CommitScope::scoped(IsolationLevel::Serializable, Predicate::AlwaysTrue);
        let error = commit_overwrite(&catalog, &stale, None, Vec::new(), new_files, &scope, &[])
            .await
            .expect_err("the keyed merge overwrite refuses");
        assert_referenced_kept_and_fresh_swept(&error, &referenced, &fresh);
    }

    #[tokio::test]
    async fn keyed_refused_row_delta_keeps_a_snapshot_referenced_file() {
        let warehouse = TempDir::new().expect("temp warehouse");
        let (catalog, stale, referenced) = keyed_fixture_with_referenced_file(&warehouse).await;
        let fresh = stage_one_batch(&stale).await;
        let mut new_files = referenced.clone();
        new_files.extend(fresh.clone());
        let scope = CommitScope::scoped(IsolationLevel::Serializable, Predicate::AlwaysTrue);
        let policy = scope.row_delta(RowDeltaKind::Merge);
        let error = commit_row_delta_kind(
            &catalog,
            &stale,
            None,
            Vec::new(),
            new_files,
            WriteConcurrency::default(),
            &policy,
        )
        .await
        .expect_err("the keyed row delta refuses");
        assert_referenced_kept_and_fresh_swept(&error, &referenced, &fresh);
    }
}
