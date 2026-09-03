use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use iceberg::Catalog;
use iceberg::expr::Predicate;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use tracing::Instrument;
use uuid::Uuid;

use super::KnownPartitions;
use super::OPERATION_ID_PROP;
use super::abort;
use super::dv_close;
use super::iceberg_err;
use crate::write::concurrency::WriteConcurrency;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowDeltaPolicy {
    pub kind: RowDeltaKind,
    pub isolation: IsolationLevel,
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
    commit_on_ref(catalog, table, snapshot_id, affected, new_files, None).await
}

pub(crate) async fn commit_on_ref(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    affected: Vec<DataFile>,
    new_files: Vec<DataFile>,
    branch: Option<&str>,
) -> Result<()> {
    let isolation = resolve_merge_isolation(table)?;
    commit_overwrite_on_ref(
        catalog,
        table,
        snapshot_id,
        affected,
        new_files,
        isolation,
        branch,
    )
    .await
}

pub(crate) async fn commit_overwrite(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    affected: Vec<DataFile>,
    new_files: Vec<DataFile>,
    isolation: IsolationLevel,
) -> Result<()> {
    commit_overwrite_on_ref(
        catalog,
        table,
        snapshot_id,
        affected,
        new_files,
        isolation,
        None,
    )
    .await
}

pub(crate) async fn commit_overwrite_on_ref(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    affected: Vec<DataFile>,
    new_files: Vec<DataFile>,
    isolation: IsolationLevel,
    branch: Option<&str>,
) -> Result<()> {
    if affected.is_empty() && new_files.is_empty() {
        return Ok(());
    }
    let new_file_paths = abort::written_file_paths(&new_files);
    let summary = HashMap::from([(OPERATION_ID_PROP.to_string(), Uuid::new_v4().to_string())]);
    let tx = Transaction::new(table);
    let tx = if affected.is_empty() {
        let mut action = tx
            .overwrite_files()
            .add_files(new_files)
            .conflict_detection_filter(Predicate::AlwaysTrue)
            .case_sensitive(true)
            .set_snapshot_properties(summary);
        if isolation == IsolationLevel::Serializable {
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
            .conflict_detection_filter(Predicate::AlwaysTrue)
            .validate_no_conflicting_deletes()
            .case_sensitive(true)
            .set_snapshot_properties(summary);
        if isolation == IsolationLevel::Serializable {
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
    match tx.commit(catalog.as_ref()).await {
        Ok(_) => Ok(()),
        Err(error) => {
            abort::delete_written_files_best_effort(table, &new_file_paths, &error).await;
            Err(iceberg_err(error))
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
        RowDeltaPolicy {
            kind: RowDeltaKind::Merge,
            isolation,
        },
        branch,
        KnownPartitions::new(),
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
    branch: Option<&str>,
    known_partitions: KnownPartitions,
) -> Result<()> {
    let isolation = resolve_merge_isolation(table)?;
    commit_row_delta_kind_on_ref(
        catalog,
        table,
        snapshot_id,
        pairs,
        data_files,
        concurrency,
        RowDeltaPolicy {
            kind: RowDeltaKind::Merge,
            isolation,
        },
        branch,
        known_partitions,
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
    policy: RowDeltaPolicy,
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
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_row_delta_kind_with_partitions(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    pairs: Vec<crate::write::position_delete::PositionDeletePair>,
    data_files: Vec<DataFile>,
    concurrency: WriteConcurrency,
    policy: RowDeltaPolicy,
    known_partitions: KnownPartitions,
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
    policy: RowDeltaPolicy,
    branch: Option<&str>,
    known_partitions: KnownPartitions,
) -> Result<()> {
    if pairs.is_empty() && data_files.is_empty() {
        return Ok(());
    }
    let data_file_paths = abort::written_file_paths(&data_files);
    let pair_count = pairs.len() as u64;
    let data_file_count = data_files.len() as u64;
    let mut prepared = dv_close::prepare_row_delta_deletes(
        table,
        &pairs,
        concurrency,
        known_partitions,
        snapshot_id,
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

    let summary = HashMap::from([(OPERATION_ID_PROP.to_string(), Uuid::new_v4().to_string())]);
    let tx = Transaction::new(table);
    let mut action = tx.row_delta().add_data_files(data_files);
    action = prepared.apply(action);
    action = action
        .conflict_detection_filter(Predicate::AlwaysTrue)
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
    if policy.isolation == IsolationLevel::Serializable {
        action = action.validate_no_conflicting_data_files();
    }
    if let Some(pin) = snapshot_id {
        action = action.validate_from_snapshot(pin);
    }
    let action = crate::write::commit_target::maybe_to_branch(action, branch, |action, name| {
        action.to_branch(name)
    });
    let tx = action.apply(tx).map_err(iceberg_err)?;
    match tx
        .commit(catalog.as_ref())
        .instrument(tracing::info_span!(
            "merge.commit",
            data_files = data_file_count,
            delete_files = delete_file_count
        ))
        .await
    {
        Ok(_) => Ok(()),
        Err(error) => {
            let mut abort_paths = data_file_paths;
            abort_paths.extend(delete_file_paths);
            abort::delete_written_files_best_effort(table, &abort_paths, &error).await;
            Err(iceberg_err(error))
        }
    }
}
