use std::collections::{HashMap, HashSet};

use datafusion::error::{DataFusionError, Result};
use iceberg::delete_vector_container::{
    DvContainerClose, close_touched_dv_containers_with_partitions,
};
use iceberg::spec::{DataFile, FormatVersion, ManifestList};
use iceberg::table::Table;
use iceberg::transaction::RowDeltaAction;

use super::KnownPartitions;
use super::iceberg_err;
use crate::write::concurrency::WriteConcurrency;
use crate::write::position_delete::PositionDeletePair;

pub(super) struct PreparedDeletes {
    pub referenced: HashSet<String>,
    pub arm_validate_deleted_files_on_delete: bool,
    pub(super) kind: PreparedKind,
}

pub(super) enum PreparedKind {
    PositionDeletes(Vec<DataFile>),
    DeletionVectors(DvContainerClose),
}

pub(super) async fn prepare_row_delta_deletes(
    table: &Table,
    pairs: &[PositionDeletePair],
    concurrency: WriteConcurrency,
    known_partitions: KnownPartitions,
    snapshot_id: Option<i64>,
    staging: &crate::write::write_options::WriterStagingOverrides,
) -> Result<PreparedDeletes> {
    match table.metadata().format_version() {
        FormatVersion::V2 => {
            let mut referenced: HashSet<String> = HashSet::new();
            for (path, _) in pairs {
                if !referenced.contains(path.as_ref()) {
                    referenced.insert(path.as_ref().to_string());
                }
            }
            let delete_files = crate::write::position_delete::write_position_deletes(
                table,
                pairs,
                concurrency,
                staging,
            )
            .await?;
            Ok(PreparedDeletes {
                referenced,
                arm_validate_deleted_files_on_delete: false,
                kind: PreparedKind::PositionDeletes(delete_files),
            })
        }
        FormatVersion::V3 => {
            if pairs.is_empty() {
                return Ok(PreparedDeletes {
                    referenced: HashSet::new(),
                    arm_validate_deleted_files_on_delete: true,
                    kind: PreparedKind::PositionDeletes(Vec::new()),
                });
            }
            let plan = plan_deletion_vectors(table, pairs, known_partitions, snapshot_id).await?;
            Ok(PreparedDeletes {
                referenced: plan.referenced,
                arm_validate_deleted_files_on_delete: true,
                kind: PreparedKind::DeletionVectors(plan.close),
            })
        }
        FormatVersion::V1 => Err(DataFusionError::NotImplemented(
            "merge-on-read RowDelta for format version V1 is not implemented".to_string(),
        )),
    }
}

impl PreparedDeletes {
    pub(super) fn delete_file_changes(&self) -> (&[DataFile], &[DataFile]) {
        match &self.kind {
            PreparedKind::PositionDeletes(files) => (files.as_slice(), &[]),
            PreparedKind::DeletionVectors(close) => {
                (close.added.as_slice(), close.removed.as_slice())
            }
        }
    }

    pub(super) fn into_row_delta(self, action: RowDeltaAction) -> (RowDeltaAction, Vec<DataFile>) {
        match self.kind {
            PreparedKind::PositionDeletes(files) => (action, files),
            PreparedKind::DeletionVectors(close) if close.removed.is_empty() => {
                (action, close.added)
            }
            PreparedKind::DeletionVectors(close) => {
                (action.remove_deletes_many(close.removed), close.added)
            }
        }
    }
}

struct DvCommitPlan {
    referenced: HashSet<String>,
    close: DvContainerClose,
}

async fn plan_deletion_vectors(
    table: &Table,
    pairs: &[PositionDeletePair],
    mut known_partitions: KnownPartitions,
    snapshot_id: Option<i64>,
) -> Result<DvCommitPlan> {
    let mut new_positions: HashMap<String, Vec<u64>> = HashMap::new();
    for (path, position) in pairs {
        let position = u64::try_from(*position).map_err(|_| {
            DataFusionError::Internal(format!(
                "deletion-vector: negative row position {position} for data file `{path}`"
            ))
        })?;
        match new_positions.get_mut(path.as_ref()) {
            Some(slot) => slot.push(position),
            None => {
                new_positions.insert(path.as_ref().to_string(), vec![position]);
            }
        }
    }
    known_partitions.retain(|path, _| new_positions.contains_key(path));
    let manifest_list = scanned_manifest_list(table, snapshot_id).await?;
    let close = close_touched_dv_containers_with_partitions(
        table,
        &new_positions,
        snapshot_id,
        &known_partitions,
        manifest_list.as_ref(),
    )
    .await
    .map_err(iceberg_err)?;
    Ok(DvCommitPlan {
        referenced: close.referenced_data_files(),
        close,
    })
}

async fn scanned_manifest_list(
    table: &Table,
    snapshot_id: Option<i64>,
) -> Result<Option<ManifestList>> {
    let metadata = table.metadata();
    let snapshot = match snapshot_id {
        Some(id) => metadata.snapshot_by_id(id),
        None => metadata.current_snapshot(),
    };
    let Some(snapshot) = snapshot else {
        return Ok(None);
    };
    snapshot
        .load_manifest_list(table.file_io(), metadata)
        .await
        .map(Some)
        .map_err(iceberg_err)
}
