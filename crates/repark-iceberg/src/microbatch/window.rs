use std::num::{NonZeroU64, NonZeroUsize};

use futures::TryStreamExt;
use iceberg::scan::FileScanTask;
use iceberg::spec::{Operation, SnapshotRef};
use iceberg::table::Table;

use crate::microbatch::error::MicroBatchError;
use crate::microbatch::offset::{FilePosition, InputOffset, SnapshotId, TableUuid};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReadCaps {
    pub max_files: Option<NonZeroUsize>,
    pub max_rows: Option<NonZeroU64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartPosition {
    Earliest,
    FromTimestamp { millis: i64 },
    AfterSnapshot(SnapshotId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowLimit {
    Capped,
    Unbounded,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlannedFile {
    pub snapshot: SnapshotId,
    pub position: FilePosition,
    pub task: FileScanTask,
    pub record_count: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowPlan {
    pub start: InputOffset,
    pub end: InputOffset,
    pub files: Vec<PlannedFile>,
    pub num_input_rows: u64,
}

#[derive(Debug, Clone)]
pub struct WindowPlanner {
    table: Table,
    caps: ReadCaps,
}

impl WindowPlanner {
    #[must_use]
    pub fn new(table: Table, caps: ReadCaps) -> Self {
        Self { table, caps }
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn initial_offset(
        &self,
        start: &StartPosition,
    ) -> Result<Option<InputOffset>, MicroBatchError> {
        let Some(head) = self.table.metadata().current_snapshot() else {
            return Ok(None);
        };
        let head_id = head.snapshot_id();
        match *start {
            StartPosition::Earliest => {
                let chain = self.ancestry(head_id);
                let Some(oldest) = chain.last() else {
                    return Err(self.orphaned_head(head_id));
                };
                if let Some(missing) = oldest.parent_snapshot_id()
                    && self.table.metadata().snapshot_by_id(missing).is_none()
                {
                    return Err(MicroBatchError::TruncatedHistory {
                        table: self.table_name(),
                        oldest: SnapshotId::new(oldest.snapshot_id()),
                        missing_parent: SnapshotId::new(missing),
                    });
                }
                Ok(Some(self.offset(oldest.snapshot_id(), 0)))
            }
            StartPosition::FromTimestamp { millis } => {
                for snapshot in self.ancestry(head_id).iter().rev() {
                    if snapshot.timestamp_ms() >= millis {
                        return Ok(Some(self.offset(snapshot.snapshot_id(), 0)));
                    }
                }
                let count = self.added_file_count(head_id).await?;
                Ok(Some(self.offset(head_id, count)))
            }
            StartPosition::AfterSnapshot(snapshot) => {
                if self
                    .table
                    .metadata()
                    .snapshot_by_id(snapshot.get())
                    .is_none()
                {
                    let Some(oldest) = self
                        .ancestry(head_id)
                        .last()
                        .map(|entry| entry.snapshot_id())
                    else {
                        return Err(self.orphaned_head(head_id));
                    };
                    return Err(MicroBatchError::SourceSnapshotExpired {
                        table: self.table_name(),
                        snapshot,
                        oldest: SnapshotId::new(oldest),
                    });
                }
                let count = self.added_file_count(snapshot.get()).await?;
                Ok(Some(self.offset(snapshot.get(), count)))
            }
        }
    }

    #[allow(clippy::missing_errors_doc)]
    pub async fn next_window(
        &self,
        from: &InputOffset,
        limit: WindowLimit,
    ) -> Result<Option<WindowPlan>, MicroBatchError> {
        let current = TableUuid::of(&self.table);
        if current != from.table {
            return Err(MicroBatchError::SourceReplaced {
                table: self.table_name(),
                recorded: from.table,
                current,
            });
        }
        let Some(head) = self.table.metadata().current_snapshot() else {
            return Err(MicroBatchError::Catalog(format!(
                "table {} holds no snapshots; cannot resume from snapshot {}",
                self.table_name(),
                from.snapshot.get()
            )));
        };
        let head_id = head.snapshot_id();
        let from_id = from.snapshot.get();
        let chain = self.ancestry(head_id);
        if !chain
            .iter()
            .any(|snapshot| snapshot.snapshot_id() == from_id)
        {
            let Some(oldest) = chain.last().map(|entry| entry.snapshot_id()) else {
                return Err(self.orphaned_head(head_id));
            };
            return Err(MicroBatchError::SourceSnapshotExpired {
                table: self.table_name(),
                snapshot: from.snapshot,
                oldest: SnapshotId::new(oldest),
            });
        }
        let mut candidates = remainder_files(from, self.added_files(from_id).await?);
        if from_id != head_id {
            self.fail_loud(from_id, head_id).await?;
            for snapshot in self.window_snapshots(from_id, head_id) {
                let id = snapshot.snapshot_id();
                match snapshot.summary().operation {
                    Operation::Append => {
                        let mut position = 0;
                        for task in self.added_files(id).await? {
                            let record_count = task.record_count.unwrap_or(0);
                            candidates.push(PlannedFile {
                                snapshot: SnapshotId::new(id),
                                position: FilePosition::new(position),
                                task,
                                record_count,
                            });
                            position = position.saturating_add(1);
                        }
                    }
                    Operation::Replace => {}
                    Operation::Overwrite | Operation::Delete => {
                        return Err(MicroBatchError::Catalog(format!(
                            "table {} snapshot {id} ({}) reached planning after the fail-loud check passed",
                            self.table_name(),
                            snapshot.summary().operation.as_str()
                        )));
                    }
                }
            }
        }
        Ok(select_window(from, candidates, &self.caps, limit))
    }

    fn table_name(&self) -> String {
        self.table.identifier().to_string()
    }

    fn offset(&self, snapshot: i64, position: u64) -> InputOffset {
        InputOffset {
            table: TableUuid::of(&self.table),
            table_name: self.table_name(),
            snapshot: SnapshotId::new(snapshot),
            position: FilePosition::new(position),
        }
    }

    fn orphaned_head(&self, head: i64) -> MicroBatchError {
        MicroBatchError::Catalog(format!(
            "table {} head snapshot {head} is missing from its own metadata",
            self.table_name()
        ))
    }

    fn ancestry(&self, head: i64) -> Vec<SnapshotRef> {
        let metadata = self.table.metadata();
        let mut chain = Vec::new();
        let mut current = metadata.snapshot_by_id(head);
        while let Some(snapshot) = current {
            chain.push(snapshot.clone());
            current = snapshot
                .parent_snapshot_id()
                .and_then(|parent| metadata.snapshot_by_id(parent));
        }
        chain
    }

    fn window_snapshots(&self, from: i64, head: i64) -> Vec<SnapshotRef> {
        let mut window = Vec::new();
        let mut past_from = false;
        for snapshot in self.ancestry(head).iter().rev() {
            if past_from {
                window.push(snapshot.clone());
            } else if snapshot.snapshot_id() == from {
                past_from = true;
            }
        }
        window
    }

    async fn added_files(&self, snapshot: i64) -> Result<Vec<FileScanTask>, MicroBatchError> {
        let parent = self
            .table
            .metadata()
            .snapshot_by_id(snapshot)
            .and_then(|entry| entry.parent_snapshot_id());
        let mut scan = self
            .table
            .incremental_append_scan()
            .to_snapshot_id(snapshot);
        if let Some(from) = parent {
            scan = scan.from_snapshot_id_exclusive(from);
        }
        let planned = scan
            .build()
            .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
        let stream = planned
            .plan_files()
            .await
            .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
        let mut tasks: Vec<FileScanTask> = stream
            .try_collect()
            .await
            .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
        tasks.sort_by(|left, right| left.data_file_path().cmp(right.data_file_path()));
        Ok(tasks)
    }

    async fn added_file_count(&self, snapshot: i64) -> Result<u64, MicroBatchError> {
        let files = self.added_files(snapshot).await?;
        u64::try_from(files.len()).map_err(|_| {
            MicroBatchError::Catalog(format!(
                "table {} snapshot {snapshot} lists more files than a position holds",
                self.table_name()
            ))
        })
    }

    async fn fail_loud(&self, from: i64, head: i64) -> Result<(), MicroBatchError> {
        let planned = match self
            .table
            .incremental_append_scan()
            .from_snapshot_id_exclusive(from)
            .to_snapshot_id(head)
            .with_fail_on_non_append(true)
            .build()
        {
            Ok(planned) => planned,
            Err(error) => return Err(MicroBatchError::Catalog(error.to_string())),
        };
        match planned.plan_files().await {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == iceberg::ErrorKind::PreconditionFailed => {
                Err(self.refused_snapshot(from, head, &error))
            }
            Err(error) => Err(MicroBatchError::Catalog(error.to_string())),
        }
    }

    fn refused_snapshot(&self, from: i64, head: i64, error: &iceberg::Error) -> MicroBatchError {
        for snapshot in self.window_snapshots(from, head) {
            let operation = snapshot.summary().operation.clone();
            if matches!(operation, Operation::Overwrite | Operation::Delete) {
                return MicroBatchError::NonAppendSnapshot {
                    table: self.table_name(),
                    snapshot: SnapshotId::new(snapshot.snapshot_id()),
                    operation,
                    from: Some(SnapshotId::new(from)),
                    to: SnapshotId::new(head),
                };
            }
        }
        MicroBatchError::Catalog(format!(
            "table {} window ({from}, {head}] refused: {error}",
            self.table_name(),
        ))
    }
}

fn remainder_files(from: &InputOffset, tasks: Vec<FileScanTask>) -> Vec<PlannedFile> {
    let skip = usize::try_from(from.position.get()).unwrap_or(usize::MAX);
    let mut position = from.position.get();
    let mut files = Vec::new();
    for task in tasks.into_iter().skip(skip) {
        let record_count = task.record_count.unwrap_or(0);
        files.push(PlannedFile {
            snapshot: from.snapshot,
            position: FilePosition::new(position),
            task,
            record_count,
        });
        position = position.saturating_add(1);
    }
    files
}

fn select_window(
    from: &InputOffset,
    candidates: Vec<PlannedFile>,
    caps: &ReadCaps,
    limit: WindowLimit,
) -> Option<WindowPlan> {
    let mut files = Vec::new();
    let mut rows = 0u64;
    for candidate in candidates {
        if matches!(limit, WindowLimit::Capped) && !files.is_empty() {
            let files_full = caps.max_files.is_some_and(|max| files.len() >= max.get());
            let rows_full = caps
                .max_rows
                .is_some_and(|max| rows.saturating_add(candidate.record_count) > max.get());
            if files_full || rows_full {
                break;
            }
        }
        rows = rows.saturating_add(candidate.record_count);
        files.push(candidate);
    }
    let last = files.last()?;
    let end = InputOffset {
        table: from.table,
        table_name: from.table_name.clone(),
        snapshot: last.snapshot,
        position: FilePosition::new(last.position.get().saturating_add(1)),
    };
    Some(WindowPlan {
        start: from.clone(),
        end,
        files,
        num_input_rows: rows,
    })
}

#[cfg(test)]
#[path = "window_tests.rs"]
mod tests;
