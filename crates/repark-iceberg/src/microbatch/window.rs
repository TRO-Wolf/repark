use std::num::{NonZeroU64, NonZeroUsize};

use futures::TryStreamExt;
use iceberg::scan::FileScanTask;
use iceberg::spec::{DataContentType, ManifestContentType, ManifestStatus, Operation, SnapshotRef};
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
    #[cfg(test)]
    planned: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl WindowPlanner {
    #[must_use]
    pub fn new(table: Table, caps: ReadCaps) -> Self {
        Self {
            table,
            caps,
            #[cfg(test)]
            planned: std::sync::Arc::default(),
        }
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
                self.landing(oldest, head_id).await.map(Some)
            }
            StartPosition::FromTimestamp { millis } => {
                let chain = self.ancestry(head_id);
                match chain
                    .iter()
                    .rev()
                    .find(|snapshot| snapshot.timestamp_ms() >= millis)
                {
                    Some(landing) => self.landing(landing, head_id).await.map(Some),
                    None => Ok(None),
                }
            }
            StartPosition::AfterSnapshot(snapshot) => {
                let Some(named) = self.table.metadata().snapshot_by_id(snapshot.get()) else {
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
                };
                let count = self.added_file_count(named).await?;
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
        let Some(start) = chain
            .iter()
            .find(|snapshot| snapshot.snapshot_id() == from_id)
        else {
            let Some(oldest) = chain.last().map(|entry| entry.snapshot_id()) else {
                return Err(self.orphaned_head(head_id));
            };
            return Err(MicroBatchError::SourceSnapshotExpired {
                table: self.table_name(),
                snapshot: from.snapshot,
                oldest: SnapshotId::new(oldest),
            });
        };
        let mut window = Window::new(&self.caps, limit);
        self.enter_start(start, from, head_id, &mut window).await?;
        for snapshot in chain
            .iter()
            .rev()
            .skip_while(|snapshot| snapshot.snapshot_id() != from_id)
            .skip(1)
        {
            if window.full() {
                break;
            }
            let id = snapshot.snapshot_id();
            match snapshot.summary().operation {
                Operation::Append => {
                    let mut position = 0u64;
                    for task in self.added_tasks(id).await? {
                        if window.full() {
                            break;
                        }
                        window.push(self.planned_file(id, position, task)?);
                        position = position.saturating_add(1);
                    }
                }
                Operation::Replace => {}
                Operation::Overwrite | Operation::Delete => {
                    if window.files.is_empty() || limit == WindowLimit::Unbounded {
                        return Err(self.non_append(snapshot, Some(from.snapshot), head_id));
                    }
                    break;
                }
            }
        }
        Ok(window.finish(from))
    }

    async fn landing(
        &self,
        snapshot: &SnapshotRef,
        head: i64,
    ) -> Result<InputOffset, MicroBatchError> {
        if matches!(
            snapshot.summary().operation,
            Operation::Overwrite | Operation::Delete
        ) && self.added_file_count(snapshot).await? == 0
        {
            return Err(self.non_append(snapshot, None, head));
        }
        Ok(self.offset(snapshot.snapshot_id(), 0))
    }

    async fn enter_start(
        &self,
        start: &SnapshotRef,
        from: &InputOffset,
        head: i64,
        window: &mut Window<'_>,
    ) -> Result<(), MicroBatchError> {
        let id = start.snapshot_id();
        match start.summary().operation {
            Operation::Append => {
                let tasks = self.added_tasks(id).await?;
                self.check_position(from, self.count_of(id, tasks.len())?)?;
                let mut position = 0u64;
                for task in tasks {
                    if position >= from.position.get() {
                        if window.full() {
                            break;
                        }
                        window.push(self.planned_file(id, position, task)?);
                    }
                    position = position.saturating_add(1);
                }
                Ok(())
            }
            Operation::Replace => {
                let count = self.manifest_added_count(start).await?;
                self.check_position(from, count)
            }
            Operation::Overwrite | Operation::Delete => {
                let count = self.manifest_added_count(start).await?;
                self.check_position(from, count)?;
                if from.position.get() < count {
                    return Err(self.non_append(start, Some(from.snapshot), head));
                }
                Ok(())
            }
        }
    }

    fn check_position(&self, from: &InputOffset, files: u64) -> Result<(), MicroBatchError> {
        if from.position.get() > files {
            return Err(MicroBatchError::OffsetPositionOutOfRange {
                table: self.table_name(),
                snapshot: from.snapshot,
                position: from.position,
                files,
            });
        }
        Ok(())
    }

    fn non_append(
        &self,
        snapshot: &SnapshotRef,
        from: Option<SnapshotId>,
        head: i64,
    ) -> MicroBatchError {
        MicroBatchError::NonAppendSnapshot {
            table: self.table_name(),
            snapshot: SnapshotId::new(snapshot.snapshot_id()),
            operation: snapshot.summary().operation.clone(),
            from,
            to: SnapshotId::new(head),
        }
    }

    fn planned_file(
        &self,
        snapshot: i64,
        position: u64,
        task: FileScanTask,
    ) -> Result<PlannedFile, MicroBatchError> {
        let Some(record_count) = task.record_count else {
            return Err(MicroBatchError::Catalog(format!(
                "table {} snapshot {snapshot} file {} carries no record count; the window cannot be bounded",
                self.table_name(),
                task.data_file_path()
            )));
        };
        Ok(PlannedFile {
            snapshot: SnapshotId::new(snapshot),
            position: FilePosition::new(position),
            task,
            record_count,
        })
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

    fn count_of(&self, snapshot: i64, files: usize) -> Result<u64, MicroBatchError> {
        u64::try_from(files).map_err(|_| {
            MicroBatchError::Catalog(format!(
                "table {} snapshot {snapshot} lists more files than a position holds",
                self.table_name()
            ))
        })
    }

    async fn added_file_count(&self, snapshot: &SnapshotRef) -> Result<u64, MicroBatchError> {
        if matches!(snapshot.summary().operation, Operation::Append) {
            let id = snapshot.snapshot_id();
            let files = self.added_tasks(id).await?.len();
            return self.count_of(id, files);
        }
        self.manifest_added_count(snapshot).await
    }

    async fn added_tasks(&self, snapshot: i64) -> Result<Vec<FileScanTask>, MicroBatchError> {
        self.note_planned();
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

    async fn manifest_added_count(&self, snapshot: &SnapshotRef) -> Result<u64, MicroBatchError> {
        self.note_planned();
        let id = snapshot.snapshot_id();
        let io = self.table.file_io();
        let list = snapshot
            .load_manifest_list(io, self.table.metadata())
            .await
            .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
        let mut count = 0u64;
        for manifest in list.entries() {
            if manifest.content != ManifestContentType::Data || manifest.added_snapshot_id != id {
                continue;
            }
            let loaded = manifest
                .load_manifest(io)
                .await
                .map_err(|error| MicroBatchError::Catalog(error.to_string()))?;
            for entry in loaded.entries() {
                if entry.status() == ManifestStatus::Added
                    && entry.snapshot_id() == Some(id)
                    && entry.content_type() == DataContentType::Data
                {
                    count = count.saturating_add(1);
                }
            }
        }
        Ok(count)
    }

    #[cfg(test)]
    fn note_planned(&self) {
        self.planned
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(not(test))]
    #[allow(clippy::unused_self)]
    fn note_planned(&self) {}

    #[cfg(test)]
    fn planned_listings(&self) -> usize {
        self.planned.load(std::sync::atomic::Ordering::Relaxed)
    }
}

struct Window<'caps> {
    caps: &'caps ReadCaps,
    limit: WindowLimit,
    files: Vec<PlannedFile>,
    rows: u64,
}

impl<'caps> Window<'caps> {
    fn new(caps: &'caps ReadCaps, limit: WindowLimit) -> Self {
        Self {
            caps,
            limit,
            files: Vec::new(),
            rows: 0,
        }
    }

    fn full(&self) -> bool {
        if matches!(self.limit, WindowLimit::Unbounded) {
            return false;
        }
        let files_full = self
            .caps
            .max_files
            .is_some_and(|max| self.files.len() >= max.get());
        let rows_full = self.caps.max_rows.is_some_and(|max| self.rows >= max.get());
        files_full || rows_full
    }

    fn push(&mut self, file: PlannedFile) {
        self.rows = self.rows.saturating_add(file.record_count);
        self.files.push(file);
    }

    fn finish(self, from: &InputOffset) -> Option<WindowPlan> {
        let last = self.files.last()?;
        let end = InputOffset {
            table: from.table,
            table_name: from.table_name.clone(),
            snapshot: last.snapshot,
            position: FilePosition::new(last.position.get().saturating_add(1)),
        };
        Some(WindowPlan {
            start: from.clone(),
            end,
            files: self.files,
            num_input_rows: self.rows,
        })
    }
}

#[cfg(test)]
#[path = "window_tests.rs"]
mod tests;
