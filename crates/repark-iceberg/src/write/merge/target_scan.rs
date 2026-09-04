use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use futures::{StreamExt, TryStreamExt};
use iceberg::arrow::ArrowReaderBuilder;
use iceberg::expr::Predicate;
use iceberg::scan::FileScanTask;
use iceberg::spec::Struct;
use iceberg::table::Table;
use tracing::Instrument;

use super::{conform_scan_batch, iceberg_err};
use crate::write::file_scoped_rewrite::filter_tasks_to_allowlist_nonempty;

type PlannedFileTasks = Arc<futures::lock::Mutex<Option<Arc<Vec<FileScanTask>>>>>;

pub(crate) type KnownPartitions = HashMap<String, (i32, Struct)>;

pub(crate) type PartitionSink = Arc<Mutex<KnownPartitions>>;

#[must_use]
pub(crate) fn new_partition_sink() -> PartitionSink {
    Arc::new(Mutex::new(HashMap::new()))
}

#[must_use]
pub(crate) fn drain_partition_sink(sink: &PartitionSink) -> KnownPartitions {
    match sink.lock() {
        Ok(mut guard) => std::mem::take(&mut *guard),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

fn record_scanned_partitions(sink: &PartitionSink, tasks: &[FileScanTask]) {
    let Ok(mut guard) = sink.lock() else {
        return;
    };
    for task in tasks {
        let (Some(spec), Some(partition)) = (task.partition_spec.as_ref(), task.partition.as_ref())
        else {
            continue;
        };
        if guard.contains_key(task.data_file_path.as_ref()) {
            continue;
        }
        guard.insert(
            task.data_file_path.as_ref().to_string(),
            (spec.spec_id(), partition.clone()),
        );
    }
}

async fn planned_or_plan(
    cell: &PlannedFileTasks,
    table: &Table,
    snapshot_id: i64,
    select_columns: Vec<String>,
    filter: Option<Predicate>,
    concurrency_limit: Option<usize>,
    file_path_allowlist: Option<&std::collections::HashSet<String>>,
) -> Result<Arc<Vec<FileScanTask>>, DataFusionError> {
    let mut guard = cell.lock().await;
    if let Some(tasks) = guard.as_ref() {
        return Ok(Arc::clone(tasks));
    }
    note_plan_files_invocation();
    let planned = plan_file_scan_tasks(
        table,
        snapshot_id,
        select_columns,
        filter,
        concurrency_limit,
        file_path_allowlist,
    )
    .await?;
    let tasks = Arc::new(planned);
    *guard = Some(Arc::clone(&tasks));
    Ok(tasks)
}

async fn plan_file_scan_tasks(
    table: &Table,
    snapshot_id: i64,
    select_columns: Vec<String>,
    filter: Option<Predicate>,
    concurrency_limit: Option<usize>,
    file_path_allowlist: Option<&std::collections::HashSet<String>>,
) -> Result<Vec<FileScanTask>, DataFusionError> {
    let mut builder = table.scan().snapshot_id(snapshot_id).select(select_columns);
    if let Some(predicate) = filter {
        builder = builder.with_filter(predicate);
    }
    if let Some(limit) = concurrency_limit {
        builder = builder.with_concurrency_limit(limit);
    }
    let scan = builder.build().map_err(iceberg_err)?;
    let planned: Vec<_> = scan
        .plan_files()
        .await
        .map_err(iceberg_err)?
        .try_collect()
        .await
        .map_err(iceberg_err)?;
    match file_path_allowlist {
        Some(allowlist) => filter_tasks_to_allowlist_nonempty(planned, allowlist),
        None => Ok(planned),
    }
}

#[cfg(test)]
thread_local! {
    pub(crate) static PLAN_FILES_INVOCATIONS: std::cell::RefCell<
        Option<Arc<std::sync::atomic::AtomicUsize>>,
    > = const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn note_plan_files_invocation() {
    PLAN_FILES_INVOCATIONS.with(|slot| {
        if let Some(counter) = slot.borrow().as_ref() {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    });
}

#[cfg(not(test))]
fn note_plan_files_invocation() {}

/// Re-scannable partition stream over the pinned snapshot, replacing the full-target `MemTable`.
#[derive(Debug)]
pub(crate) struct TargetScanStream {
    table: Table,
    snapshot_id: Option<i64>,
    scratch_schema: SchemaRef,
    select_columns: Vec<String>,
    /// Residual Iceberg predicate (join-key min/max bounds) — `None` = unfiltered scan.
    filter: Option<Predicate>,
    /// When set, passed to `TableScanBuilder::with_concurrency_limit`.
    concurrency_limit: Option<usize>,
    /// R-MERGE-FILE-SCAN: when set, only open data files whose path is in this set.
    file_path_allowlist: Option<std::sync::Arc<std::collections::HashSet<String>>>,
    partition_sink: Option<PartitionSink>,
    planned_file_tasks: PlannedFileTasks,
    /// Span current at CONSTRUCTION (inside the merge's instrumented body).
    trace_parent: tracing::Span,
}

impl TargetScanStream {
    /// The select list is the target's data columns followed by the two identity metadata columns.
    pub(crate) fn new(
        table: Table,
        snapshot_id: Option<i64>,
        scratch_schema: SchemaRef,
        _write_schema: &SchemaRef,
        filter: Option<Predicate>,
        concurrency_limit: Option<usize>,
        file_path_allowlist: Option<std::sync::Arc<std::collections::HashSet<String>>>,
    ) -> Self {
        let select_columns: Vec<String> = scratch_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        Self {
            table,
            snapshot_id,
            scratch_schema,
            select_columns,
            filter,
            concurrency_limit,
            file_path_allowlist,
            partition_sink: None,
            planned_file_tasks: Arc::new(futures::lock::Mutex::new(None)),
            trace_parent: tracing::Span::current(),
        }
    }

    #[must_use]
    pub(crate) fn with_partition_sink(mut self, sink: PartitionSink) -> Self {
        self.partition_sink = Some(sink);
        self
    }
}

impl PartitionStream for TargetScanStream {
    fn schema(&self) -> &SchemaRef {
        &self.scratch_schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let scratch = Arc::clone(&self.scratch_schema);
        let Some(pin) = self.snapshot_id else {
            // No snapshot yet ⇒ an empty target (no rows to scan, no file to open).
            return Box::pin(RecordBatchStreamAdapter::new(
                scratch,
                futures::stream::empty(),
            ));
        };
        let table = self.table.clone();
        let select_columns = self.select_columns.clone();
        let map_schema = Arc::clone(&scratch);
        let filter = self.filter.clone();
        let concurrency_limit = self.concurrency_limit;
        let file_path_allowlist = self.file_path_allowlist.clone();
        let partition_sink = self.partition_sink.clone();
        let planned_file_tasks = Arc::clone(&self.planned_file_tasks);
        let trace_parent = self.trace_parent.clone();
        // Open the pinned scan lazily and conform each arriving batch onto the scratch schema.
        let opened =
            async move {
                let arrow = if file_path_allowlist.is_some() || partition_sink.is_some() {
                    let tasks = planned_or_plan(
                        &planned_file_tasks,
                        &table,
                        pin,
                        select_columns,
                        filter,
                        concurrency_limit,
                        file_path_allowlist.as_deref(),
                    )
                    .await?;
                    if let Some(sink) = partition_sink.as_ref() {
                        record_scanned_partitions(sink, tasks.as_ref());
                    }
                    let task_stream: iceberg::scan::FileScanTaskStream = Box::pin(
                        futures::stream::iter(tasks.as_ref().clone().into_iter().map(Ok)),
                    );
                    let mut reader = ArrowReaderBuilder::new(table.file_io().clone());
                    if let Some(limit) = concurrency_limit {
                        reader = reader.with_data_file_concurrency_limit(limit);
                    }
                    reader.build().read(task_stream).map_err(iceberg_err)?
                } else {
                    let mut builder = table.scan().snapshot_id(pin).select(select_columns);
                    if let Some(predicate) = filter {
                        builder = builder.with_filter(predicate);
                    }
                    if let Some(limit) = concurrency_limit {
                        builder = builder.with_concurrency_limit(limit);
                    }
                    builder
                        .build()
                        .map_err(iceberg_err)?
                        .to_arrow()
                        .await
                        .map_err(iceberg_err)?
                };
                Ok::<_, DataFusionError>(arrow.map(move |batch| {
                    conform_scan_batch(&map_schema, &batch.map_err(iceberg_err)?)
                }))
            }
            .instrument(tracing::info_span!(
                parent: trace_parent.id(),
                "merge.target_scan"
            ));
        let stream = futures::stream::once(opened).try_flatten();
        Box::pin(RecordBatchStreamAdapter::new(scratch, stream))
    }
}
