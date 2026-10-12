//! `MERGE INTO` executor with copy-on-write and merge-on-read arms.

use std::borrow::Cow;
#[cfg(test)]
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(test)]
use datafusion::arrow::array::StringArray;
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
#[cfg(test)]
pub(super) use datafusion::arrow::datatypes::Field;
use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::error::{DataFusionError, Result};
#[cfg(test)]
use datafusion::execution::TaskContext;
#[cfg(test)]
use datafusion::physical_plan::SendableRecordBatchStream;
#[cfg(test)]
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::SessionContext;
use futures::channel::mpsc;
use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use iceberg::expr::Predicate;
#[cfg(test)]
use iceberg::spec::ManifestContentType;
use iceberg::spec::{DataFile, FormatVersion};
use iceberg::table::Table;

use iceberg::Catalog;
#[cfg(test)]
use iceberg::TableIdent;
use iceberg::writer::IcebergWriter;

use repark_common::spark_error;
use uuid::Uuid;

mod abort;
mod copy_on_write;
pub(crate) mod cow_scratch;
mod dv_close;
mod file_sink;
mod insert;
mod merge_on_read;
mod merge_sql;
mod not_matched_by_source;
pub(crate) mod row_lineage;
pub(super) mod session_staging;
mod snapshot_commit;
pub(crate) mod spec;
pub(crate) mod target_scan;

use copy_on_write::plan_and_commit_cow;
pub(super) use copy_on_write::resolve_affected_data_files;
pub use file_sink::{
    write_data_files, write_data_files_from_stream, write_data_files_from_stream_with_concurrency,
    write_data_files_with_concurrency,
};
use insert::store_assignment_then_sql;
#[cfg(test)]
use merge_on_read::consume_matched_work_batch;
use merge_on_read::plan_and_commit_mor;
use merge_sql::MergeSql;
pub use not_matched_by_source::{NotMatchedBySourceAction, NotMatchedBySourceClause};
pub use spec::{InsertAction, InsertClause, MatchedAction, MatchedClause, MergeSpec};
pub(crate) use target_scan::{
    KnownPartitions, PartitionSink, TargetScanStream, drain_partition_sink, new_partition_sink,
    residual_join_key_filter,
};

use crate::catalog::uuid_presentation::{convert_uuid_column, presented_arrow_schema};
#[cfg(test)]
use crate::write::concurrency::WriteConcurrency;
use crate::write::conflict_filter::from_merge_on;
pub(crate) use crate::write::name_resolution::{
    CaseInsensitiveColumnIndex, SourceMatch, dedup_key, resolve_write_column,
};
use crate::write::scan_concurrency::scan_concurrency_from_ctx;
use crate::write::scan_prune::file_scoped_rewrite_from_ctx;

/// The reserved `_file` metadata column the core scan projects.
pub(super) const FILE_PATH_COL: &str = "_file";

/// The reserved `_pos` metadata column the core scan projects.
pub(super) const POS_COL: &str = "_pos";

/// Prefix of the sentinel column added so `LEFT JOIN` match-detection ignores user-key nullability.
const MATCH_FLAG_PREFIX: &str = "__repark_matched_";

/// Snapshot-summary key stamping every MERGE commit with a unique id.
pub const OPERATION_ID_PROP: &str = "engine.operation-id";

/// Execute a lowered `MERGE INTO` against an Iceberg table — copy-on-write, one atomic commit.
/// # Errors
/// `NotImplemented` for the documented v1 limits; `MERGE_CARDINALITY_VIOLATION` when a target row
pub async fn execute_merge(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    spec: &MergeSpec,
) -> Result<()> {
    // Serialize merge under `cfg(test)` so concurrent MERGEs do not interleave on shared fixtures.
    #[cfg(test)]
    let _merge_serialize = {
        static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
        LOCK.lock().await
    };
    let table = catalog
        .load_table(&spec.target)
        .await
        .map_err(iceberg_err)?;
    let mode = resolve_merge_mode(&table)?;
    let table = if spec.schema_evolution && spec.assigns_columns() {
        union_source_schema(ctx, catalog, &table, spec).await?
    } else {
        table
    };

    let write_schema =
        Arc::new(presented_arrow_schema(table.metadata().current_schema()).map_err(iceberg_err)?);
    reserved_name_guard(&write_schema)?;
    let spec = expand_star_clauses(ctx, spec, &write_schema).await?;
    let spec = spec.as_ref();
    validate_update_columns(spec, &write_schema)?;
    let snapshot_id =
        crate::write::commit_target::snapshot_id_for_commit(&table, spec.commit_branch.as_deref());

    let scratch = row_lineage::scratch_schema_for_table(&write_schema, &table);
    // PERF-04: join-key min/max bounds may be pushed onto the primary target scan when safe.
    let file_scoped = file_scoped_rewrite_from_ctx(ctx);
    let residual = residual_join_key_filter(ctx, spec, &write_schema, mode, file_scoped).await?;
    let scan_concurrency = scan_concurrency_from_ctx(ctx);
    let partitions = new_partition_sink();
    let source: Arc<dyn PartitionStream> = Arc::new(
        TargetScanStream::new(
            table.clone(),
            snapshot_id,
            Arc::clone(&scratch),
            &write_schema,
            residual,
            scan_concurrency.concurrency_limit,
            None, // full snapshot for discovery + insert anti-join (filter may prune files/rows)
        )
        .with_partition_sink(Arc::clone(&partitions)),
    );
    let target_name = register_streaming_target(ctx, Arc::clone(&scratch), source)?;
    let target = MergeTarget {
        table: &table,
        write_schema: &write_schema,
        snapshot_id,
        partitions,
        conflict_filter: merge_conflict_filter(spec, &table),
    };
    let result = plan_and_commit(ctx, catalog, spec, &target, &target_name, mode).await;
    // Non-fatal for the MERGE result — but never silent (resource leak under repeated MERGEs).
    let _ = deregister_merge_scratch(ctx, &target_name);
    result
}

/// Test-only instrument handles (PERF-19 pass / PERF-01 discovery alloc / PERF-04 residual push).
#[cfg(test)]
#[derive(Clone, Default)]
struct MergeTestInstruments {
    logical_pass: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    discovery_path_alloc: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
    residual_push: Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>,
}

#[cfg(test)]
tokio::task_local! {
    static MERGE_TEST_INSTRUMENTS: MergeTestInstruments;
}

#[cfg(test)]
fn note_residual_push() {
    let _ = MERGE_TEST_INSTRUMENTS.try_with(|instruments| {
        if let Some(counter) = instruments.residual_push.as_ref() {
            counter.fetch_add(1, Ordering::SeqCst);
        }
    });
}

#[cfg(not(test))]
fn note_residual_push() {}

/// Drop the MERGE scratch streaming target.
pub(super) fn deregister_merge_scratch(
    ctx: &SessionContext,
    target_name: &str,
) -> std::result::Result<(), DataFusionError> {
    match ctx.deregister_table(target_name) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => {
            // Unexpected after we registered the scratch.
            tracing::warn!(
                target = "repark_write::merge",
                scratch = %target_name,
                "MERGE scratch table was not registered at deregister time"
            );
            Err(DataFusionError::Plan(format!(
                "MERGE scratch table '{target_name}' was not found at deregister time"
            )))
        }
        Err(error) => {
            tracing::warn!(
                target = "repark_write::merge",
                scratch = %target_name,
                error = %error,
                "failed to deregister MERGE scratch table"
            );
            Err(error)
        }
    }
}

/// The table property selecting how a `MERGE INTO` materialises its row-level changes.
const MERGE_MODE_PROP: &str = "write.merge.mode";

/// How a `MERGE INTO` writes its row-level changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeMode {
    /// `copy-on-write` (the Iceberg default when the property is unset): rewrite whole data files.
    CopyOnWrite,
    /// `merge-on-read` leaves data files intact; deletes and new rows commit in one `RowDelta`.
    MergeOnRead,
}

/// Resolve the merge mode and reject unsupported formats before any IO.
fn resolve_merge_mode(table: &Table) -> Result<MergeMode> {
    match table.metadata().properties().get(MERGE_MODE_PROP) {
        None => return Ok(MergeMode::CopyOnWrite),
        Some(mode) if mode.trim().eq_ignore_ascii_case("copy-on-write") => {
            return Ok(MergeMode::CopyOnWrite);
        }
        Some(mode) if mode.trim().eq_ignore_ascii_case("merge-on-read") => {}
        Some(mode) => {
            return Err(DataFusionError::NotImplemented(format!(
                "{MERGE_MODE_PROP} = '{mode}' is not a recognised mode: MERGE INTO supports \
                 'copy-on-write' and 'merge-on-read'"
            )));
        }
    }

    let format_version = table.metadata().format_version();
    if format_version < FormatVersion::V2 {
        return Err(crate::write::illegal_argument::illegal_argument_error(
            "Deletes are supported in V2 and above".to_string(),
        ));
    }
    crate::write::position_delete::parse_delete_granularity(
        table
            .metadata()
            .properties()
            .get(crate::write::position_delete::DELETE_GRANULARITY_PROP)
            .map(String::as_str),
    )?;
    Ok(MergeMode::MergeOnRead)
}

/// The scratch target adds `_file` + `_pos` to the TARGET's columns.
pub(super) fn reserved_name_guard(write_schema: &ArrowSchema) -> Result<()> {
    for reserved in [FILE_PATH_COL, POS_COL] {
        if write_schema.field_with_name(reserved).is_ok() {
            return Err(DataFusionError::Plan(format!(
                "MERGE INTO cannot run against a table with a column named `{reserved}` \
                 (reserved by the merge executor)"
            )));
        }
    }
    Ok(())
}

/// Every `UPDATE SET` target column must exist in the target schema.
fn validate_update_columns(spec: &MergeSpec, write_schema: &ArrowSchema) -> Result<()> {
    for clause in &spec.matched {
        let assignments = match &clause.action {
            MatchedAction::Update { assignments } => assignments,
            MatchedAction::Delete => continue,
            MatchedAction::UpdateAll => {
                return Err(DataFusionError::Internal(
                    "MERGE `UPDATE SET *` reached SQL generation unexpanded (executor bug)"
                        .to_string(),
                ));
            }
        };
        let mut seen = HashSet::with_capacity(assignments.len());
        for (column, _) in assignments {
            let canonical =
                resolve_write_column(write_schema, column, spec.case_insensitive, || {
                    format!("MERGE UPDATE SET column `{column}` does not exist in the target table")
                })?;
            if !seen.insert(dedup_key(&canonical, spec.case_insensitive)) {
                return Err(DataFusionError::Plan(format!(
                    "MERGE UPDATE SET names column `{column}` more than once"
                )));
            }
        }
    }
    not_matched_by_source::validate_update_columns(spec, write_schema)
}

/// Expand `UPDATE SET *` / `INSERT *` into explicit per-column clauses (Spark star resolution).
async fn expand_star_clauses<'a>(
    ctx: &SessionContext,
    spec: &'a MergeSpec,
    write_schema: &ArrowSchema,
) -> Result<Cow<'a, MergeSpec>> {
    let has_star = spec
        .matched
        .iter()
        .any(|clause| matches!(clause.action, MatchedAction::UpdateAll))
        || spec
            .not_matched
            .iter()
            .any(|clause| matches!(clause.action, InsertAction::All));
    if !has_star {
        return Ok(Cow::Borrowed(spec));
    }

    let (source_names, stored_types) = source_columns(ctx, spec).await?;
    let source_index = CaseInsensitiveColumnIndex::new(source_names.iter().map(String::as_str));
    let columns: Vec<String> = write_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let mut values_sql: Vec<String> = Vec::with_capacity(columns.len());
    let mut stored: Vec<Option<&DataType>> = Vec::with_capacity(columns.len());
    let mut missing: Vec<String> = Vec::new();
    for column in &columns {
        match source_index.resolve_scoped(column, spec.case_insensitive) {
            SourceMatch::Unique(index) => {
                values_sql.push(format!(
                    "{}.{}",
                    spec.source_alias,
                    quote_ident(source_index.source_name(index))
                ));
                stored.push(stored_types.as_ref().and_then(|types| types.get(index)));
            }
            SourceMatch::Missing => missing.push(column.clone()),
            SourceMatch::Ambiguous(colliding) => {
                return Err(DataFusionError::Plan(format!(
                    "MERGE `UPDATE SET *` / `INSERT *`: target column `{column}` is ambiguous — \
                     source columns `{}` all resolve to it (Spark case-insensitive resolution \
                     rejects the collision)",
                    colliding.join("`, `")
                )));
            }
        }
    }
    if !missing.is_empty() {
        let column_name = format!("`{}`", missing[0]);
        let suggestions = format!("`{}`", source_names.join("`, `"));
        return Err(DataFusionError::Plan(spark_error::message(
            spark_error::UNRESOLVED_COLUMN_WITH_SUGGESTION,
            &[
                ("columnName", column_name.as_str()),
                ("suggestions", suggestions.as_str()),
            ],
        )));
    }

    // Compute the expansion once; each star clause takes a clone (clauses stay independent).
    let assignments: Vec<(String, String)> = columns
        .iter()
        .zip(&values_sql)
        .zip(write_schema.fields().iter().zip(&stored))
        .map(|((column, value), (field, source))| {
            let stored_value = crate::write::update_cast::nested_micro_ntz_wall_sql(
                value,
                *source,
                field.data_type(),
            );
            (
                column.clone(),
                stored_value.unwrap_or_else(|| value.clone()),
            )
        })
        .collect();

    let mut expanded = spec.clone();
    for clause in &mut expanded.matched {
        if matches!(clause.action, MatchedAction::UpdateAll) {
            clause.action = MatchedAction::Update {
                assignments: assignments.clone(),
            };
        }
    }
    for clause in &mut expanded.not_matched {
        if matches!(clause.action, InsertAction::All) {
            clause.action = InsertAction::Explicit {
                columns: columns.clone(),
                values_sql: values_sql.clone(),
            };
        }
    }
    Ok(Cow::Owned(expanded))
}

/// Source column names in schema order, from planning `SELECT * FROM <source> LIMIT 0`.
async fn union_source_schema(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    spec: &MergeSpec,
) -> Result<Table> {
    let probe = format!(
        "SELECT * FROM {} AS {} LIMIT 0",
        spec.source_from_sql, spec.source_alias
    );
    let arrow = ctx.sql(&probe).await?.schema().as_arrow().clone();
    let incoming = crate::write::schema_evolution::incoming_schema(&arrow)?;
    crate::write::schema_evolution::evolve_merge_schema(catalog, table, incoming).await
}

async fn source_columns(
    ctx: &SessionContext,
    spec: &MergeSpec,
) -> Result<(Vec<String>, Option<Vec<DataType>>)> {
    let probe = format!(
        "SELECT * FROM {} AS {} LIMIT 0",
        spec.source_from_sql, spec.source_alias
    );
    let frame = ctx.sql(&probe).await?;
    let names: Vec<String> = frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let stored = crate::write::ntz_store::analyzed_types(ctx, frame.logical_plan())
        .filter(|types| types.len() == names.len());
    Ok((names, stored))
}

/// Scratch-target schema: every target data column, then `_file` (Utf8) and `_pos` (Int64).
pub(super) fn scratch_schema(write_schema: &SchemaRef) -> SchemaRef {
    row_lineage::scratch_schema_with_lineage(write_schema, false)
}

/// Test-only **logical** target-SQL pass counter (PERF-19 / Q16).
#[cfg(test)]
fn note_logical_target_sql_pass() {
    let _ = MERGE_TEST_INSTRUMENTS.try_with(|instruments| {
        if let Some(counter) = instruments.logical_pass.as_ref() {
            counter.fetch_add(1, Ordering::SeqCst);
        }
    });
}

#[cfg(not(test))]
fn note_logical_target_sql_pass() {}

/// Map one pinned-scan batch onto the scratch schema: reorder by name and cast `_file` and `_pos`.
pub(super) fn conform_scan_batch(scratch: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(scratch.fields().len());
    for field in scratch.fields() {
        let column = named_column(batch, field.name())?;
        let conformed = match field.name().as_str() {
            FILE_PATH_COL => cast_with_options(column, &DataType::Utf8, &strict_cast())?,
            POS_COL => cast_with_options(column, &DataType::Int64, &strict_cast())?,
            _ => crate::write::conform::promoted_scan_column(column, field.data_type())?,
        };
        columns.push(conformed);
    }
    Ok(RecordBatch::try_new(scratch.clone(), columns)?)
}

/// Register the pinned target as a streaming relation under a collision-proof scratch name.
pub(super) fn register_streaming_target(
    ctx: &SessionContext,
    scratch_schema: SchemaRef,
    source: Arc<dyn PartitionStream>,
) -> Result<String> {
    let provider = StreamingTable::try_new(scratch_schema, vec![source])?;
    cow_scratch::register_scratch_provider(ctx, Arc::new(provider), "merge_target")
}

/// The merge pipeline over the registered scratch target.
struct MergeTarget<'a> {
    /// The loaded Iceberg target table.
    table: &'a Table,
    /// The target's Arrow write schema (the Iceberg current schema, Arrow-converted).
    write_schema: &'a SchemaRef,
    /// The snapshot every merge query reads — the OCC `validate_from_snapshot` anchor.
    snapshot_id: Option<i64>,
    partitions: PartitionSink,
    conflict_filter: Predicate,
}

fn merge_conflict_filter(spec: &MergeSpec, table: &Table) -> Predicate {
    if not_matched_by_source::is_present(spec) {
        return Predicate::AlwaysTrue;
    }
    from_merge_on(
        &spec.on_sql,
        &spec.target_alias,
        &spec.source_alias,
        table.metadata().current_schema(),
    )
}

async fn plan_and_commit(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    spec: &MergeSpec,
    target: &MergeTarget<'_>,
    target_name: &str,
    mode: MergeMode,
) -> Result<()> {
    let match_flag = format!("{MATCH_FLAG_PREFIX}{}", Uuid::new_v4().simple());
    let sql = MergeSql {
        spec,
        target_name,
        match_flag: &match_flag,
        carry_lineage: row_lineage::table_carries_merge_lineage(target.table),
    };
    // R-MERGE-ONEPASS Stage A: cardinality is folded into match discovery.
    match mode {
        MergeMode::CopyOnWrite => plan_and_commit_cow(ctx, catalog, spec, target, &sql).await,
        MergeMode::MergeOnRead => plan_and_commit_mor(ctx, catalog, spec, target, &sql).await,
    }
}

/// Stream a SQL query as `RecordBatch` results (no full collect).
async fn stream_sql(
    ctx: &SessionContext,
    sql: &str,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Unpin + use<>> {
    note_logical_target_sql_pass();
    let dataframe = ctx.sql(sql).await?;
    dataframe.execute_stream().await
}

/// Spark `isCardinalityCheckNeeded`: skip only a lone unconditional MATCHED DELETE.
#[must_use]
fn skip_cardinality(spec: &MergeSpec) -> bool {
    matches!(
        spec.matched.as_slice(),
        [MatchedClause {
            predicate_sql: None,
            action: MatchedAction::Delete,
        }]
    )
}

/// Test-only: count path `String` allocations retained by COW Stage A discovery (PERF-01).
#[cfg(test)]
fn note_discovery_path_alloc() {
    let _ = MERGE_TEST_INSTRUMENTS.try_with(|instruments| {
        if let Some(counter) = instruments.discovery_path_alloc.as_ref() {
            counter.fetch_add(1, Ordering::SeqCst);
        }
    });
}

#[cfg(not(test))]
fn note_discovery_path_alloc() {}

/// Fail loud when a Stage A/B Int64 flag column is null (Arrow `.value` would silently yield 0).
fn require_non_null_i64(array: &Int64Array, row: usize, label: &str) -> Result<i64> {
    if array.is_null(row) {
        return Err(DataFusionError::Internal(format!(
            "matched_work/match-discovery row has a NULL {label}"
        )));
    }
    Ok(array.value(row))
}

/// Cast one batch onto the Iceberg write schema (strict casts, field order by name).
pub(super) fn cast_one_batch_to_write_schema(
    write_schema: &SchemaRef,
    batch: &RecordBatch,
) -> Result<RecordBatch> {
    let columns = write_schema
        .fields()
        .iter()
        .map(|field| convert_uuid_column(named_column(batch, field.name())?, field.data_type()))
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(write_schema.clone(), columns)?)
}

/// A minimal batch sink: write batches, then close into the produced data files.
pub(crate) trait BatchWriter {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;
    async fn finish(&mut self) -> Result<Vec<DataFile>>;
}

/// Production [`BatchWriter`] over the fork's `DataFileWriter`.
pub(crate) struct ForkBatchWriter<W: IcebergWriter> {
    pub(crate) inner: W,
}

impl<W: IcebergWriter> BatchWriter for ForkBatchWriter<W> {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.inner.write(batch).await.map_err(iceberg_err)
    }

    async fn finish(&mut self) -> Result<Vec<DataFile>> {
        self.inner.close().await.map_err(iceberg_err)
    }
}

/// Drive a record-batch stream into one [`BatchWriter`], writing each batch as it arrives.
pub(crate) async fn write_stream_into<K, S>(mut sink: K, mut stream: S) -> Result<Vec<DataFile>>
where
    K: BatchWriter,
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    while let Some(batch) = stream.try_next().await? {
        sink.write_batch(batch).await?;
    }
    sink.finish().await
}

/// Fan batches to `max_concurrent` independent writers (round-robin).
pub(crate) async fn write_stream_into_parallel<S, F, Fut, W>(
    max_concurrent: usize,
    stream: S,
    mut make_writer: F,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<W>>,
    W: IcebergWriter + Send + 'static,
{
    debug_assert!(max_concurrent >= 2);

    let mut sinks = Vec::with_capacity(max_concurrent);
    for _ in 0..max_concurrent {
        sinks.push(ForkBatchWriter {
            inner: make_writer().await?,
        });
    }
    write_stream_into_parallel_sinks(max_concurrent, stream, sinks).await
}

/// Parallel drive over already-built [`BatchWriter`] sinks (production + test double path).
async fn write_stream_into_parallel_sinks<S, K>(
    max_concurrent: usize,
    mut stream: S,
    sinks: Vec<K>,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
    K: BatchWriter,
{
    debug_assert_eq!(sinks.len(), max_concurrent);
    debug_assert!(max_concurrent >= 2);

    // Shared abort flag: set on source error or worker write error.
    let aborted = Arc::new(AtomicBool::new(false));
    let mut senders = Vec::with_capacity(max_concurrent);
    let mut worker_futures = Vec::with_capacity(max_concurrent);

    for mut sink in sinks {
        let (tx, rx) = mpsc::channel::<RecordBatch>(1);
        let aborted = Arc::clone(&aborted);
        worker_futures.push(async move {
            let mut rx = rx;
            while let Some(batch) = rx.next().await {
                if aborted.load(Ordering::SeqCst) {
                    // Sibling already failed or dispatcher aborted — do not finish.
                    return Ok(Vec::new());
                }
                if let Err(error) = sink.write_batch(batch).await {
                    aborted.store(true, Ordering::SeqCst);
                    // Drop sink WITHOUT finish — partial file stays unpublished.
                    return Err(error);
                }
            }
            if aborted.load(Ordering::SeqCst) {
                // Clean channel close was actually an abort (source error / sibling failure).
                return Ok(Vec::new());
            }
            sink.finish().await
        });
        senders.push(tx);
    }

    let aborted_for_dispatch = Arc::clone(&aborted);
    let dispatcher = async move {
        let mut index = 0usize;
        loop {
            match stream.next().await {
                None => {
                    // Source exhausted cleanly — close channels so workers finish.
                    drop(senders);
                    return Ok::<(), DataFusionError>(());
                }
                Some(Ok(batch)) => {
                    let slot = index % max_concurrent;
                    if senders[slot].send(batch).await.is_err() {
                        // Worker side closed (usually after a write error).
                        aborted_for_dispatch.store(true, Ordering::SeqCst);
                        drop(senders);
                        return Err(DataFusionError::Execution(
                            "write worker channel closed before the source stream was exhausted"
                                .into(),
                        ));
                    }
                    index = index.wrapping_add(1);
                }
                Some(Err(error)) => {
                    // Source error: signal abort, drop senders (workers exit recv without finish).
                    aborted_for_dispatch.store(true, Ordering::SeqCst);
                    drop(senders);
                    return Err(error);
                }
            }
        }
    };

    let (dispatch_result, worker_results) =
        futures::future::join(dispatcher, futures::future::join_all(worker_futures)).await;

    // Prefer the first worker root-cause over the dispatcher's secondary channel-closed error.
    let mut files = Vec::new();
    let mut first_worker_error: Option<DataFusionError> = None;
    for result in worker_results {
        match result {
            Ok(part) => files.extend(part),
            Err(error) => {
                if first_worker_error.is_none() {
                    first_worker_error = Some(error);
                }
            }
        }
    }
    if let Some(error) = first_worker_error {
        return Err(error);
    }
    dispatch_result?;
    Ok(files)
}

pub(super) use snapshot_commit::*;

fn named_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    batch.column_by_name(name).ok_or_else(|| {
        DataFusionError::Internal(format!("merge-internal column `{name}` missing from batch"))
    })
}

fn strict_cast() -> CastOptions<'static> {
    CastOptions {
        safe: false,
        ..CastOptions::default()
    }
}

#[cfg(test)]
fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// A double-quoted SQL identifier with embedded quotes doubled.
pub(super) fn quote_ident(name: &str) -> String {
    crate::write::idents::quote_ident_spark(name)
}

/// Fold an iceberg error into the DataFusion error this crate's SQL callers carry.
pub(crate) fn iceberg_err(err: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

#[cfg(test)]
mod tests;
