//! `MERGE INTO` executor with copy-on-write and merge-on-read arms.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
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
use iceberg::arrow::{FieldMatchMode, schema_to_arrow_schema};
use iceberg::expr::Predicate;
use iceberg::spec::{
    DataFile, DataFileFormat, FormatVersion, ManifestContentType, PartitionKey, Struct,
};
use iceberg::table::Table;

use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableIdent};

use tracing::Instrument;
use uuid::Uuid;

mod abort;
mod cow_scratch;
mod dv_close;
mod insert;
mod not_matched_by_source;
pub(crate) mod row_lineage;
mod snapshot_commit;
mod target_scan;

use insert::{
    insert_projection, insert_stream_checked, store_assignment_then_sql, update_stream_checked,
};
pub use not_matched_by_source::{NotMatchedBySourceAction, NotMatchedBySourceClause};
pub(crate) use target_scan::{
    KnownPartitions, PartitionSink, TargetScanStream, drain_partition_sink, new_partition_sink,
};

use crate::write::concurrency::{WriteConcurrency, concurrency_from_ctx};
use crate::write::conform::{conform_batch_retaining_unmapped_columns, write_default_column_names};
use crate::write::name_resolution::{CaseInsensitiveColumnIndex, SourceMatch};
use crate::write::scan_concurrency::scan_concurrency_from_ctx;
use crate::write::scan_prune::{
    bare_equalities_from_on, file_scoped_rewrite_from_ctx, residual_bounds_predicate,
    scan_pruning_from_ctx,
};

/// The reserved `_file` metadata column the core scan projects.
pub(super) const FILE_PATH_COL: &str = "_file";

/// The reserved `_pos` metadata column the core scan projects.
pub(super) const POS_COL: &str = "_pos";

/// Prefix of the sentinel column added so `LEFT JOIN` match-detection ignores user-key nullability.
const MATCH_FLAG_PREFIX: &str = "__repark_matched_";

/// Snapshot-summary key stamping every MERGE commit with a unique id.
pub const OPERATION_ID_PROP: &str = "engine.operation-id";

/// A lowered `MERGE INTO` statement.
#[derive(Debug, Clone)]
pub struct MergeSpec {
    /// The Iceberg target table.
    pub target: TableIdent,
    /// Alias the statement uses for the target (defaults to the bare table name upstream).
    pub target_alias: String,
    /// What to put after `FROM` for the source: a table reference or a parenthesized subquery.
    pub source_from_sql: String,
    /// Alias the statement uses for the source.
    pub source_alias: String,
    /// The `ON` join condition, SQL-rendered.
    pub on_sql: String,
    /// `WHEN MATCHED` clauses, in declaration order.
    pub matched: Vec<MatchedClause>,
    /// `WHEN NOT MATCHED [BY TARGET]` clauses, in declaration order.
    pub not_matched: Vec<InsertClause>,
    /// `WHEN NOT MATCHED BY SOURCE` clauses, in declaration order.
    pub not_matched_by_source: Vec<NotMatchedBySourceClause>,
    pub commit_branch: Option<String>,
}

/// One `WHEN MATCHED [AND …] THEN UPDATE/DELETE` clause.
#[derive(Debug, Clone)]
pub struct MatchedClause {
    /// The `AND …` predicate, if present (SQL-rendered).
    pub predicate_sql: Option<String>,
    /// What the clause does to a matched row.
    pub action: MatchedAction,
}

/// The action of a `WHEN MATCHED` clause.
#[derive(Debug, Clone)]
pub enum MatchedAction {
    /// `UPDATE SET col = expr, …` — `(column, SQL expression)` pairs.
    Update {
        /// Assignments as `(target column, SQL expression)` pairs.
        assignments: Vec<(String, String)>,
    },
    /// `UPDATE SET *` — every target column from the same-named source column.
    UpdateAll,
    /// `DELETE` — drop the matched row.
    Delete,
}

/// One `WHEN NOT MATCHED [AND …] THEN INSERT …` clause.
#[derive(Debug, Clone)]
pub struct InsertClause {
    /// The `AND …` predicate, if present (SQL-rendered).
    pub predicate_sql: Option<String>,
    /// What the clause inserts.
    pub action: InsertAction,
}

/// What a `WHEN NOT MATCHED` clause inserts.
#[derive(Debug, Clone)]
pub enum InsertAction {
    /// `INSERT (…) VALUES (…)` — explicit columns and expressions.
    Explicit {
        /// Insert column list; empty means positional (all target columns in schema order).
        columns: Vec<String>,
        /// The `VALUES` expressions, SQL-rendered, one per column.
        values_sql: Vec<String>,
    },
    /// `INSERT *` — every target column from the same-named source column.
    All,
}

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

    let write_schema =
        Arc::new(schema_to_arrow_schema(table.metadata().current_schema()).map_err(iceberg_err)?);
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
    };
    let result = plan_and_commit(ctx, catalog, spec, &target, &target_name, mode).await;
    // Non-fatal for the MERGE result — but never silent (resource leak under repeated MERGEs).
    let _ = deregister_merge_scratch(ctx, &target_name);
    result
}

/// PERF-04: residual join-key bounds.
async fn residual_join_key_filter(
    ctx: &SessionContext,
    spec: &MergeSpec,
    write_schema: &SchemaRef,
    mode: MergeMode,
    file_scoped_rewrite: bool,
) -> Result<Option<Predicate>> {
    if !scan_pruning_from_ctx(ctx) {
        return Ok(None);
    }
    if not_matched_by_source::is_present(spec) {
        return Ok(None);
    }
    // COW full-target rewrite must not residual-filter the primary, or unmatched survivors drop.
    if matches!(mode, MergeMode::CopyOnWrite) && !file_scoped_rewrite {
        return Ok(None);
    }
    let equalities = bare_equalities_from_on(&spec.on_sql, &spec.target_alias, &spec.source_alias);
    if equalities.is_empty() {
        return Ok(None);
    }
    let residual = residual_bounds_predicate(
        ctx,
        &spec.source_from_sql,
        &spec.source_alias,
        write_schema.as_ref(),
        &equalities,
    )
    .await;
    if residual.is_some() {
        note_residual_push();
    }
    Ok(residual)
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
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format =
        DataFileFormat::from_str(&table_props.write_format_default).map_err(iceberg_err)?;
    if file_format != DataFileFormat::Parquet {
        return Err(DataFusionError::NotImplemented(format!(
            "MERGE INTO writes only Parquet data files yet (table default is {file_format})"
        )));
    }
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
        return Err(DataFusionError::NotImplemented(format!(
            "merge-on-read MERGE INTO writes Parquet position deletes on V2 and deletion vectors \
             on V3 (this table is {format_version:?}; V1 has no delete files) — use \
             write.merge.mode = 'copy-on-write' instead"
        )));
    }
    // pins: mw-9-delete-granularity/C-004 — refuse unknown granularity BEFORE any data write
    // so a MATCHED UPDATE cannot orphan parquet (same class as the V2/format gate above).
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
        // Case-insensitive duplicates would silent first-win in `rewrite_column`.
        let mut seen = HashSet::with_capacity(assignments.len());
        for (column, _) in assignments {
            let Some(canonical) = resolve_schema_field_name(write_schema, column) else {
                return Err(DataFusionError::Plan(format!(
                    "MERGE UPDATE SET column `{column}` does not exist in the target table"
                )));
            };
            if !seen.insert(canonical.to_ascii_lowercase()) {
                return Err(DataFusionError::Plan(format!(
                    "MERGE UPDATE SET names column `{column}` more than once \
                     (case-insensitive)"
                )));
            }
        }
    }
    not_matched_by_source::validate_update_columns(spec, write_schema)
}

/// Resolve `name` against `schema` case-insensitively (Spark `caseSensitive=false`).
fn resolve_schema_field_name<'a>(schema: &'a ArrowSchema, name: &str) -> Option<&'a str> {
    let mut found: Option<&str> = None;
    for field in schema.fields() {
        if field.name().eq_ignore_ascii_case(name) {
            if found.is_some() {
                // Two schema fields differing only by case — refuse ambiguous resolution.
                return None;
            }
            found = Some(field.name().as_str());
        }
    }
    found
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

    // Resolve every target column to its source column by name (case-insensitively).
    let source_names = source_column_names(ctx, spec).await?;
    let source_index = CaseInsensitiveColumnIndex::new(source_names.iter().map(String::as_str));
    let columns: Vec<String> = write_schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    let mut values_sql: Vec<String> = Vec::with_capacity(columns.len());
    let mut missing: Vec<String> = Vec::new();
    for column in &columns {
        match source_index.resolve(column) {
            SourceMatch::Unique(index) => values_sql.push(format!(
                "{}.{}",
                spec.source_alias,
                quote_ident(source_index.source_name(index))
            )),
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
        return Err(DataFusionError::Plan(format!(
            "MERGE `UPDATE SET *` / `INSERT *` requires the source to provide every target \
             column; missing from the source: `{}` (columns resolve by name, case-insensitively \
             — Spark default)",
            missing.join("`, `")
        )));
    }

    // Compute the expansion once; each star clause takes a clone (clauses stay independent).
    let assignments: Vec<(String, String)> = columns
        .iter()
        .cloned()
        .zip(values_sql.iter().cloned())
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
async fn source_column_names(ctx: &SessionContext, spec: &MergeSpec) -> Result<Vec<String>> {
    let probe = format!(
        "SELECT * FROM {} AS {} LIMIT 0",
        spec.source_from_sql, spec.source_alias
    );
    let frame = ctx.sql(&probe).await?;
    Ok(frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect())
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
            _ => column.clone(),
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

/// The copy-on-write arm: discover affected files, rewrite plus insert, then Parquet write.
async fn plan_and_commit_cow(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    spec: &MergeSpec,
    target: &MergeTarget<'_>,
    sql: &MergeSql<'_>,
) -> Result<()> {
    let MergeTarget {
        table,
        write_schema,
        snapshot_id,
        partitions: _,
    } = *target;
    let (affected, new_files) = async {
        let affected = if not_matched_by_source::is_present(spec) {
            if !spec.matched.is_empty() {
                let _ = affected_files(ctx, sql, skip_cardinality(spec)).await?;
            }
            not_matched_by_source::all_current_data_file_paths(table).await?
        } else if spec.matched.is_empty() {
            Vec::new()
        } else {
            affected_files(ctx, sql, skip_cardinality(spec)).await?
        };
        // R-MERGE-STREAM-OUT: pipe rewrite and insert SQL streams into concurrent file writers.
        let mut streams: Vec<std::pin::Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> =
            Vec::new();
        // Scratches to drop on every exit of this block (file-scoped target and/or path table).
        let mut rewrite_scratches = cow_scratch::MergeScratchGuard::new(ctx);
        if !affected.is_empty() {
            let rewrite_scratch = cow_scratch::maybe_register_file_scoped_rewrite_target(
                ctx,
                table,
                snapshot_id,
                write_schema,
                &affected,
            )?;
            let rewrite_sql = if let Some(ref rewrite_name) = rewrite_scratch {
                rewrite_scratches.push(rewrite_name.clone());
                sql.rewrite_sql_allowlisted(rewrite_name, write_schema)
            } else {
                let path_table = cow_scratch::register_affected_paths_table(ctx, &affected)?;
                rewrite_scratches.push(path_table.clone());
                sql.rewrite_sql_path_semijoin(sql.target_name, &path_table, write_schema)
            };
            let rewrite_stream =
                update_stream_checked(ctx, sql, &rewrite_sql, write_schema).await?;
            streams.push(Box::pin(rewrite_stream));
        }
        for index in 0..spec.not_matched.len() {
            let insert_sql = sql.insert_sql(index, write_schema)?;
            streams.push(Box::pin(
                insert_stream_checked(ctx, &insert_sql, write_schema).await?,
            ));
        }
        let concurrency = concurrency_from_ctx(ctx);
        let chained = futures::stream::iter(streams).flatten();
        let write_result =
            write_new_data_files_from_stream(table, write_schema, chained, concurrency).await;
        // Explicit drop before Ok so cleanup is ordered before the join span ends.
        drop(rewrite_scratches);
        Ok::<_, DataFusionError>((affected, write_result?))
    }
    .instrument(tracing::info_span!("merge.join"))
    .await?;

    let file_count = new_files.len() as u64;
    let affected_entries = resolve_affected_data_files(table, snapshot_id, &affected).await?;
    // write_data span wraps only the file count after streaming write.
    tracing::info_span!("merge.write_data", files = file_count).in_scope(|| ());
    commit_on_ref(
        catalog,
        table,
        snapshot_id,
        affected_entries,
        new_files,
        spec.commit_branch.as_deref(),
    )
    .instrument(tracing::info_span!("merge.commit", files = file_count))
    .await
}

/// The merge-on-read arm (Group T): data files are left COMPLETELY untouched.
async fn plan_and_commit_mor(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    spec: &MergeSpec,
    target: &MergeTarget<'_>,
    sql: &MergeSql<'_>,
) -> Result<()> {
    let MergeTarget {
        table,
        write_schema,
        snapshot_id,
        partitions,
    } = target;
    let (table, write_schema, snapshot_id) = (*table, *write_schema, *snapshot_id);
    // R-MERGE-ONEPASS Stage B (MoR): one INNER JOIN yields cardinality, deletes, and UPDATE values.
    let (pairs, data_files) = async {
        let mut streams: Vec<std::pin::Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> =
            Vec::new();
        let mut pairs = if spec.matched.is_empty() {
            Vec::new()
        } else {
            let (pairs, update_batches) = matched_work_mor(
                ctx,
                sql,
                write_schema,
                skip_cardinality(spec),
                &sql.matched_work_sql(write_schema),
            )
            .await?;
            if !update_batches.is_empty() {
                streams.push(Box::pin(futures::stream::iter(
                    update_batches.into_iter().map(Ok),
                )));
            }
            pairs
        };
        if not_matched_by_source::is_present(spec) {
            let (nmbs_pairs, nmbs_updates) = matched_work_mor(
                ctx,
                sql,
                write_schema,
                true,
                &not_matched_by_source::mor_work_sql(sql, write_schema),
            )
            .await?;
            pairs.extend(nmbs_pairs);
            if !nmbs_updates.is_empty() {
                streams.push(Box::pin(futures::stream::iter(
                    nmbs_updates.into_iter().map(Ok),
                )));
            }
        }
        for index in 0..spec.not_matched.len() {
            let insert_sql = sql.insert_sql(index, write_schema)?;
            streams.push(Box::pin(
                insert_stream_checked(ctx, &insert_sql, write_schema).await?,
            ));
        }
        let concurrency = concurrency_from_ctx(ctx);
        let chained = futures::stream::iter(streams).flatten();
        let data_files =
            write_new_data_files_from_stream(table, write_schema, chained, concurrency).await?;
        Ok::<_, DataFusionError>((pairs, data_files))
    }
    .instrument(tracing::info_span!("merge.join"))
    .await?;

    let file_count = data_files.len() as u64;
    let concurrency = concurrency_from_ctx(ctx);
    tracing::info_span!("merge.write_data", files = file_count).in_scope(|| ());
    commit_row_delta_on_ref_with_partitions(
        catalog,
        table,
        snapshot_id,
        pairs,
        data_files,
        concurrency,
        spec.commit_branch.as_deref(),
        drain_partition_sink(partitions),
    )
    .await
}

/// R-MERGE-STREAM-OUT: cast each batch to the write schema and pipe into the streaming writers.
pub(super) async fn write_new_data_files_from_stream<S>(
    table: &Table,
    write_schema: &SchemaRef,
    stream: S,
    concurrency: WriteConcurrency,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let write_schema = Arc::clone(write_schema);
    let cast_stream = stream.try_filter_map(move |batch| {
        let write_schema = Arc::clone(&write_schema);
        async move {
            if batch.num_rows() == 0 {
                return Ok(None);
            }
            Ok(Some(row_lineage::attach_present_lineage(
                cast_one_batch_to_write_schema(&write_schema, &batch)?,
                &batch,
            )?))
        }
    });
    // Pin the stream so partitioned/unpartitioned helpers get Unpin.
    let cast_stream = std::pin::pin!(cast_stream);
    if table.metadata().default_partition_spec().is_unpartitioned() {
        write_data_files_from_stream_with_concurrency(table, cast_stream, concurrency).await
    } else if row_lineage::table_carries_merge_lineage(table) {
        row_lineage::write_partitioned_lineage_files(table, cast_stream).await
    } else {
        crate::write::append::write_partitioned_data_files_from_stream_with_concurrency(
            table,
            cast_stream,
            concurrency,
        )
        .await
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

/// Resolve affected `_file` paths to [`DataFile`] entries by walking the pinned snapshot.
pub(super) async fn resolve_affected_data_files(
    table: &Table,
    snapshot_id: Option<i64>,
    affected: &[String],
) -> Result<Vec<DataFile>> {
    // Span is the P2a hour-0 measurement seam for scout #6 (manifest walk share of MERGE wall).
    async {
        if affected.is_empty() {
            return Ok(Vec::new());
        }
        let metadata = table.metadata();
        let snapshot = match snapshot_id {
            Some(id) => metadata.snapshot_by_id(id),
            None => metadata.current_snapshot(),
        }
        .ok_or_else(|| {
            DataFusionError::Internal(
                "affected files exist but the table has no snapshot".to_string(),
            )
        })?;
        let manifest_list = snapshot
            .load_manifest_list(table.file_io(), metadata)
            .await
            .map_err(iceberg_err)?;
        let wanted: HashSet<&str> = affected.iter().map(String::as_str).collect();
        let mut files = Vec::with_capacity(affected.len());
        // Serial walk retained: P2a hour-0 local-fs attributes were well under 10% of MERGE wall.
        for manifest_file in manifest_list.entries() {
            if manifest_file.content != ManifestContentType::Data {
                continue;
            }
            let manifest = manifest_file
                .load_manifest(table.file_io())
                .await
                .map_err(iceberg_err)?;
            for entry in manifest.entries() {
                if entry.is_alive() && wanted.contains(entry.data_file().file_path()) {
                    files.push(entry.data_file().clone());
                }
            }
        }
        if files.len() != wanted.len() {
            return Err(DataFusionError::Internal(format!(
                "resolved {} of {} affected data files in the pinned snapshot's manifests",
                files.len(),
                wanted.len()
            )));
        }
        Ok(files)
    }
    .instrument(tracing::info_span!(
        "merge.resolve_affected",
        affected = affected.len()
    ))
    .await
}

/// Spark's `MERGE_CARDINALITY_VIOLATION` message.
const CARDINALITY_VIOLATION_MSG: &str = "MERGE_CARDINALITY_VIOLATION: a target row matched more \
    than one source row; deduplicate the source or tighten the ON condition";

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

/// R-MERGE-ONEPASS Stage A: stream match discovery and fold to distinct affected `_file` paths.
async fn affected_files(
    ctx: &SessionContext,
    sql: &MergeSql<'_>,
    skip_cardinality: bool,
) -> Result<Vec<String>> {
    let mut stream = stream_sql(ctx, &sql.match_discovery_sql()).await?;
    // Single owned `String` per distinct path (HashSet only; collect at end).
    let mut seen: HashSet<String> = HashSet::new();
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        fold_discovery_batch_into_affected(&batch, &mut seen, skip_cardinality)?;
    }
    Ok(seen.into_iter().collect())
}

/// Process one Stage A discovery batch: cardinality check + distinct mutated `_file` paths.
fn fold_discovery_batch_into_affected(
    batch: &RecordBatch,
    seen: &mut HashSet<String>,
    skip_cardinality: bool,
) -> Result<()> {
    if batch.num_columns() < 4 {
        return Err(DataFusionError::Internal(format!(
            "match-discovery batch has {} columns; expected >= 4",
            batch.num_columns()
        )));
    }
    let files_col = cast_with_options(batch.column(0), &DataType::Utf8, &strict_cast())?;
    let paths = files_col
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("match-discovery `_file` is not a string array".into())
        })?;
    let positions = cast_with_options(batch.column(1), &DataType::Int64, &strict_cast())?;
    let ordinals = positions
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("match-discovery `_pos` is not an Int64 array".into())
        })?;
    let counts = cast_with_options(batch.column(2), &DataType::Int64, &strict_cast())?;
    let match_counts = counts
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("match-discovery match_count is not Int64".into())
        })?;
    let mutated = cast_with_options(batch.column(3), &DataType::Int64, &strict_cast())?;
    let is_mutated = mutated
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal("match-discovery is_mutated is not Int64".into())
        })?;
    for row in 0..batch.num_rows() {
        if paths.is_null(row) || ordinals.is_null(row) {
            return Err(DataFusionError::Internal(
                "match-discovery row has a NULL `_file`/`_pos` row identity".to_string(),
            ));
        }
        // Same fail-loud null flags as Stage B.
        let match_count = require_non_null_i64(match_counts, row, "match_count")?;
        let is_mutated_flag = require_non_null_i64(is_mutated, row, "is_mutated")?;
        if match_count > 1 && !skip_cardinality {
            return Err(DataFusionError::Execution(
                CARDINALITY_VIOLATION_MSG.to_string(),
            ));
        }
        if is_mutated_flag == 1 {
            let path_str = paths.value(row);
            if !seen.contains(path_str) {
                // PERF-01: one owned path String per distinct affected file (not per matched row).
                note_discovery_path_alloc();
                seen.insert(path_str.to_string());
            }
        }
    }
    Ok(())
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

/// R-MERGE-ONEPASS Stage B (`MoR`): one INNER JOIN pass for cardinality + pos-deletes + UPDATE
async fn matched_work_mor(
    ctx: &SessionContext,
    sql: &MergeSql<'_>,
    write_schema: &SchemaRef,
    skip_cardinality: bool,
    work_sql: &str,
) -> Result<(
    Vec<crate::write::position_delete::PositionDeletePair>,
    Vec<RecordBatch>,
)> {
    let mut stream = update_stream_checked(ctx, sql, work_sql, write_schema).await?;
    let mut path_intern: HashMap<String, usize> = HashMap::new();
    let mut unique_paths: Vec<std::sync::Arc<str>> = Vec::new();
    let mut seen_pair: HashSet<(usize, i64)> = HashSet::new();
    let mut pair_indices: Vec<(usize, i64)> = Vec::new();
    let mut update_batches = Vec::new();
    let data_field_count = write_schema.fields().len();

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        consume_matched_work_batch(
            &batch,
            write_schema,
            data_field_count,
            &mut path_intern,
            &mut unique_paths,
            &mut seen_pair,
            &mut pair_indices,
            &mut update_batches,
            skip_cardinality,
        )?;
    }
    let pairs = pair_indices
        .into_iter()
        .map(|(path_index, pos)| (std::sync::Arc::clone(&unique_paths[path_index]), pos))
        .collect();
    Ok((pairs, update_batches))
}

/// Process one Stage B `matched_work` batch: cardinality, intern, pos-deletes, and UPDATE slices.
#[allow(clippy::too_many_arguments)]
fn consume_matched_work_batch(
    batch: &RecordBatch,
    write_schema: &SchemaRef,
    data_field_count: usize,
    path_intern: &mut HashMap<String, usize>,
    unique_paths: &mut Vec<std::sync::Arc<str>>,
    seen_pair: &mut HashSet<(usize, i64)>,
    pair_indices: &mut Vec<(usize, i64)>,
    update_batches: &mut Vec<RecordBatch>,
    skip_cardinality: bool,
) -> Result<()> {
    if batch.num_columns() < 5 + data_field_count {
        return Err(DataFusionError::Internal(format!(
            "matched_work batch has {} columns; expected >= {}",
            batch.num_columns(),
            5 + data_field_count
        )));
    }
    let projected_field_count = batch.num_columns() - 5;
    let files = cast_with_options(batch.column(0), &DataType::Utf8, &strict_cast())?;
    let paths = files
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            DataFusionError::Internal("matched_work `_file` is not a string array".into())
        })?;
    let positions = cast_with_options(batch.column(1), &DataType::Int64, &strict_cast())?;
    let ordinals = positions
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("matched_work `_pos` is not Int64".into()))?;
    let counts = cast_with_options(batch.column(2), &DataType::Int64, &strict_cast())?;
    let match_counts = counts
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("matched_work match_count is not Int64".into()))?;
    let mutated = cast_with_options(batch.column(3), &DataType::Int64, &strict_cast())?;
    let is_mutated = mutated
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("matched_work is_mutated is not Int64".into()))?;
    let updated = cast_with_options(batch.column(4), &DataType::Int64, &strict_cast())?;
    let is_update = updated
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| DataFusionError::Internal("matched_work is_update is not Int64".into()))?;

    let mut update_indices: Vec<u32> = Vec::new();
    for row in 0..batch.num_rows() {
        if paths.is_null(row) || ordinals.is_null(row) {
            return Err(DataFusionError::Internal(
                "matched_work row has a NULL `_file`/`_pos` row identity".to_string(),
            ));
        }
        // Flag columns are CASE/window outputs and must be non-null.
        let match_count = require_non_null_i64(match_counts, row, "match_count")?;
        let is_mutated_flag = require_non_null_i64(is_mutated, row, "is_mutated")?;
        let is_update_flag = require_non_null_i64(is_update, row, "is_update")?;
        if match_count > 1 && !skip_cardinality {
            return Err(DataFusionError::Execution(
                CARDINALITY_VIOLATION_MSG.to_string(),
            ));
        }
        let path_str = paths.value(row);
        let path_index = if let Some(&index) = path_intern.get(path_str) {
            index
        } else {
            let index = unique_paths.len();
            let path_arc: std::sync::Arc<str> = std::sync::Arc::from(path_str);
            unique_paths.push(std::sync::Arc::clone(&path_arc));
            // HashMap key is owned String once per unique path; pair paths share Arc.
            path_intern.insert(path_str.to_string(), index);
            index
        };
        let pos = ordinals.value(row);
        if is_mutated_flag == 1 && seen_pair.insert((path_index, pos)) {
            pair_indices.push((path_index, pos));
        }
        if is_update_flag == 1 {
            update_indices.push(u32::try_from(row).map_err(|_| {
                DataFusionError::Internal("matched_work row index exceeds u32".into())
            })?);
        }
    }
    if !update_indices.is_empty() {
        // Slice UPDATE projection columns first, then take rows.
        let data_fields: Vec<datafusion::arrow::datatypes::Field> = (0..projected_field_count)
            .map(|index| {
                if index < data_field_count {
                    let source = write_schema.field(index);
                    datafusion::arrow::datatypes::Field::new(
                        source.name(),
                        batch.column(5 + index).data_type().clone(),
                        source.is_nullable(),
                    )
                } else {
                    batch.schema().field(5 + index).as_ref().clone()
                }
            })
            .collect();
        let data_columns: Vec<ArrayRef> = (0..projected_field_count)
            .map(|index| batch.column(5 + index).clone())
            .collect();
        let data_batch =
            RecordBatch::try_new(Arc::new(ArrowSchema::new(data_fields)), data_columns)?;
        let indices = datafusion::arrow::array::UInt32Array::from(update_indices);
        let taken = datafusion::arrow::compute::take_record_batch(&data_batch, &indices)
            .map_err(|error| DataFusionError::ArrowError(Box::new(error), None))?;
        update_batches.push(taken);
    }
    Ok(())
}

/// Fail loud when a Stage A/B Int64 flag column is null (Arrow `.value` would silently yield 0).
fn require_non_null_i64(array: &Int64Array, row: usize, label: &str) -> Result<i64> {
    if array.is_null(row) {
        return Err(DataFusionError::Internal(format!(
            "matched_work/match-discovery row has a NULL {label}"
        )));
    }
    Ok(array.value(row))
}

/// Builds the SQL the merge runs.
struct MergeSql<'a> {
    spec: &'a MergeSpec,
    target_name: &'a str,
    /// The per-execution match-sentinel column name (UUID-suffixed — see `MATCH_FLAG_PREFIX`).
    match_flag: &'a str,
    carry_lineage: bool,
}

impl MergeSql<'_> {
    /// `FROM` fragment for the scratch target, aliased as the statement's target alias.
    fn target_from(&self) -> String {
        // Engine UUID names today; still route through quote_ident.
        format!(
            "{} AS {}",
            cow_scratch::quote_scratch_name(self.target_name),
            self.spec.target_alias
        )
    }

    /// `FROM` fragment for the source, wrapped to add the always-TRUE match sentinel.
    fn source_from(&self) -> String {
        format!(
            "(SELECT *, TRUE AS {} FROM {} AS __repark_src_inner) AS {}",
            self.match_flag, self.spec.source_from_sql, self.spec.source_alias
        )
    }

    /// Match detection after the LEFT JOIN — sentinel-based, immune to nullable join keys.
    fn matched(&self) -> String {
        format!("{}.{} IS NOT NULL", self.spec.source_alias, self.match_flag)
    }

    /// A clause predicate as a 2-valued applies-test: NULL ⇒ FALSE (Spark clause semantics).
    fn applies(predicate_sql: Option<&str>) -> String {
        predicate_sql.map_or_else(|| "TRUE".to_string(), |p| format!("COALESCE(({p}), FALSE)"))
    }

    /// First-match clause id over an ordered predicate list — O CASE, not O AND-chains.
    fn clause_id_case(predicates: &[Option<&str>]) -> String {
        if predicates.is_empty() {
            return "CAST(NULL AS BIGINT)".to_string();
        }
        let branches: Vec<String> = predicates
            .iter()
            .enumerate()
            .map(|(index, predicate)| format!("WHEN {} THEN {index}", Self::applies(*predicate)))
            .collect();
        format!("CASE {} END", branches.join(" "))
    }

    /// First-match-wins residual: no earlier clause applies.
    #[cfg(test)]
    fn prior_clauses_do_not_apply(predicates: &[Option<&str>], index: usize) -> String {
        if index == 0 {
            return "TRUE".to_string();
        }
        let clause_id = Self::clause_id_case(predicates);
        format!("(({clause_id}) IS NULL OR ({clause_id}) >= {index})")
    }

    /// Pre-#18 O AND-chain of `NOT applies`.
    #[cfg(test)]
    fn prior_clauses_do_not_apply_legacy(predicates: &[Option<&str>], index: usize) -> String {
        let negations: Vec<String> = predicates[..index]
            .iter()
            .map(|predicate| format!("NOT {}", Self::applies(*predicate)))
            .collect();
        if negations.is_empty() {
            "TRUE".to_string()
        } else {
            negations.join(" AND ")
        }
    }

    /// The matched-clause predicates in declaration order (for first-match `clause_id`).
    fn matched_predicates(&self) -> Vec<Option<&str>> {
        self.spec
            .matched
            .iter()
            .map(|clause| clause.predicate_sql.as_deref())
            .collect()
    }

    /// Matched-side first-match clause id: `NULL` when unmatched or no WHEN MATCHED clause applies.
    fn matched_clause_id_expr(&self) -> String {
        let predicates = self.matched_predicates();
        if predicates.is_empty() {
            return "CAST(NULL AS BIGINT)".to_string();
        }
        let mut branches = Vec::with_capacity(predicates.len() + 1);
        branches.push(format!("WHEN NOT ({}) THEN NULL", self.matched()));
        for (index, predicate) in predicates.iter().enumerate() {
            branches.push(format!("WHEN {} THEN {index}", Self::applies(*predicate)));
        }
        format!("CASE {} END", branches.join(" "))
    }

    /// Per-clause case-insensitive assignment maps (column lower → SQL expr) for UPDATE clauses.
    fn update_assignment_lookup(&self) -> Vec<Option<HashMap<String, &str>>> {
        self.spec
            .matched
            .iter()
            .map(|clause| match &clause.action {
                MatchedAction::Update { assignments } => {
                    let mut map = HashMap::with_capacity(assignments.len());
                    for (name, expr) in assignments {
                        map.insert(name.to_ascii_lowercase(), expr.as_str());
                    }
                    Some(map)
                }
                MatchedAction::UpdateAll | MatchedAction::Delete => None,
            })
            .collect()
    }

    /// Projection list for rewrite / `matched_work` / `updated_rows` — assignment maps built once.
    fn rewrite_projection(&self, write_schema: &ArrowSchema) -> String {
        let maps = self.update_assignment_lookup();
        let user = write_schema
            .fields()
            .iter()
            .map(|field| {
                self.rewrite_column_with_maps(&maps, field.name(), Some(field.data_type()))
            })
            .collect::<Vec<_>>()
            .join(", ");
        row_lineage::maybe_append_lineage_projection(self, user)
    }

    /// R-MERGE-ONEPASS Stage A: one grouped join pass — `match_count` + `is_mutated`.
    fn match_discovery_sql(&self) -> String {
        let ta = &self.spec.target_alias;
        format!(
            "SELECT {ta}.\"{FILE_PATH_COL}\", {ta}.\"{POS_COL}\", \
             count(*) AS match_count, \
             MAX(CASE WHEN ({mutated}) THEN 1 ELSE 0 END) AS is_mutated \
             FROM {source} JOIN {target} ON {on} \
             GROUP BY {ta}.\"{FILE_PATH_COL}\", {ta}.\"{POS_COL}\"",
            target = self.target_from(),
            source = self.source_from(),
            on = self.spec.on_sql,
            mutated = self.mutated(),
        )
    }

    /// The disjunction "some WHEN MATCHED clause mutates this row".
    pub(super) fn mutated(&self) -> String {
        self.spec
            .matched
            .iter()
            .map(|clause| Self::applies(clause.predicate_sql.as_deref()))
            .collect::<Vec<_>>()
            .join(" OR ")
    }

    /// Stage B merge-on-read: one INNER JOIN producing identity + match flags + UPDATE projection.
    fn matched_work_sql(&self, write_schema: &ArrowSchema) -> String {
        let ta = &self.spec.target_alias;
        let projection = self.rewrite_projection(write_schema);
        format!(
            "SELECT {ta}.\"{FILE_PATH_COL}\", {ta}.\"{POS_COL}\", \
             count(*) OVER (PARTITION BY {ta}.\"{FILE_PATH_COL}\", {ta}.\"{POS_COL}\") \
               AS match_count, \
             CASE WHEN ({mutated}) THEN 1 ELSE 0 END AS is_mutated, \
             CASE WHEN ({updated}) THEN 1 ELSE 0 END AS is_update, \
             {projection} \
             FROM {source} JOIN {target} ON {on}",
            target = self.target_from(),
            source = self.source_from(),
            on = self.spec.on_sql,
            mutated = self.mutated(),
            updated = self.update_applies(),
        )
    }

    /// True when the row's FIRST applicable clause is an UPDATE.
    pub(super) fn update_applies(&self) -> String {
        let update_ids: Vec<String> = self
            .spec
            .matched
            .iter()
            .enumerate()
            .filter(|(_, clause)| matches!(clause.action, MatchedAction::Update { .. }))
            .map(|(index, _)| index.to_string())
            .collect();
        if update_ids.is_empty() {
            "FALSE".to_string()
        } else if update_ids.len() == 1 {
            format!(
                "COALESCE(({}) = {}, FALSE)",
                self.matched_clause_id_expr(),
                update_ids[0]
            )
        } else {
            format!(
                "COALESCE(({}) IN ({}), FALSE)",
                self.matched_clause_id_expr(),
                update_ids.join(", ")
            )
        }
    }

    /// Rewrite SQL against a file-scoped target whose task allowlist already restricts `_file`.
    fn rewrite_sql_allowlisted(
        &self,
        target_table_name: &str,
        write_schema: &ArrowSchema,
    ) -> String {
        let ta = &self.spec.target_alias;
        let projection = self.rewrite_projection(write_schema);
        let deleted = self.delete_applies();
        let quoted_target = cow_scratch::quote_scratch_name(target_table_name);
        format!(
            "SELECT {projection} FROM {quoted_target} AS {ta} LEFT JOIN {source} ON {on} \
             WHERE NOT ({deleted})",
            source = self.source_from(),
            on = self.spec.on_sql,
        )
    }

    /// Rewrite SQL against the full snapshot target, semi-joined to a registered path `MemTable`.
    fn rewrite_sql_path_semijoin(
        &self,
        target_table_name: &str,
        path_table_name: &str,
        write_schema: &ArrowSchema,
    ) -> String {
        let ta = &self.spec.target_alias;
        let projection = self.rewrite_projection(write_schema);
        let deleted = self.delete_applies();
        let quoted_target = cow_scratch::quote_scratch_name(target_table_name);
        let quoted_paths = cow_scratch::quote_scratch_name(path_table_name);
        format!(
            "SELECT {projection} FROM {quoted_target} AS {ta} \
             INNER JOIN {quoted_paths} AS __repark_aff \
               ON {ta}.\"{FILE_PATH_COL}\" = __repark_aff.\"{path_col}\" \
             LEFT JOIN {source} ON {on} \
             WHERE NOT ({deleted})",
            source = self.source_from(),
            on = self.spec.on_sql,
            path_col = cow_scratch::AFFECTED_PATHS_COL,
        )
    }

    /// The `CASE` projecting one output column through the ordered UPDATE clauses (unit pins).
    #[cfg(test)]
    fn rewrite_column(&self, column: &str) -> String {
        let maps = self.update_assignment_lookup();
        self.rewrite_column_with_maps(&maps, column, None)
    }

    /// Like [`Self::rewrite_column`] but reuses a pre-built assignment lookup (scout #18).
    fn rewrite_column_with_maps(
        &self,
        maps: &[Option<HashMap<String, &str>>],
        column: &str,
        store_type: Option<&DataType>,
    ) -> String {
        let ta = &self.spec.target_alias;
        let quoted = quote_ident(column);
        let original = format!("{ta}.{quoted}");
        let key = column.to_ascii_lowercase();
        // DELETE rows never reach this projection.
        let branches: Vec<String> = maps
            .iter()
            .enumerate()
            .filter_map(|(index, map_opt)| {
                let expr = map_opt.as_ref()?.get(&key)?;
                let then_expr = match store_type {
                    Some(data_type) => store_assignment_then_sql(expr, data_type),
                    None => (*expr).to_string(),
                };
                Some(format!("WHEN {index} THEN ({then_expr})"))
            })
            .collect();
        let else_expr = not_matched_by_source::rewrite_else(self, column, &original, store_type);
        if branches.is_empty() {
            format!("{else_expr} AS {quoted}")
        } else {
            format!(
                "CASE ({clause_id}) {} ELSE {else_expr} END AS {quoted}",
                branches.join(" "),
                clause_id = self.matched_clause_id_expr(),
            )
        }
    }

    /// True when the row's first applicable clause is a DELETE.
    pub(super) fn delete_applies(&self) -> String {
        let delete_ids: Vec<String> = self
            .spec
            .matched
            .iter()
            .enumerate()
            .filter(|(_, clause)| matches!(clause.action, MatchedAction::Delete))
            .map(|(index, _)| index.to_string())
            .collect();
        let matched_delete = if delete_ids.is_empty() {
            "FALSE".to_string()
        } else if delete_ids.len() == 1 {
            format!(
                "COALESCE(({}) = {}, FALSE)",
                self.matched_clause_id_expr(),
                delete_ids[0]
            )
        } else {
            format!(
                "COALESCE(({}) IN ({}), FALSE)",
                self.matched_clause_id_expr(),
                delete_ids.join(", ")
            )
        };
        not_matched_by_source::combined_delete_applies(&matched_delete, self)
    }

    /// The rows insert clause `index` adds: source rows with no target match.
    fn insert_sql(&self, index: usize, write_schema: &ArrowSchema) -> Result<String> {
        let clause = &self.spec.not_matched[index];
        let projection = insert_projection(clause, write_schema)?;
        let predicates: Vec<Option<&str>> = self
            .spec
            .not_matched
            .iter()
            .map(|c| c.predicate_sql.as_deref())
            .collect();
        let clause_id = Self::clause_id_case(&predicates);
        Ok(format!(
            "SELECT {projection} FROM (SELECT {sa}.*, {ta}.\"{POS_COL}\" AS \"{NOT_MATCHED_POS_SENTINEL}\" \
             FROM {source} LEFT JOIN {target} ON {on}) AS {sa} \
             WHERE \"{NOT_MATCHED_POS_SENTINEL}\" IS NULL AND ({clause_id}) = {index}",
            source = self.source_from(),
            target = self.target_from(),
            on = self.spec.on_sql,
            ta = self.spec.target_alias,
            sa = self.spec.source_alias,
        ))
    }
}

/// Sentinel alias carrying target `_pos` through source-only insert, named to avoid collisions.
const NOT_MATCHED_POS_SENTINEL: &str = "__repark_not_matched_pos";

/// Cast one batch onto the Iceberg write schema (strict casts, field order by name).
fn cast_one_batch_to_write_schema(
    write_schema: &SchemaRef,
    batch: &RecordBatch,
) -> Result<RecordBatch> {
    let columns = write_schema
        .fields()
        .iter()
        .map(|field| {
            let column = named_column(batch, field.name())?;
            Ok(cast_with_options(
                column,
                field.data_type(),
                &strict_cast(),
            )?)
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(write_schema.clone(), columns)?)
}

/// Write batches as Parquet data files through iceberg's writer stack, unpartitioned (v1).
/// # Errors
/// Returns a DataFusion error if the table is not Parquet-default, writer setup fails, or a batch
pub async fn write_data_files(table: &Table, batches: Vec<RecordBatch>) -> Result<Vec<DataFile>> {
    write_data_files_with_concurrency(table, batches, WriteConcurrency::default()).await
}

/// [`write_data_files`] with explicit [`WriteConcurrency`] (`repark.write.max-concurrent-files`).
/// # Errors
/// Same as [`write_data_files`].
pub async fn write_data_files_with_concurrency(
    table: &Table,
    batches: Vec<RecordBatch>,
    concurrency: WriteConcurrency,
) -> Result<Vec<DataFile>> {
    write_data_files_from_stream_with_concurrency(
        table,
        futures::stream::iter(batches.into_iter().map(Ok::<_, DataFusionError>)),
        concurrency,
    )
    .await
}

/// Stream batches into unpartitioned Parquet writers as the source produces each batch.
/// # Errors
/// Returns a DataFusion error if the table is not Parquet-default or the writer/source fails.
pub async fn write_data_files_from_stream<S>(table: &Table, stream: S) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    write_data_files_from_stream_with_concurrency(table, stream, WriteConcurrency::default()).await
}

/// [`write_data_files_from_stream`] with explicit [`WriteConcurrency`].
/// # Errors
/// Same as [`write_data_files_from_stream`], plus a plan error when `max_concurrent_files < 1`.
pub async fn write_data_files_from_stream_with_concurrency<S>(
    table: &Table,
    stream: S,
    concurrency: WriteConcurrency,
) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    let max_concurrent = concurrency.max_concurrent_files;
    if max_concurrent < 1 {
        return Err(DataFusionError::Plan(format!(
            "repark.write.max-concurrent-files must be >= 1 (got {max_concurrent})"
        )));
    }
    let current_schema = table.metadata().current_schema();
    let write_schema = Arc::new(schema_to_arrow_schema(current_schema).map_err(iceberg_err)?);
    let write_default_columns = write_default_column_names(current_schema);
    let conformed = stream.map(move |item| {
        conform_batch_retaining_unmapped_columns(&write_schema, &write_default_columns, &item?)
    });
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format =
        DataFileFormat::from_str(&table_props.write_format_default).map_err(iceberg_err)?;
    if file_format != DataFileFormat::Parquet {
        return Err(DataFusionError::NotImplemented(format!(
            "MERGE INTO writes only Parquet data files yet (table default is {file_format})"
        )));
    }
    let build_writer = || async { build_unpartitioned_data_file_writer(table).await };
    if max_concurrent == 1 {
        let writer = build_writer().await?;
        return write_stream_into(ForkBatchWriter { inner: writer }, conformed).await;
    }
    write_stream_into_parallel(max_concurrent, conformed, build_writer).await
}

/// Open one unpartitioned Parquet `DataFileWriter` for `table` (unique file-name UUID per call).
async fn build_unpartitioned_data_file_writer(table: &Table) -> Result<impl IcebergWriter + use<>> {
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format =
        DataFileFormat::from_str(&table_props.write_format_default).map_err(iceberg_err)?;
    let parquet_builder = ParquetWriterBuilder::new_with_match_mode(
        crate::write::writer_props::writer_properties_for(table)?,
        row_lineage::iceberg_parquet_schema(table)?,
        FieldMatchMode::Name,
    );
    let location_generator =
        DefaultLocationGenerator::new(table.metadata().clone()).map_err(iceberg_err)?;
    let file_name_generator =
        DefaultFileNameGenerator::new(Uuid::new_v4().to_string(), None, file_format);
    let rolling_builder = RollingFileWriterBuilder::new(
        parquet_builder,
        table_props.write_target_file_size_bytes,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    let unpartitioned_key = PartitionKey::new(
        table.metadata().default_partition_spec().as_ref().clone(),
        table.metadata().current_schema().clone(),
        Struct::empty(),
    )
    .map_err(iceberg_err)?;
    DataFileWriterBuilder::new(rolling_builder)
        .build(Some(unpartitioned_key))
        .await
        .map_err(iceberg_err)
}

/// A minimal batch sink: write batches, then close into the produced data files.
trait BatchWriter {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;
    async fn finish(&mut self) -> Result<Vec<DataFile>>;
}

/// Production [`BatchWriter`] over the fork's `DataFileWriter`.
struct ForkBatchWriter<W: IcebergWriter> {
    inner: W,
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
async fn write_stream_into<K, S>(mut sink: K, mut stream: S) -> Result<Vec<DataFile>>
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
async fn write_stream_into_parallel<S, F, Fut, W>(
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
