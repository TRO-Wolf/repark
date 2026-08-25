//! `MERGE INTO` — the `RePark`-owned Spark-semantics executor (copy-on-write + merge-on-read).
//!
//! The fork's `ENGINE_CONTRACT.md` §6 makes MERGE engine-owned: DataFusion has no MERGE planner
//! and iceberg-core deliberately carries no SQL semantics. This module implements the COW recipe
//! from §4–§5: pin the current snapshot, find the data files that contain rows a WHEN MATCHED
//! clause mutates, rewrite exactly those files (survivors kept, matched rows updated/dropped),
//! write the WHEN NOT MATCHED insert rows, and commit everything as ONE `OverwriteFiles`
//! transaction under SERIALIZABLE isolation (§5 row 142, Java's MERGE default) —
//! `.delete_data_files(affected).add_files(new)` + `validate_from_snapshot(pin)` + BOTH
//! `validate_no_conflicting_deletes()` (armed by the FULL `DataFile` entries; the path-only
//! `delete_files` form would leave it structurally inert) AND `validate_no_conflicting_data()`
//! (rejects a concurrent add matching the ON condition — the F-BR-1 silent-duplicate guard) +
//! a conservative `AlwaysTrue` conflict-detection filter + an `engine.operation-id`
//! snapshot-summary stamp (§8 mitigation for the ambiguous-commit gap, fork row R157).
//!
//! An **insert-only** MERGE (no WHEN MATCHED clause matched anything ⇒ nothing rewritten) still
//! pinned snapshot S to decide which source rows were NOT MATCHED, so it cannot append blindly: a
//! concurrent commit that added a matching row between S and the commit would make the append a
//! silent duplicate. It commits an ADD-ONLY `OverwriteFiles` (recorded `Operation::Append`,
//! identical snapshot semantics to `fast_append`) carrying the §5 SERIALIZABLE-isolation
//! `validate_no_conflicting_data()` guard on the same pin + `AlwaysTrue` filter — the only §5
//! check that fires for a pure append (`validate_no_conflicting_deletes` is a dead no-op with no
//! removed files, so it is not set). See [`commit`].
//!
//! Row identity is `(_file, _pos)` — the two reserved metadata columns the pinned core scan
//! surfaces (fork rev `c10ea425`: `_pos` is the per-file 0-based physical ordinal — `reader.rs`
//! `finish_whole_file_scan_task` threads a per-file counter), a stable, RE-SCAN-INVARIANT identity
//! that supersedes the former materialize-time synthesized `__repark_row_id` counter. The pinned
//! target is consumed as a **streaming, re-scannable** `StreamingTable` over the snapshot scan
//! (never a full-target `MemTable`), so the whole target is never resident as rows. Peak scan-side
//! residency is `O(num_cpus × largest data file)` — the fork whole-file-materializes a data file
//! when `_pos` is projected (reader.rs at c10ea425) — plus the per-query join working set
//! (`O(source)` for the rewrite/cardinality joins, `O(target keys)` for the insert anti-join), NOT
//! `O(target rows)`. That is the OTH-001/SAF-001 bound: the persistent full-target `MemTable` is gone. The join/clause computation
//! runs as DataFusion SQL over that streaming target, so Spark expressions in `ON` / `WHEN … AND` /
//! `SET` / `VALUES` evaluate with DataFusion semantics — the same engine that runs them in reads.
//!
//! Clause semantics follow Spark: clauses apply in declaration order (first match wins — encoded
//! as a single O(C) `clause_id` CASE, scout #18; rewrite columns are `CASE (clause_id) WHEN i
//! THEN …`), a clause predicate evaluating to NULL means "does not apply" (every predicate is
//! wrapped `COALESCE((p), FALSE)` — 3-valued-logic footgun), and a target row matched by more
//! than one source row raises `MERGE_CARDINALITY_VIOLATION` except a lone unconditional DELETE.
//! The star forms (`UPDATE SET *` / `INSERT *`) expand with Spark's star resolution before any SQL
//! is generated: every TARGET column takes the same-named source column — a target column missing
//! from the source errors up front, extra source columns are ignored (see [`expand_star_clauses`]).
//!
//! **Partitioned targets (A4 + Group R)** run — identity AND non-identity transforms
//! (bucket/truncate/temporal): both arms route their new files (rewritten COW survivors + inserted
//! rows) through the SAME A1/U1 fanout `append` uses
//! ([`crate::write::append::write_partitioned_data_files`] — one fanout in the engine, never a second, in
//! the fork's computed-transform mode since Group P), so every produced [`DataFile`] carries its
//! transform-computed partition value at the manifest level and a partition-key-changing UPDATE
//! re-routes its survivor to the new partition (fork `ENGINE_CONTRACT` §4 UPDATE/COW: "a
//! partition-key-changing UPDATE re-routes rows via the partition-aware writer"; §7 "fan out rows by
//! partition before writing"). The `commit` seam is partition-agnostic, so the FULL serializable OCC
//! posture carries over verbatim to every partitioned path.
//!
//! **Merge-on-read (Group T).** `write.merge.mode = 'merge-on-read'` selects a sibling WRITE arm
//! that shares ALL of the above — same streamed target, same `(_file, _pos)` identity, same
//! first-match-wins clause resolution, same cardinality check — and differs only in what it writes
//! and how it commits ([`plan_and_commit_mor`]). Data files are left COMPLETELY untouched: every
//! mutated row (DELETE and UPDATE alike) contributes its `(_file, _pos)` to a **position-delete
//! file** ([`crate::write::position_delete`]), UPDATE additionally re-emits its NEW values as a fresh
//! data-file row (merge-on-read UPDATE == delete-old + insert-new), and the delete files + data
//! files commit together in ONE `RowDelta` snapshot ([`commit_row_delta`]) under the same
//! SERIALIZABLE isolation, with the `validate_data_files_exist` / `validate_deleted_files` /
//! `validate_no_conflicting_delete_files` guards Java arms for `command == UPDATE || MERGE`
//! (`SparkPositionDeltaWrite.commit`). The next scan applies the deletes (fork `GAP_MATRIX` row
//! R117), so a merge-on-read MERGE and a copy-on-write MERGE of the same source into the same target
//! are SCAN-EQUIVALENT while being physically different — the differential the Group T pins assert.
//!
//! **Merge-on-read × transform partitioning (Group Y).** merge-on-read runs on EVERY partitioning
//! the copy-on-write arm supports — unpartitioned, identity, and non-identity transforms
//! (bucket/truncate/temporal). Three mechanisms compose it, each pinned rather than assumed: the
//! delete-file stamp is the OWNING data file's own `(spec_id, partition)` read off the manifests,
//! which for a transform-partitioned file is ALREADY the transformed value (bucket ordinal /
//! truncated prefix / day) — no recomputation, so the stamp is transform-agnostic by construction;
//! the scan applies position deletes against that TRANSFORMED partition `Struct` (fork `GAP_MATRIX`
//! row R117, interop-proven both directions on `truncate[10]`); and the new data files ride the
//! SAME Group P computed-mode fanout, so a partition-key-changing UPDATE re-routes its new row to
//! the new partition while its OLD row is position-deleted in the OLD one. Pins Y1-Y8.
//!
//! v1 limits (deterministic `NotImplemented`, tracked in `task/todo.md`): a non-Parquet
//! `write.format.default`, an unrecognised `write.merge.mode`, merge-on-read on a non-V2 table
//! (position deletes are V2-only; V3 needs deletion vectors), `WHEN NOT MATCHED BY SOURCE`,
//! `INSERT ROW`. Also out of merge-on-read scope: equality deletes (position deletes only),
//! deletion vectors, and the fork's sorting position-delete writer variant.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::compute::{CastOptions, cast_with_options};
use datafusion::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::datasource::MemTable;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::prelude::SessionContext;
use futures::channel::mpsc;
use futures::{SinkExt, Stream, StreamExt, TryStreamExt};
use iceberg::arrow::{ArrowReaderBuilder, FieldMatchMode, schema_to_arrow_schema};
use iceberg::expr::Predicate;
use iceberg::spec::{
    DataFile, DataFileFormat, FormatVersion, ManifestContentType, PartitionKey, Struct,
};
// scan pruning residual filters use `iceberg::expr::Predicate` via `scan_prune`
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
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
mod insert;
use insert::{
    insert_projection, insert_stream_checked, store_assignment_then_sql, update_stream_checked,
};

use crate::write::concurrency::{WriteConcurrency, concurrency_from_ctx};
use crate::write::file_scoped_rewrite::{allowlist_from_paths, filter_tasks_to_allowlist_nonempty};
use crate::write::name_resolution::{CaseInsensitiveColumnIndex, SourceMatch};
use crate::write::scan_concurrency::scan_concurrency_from_ctx;
use crate::write::scan_prune::{
    bare_equalities_from_on, file_scoped_rewrite_from_ctx, residual_bounds_predicate,
    scan_pruning_from_ctx,
};

/// The reserved `_file` metadata column the core scan projects (fork `metadata_columns.rs`
/// `RESERVED_COL_NAME_FILE`) — the file path, one half of the streamed row identity.
pub(super) const FILE_PATH_COL: &str = "_file";

/// The reserved `_pos` metadata column the core scan projects (fork `metadata_columns.rs`
/// `RESERVED_COL_NAME_POS`, surfaced at rev `c10ea425`) — the per-file 0-based physical ordinal.
/// `(_file, _pos)` is the stable, re-scan-invariant per-row identity the streamed target join uses,
/// superseding the former materialize-time synthesized `__repark_row_id` counter.
pub(super) const POS_COL: &str = "_pos";

/// Prefix of the sentinel column added to the source side so `LEFT JOIN` match-detection never
/// depends on the nullability of user join keys. The full name gets a per-execution UUID suffix:
/// the source is arbitrary user SQL whose schema we never inspect, so no fixed name is safe.
const MATCH_FLAG_PREFIX: &str = "__repark_matched_";

/// Snapshot-summary key stamping every MERGE commit with a unique id — the fork
/// `ENGINE_CONTRACT` §8 mitigation for the ambiguous-commit-outcome gap (row R157): on a
/// transport-ambiguous failure, scan recent snapshots for this id before re-running.
pub const OPERATION_ID_PROP: &str = "engine.operation-id";

/// A lowered `MERGE INTO` statement — plain strings, no sqlparser types, so the SQL front end
/// owns dialect concerns and this module owns execution.
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
    /// `UPDATE SET *` — every target column from the same-named source column. A marker only:
    /// [`expand_star_clauses`] rewrites it into an explicit [`MatchedAction::Update`] against
    /// the target schema before any SQL is generated.
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
    /// `INSERT *` — every target column from the same-named source column. A marker only:
    /// [`expand_star_clauses`] rewrites it into an explicit [`InsertAction::Explicit`] against
    /// the target schema before any SQL is generated.
    All,
}

/// ===========================================================================================
/// Execute a lowered `MERGE INTO` against an Iceberg table — copy-on-write, one atomic commit.
///
/// Pins the target's current snapshot, registers it as a STREAMING, re-scannable relation (its
/// data columns plus `_file` + `_pos` — the `(_file, _pos)` row identity; never a full-target
/// `MemTable`), runs the cardinality check and clause application as DataFusion
/// SQL, rewrites exactly the affected data files, writes insert rows, and commits a single
/// `OverwriteFiles` (add-only, recorded `Operation::Append`, when nothing is rewritten) with the
/// §5 SERIALIZABLE validations pinned to the scanned snapshot — the rewrite path carries BOTH
/// `validate_no_conflicting_deletes` and `validate_no_conflicting_data`, the insert-only path just
/// `validate_no_conflicting_data` (a pure append removes nothing) — plus the §8
/// `engine.operation-id` summary stamp. A MERGE that changes nothing commits nothing.
/// ===========================================================================================
///
/// # Errors
/// `NotImplemented` for the documented v1 limits; `MERGE_CARDINALITY_VIOLATION` when a target
/// row matches more than one source row (except a lone unconditional DELETE); otherwise any
/// error (a failed §5 validation is non-retryable by design and surfaces as-is).
pub async fn execute_merge(
    ctx: &SessionContext,
    catalog: &Arc<dyn Catalog>,
    spec: &MergeSpec,
) -> Result<()> {
    // Serialize merge execution under `cfg(test)` so concurrent MERGEs do not interleave
    // on shared catalog/scratch fixtures. Instrument counters (PERF-19/01/04) are **task-local**
    // — see `MERGE_TEST_INSTRUMENTS` — so parallel cargo tests never clobber each other's Arc
    // slots (critic-octo C1: process-global `Mutex<Option<Arc>>` install raced install→execute).
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
    let snapshot_id = table
        .metadata()
        .current_snapshot()
        .map(|snapshot| snapshot.snapshot_id());

    let scratch = scratch_schema(&write_schema);
    // PERF-04 bounded residual (2026-08-04): join-key min/max bounds may be pushed onto the
    // primary target scan when safe. R-PERF-MERGE-PRUNE residual-filter hazard still holds for
    // COW rewrite through a residual-filtered primary — survivors co-located with a match get
    // dropped. We therefore only push when (a) MoR (no survivor rewrite via primary), or
    // (b) COW with file-scoped rewrite ON (default): rewrite uses a separate whole-file
    // allowlisted `TargetScanStream` with filter=None. COW + file-scoped OFF → unfiltered.
    // Fork metrics-only (file prune without residual) remains the queue item for a universal
    // path. See ledger PERF-04 design + `scan_prune.rs`.
    let file_scoped = file_scoped_rewrite_from_ctx(ctx);
    let residual = residual_join_key_filter(ctx, spec, &write_schema, mode, file_scoped).await?;
    let scan_concurrency = scan_concurrency_from_ctx(ctx);
    let source: Arc<dyn PartitionStream> = Arc::new(TargetScanStream::new(
        table.clone(),
        snapshot_id,
        Arc::clone(&scratch),
        &write_schema,
        residual,
        scan_concurrency.concurrency_limit,
        None, // full snapshot for discovery + insert anti-join (filter may prune files/rows)
    ));
    let target_name = register_streaming_target(ctx, Arc::clone(&scratch), source)?;
    let target = MergeTarget {
        table: &table,
        write_schema: &write_schema,
        snapshot_id,
    };
    let result = plan_and_commit(ctx, catalog, spec, &target, &target_name, mode).await;
    // Non-fatal for the MERGE result — but never silent (resource leak under repeated MERGEs).
    let _ = deregister_merge_scratch(ctx, &target_name);
    result
}

/// ===========================================================================================
/// PERF-04: residual join-key bounds. Thin caller — M1/M6/M7 live in `scan_prune.rs`.
///
/// **Safe shapes only (when in doubt → None / over-scan):**
/// - conf `repark.merge.scan-pruning` true (default)
/// - ON yields ≥1 bare equality (`t.col = s.col`); OR / `<=>` / expression-only → None
/// - source field Arrow type **identical** to target Int32/Int64 key; else skip conjunct
/// - source min/max non-null (empty source → None); any probe failure → skip conjunct (M6)
/// - `MoR` always OK; COW only when `file_scoped_rewrite` (rewrite is a separate unfiltered scan)
///
/// General multi-clause / non-equi ON pushdown is OUT (r25 seed). Wrong push = S0 lost rows;
/// over-scan residual is OK.
/// ===========================================================================================
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
    // COW + full-target rewrite path: residual on the primary would drop unmatched survivors
    // in affected files (R-PERF-MERGE-PRUNE STOP). File-scoped rewrite avoids that path.
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
/// Task-local so parallel `#[tokio::test]` tasks cannot race a process-global slot.
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

/// Drop the MERGE scratch streaming target. Failure is non-fatal for the MERGE outcome, but
/// surfaces via `tracing::warn!` so failed cleanup is never swallowed (SAF-010 / WU-5).
///
/// DataFusion's memory catalog returns `Ok(None)` when the name is absent (not `Err`), so both
/// `Err` and `Ok(None)` are treated as failed cleanup and warned. Returns `Ok(())` only when a
/// table was actually removed (`Ok(Some(_))`). The `Err` return is for tests / callers that need
/// to assert the failure path; `execute_merge` discards it.
pub(super) fn deregister_merge_scratch(
    ctx: &SessionContext,
    target_name: &str,
) -> std::result::Result<(), DataFusionError> {
    match ctx.deregister_table(target_name) {
        Ok(Some(_)) => Ok(()),
        Ok(None) => {
            // Unexpected after we registered the scratch — MemTable may still be held if this
            // was a race or a double-deregister. Never silent.
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

/// The table property selecting how a `MERGE INTO` materialises its row-level changes (Iceberg
/// standard, Spark's `SparkRowLevelOperationBuilder` reads the same key).
const MERGE_MODE_PROP: &str = "write.merge.mode";

/// How a `MERGE INTO` writes its row-level changes — the `write.merge.mode` table property,
/// resolved once before any IO and threaded to the commit arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeMode {
    /// `copy-on-write` (the Iceberg default when the property is unset): rewrite whole data files.
    CopyOnWrite,
    /// `merge-on-read`: leave data files intact, write position-delete files for the mutated rows
    /// and new data files for updated/inserted rows, commit both in ONE `RowDelta`.
    MergeOnRead,
}

/// ===========================================================================================
/// v1 scope gate + mode resolution, both checked before any IO.
///
/// Partitioned targets ARE supported (A4 + Group R) — identity AND non-identity transforms
/// (bucket/truncate/temporal): the rewritten COW survivors and inserted rows route through the SAME
/// A1/U1 fanout as `append` (`write_partitioned_data_files`, in the fork's computed-transform mode
/// since Group P), so every produced `DataFile` carries its transform-computed partition value (fork
/// `ENGINE_CONTRACT` §4 UPDATE/COW + §7 "fan out rows by partition before writing").
///
/// What stays a deterministic `NotImplemented`, never a wrong answer:
///   * a non-Parquet `write.format.default` — BOTH modes write Parquet only, and the partitioned
///     write path does not re-check format, so it must be guarded here (as `append` does);
///   * a `write.merge.mode` value that is neither `copy-on-write` nor `merge-on-read`;
///   * **merge-on-read on a non-V2 table** — position-delete files exist only in V2. V1 has no
///     delete files at all, and V3 mandates Puffin deletion vectors, which the fork's
///     `PositionDeleteFileWriter` does not produce (row R113). Guarded BEFORE any write so a
///     commit-time format rejection can never orphan an already-written delete file;
///
/// **Group Y retired the transform gate.** merge-on-read used to refuse a NON-identity-transform
/// -partitioned table as UNPROVEN (not broken). It is now PROVEN and runs: the delete-file stamp is
/// each data file's OWN `(spec_id, partition)` read off the manifests, which for a
/// transform-partitioned file is ALREADY the transformed value (the bucket ordinal / truncated
/// prefix / day) — nothing recomputes it, so the stamp is transform-agnostic *by construction*; the
/// scan applies position deletes against the TRANSFORMED partition `Struct` (fork `GAP_MATRIX` row
/// R117, interop-proven both directions on `truncate[10]`); and the new data files (updated
/// new-values + inserts) ride the SAME Group P computed-mode fanout the copy-on-write arm uses, so
/// a partition-key-changing UPDATE re-routes to the new partition. Pins Y1-Y8.
///
/// ===========================================================================================
fn resolve_merge_mode(table: &Table) -> Result<MergeMode> {
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format =
        DataFileFormat::from_str(&table_props.write_format_default).map_err(iceberg_err)?;
    if file_format != DataFileFormat::Parquet {
        return Err(DataFusionError::NotImplemented(format!(
            "MERGE INTO writes only Parquet data files yet (table default is {file_format})"
        )));
    }
    // Below this point the mode is merge-on-read; both other arms return.
    // Iceberg-Java's `RowLevelOperationMode.fromName` is equalsIgnoreCase, and the sibling MoR
    // valve (position_delete.rs) already trims+ignores case — match both (audit M12).
    // pins: v3r-1-rulings/C-003 — V3-COW-1: both copy-on-write arms refuse v3 before any write.
    match table.metadata().properties().get(MERGE_MODE_PROP) {
        None => {
            crate::write::row_lineage_guard::refuse_v3_cow_dml_that_would_reassign_row_lineage(
                table,
                "MERGE INTO",
            )?;
            return Ok(MergeMode::CopyOnWrite);
        }
        Some(mode) if mode.trim().eq_ignore_ascii_case("copy-on-write") => {
            crate::write::row_lineage_guard::refuse_v3_cow_dml_that_would_reassign_row_lineage(
                table,
                "MERGE INTO",
            )?;
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
    if format_version != FormatVersion::V2 {
        return Err(DataFusionError::NotImplemented(format!(
            "merge-on-read MERGE INTO writes Parquet position deletes, which require a V2 table \
             (this table is {format_version:?}; V1 has no delete files and V3 mandates deletion \
             vectors, not yet supported) — use write.merge.mode = 'copy-on-write' instead"
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

/// The scratch target adds `_file` + `_pos` to the TARGET's columns; a target column with one of
/// those names would collide with the reserved metadata columns, so refuse it outright. (The
/// source-side match sentinel needs no guard — its name is UUID-suffixed per execution.)
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

/// Every `UPDATE SET` target column must exist in the target schema — a typo'd column would
/// otherwise fall through `rewrite_column`'s name match and the MERGE would commit with the
/// update silently dropped. Names resolve **case-insensitively** (Spark default
/// `spark.sql.caseSensitive=false`; audit BUG-006). Runs on the expanded spec, so it doubles as
/// the guard that no star marker survives into SQL generation (`rewrite_column` would silently
/// treat an unexpanded `UpdateAll` as a no-op).
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
        // Case-insensitive duplicates would silent first-win in `rewrite_column` (Critic-1 Q-003 /
        // Critic-2 SAF-001). Reject like `insert_projection`.
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
    Ok(())
}

/// Resolve `name` against `schema` case-insensitively (Spark `caseSensitive=false`).
/// Returns the schema field's canonical name when exactly one field matches.
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

/// ===========================================================================================
/// Expand `UPDATE SET *` / `INSERT *` markers into explicit per-column clauses — Spark's star
/// resolution: every TARGET column takes the source column that resolves to it BY NAME,
/// case-insensitively (Spark default `spark.sql.caseSensitive=false`;
/// `TableOutputResolver.reorderColumnsByName`). The generated SQL references the ACTUAL source
/// name (`SET a = s."A", …` when the source spells it `A`), so a differently-cased source column
/// still resolves in DataFusion. A target column no source column resolves to is an error up
/// front (never a NULL-fill); two source columns colliding on one target (case-differing or an
/// exact duplicate) is a loud AMBIGUOUS error naming both; extra source columns are ignored,
/// exactly as Spark's analyzer resolves the stars. The source schema comes from a LIMIT-0 plan —
/// planned, never executed.
/// ===========================================================================================
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

    // Resolve every target column to its source column by name (case-insensitively). `values_sql`
    // references the resolved source name so a `SOURCE` column cased differently from the target
    // still binds; the assignment/insert target keeps the TARGET name.
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

/// The source's column names in schema order, from planning (not executing)
/// `SELECT * FROM <source> LIMIT 0` under the statement's source alias. Order is preserved so the
/// case-insensitive resolver can report a stable, original-cased name per column.
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

/// ===========================================================================================
/// The scratch-target schema: every target data column, then `_file` (Utf8) and `_pos` (Int64) —
/// the two reserved metadata columns the pinned core scan surfaces, together the `(_file, _pos)`
/// per-row identity the merge queries group and anti-join on.
/// ===========================================================================================
pub(super) fn scratch_schema(write_schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Field> = write_schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.push(Field::new(FILE_PATH_COL, DataType::Utf8, false));
    fields.push(Field::new(POS_COL, DataType::Int64, false));
    Arc::new(ArrowSchema::new(fields))
}

/// ===========================================================================================
/// A re-scannable partition stream over the pinned target snapshot — the streaming replacement
/// for the former full-target `MemTable` (OTH-001/SAF-001). Each `execute` re-runs
/// `table.scan().snapshot_id(pin).select([data…, _file, _pos]).to_arrow()` and maps every batch
/// onto the scratch schema (columns reordered by name; `_file` cast REE→Utf8) AS IT IS PRODUCED —
/// O(batch), never a full collect, so nothing is retained after the merge join consumes it and each
/// merge query re-scans rather than reading a materialized copy. `snapshot_id == None` (a table
/// with no snapshot yet) yields NO batches, so the merge sees an empty target.
///
/// `(_file, _pos)` is stable across these re-scans: `_file` is the file path and `_pos` is the
/// per-file 0-based physical ordinal (fork `reader.rs` `finish_whole_file_scan_task`), both pure
/// functions of the pinned snapshot's physical layout — independent of scan order or parallelism.
/// That re-scan invariance is what makes streaming (re-scanning) correct where the old
/// materialize-once synthesized counter could not have been re-derived.
/// ===========================================================================================
#[derive(Debug)]
pub(super) struct TargetScanStream {
    table: Table,
    snapshot_id: Option<i64>,
    scratch_schema: SchemaRef,
    select_columns: Vec<String>,
    /// Residual Iceberg predicate (join-key min/max bounds) — `None` = unfiltered scan.
    filter: Option<Predicate>,
    /// When set, passed to `TableScanBuilder::with_concurrency_limit`. `None` keeps the fork
    /// default (`num_cpus`). Session conf: `repark.scan.concurrency-limit`.
    concurrency_limit: Option<usize>,
    /// R-MERGE-FILE-SCAN: when set, only open data files whose path is in this set (whole-file
    /// reads of the subset — survivor-safe). `None` = full snapshot (discovery / insert anti-join).
    file_path_allowlist: Option<std::sync::Arc<std::collections::HashSet<String>>>,
    /// Span current at CONSTRUCTION (inside the merge's instrumented body). `execute()` runs on
    /// DataFusion's threads where the merge context is not entered — parenting
    /// `merge.target_scan` here keeps it nested under the merge in every profile/capture.
    trace_parent: tracing::Span,
}

impl TargetScanStream {
    /// The select list is the target's data columns followed by the two identity metadata columns.
    pub(super) fn new(
        table: Table,
        snapshot_id: Option<i64>,
        scratch_schema: SchemaRef,
        write_schema: &SchemaRef,
        filter: Option<Predicate>,
        concurrency_limit: Option<usize>,
        file_path_allowlist: Option<std::sync::Arc<std::collections::HashSet<String>>>,
    ) -> Self {
        let mut select_columns: Vec<String> = write_schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();
        select_columns.push(FILE_PATH_COL.to_string());
        select_columns.push(POS_COL.to_string());
        Self {
            table,
            snapshot_id,
            scratch_schema,
            select_columns,
            filter,
            concurrency_limit,
            file_path_allowlist,
            trace_parent: tracing::Span::current(),
        }
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
        let trace_parent = self.trace_parent.clone();
        // Open the pinned scan lazily (its future runs on first poll), then flatten its batch
        // stream, conforming each batch onto the scratch schema as it arrives — O(batch).
        // Optional residual filter (R-PERF-MERGE-PRUNE) is the ONLY predicate pushed today.
        // Optional concurrency limit (R-SCAN-CONCURRENCY) — unset keeps the fork num_cpus default.
        // Optional file-path allowlist (R-MERGE-FILE-SCAN): plan_files + filter + ArrowReader
        // so COW rewrite only opens affected data files (whole-file, survivor-safe).
        // Span name is load-bearing for live RUST_LOG phase profiles (R-MERGE-TRACING).
        // Pass-count pins (PERF-19) instrument logical SQL consumptions via `stream_sql`,
        // not `PartitionStream::execute` (DF re-plans make execute counts flaky).
        let opened =
            async move {
                let mut builder = table.scan().snapshot_id(pin).select(select_columns);
                if let Some(predicate) = filter {
                    builder = builder.with_filter(predicate);
                }
                if let Some(limit) = concurrency_limit {
                    builder = builder.with_concurrency_limit(limit);
                }
                let scan = builder.build().map_err(iceberg_err)?;
                let arrow = if let Some(allowlist) = file_path_allowlist {
                    // File-scoped path: plan all tasks, keep only allowlisted data_file_path,
                    // read via the same ArrowReader stack as to_arrow() (identity columns equal).
                    // BUG-009: non-empty allowlist + zero matching tasks refuses loud (path miss
                    // would empty the rewrite stream while COW still deletes affected files).
                    let planned: Vec<_> = scan
                        .plan_files()
                        .await
                        .map_err(iceberg_err)?
                        .try_collect()
                        .await
                        .map_err(iceberg_err)?;
                    let filtered = filter_tasks_to_allowlist_nonempty(planned, allowlist.as_ref())?;
                    let task_stream: iceberg::scan::FileScanTaskStream =
                        Box::pin(futures::stream::iter(filtered.into_iter().map(Ok)));
                    let mut reader = ArrowReaderBuilder::new(table.file_io().clone());
                    if let Some(limit) = concurrency_limit {
                        reader = reader.with_data_file_concurrency_limit(limit);
                    }
                    reader.build().read(task_stream).map_err(iceberg_err)?
                } else {
                    scan.to_arrow().await.map_err(iceberg_err)?
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

/// Test-only **logical** target-SQL pass counter (PERF-19 / Q16).
///
/// Counts repark-owned `stream_sql` invocations during a MERGE — each is one planned
/// consumption of a MERGE-generated statement, immune to DataFusion
/// `PartitionStream::execute` re-plan multiplicity under parallel cargo tests.
/// Armed via [`MERGE_TEST_INSTRUMENTS`] task-local (not a process-global slot).
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

/// Map one pinned-scan batch onto the scratch schema: reorder columns by name, cast `_file`
/// (a REE(Utf8) constant from the scan) to `Utf8` and `_pos` to `Int64`, pass the data columns
/// through. O(batch) — the streaming per-batch analogue of the former `scan_target` collect loop.
fn conform_scan_batch(scratch: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch> {
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

/// Register the pinned target as a re-scannable STREAMING relation under a collision-proof scratch
/// name — a `StreamingTable` over `source`, NOT a `MemTable`, so the whole target is never
/// collected (OTH-001/SAF-001). The caller deregisters it when the merge finishes (success or
/// failure). This is the seam the memory-profile pin mutates: swapping the `StreamingTable` for a
/// collect-then-`MemTable` here materializes the entire target up front and turns the pin RED.
pub(super) fn register_streaming_target(
    ctx: &SessionContext,
    scratch_schema: SchemaRef,
    source: Arc<dyn PartitionStream>,
) -> Result<String> {
    let name = format!("__repark_merge_target_{}", Uuid::new_v4().simple());
    let provider = StreamingTable::try_new(scratch_schema, vec![source])?;
    ctx.register_table(name.as_str(), Arc::new(provider))?;
    Ok(name)
}

/// ===========================================================================================
/// The merge pipeline over the registered scratch target. The clause RESOLUTION is identical for
/// both write modes — same streamed target, same `(_file, _pos)` identity, same first-match-wins
/// prefix negation, same cardinality check; only the WRITE and the COMMIT differ, so the
/// mode-independent prologue lives here and the two arms diverge below.
/// ===========================================================================================
/// The resolved target of one MERGE execution — everything both write arms need about the table
/// they are committing against, bundled so the two arms share one signature.
struct MergeTarget<'a> {
    /// The loaded Iceberg target table.
    table: &'a Table,
    /// The target's Arrow write schema (the Iceberg current schema, Arrow-converted).
    write_schema: &'a SchemaRef,
    /// The snapshot every merge query reads — the OCC `validate_from_snapshot` anchor. `None` for a
    /// table with no snapshot yet (an empty target, which depended on no prior state).
    snapshot_id: Option<i64>,
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
    };
    // R-MERGE-ONEPASS Stage A: cardinality is folded into match discovery (affected_files /
    // mutated_positions) — no separate full-target cardinality pass. Insert-only MERGEs still
    // skip discovery (same as pre-Stage-A when matched is empty).
    match mode {
        MergeMode::CopyOnWrite => plan_and_commit_cow(ctx, catalog, spec, target, &sql).await,
        MergeMode::MergeOnRead => plan_and_commit_mor(ctx, catalog, spec, target, &sql).await,
    }
}

/// ===========================================================================================
/// The copy-on-write arm: affected-file discovery → whole-file rewrite + insert queries → Parquet
/// write → one `OverwriteFiles` commit. Every data file containing at least one mutated row is
/// rewritten in full (survivors carried, matched rows updated, deleted rows dropped).
/// ===========================================================================================
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
    } = *target;
    let (affected, new_files) = async {
        let affected = if spec.matched.is_empty() {
            Vec::new()
        } else {
            affected_files(ctx, sql, skip_cardinality(spec)).await?
        };
        // R-MERGE-STREAM-OUT: pipe rewrite + insert SQL streams into concurrent file writers
        // without collecting the full change set first.
        let mut streams: Vec<std::pin::Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> =
            Vec::new();
        // Scratches to drop on every exit of this block (file-scoped target and/or path table).
        // Critic-octo C1-Q3: `stream_sql` / insert plan failures must not leave scratches registered.
        let mut rewrite_scratches = MergeScratchGuard::new(ctx);
        if !affected.is_empty() {
            // R-MERGE-FILE-SCAN: rewrite against a file-scoped target scan when enabled.
            // Scout #18: when the allowlist already scopes tasks, drop redundant `_file IN (...)`;
            // otherwise register a path MemTable and semi-join (no giant path literal list).
            let rewrite_scratch = maybe_register_file_scoped_rewrite_target(
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
                let path_table = register_affected_paths_table(ctx, &affected)?;
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
        // Explicit drop before Ok so cleanup is ordered before the join span ends (Drop also
        // runs on Err paths via the guard).
        drop(rewrite_scratches);
        Ok::<_, DataFusionError>((affected, write_result?))
    }
    .instrument(tracing::info_span!("merge.join"))
    .await?;

    let file_count = new_files.len() as u64;
    let affected_entries = resolve_affected_data_files(table, &affected).await?;
    // write_data span wraps only the file count after streaming write (join span covers SQL+write
    // interleaving for stream-out; keep a commit-adjacent write_data for phase profiles).
    tracing::info_span!("merge.write_data", files = file_count).in_scope(|| ());
    commit(catalog, table, snapshot_id, affected_entries, new_files)
        .instrument(tracing::info_span!("merge.commit", files = file_count))
        .await
}

/// ===========================================================================================
/// Register a file-scoped streaming target for COW rewrite when conf allows and `affected`
/// is a non-empty proper subset of the snapshot (empty/all → None, keep full-scan rewrite).
/// ===========================================================================================
fn maybe_register_file_scoped_rewrite_target(
    ctx: &SessionContext,
    table: &Table,
    snapshot_id: Option<i64>,
    write_schema: &SchemaRef,
    affected: &[String],
) -> Result<Option<String>> {
    if !file_scoped_rewrite_from_ctx(ctx) || affected.is_empty() {
        return Ok(None);
    }
    // When every live data file is affected, file-scoping is a no-op — keep the full path
    // (avoids a second registration and matches "ALL files" escape in the slate brief).
    // We cannot cheaply know "all" without another plan_files; treat a single-file allowlist
    // as always worth scoping, multi-file always scope when conf is on (correct even if all).
    // Plan: "Keep the unfiltered path when the affected set is empty or is ALL files".
    // ALL-files detection: deferred to a full plan_files compare only if needed; for local
    // multi-file pins, affected ⊂ all. Scoping when allowlist == all files still opens N tasks.
    let allowlist = allowlist_from_paths(affected);
    let scratch = scratch_schema(write_schema);
    let scan_concurrency = scan_concurrency_from_ctx(ctx);
    let source: Arc<dyn PartitionStream> = Arc::new(TargetScanStream::new(
        table.clone(),
        snapshot_id,
        Arc::clone(&scratch),
        write_schema,
        None,
        scan_concurrency.concurrency_limit,
        Some(allowlist),
    ));
    let name = register_streaming_target(ctx, scratch, source)?;
    Ok(Some(name))
}

/// ===========================================================================================
/// The merge-on-read arm (Group T): data files are left COMPLETELY untouched. Every row a WHEN
/// MATCHED clause mutates — DELETE and UPDATE alike — contributes its `(_file, _pos)` identity to a
/// position-delete file; UPDATE additionally re-emits the row's NEW values as a fresh data-file row
/// (merge-on-read UPDATE == delete-old + insert-new), and WHEN NOT MATCHED inserts append as usual.
/// Position-delete files and new data files commit together in ONE `RowDelta` snapshot; the next
/// scan applies the deletes (fork row R117), so the visible table state is identical to what the
/// copy-on-write arm would have produced for the same source and target.
///
/// Stage B folds cardinality + pos-deletes + UPDATE projection into [`MergeSql::matched_work_sql`]
/// (one INNER JOIN); inserts remain a separate LEFT JOIN anti-scan.
/// ===========================================================================================
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
    } = *target;
    // R-MERGE-ONEPASS Stage B (MoR): one INNER JOIN pass yields cardinality + position-delete
    // pairs + UPDATE new-values (matched_work). Inserts remain a separate LEFT JOIN anti-scan.
    // R-MERGE-STREAM-OUT: UPDATE batches + insert SQL streams feed concurrent writers.
    let (pairs, data_files) = async {
        let mut streams: Vec<std::pin::Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>> =
            Vec::new();
        let pairs = if spec.matched.is_empty() {
            Vec::new()
        } else {
            let (pairs, update_batches) =
                matched_work_mor(ctx, sql, write_schema, skip_cardinality(spec)).await?;
            if !update_batches.is_empty() {
                streams.push(Box::pin(futures::stream::iter(
                    update_batches.into_iter().map(Ok),
                )));
            }
            pairs
        };
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
    commit_row_delta(catalog, table, snapshot_id, pairs, data_files, concurrency).await
}

/// ===========================================================================================
/// R-MERGE-STREAM-OUT: cast each batch to the write schema and pipe into the same streaming
/// writers CTAS uses (`write_*_from_stream_with_concurrency`). Peak memory is O(batch × K),
/// and the first PUT can start before the SQL stream is exhausted.
///
/// Shared write seam for BOTH modes (COW rewrite survivors + inserts, `MoR` updated-new-values +
/// inserts). A4 + Group R: a partitioned target routes through the SAME A1/U1 fanout `append`
/// uses (fork computed-transform mode). QUAL-08: the superseded collect-then-write adapter
/// (`write_new_data_files` / `insert_rows`) was deleted — stream-out is the only path.
/// ===========================================================================================
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
            Ok(Some(cast_one_batch_to_write_schema(&write_schema, &batch)?))
        }
    });
    // Pin the stream so partitioned/unpartitioned helpers get Unpin.
    let cast_stream = std::pin::pin!(cast_stream);
    if table.metadata().default_partition_spec().is_unpartitioned() {
        write_data_files_from_stream_with_concurrency(table, cast_stream, concurrency).await
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

/// ===========================================================================================
/// Resolve affected `_file` paths back to their full [`DataFile`] entries by walking the pinned
/// snapshot's data manifests. The full entries matter: the fork's
/// `validate_no_conflicting_deletes` only runs against `deleted_data_files` — committing bare
/// paths (`delete_files`) leaves the snapshot-isolation validation structurally unarmed, and a
/// concurrent merge-on-read `DELETE` whose delete file targets a rewritten data file would be
/// silently resurrected.
/// ===========================================================================================
pub(super) async fn resolve_affected_data_files(
    table: &Table,
    affected: &[String],
) -> Result<Vec<DataFile>> {
    // Span is the P2a hour-0 measurement seam for scout #6 (manifest walk share of MERGE wall).
    async {
        if affected.is_empty() {
            return Ok(Vec::new());
        }
        let metadata = table.metadata();
        let snapshot = metadata.current_snapshot().ok_or_else(|| {
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
        // Serial walk retained: P2a hour-0 on local-fs attributes ≪10% of MERGE wall to this
        // resolve (see task/p2a-cdf-merge-ledger.md) → scout #6 concurrent load closed as WIN.
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

/// Spark's `MERGE_CARDINALITY_VIOLATION` message — shared by Stage A discovery and any residual
/// callers so existing pins that match the string stay green UNCHANGED.
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

/// ===========================================================================================
/// R-MERGE-ONEPASS Stage A + PERF-01: stream match discovery and fold to **distinct affected
/// `_file` paths** (the COW rewrite set). Cardinality is enforced per grouped row in the same
/// pass (`match_count > 1` → same error as before, unless [`skip_cardinality`]).
///
/// Driver-side retained state is **O(distinct affected files)**, not O(matched rows): only a
/// first-seen path allocates a `String` into the result set (path interning pattern from `MoR`
/// Stage B / P2a). The previous collect of `Vec<(String, i64, i64, i64)>` per matched row is
/// gone — that was ~7–10 GB at 50 M matches, outside the DataFusion memory pool.
/// ===========================================================================================
async fn affected_files(
    ctx: &SessionContext,
    sql: &MergeSql<'_>,
    skip_cardinality: bool,
) -> Result<Vec<String>> {
    let mut stream = stream_sql(ctx, &sql.match_discovery_sql()).await?;
    // Single owned `String` per distinct path (HashSet only; collect at end). First-seen
    // membership uses `contains(&str)` so non-first rows never allocate (PERF-01 / C1-Q-003).
    let mut seen: HashSet<String> = HashSet::new();
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        fold_discovery_batch_into_affected(&batch, &mut seen, skip_cardinality)?;
    }
    Ok(seen.into_iter().collect())
}

/// Process one Stage A discovery batch: cardinality check + distinct mutated `_file` paths.
///
/// Allocates a path `String` only when the path is first seen as mutated (PERF-01).
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
        // Critic-octo C3-Q1: same fail-loud null flags as Stage B (C1-S1).
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
/// Armed via [`MERGE_TEST_INSTRUMENTS`] task-local.
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

/// ===========================================================================================
/// ===========================================================================================
/// R-MERGE-ONEPASS Stage B (`MoR`): one INNER JOIN pass for cardinality + pos-deletes + UPDATE rows.
///
/// Layout per batch: `_file`, `_pos`, `match_count`, `is_mutated`, `is_update`, then one column
/// per write-schema field (UPDATE projection). `match_count > 1` → cardinality error unless
/// [`skip_cardinality`] (same message).
///
/// Scout #2: streams the join result (no full `collect`); interns `_file` paths so the seen-pair
/// set and pos-delete list do not re-`to_string` every row for the same data file.
/// P2a: intern stores `Arc<str>` and `PositionDeletePair` shares that Arc (no path String clone
/// into `position_delete.rs`).
/// ===========================================================================================
async fn matched_work_mor(
    ctx: &SessionContext,
    sql: &MergeSql<'_>,
    write_schema: &SchemaRef,
    skip_cardinality: bool,
) -> Result<(
    Vec<crate::write::position_delete::PositionDeletePair>,
    Vec<RecordBatch>,
)> {
    // === r20 P2a: merge ===
    let mut stream =
        update_stream_checked(ctx, sql, &sql.matched_work_sql(write_schema), write_schema).await?;
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

/// ===========================================================================================
/// Process one Stage B `matched_work` batch: cardinality check, path intern, pos-delete pairs,
/// and UPDATE-row projection slices.
/// ===========================================================================================
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
        // Critic-octo C1-S1 / C3-Q1: flag columns are CASE/window outputs and must be non-null.
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
        // Slice UPDATE projection columns first, then take rows. Do NOT force `write_schema`
        // types here — DF may emit widened ints; `write_new_data_files_from_stream` casts via
        // `cast_one_batch_to_write_schema` per batch.
        let data_fields: Vec<datafusion::arrow::datatypes::Field> = (0..data_field_count)
            .map(|index| {
                let source = write_schema.field(index);
                datafusion::arrow::datatypes::Field::new(
                    source.name(),
                    batch.column(5 + index).data_type().clone(),
                    source.is_nullable(),
                )
            })
            .collect();
        let data_columns: Vec<ArrayRef> = (0..data_field_count)
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

/// ===========================================================================================
/// Fail loud when a Stage A/B Int64 flag column is null (Arrow `.value` would silently yield 0).
/// Shared by match discovery and `matched_work` consume (critic-octo C1-S1 / C3-Q1).
/// ===========================================================================================
fn require_non_null_i64(array: &Int64Array, row: usize, label: &str) -> Result<i64> {
    if array.is_null(row) {
        return Err(DataFusionError::Internal(format!(
            "matched_work/match-discovery row has a NULL {label}"
        )));
    }
    Ok(array.value(row))
}

/// Column name for the scout-#18 affected-path `MemTable` (semi-join key).
const AFFECTED_PATHS_COL: &str = "path";

/// ===========================================================================================
/// RAII guard for MERGE scratch tables registered during COW rewrite (file-scoped target and/or
/// path `MemTable`). Drops every name via [`deregister_merge_scratch`] on success **and** on early
/// `?` exits (critic-octo C1-Q3).
/// ===========================================================================================
struct MergeScratchGuard<'a> {
    ctx: &'a SessionContext,
    names: Vec<String>,
}

impl<'a> MergeScratchGuard<'a> {
    fn new(ctx: &'a SessionContext) -> Self {
        Self {
            ctx,
            names: Vec::new(),
        }
    }

    fn push(&mut self, name: String) {
        self.names.push(name);
    }
}

impl Drop for MergeScratchGuard<'_> {
    fn drop(&mut self) {
        for name in &self.names {
            let _ = deregister_merge_scratch(self.ctx, name);
        }
    }
}

/// ===========================================================================================
/// Register a one-column `MemTable` of affected `_file` paths for the COW rewrite semi-join
/// (scout #18 else-path when the task allowlist does not already scope the scan).
/// ===========================================================================================
fn register_affected_paths_table(ctx: &SessionContext, affected: &[String]) -> Result<String> {
    let name = format!("__repark_merge_aff_paths_{}", Uuid::new_v4().simple());
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        AFFECTED_PATHS_COL,
        DataType::Utf8,
        false,
    )]));
    let path_array = StringArray::from(
        affected
            .iter()
            .map(std::string::String::as_str)
            .collect::<Vec<_>>(),
    );
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(path_array)])?;
    let table = MemTable::try_new(schema, vec![vec![batch]]).map_err(|error| {
        DataFusionError::Internal(format!("affected-path MemTable build failed: {error}"))
    })?;
    ctx.register_table(name.as_str(), Arc::new(table))?;
    Ok(name)
}

/// Builds the SQL the merge runs. All user-provided fragments (`ON`, clause predicates, `SET`
/// expressions, `VALUES` expressions) are inlined verbatim so they resolve against the aliases
/// the user wrote; every generated reference is fully qualified and quoted.
struct MergeSql<'a> {
    spec: &'a MergeSpec,
    target_name: &'a str,
    /// The per-execution match-sentinel column name (UUID-suffixed — see `MATCH_FLAG_PREFIX`).
    match_flag: &'a str,
}

impl MergeSql<'_> {
    /// `FROM` fragment for the scratch target, aliased as the statement's target alias.
    fn target_from(&self) -> String {
        // Engine UUID names today; still route through quote_ident (octo C2-SEC-002).
        format!(
            "{} AS {}",
            quote_ident(self.target_name),
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

    /// First-match clause id over an ordered predicate list — O(C) CASE, not O(C²) AND-chains
    /// (scout #18). Evaluates WHEN arms in declaration order; 3VL via [`Self::applies`].
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
    ///
    /// Scout #18 rewrites the historical O(C²) `NOT applies(0) AND … AND NOT applies(i-1)`
    /// into an O(C) `clause_id` CASE: `clause_id IS NULL OR clause_id >= i`. Callers that need
    /// "clause i owns the row" should use `clause_id = i` (see [`Self::insert_sql`]).
    /// Production SQL uses [`Self::clause_id_case`] / [`Self::matched_clause_id_expr`] directly;
    /// this helper remains for unit pins and the generation microbench twin.
    #[cfg(test)]
    fn prior_clauses_do_not_apply(predicates: &[Option<&str>], index: usize) -> String {
        if index == 0 {
            return "TRUE".to_string();
        }
        let clause_id = Self::clause_id_case(predicates);
        format!("(({clause_id}) IS NULL OR ({clause_id}) >= {index})")
    }

    /// Pre-#18 O(C²) AND-chain of `NOT applies(j)` — kept only so the generation microbench can
    /// report an honest before/after against the same inputs (not used in production SQL).
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

    /// Matched-side first-match clause id (scout #18): `NULL` when the row is not matched or
    /// no WHEN MATCHED clause applies; otherwise the 0-based declaration index of the first
    /// applying clause. O(C) CASE text, shared by rewrite / update / delete predicates.
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
    /// Scout #18: O(1) lookup per (column, clause) instead of linear `.find` over assignments.
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
        write_schema
            .fields()
            .iter()
            .map(|field| {
                self.rewrite_column_with_maps(&maps, field.name(), Some(field.data_type()))
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// R-MERGE-ONEPASS Stage A: one grouped join pass — `match_count` (cardinality) + `is_mutated`
    /// (feeds COW affected files). `source JOIN target` keeps the small source as hash BUILD.
    /// `MAX(CASE WHEN mutated THEN 1 ELSE 0 END)` is true when any matched clause mutates the
    /// target row. (`MoR` Stage B uses [`Self::matched_work_sql`] instead.)
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

    /// The disjunction "some WHEN MATCHED clause mutates this row" — shared by the copy-on-write
    /// affected-file discovery and the merge-on-read position-delete set. No prefix negation is
    /// needed: EVERY matched clause mutates the row it applies to (UPDATE rewrites it, DELETE drops
    /// it), so "the first applicable clause mutates it" is equivalent to "any clause applies".
    fn mutated(&self) -> String {
        self.spec
            .matched
            .iter()
            .map(|clause| Self::applies(clause.predicate_sql.as_deref()))
            .collect::<Vec<_>>()
            .join(" OR ")
    }

    /// Stage B merge-on-read: one INNER JOIN producing identity + match flags + UPDATE projection.
    /// Window `count(*)` over `(_file, _pos)` enforces cardinality without a second pass.
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

    /// True when the row's FIRST applicable clause is an UPDATE (`clause_id` ∈ update indices) —
    /// the mirror of [`Self::delete_applies`]. Comparisons are `COALESCE`-wrapped so a NULL
    /// `clause_id` (unmatched / no clause) is 2-valued FALSE, never SQL UNKNOWN (`WHERE` would
    /// drop unmatched survivors under `NOT (NULL = id)`).
    fn update_applies(&self) -> String {
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

    /// Rewrite SQL against a file-scoped target whose task allowlist already restricts `_file`
    /// (scout #18): no redundant `_file IN (...)` path list.
    fn rewrite_sql_allowlisted(
        &self,
        target_table_name: &str,
        write_schema: &ArrowSchema,
    ) -> String {
        let ta = &self.spec.target_alias;
        let projection = self.rewrite_projection(write_schema);
        let deleted = self.delete_applies();
        let target = format!("{} AS {ta}", quote_ident(target_table_name));
        format!(
            "SELECT {projection} FROM {target} LEFT JOIN {source} ON {on} \
             WHERE NOT ({deleted})",
            source = self.source_from(),
            on = self.spec.on_sql,
        )
    }

    /// Rewrite SQL against the full snapshot target, semi-joined to a registered path `MemTable`
    /// (scout #18 else-path — no giant `_file IN (…)` literal list).
    fn rewrite_sql_path_semijoin(
        &self,
        target_table_name: &str,
        path_table_name: &str,
        write_schema: &ArrowSchema,
    ) -> String {
        let ta = &self.spec.target_alias;
        let projection = self.rewrite_projection(write_schema);
        let deleted = self.delete_applies();
        let quoted_target = quote_ident(target_table_name);
        let quoted_paths = quote_ident(path_table_name);
        format!(
            "SELECT {projection} FROM {quoted_target} AS {ta} \
             INNER JOIN {quoted_paths} AS __repark_aff \
               ON {ta}.\"{FILE_PATH_COL}\" = __repark_aff.\"{AFFECTED_PATHS_COL}\" \
             LEFT JOIN {source} ON {on} \
             WHERE NOT ({deleted})",
            source = self.source_from(),
            on = self.spec.on_sql,
        )
    }

    /// The `CASE` projecting one output column through the ordered UPDATE clauses (unit pins).
    #[cfg(test)]
    fn rewrite_column(&self, column: &str) -> String {
        let maps = self.update_assignment_lookup();
        self.rewrite_column_with_maps(&maps, column, None)
    }

    /// Like [`Self::rewrite_column`] but reuses a pre-built assignment lookup (scout #18).
    /// `store_type` is the target column type: wrap THEN in `arrow_cast` so CASE unifies
    /// after the ANSI gate (bool→string is store-assignable but CASE cannot coerce it).
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
        // DELETE rows never reach this projection (filtered in the WHERE); a clause that does
        // not assign this column emits no branch — mutual exclusion via clause_id, ELSE = original.
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
        if branches.is_empty() {
            format!("{original} AS {quoted}")
        } else {
            format!(
                "CASE ({clause_id}) {} ELSE {original} END AS {quoted}",
                branches.join(" "),
                clause_id = self.matched_clause_id_expr(),
            )
        }
    }

    /// True when the row's first applicable clause is a DELETE.
    ///
    /// `COALESCE`-hardened: `clause_id = delete_index` is UNKNOWN when `clause_id` is NULL
    /// (unmatched target row on the `LEFT JOIN`). `WHERE NOT (UNKNOWN)` drops the row — that would
    /// silently delete every co-located survivor in an affected file. Force 2-valued FALSE.
    fn delete_applies(&self) -> String {
        let delete_ids: Vec<String> = self
            .spec
            .matched
            .iter()
            .enumerate()
            .filter(|(_, clause)| matches!(clause.action, MatchedAction::Delete))
            .map(|(index, _)| index.to_string())
            .collect();
        if delete_ids.is_empty() {
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
        }
    }

    /// The rows insert clause `index` adds: source rows with no target match — a `LEFT JOIN`
    /// anti-form keyed on the target's `_pos` identity column (`_pos IS NULL` ⇔ the LEFT JOIN
    /// found no target row; `_pos` is required in the target, so it is NULL only on non-match),
    /// no correlated subquery, first-match-wins among the NOT MATCHED clauses (`clause_id = index`),
    /// projected onto the full target column list.
    ///
    /// **Source-only scope (audit M4).** The inner subquery projects ONLY the source columns
    /// plus a sentinel copy of the target `_pos`; the clause predicates and VALUES expressions
    /// evaluate in the OUTER query, where the target alias does not exist — so a target-column
    /// reference in a NOT MATCHED condition or VALUES fails resolution loudly (Spark resolves
    /// `InsertAction` against the source plan only and raises `UNRESOLVED_COLUMN`; the previous
    /// shape silently read the LEFT-JOIN NULL). A bare column name now resolves to the SOURCE
    /// column, also matching Spark's source-only resolution.
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

/// Sentinel alias carrying the target `_pos` through the source-only insert scope: named so no
/// plausible user column collides (a genuine collision surfaces as a loud ambiguity error).
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

/// ===========================================================================================
/// Write batches as Parquet data files through iceberg's writer stack, unpartitioned (v1).
///
/// A thin `Vec` adapter over [`write_data_files_from_stream`]: the batches are replayed as an
/// in-memory stream so the single writer-construction + drive loop lives in ONE place. Uses
/// [`WriteConcurrency::default`] (session default 4 concurrent file writers). Prefer
/// [`write_data_files_with_concurrency`] when the caller has a resolved session knob.
///
/// # Errors
/// Returns a DataFusion error if the table is not Parquet-default, writer setup fails, or a
/// batch cannot be written/closed.
/// ===========================================================================================
pub async fn write_data_files(table: &Table, batches: Vec<RecordBatch>) -> Result<Vec<DataFile>> {
    write_data_files_with_concurrency(table, batches, WriteConcurrency::default()).await
}

/// ===========================================================================================
/// [`write_data_files`] with an explicit [`WriteConcurrency`] (session conf
/// `repark.write.max-concurrent-files`).
/// ===========================================================================================
///
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

/// ===========================================================================================
/// Stream batches into unpartitioned Parquet `DataFileWriter`(s), writing each batch the instant
/// the source produces it — the bounded-memory CTAS write path (WG-2, audit SAF-002). Peak memory
/// is O(batch × concurrent open writers), never O(result).
///
/// With [`WriteConcurrency::max_concurrent_files`] `= 1` this is the historical serial loop
/// (one sink, write batch _k_ before polling batch _k+1_). With `K > 1`, batches are
/// round-robined to **K independent file writers** so their `FileIO` flushes/uploads can overlap
/// (the S3 wall-clock win for MERGE/CTAS). Iceberg data files within one snapshot are unordered
/// — fan-out does not change commit semantics. A mid-stream source error aborts WITHOUT finishing
/// remaining writers — no partial data file set is returned.
///
/// # Errors
/// Returns a DataFusion error if the table is not Parquet-default, writer setup fails, the source
/// stream yields an error, or a batch cannot be written/closed.
/// ===========================================================================================
pub async fn write_data_files_from_stream<S>(table: &Table, stream: S) -> Result<Vec<DataFile>>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    write_data_files_from_stream_with_concurrency(table, stream, WriteConcurrency::default()).await
}

/// ===========================================================================================
/// [`write_data_files_from_stream`] with explicit [`WriteConcurrency`].
/// ===========================================================================================
///
/// # Errors
/// Same as [`write_data_files_from_stream`], plus a plan error when
/// `max_concurrent_files < 1`.
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

    // Validate format once before opening any writer.
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format =
        DataFileFormat::from_str(&table_props.write_format_default).map_err(iceberg_err)?;
    if file_format != DataFileFormat::Parquet {
        return Err(DataFusionError::NotImplemented(format!(
            "MERGE INTO writes only Parquet data files yet (table default is {file_format})"
        )));
    }

    // Build one unpartitioned writer (shared construction path for serial + each parallel worker).
    let build_writer = || async { build_unpartitioned_data_file_writer(table).await };

    if max_concurrent == 1 {
        let writer = build_writer().await?;
        return write_stream_into(ForkBatchWriter { inner: writer }, stream).await;
    }

    write_stream_into_parallel(max_concurrent, stream, build_writer).await
}

/// Open one unpartitioned Parquet `DataFileWriter` for `table` (unique file-name UUID per call).
async fn build_unpartitioned_data_file_writer(table: &Table) -> Result<impl IcebergWriter + use<>> {
    let table_props = table.metadata().table_properties().map_err(iceberg_err)?;
    let file_format =
        DataFileFormat::from_str(&table_props.write_format_default).map_err(iceberg_err)?;
    let parquet_builder = ParquetWriterBuilder::new_with_match_mode(
        crate::write::writer_props::writer_properties_for(table)?,
        table.metadata().current_schema().clone(),
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

/// A minimal batch sink: write batches, then close into the produced data files. It lets the
/// streaming driver [`write_stream_into`] stay generic over the fork's `DataFileWriter` (via
/// [`ForkBatchWriter`]) AND a counting test writer, using a NATIVE `async fn` trait so the test
/// writer needs no `#[async_trait]` dependency — the fork's `IcebergWriter` is `#[async_trait]`
/// (a boxed-future object model) and cannot be hand-implemented here without adding that crate.
trait BatchWriter {
    async fn write_batch(&mut self, batch: RecordBatch) -> Result<()>;
    async fn finish(&mut self) -> Result<Vec<DataFile>>;
}

/// Production [`BatchWriter`] over the fork's `DataFileWriter` (any [`IcebergWriter`]); the generic
/// bound keeps the concrete rolling-writer type unnamed. Folds the fork's iceberg error into the
/// DataFusion error this crate carries.
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

/// ===========================================================================================
/// Drive a record-batch stream into a **single** [`BatchWriter`], writing each batch the instant
/// it is produced and finishing only after the source is exhausted. This is the serial (K=1)
/// streaming seam CTAS relies on for bounded memory and for the interleaving pin: the write of
/// batch _k_ completes before batch _k+1_ is polled. A source error aborts WITHOUT finishing —
/// no partial data file is returned.
/// ===========================================================================================
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

/// ===========================================================================================
/// Fan batches to `max_concurrent` independent writers (round-robin). Channel capacity is 1 per
/// worker so peak buffered memory stays O(K × batch). Workers and the dispatcher are polled
/// jointly (no spawn) so backpressure works on a single-threaded async runtime.
///
/// **Abort (P1-R1):** a shared abort flag distinguishes a clean channel close (source exhausted →
/// `finish`) from an abort close (source or worker error → drop sinks WITHOUT `finish`, so no
/// partial data files are closed/uploaded). Worker errors are preferred over the dispatcher's
/// secondary "channel closed" error when both fire.
/// ===========================================================================================
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

/// ===========================================================================================
/// Parallel drive over already-built [`BatchWriter`] sinks (production + test double path).
/// A mid-stream source error aborts WITHOUT finishing any sink — no partial data file is returned.
/// ===========================================================================================
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

    // Shared abort flag: set on source error or worker write error. Workers check after recv
    // (and between batches) so they never `finish()` a sink after abort — P1-R1.
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
                        // Worker side closed (usually after a write error). Prefer the worker's
                        // root-cause when collecting results — surface a secondary message only
                        // if no worker error is present.
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

    // Prefer the first worker root-cause over the dispatcher's secondary channel-closed error
    // (P1-R1). Worker Ok(empty) on abort is not an error.
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

/// Iceberg standard table property selecting MERGE isolation (Java default: serializable).
const WRITE_MERGE_ISOLATION_LEVEL: &str = "write.merge.isolation-level";

/// Isolation for overwrite / row-delta commit. MERGE reads
/// `write.merge.isolation-level` (default serializable; `snapshot` honored).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IsolationLevel {
    /// Reject concurrent conflicting data and deletes.
    Serializable,
    /// Reject only concurrent conflicting deletes.
    Snapshot,
}

/// Java row-delta recipe. MERGE keeps the UPDATE/MERGE guard set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RowDeltaKind {
    /// MERGE — UPDATE/MERGE validations (Java L251-254 + isolation-gated data).
    Merge,
    /// SQL DELETE — no `validate_deleted_files` / `validate_no_conflicting_delete_files`.
    Delete,
}

/// Verb recipe + isolation for [`commit_row_delta_kind`].
/// MERGE isolation comes from `resolve_merge_isolation`, not a hard-wired serializable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct RowDeltaPolicy {
    pub kind: RowDeltaKind,
    pub isolation: IsolationLevel,
}

/// Copy of `predicate_dml::resolve_isolation_property` onto `write.merge.isolation-level`
/// (no trim, `to_ascii_lowercase`, default serializable, garbage ⇒ Plan).
fn resolve_merge_isolation(table: &Table) -> Result<IsolationLevel> {
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

/// ===========================================================================================
/// One atomic commit. Files rewritten ⇒ `OverwriteFiles` under the `ENGINE_CONTRACT` §5 COW
/// SERIALIZABLE-isolation recipe (row 142, Java's MERGE default; snapshot drops
/// `validate_no_conflicting_data`) — BOTH
/// `validate_no_conflicting_deletes` AND `validate_no_conflicting_data` pinned to the scanned
/// snapshot, so neither a concurrent row-level delete on a rewritten file nor a concurrent add
/// matching the ON condition (the F-BR-1 silent duplicate) slips past; nothing rewritten but rows
/// inserted ⇒ an add-only `OverwriteFiles` (recorded `Operation::Append`) carrying the same §5
/// serializable `validate_no_conflicting_data` guard on the SAME pin — an insert-only MERGE also
/// raced its pinned NOT-MATCHED set against a concurrent add, so it cannot append blindly; nothing
/// at all ⇒ no commit. Every commit is stamped with a unique `engine.operation-id`
/// snapshot-summary property (§8). On `tx.commit` `Err`, [`commit_overwrite`] best-effort
/// deletes the new-file paths then re-raises the original error (M14).
/// ===========================================================================================
async fn commit(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    affected: Vec<DataFile>,
    new_files: Vec<DataFile>,
) -> Result<()> {
    let isolation = resolve_merge_isolation(table)?;
    commit_overwrite(catalog, table, snapshot_id, affected, new_files, isolation).await
}

/// Copy-on-write overwrite commit. MERGE calls [`commit`], which resolves
/// `write.merge.isolation-level` (default serializable).
///
/// On `tx.commit` error, best-effort-delete the `new_files` paths only (never
/// `affected`) and re-raise the original error. See [`abort`].
pub(super) async fn commit_overwrite(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    affected: Vec<DataFile>,
    new_files: Vec<DataFile>,
    isolation: IsolationLevel,
) -> Result<()> {
    if affected.is_empty() && new_files.is_empty() {
        return Ok(());
    }
    let new_file_paths = abort::written_file_paths(&new_files);
    let summary = HashMap::from([(OPERATION_ID_PROP.to_string(), Uuid::new_v4().to_string())]);
    let tx = Transaction::new(table);
    let tx = if affected.is_empty() {
        // Insert-only MERGE (no rewrites) STILL pinned snapshot S to compute its NOT-MATCHED set,
        // so a concurrent commit that ADDED data matching the ON condition between S and this
        // commit would turn a "not matched ⇒ INSERT" decision into a silently duplicated row.
        // Route the append through `overwrite_files` — an add-only overwrite records
        // `Operation::Append` (fork `BaseOverwriteFiles.operation()`), identical snapshot semantics
        // to `fast_append` — WITH the fork's serializable-isolation guard `validate_no_conflicting_data`
        // armed on the pinned snapshot: the commit is rejected with a non-retryable `DataInvalid` if
        // ANY concurrently-added data file could match the conservative `AlwaysTrue` conflict filter
        // (ENGINE_CONTRACT §5 COW/MERGE serializable row — the same conservative filter the rewrite
        // arm below uses; the rewrite arm carries the sibling `validate_no_conflicting_deletes`, which
        // is a dead no-op for a pure append and so is intentionally NOT set here). The filter +
        // from-snapshot are inert without `validate_no_conflicting_data` (fork docstrings), so that
        // call is the load-bearing one. As in the rewrite arm, the from-snapshot pin is armed only
        // when the scan captured a snapshot — Java runs no from-snapshot validation for an
        // empty-at-read-time table, which depended on no prior state.
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
        action.apply(tx).map_err(iceberg_err)?
    } else {
        // A MIXED MERGE (a WHEN MATCHED clause rewrote files) commits the COW `OverwriteFiles` under
        // SERIALIZABLE isolation — fork ENGINE_CONTRACT §5 row 142, Java's MERGE default — carrying
        // BOTH validations against the pinned snapshot:
        //   * `validate_no_conflicting_deletes` — `delete_data_files` (FULL entries, NOT path-only
        //     `delete_files`: the fork's check runs only against `deleted_data_files`, so a bare-path
        //     removal leaves it structurally unarmed) rejects a concurrent row-level delete that
        //     applies to a data file this rewrite REMOVES.
        //   * `validate_no_conflicting_data` — rejects a concurrent ADD of a data file that could
        //     match the conservative `AlwaysTrue` conflict filter. THIS is the F-BR-1 fix: the
        //     rewrite arm ALSO pinned snapshot S to compute its NOT-MATCHED set, so a concurrent
        //     append matching the ON condition between S and this commit would turn a
        //     "not matched ⇒ INSERT" into a silent duplicate (the audit's `[0,1,999,999]`). Without
        //     it the rewrite arm was snapshot-isolation only.
        // The two are INDEPENDENT flags (fork `overwrite_files.rs:331`) — the same `AlwaysTrue`
        // filter serves both. As in the insert-only arm, `validate_from_snapshot` is armed only when
        // the scan captured a snapshot (an empty-at-read-time table depended on no prior state — Java
        // runs no from-snapshot check).
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

/// ===========================================================================================
/// One atomic merge-on-read commit: the position-delete files for every mutated row PLUS the new
/// data files (updated new-values + inserts) in a SINGLE `RowDelta` snapshot — the fork
/// `ENGINE_CONTRACT` §5 row-delta recipe, MERGE row.
///
/// Java's `SparkPositionDeltaWrite.commit` is the oracle for which validations are armed, and MERGE
/// sits in the `command == UPDATE || MERGE` bucket, so ALL of these are set:
///   * `validate_data_files_exist(referenced)` — unconditional for every command (L243): a
///     position delete cannot apply to a data file a concurrent commit compacted or rewrote away,
///     and applying it blind would silently lose the delete (resurrecting the row) or, worse, land
///     on a different file. Java's own `if (!referencedDataFiles.isEmpty())` guard is what enables
///     the check, so an insert-only merge-on-read MERGE (no deletes, empty set) correctly leaves it
///     inert — there is nothing to reference.
///   * `validate_deleted_files()` + `validate_no_conflicting_delete_files()` — armed for
///     UPDATE/MERGE and deliberately NOT for DELETE (L251-254). This op READ the rows it mutates, so
///     a concurrent row-level delete of those same rows is a genuine conflict; and it widens the
///     files-exist check's op set from `{OVERWRITE}` to `{OVERWRITE, DELETE}` so a concurrent
///     merge-on-read DELETE that removed a referenced data file also conflicts.
///   * `validate_no_conflicting_data_files()` — the SERIALIZABLE guard (L256-258); Java's MERGE
///     default, gated on `write.merge.isolation-level` (snapshot drops this call):
///     rejects a concurrent ADD that could match the ON condition, the F-BR-1 silent-duplicate class.
///   * `conflict_detection_filter(AlwaysTrue)` — deliberately MORE conservative than the PERF-04
///     residual (audit M15). Narrowing to the residual would be WRONG: residual is source-key
///     min/max, not the ON condition. `AlwaysTrue` is the safe conservative value.
///   * `validate_from_snapshot(pin)` — anchored to the snapshot the merge queries actually read,
///     and only when the scan captured one (an empty-at-read-time table depended on no prior state;
///     Java runs no from-snapshot validation there either).
///
/// Every commit carries the §8 `engine.operation-id` summary stamp, exactly as [`commit`] does. A
/// MERGE that changes nothing commits nothing.
///
/// The position-delete files are written HERE rather than by the caller so the write and the commit
/// stay adjacent: the pairs are grouped, sorted and encoded, and the very next statement commits
/// them, so no code path can produce delete files and then take a branch that fails to commit them.
/// On `tx.commit` `Err`, [`commit_row_delta_kind`] best-effort deletes the new data-file and
/// delete-file paths then re-raises the original error (M14).
/// ===========================================================================================
async fn commit_row_delta(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    pairs: Vec<crate::write::position_delete::PositionDeletePair>,
    data_files: Vec<DataFile>,
    concurrency: WriteConcurrency,
) -> Result<()> {
    let isolation = resolve_merge_isolation(table)?;
    commit_row_delta_kind(
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
    )
    .await
}

/// Position-delete `RowDelta` commit. MERGE calls [`commit_row_delta`] (Merge +
/// `write.merge.isolation-level`, default serializable).
///
/// On `tx.commit` error, best-effort-delete the new `data_files` paths and the
/// position-delete files this function just wrote, then re-raise the original
/// error. A failed `write_position_deletes` has no successful writer result to
/// delete (partial writes are not walked). See [`abort`].
pub(super) async fn commit_row_delta_kind(
    catalog: &Arc<dyn Catalog>,
    table: &Table,
    snapshot_id: Option<i64>,
    pairs: Vec<crate::write::position_delete::PositionDeletePair>,
    data_files: Vec<DataFile>,
    concurrency: WriteConcurrency,
    policy: RowDeltaPolicy,
) -> Result<()> {
    if pairs.is_empty() && data_files.is_empty() {
        return Ok(());
    }
    let data_file_paths = abort::written_file_paths(&data_files);
    // The DATA files these position deletes reference — the `validate_data_files_exist` set. Built
    // BEFORE the write so it is derived from the pairs themselves, never from the delete files.
    let referenced: HashSet<String> = pairs
        .iter()
        .map(|(path, _)| path.as_ref().to_string())
        .collect();
    let pair_count = pairs.len() as u64;
    let data_file_count = data_files.len() as u64;
    let delete_files =
        crate::write::position_delete::write_position_deletes(table, &pairs, concurrency)
            .instrument(tracing::info_span!(
                "merge.write_deletes",
                pairs = pair_count
            ))
            .await?;
    let delete_file_paths = abort::written_file_paths(&delete_files);
    let delete_file_count = delete_files.len() as u64;

    let summary = HashMap::from([(OPERATION_ID_PROP.to_string(), Uuid::new_v4().to_string())]);
    let tx = Transaction::new(table);
    let mut action = tx
        .row_delta()
        .add_data_files(data_files)
        .add_deletes(delete_files)
        .conflict_detection_filter(Predicate::AlwaysTrue)
        .validate_data_files_exist(referenced)
        .case_sensitive(true)
        .set_snapshot_properties(summary);
    // Java `SparkPositionDeltaWrite.commit` L251-254: UPDATE/MERGE only, not DELETE.
    if matches!(policy.kind, RowDeltaKind::Merge) {
        action = action
            .validate_deleted_files()
            .validate_no_conflicting_delete_files();
    }
    if policy.isolation == IsolationLevel::Serializable {
        action = action.validate_no_conflicting_data_files();
    }
    if let Some(pin) = snapshot_id {
        action = action.validate_from_snapshot(pin);
    }
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

/// Column lookup that names the missing column instead of panicking.
fn named_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    batch.column_by_name(name).ok_or_else(|| {
        DataFusionError::Internal(format!("merge-internal column `{name}` missing from batch"))
    })
}

/// Strict cast options: an overflowing cast is an error, never a silent NULL.
fn strict_cast() -> CastOptions<'static> {
    CastOptions {
        safe: false,
        ..CastOptions::default()
    }
}

/// A single-quoted SQL string literal with embedded quotes doubled. Residual after scout #18
/// dropped the rewrite-path `_file IN (...)` list; retained for unit pins.
#[cfg(test)]
fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// A double-quoted SQL identifier with embedded quotes doubled. Every schema-derived column name
/// interpolated into generated SQL goes through here: Iceberg/Arrow place no restriction on
/// column names, so a name containing `"` must not break out of the identifier.
///
/// Delegates to [`crate::write::idents::quote_ident_spark`] (r23 QI1 single-source Spark/DF dialect).
pub(super) fn quote_ident(name: &str) -> String {
    crate::write::idents::quote_ident_spark(name)
}

/// Fold an iceberg error into the DataFusion error this crate's SQL callers carry.
pub(crate) fn iceberg_err(err: iceberg::Error) -> DataFusionError {
    DataFusionError::External(Box::new(err))
}

#[cfg(test)]
mod occ_conflict_tests;
#[cfg(test)]
mod occ_tests;
#[cfg(test)]
mod parallel_write_tests;
#[cfg(test)]
mod streaming_scan_tests;
#[cfg(test)]
mod streaming_tests;
#[cfg(test)]
#[cfg(test)]
mod tests;
