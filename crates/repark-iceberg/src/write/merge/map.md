# map — repark-iceberg/src/write/merge/

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Spark `MERGE INTO` adapter (copy-on-write + merge-on-read). The former `merge.rs` monolith
lives as this module directory (move-only; pub surface frozen).
Source comments retain OCC, streaming, and cleanup invariants; implementation narration is omitted.

## Contents

- `mod.rs` — types, `execute_merge`, plan/SQL helpers, write/commit path.
  **MW-9:** `resolve_merge_mode` parses `write.delete.granularity` on the MoR
  arm (after the V2 gate, before any scan/write) so unknown values cannot
  orphan MATCHED-UPDATE parquet. Identity DELETE/UPDATE share
  `write_position_deletes` via `commit_row_delta_kind`; their refuse-before-IO
  lives in `../predicate_dml.rs` `resolve_write_mode`.
  `mod abort;` is T5-owned. On `tx.commit` `Err`, `commit_overwrite` /
  `commit_row_delta_kind` best-effort-delete writer-result paths (M14 design A).
  **M11:** `fold_discovery_batch_into_affected` / `consume_matched_work_batch`
  take a precomputed `skip_cardinality` (lone unconditional MATCHED DELETE);
  `match_count > 1` still folds mutations / pos-deletes (double-delete is
  idempotent). V3R-1: both copy-on-write arms run the v3 guard (`V3-COW-1`) before any write.
- `dv_close.rs` — v3 `RowDelta` DV-container close. `prepare_row_delta_deletes` writes
  V2 parquet position deletes or calls `close_touched_dv_containers` on V3, then
  `apply` stamps sibling sequences. C-003 pin
  `shared_puffin_row_delta_keeps_the_untouched_sibling` calls `commit_row_delta_kind`
  on the Spark shared-Puffin fixture (id 5 must stay deleted).
  pins: rp-3-fork-repin/C-003
- `abort.rs` — `delete_written_files_best_effort` + `written_file_paths`. Delete
  set is threaded from writer results in hand; never re-derived from the table
  or manifests. `CommitStateUnknown` errors SKIP cleanup (the commit may have
  persisted — Java's `CommitStateUnknownException` rethrow-before-cleanup rule);
  reclaim is orphan-file maintenance. Per-file `FileIO::delete` failures
  `tracing::warn` and never mask the original commit error.
- `insert.rs` — NOT MATCHED INSERT machinery: `insert_projection` (clause→projection lowering,
  moved from `mod.rs` 2026-08-15), the source-only execution seam (`insert_stream_checked`),
  and the ANSI store-assignment gate (audit M4/M9). **BL-4 (2026-08-15):**
  `update_stream_checked` / `validate_update_store_assignment` plan each `UPDATE SET`
  expression in isolation (no rewrite-`CASE` unification) and run the same
  `ansi_store_assignable` / `normalize_for_assignment` matrix against
  the target column type. **WI-1 (2026-08-15):** that matrix now lives in
  [`../store_assign.rs`](../store_assign.rs) — this file keeps only the `MERGE `-prefixed
  path-label wrapper, so the shipped #111/#135 message text is byte-identical while the
  non-MERGE write lowerings share the predicate instead of forking a second copy.
  Needle `not ANSI-store-assignable`. After the gate,
  rewrite THEN arms use `arrow_cast` to the target type so CASE unifies on
  legal pairs CASE cannot coerce (bool→string). COW call site is the rewrite
  stream; MoR call site is `matched_work_mor`. Match-discovery is not gated.
  Unpartitioned writer: `#182` `PartitionKey::new(...)` is `Result`; `?` via `iceberg_err`
  (net-zero lines vs the 2700-line file ceiling).
  `residual_join_key_filter` is a thin caller of `scan_prune::residual_bounds_predicate`
  (M1/M6/M7 helpers stay out of this file; measured net-negative vs the 2700 ceiling).
  `commit_overwrite` / `commit_row_delta_kind` are `pub(super)` so identity DML
  (`../predicate_dml.rs`) reuses the COW/MoR commit arms without calling
  `execute_merge`. Identity UPDATE reuses `RowDeltaKind::Merge` (Java
  UPDATE/MERGE bucket). MERGE SQL still goes through
  `commit` / `commit_row_delta`, which resolve
  `write.merge.isolation-level` (default serializable; snapshot drops
  `validate_no_conflicting_data` / `validate_no_conflicting_data_files`;
  M15 AlwaysTrue is more conservative than the residual). Pins in
  `occ_tests.rs` (M13 parse + M19-A snapshot split + RP-1 F-0 Replace
  files-exist pin on the snapshot arm).
- `tests.rs` — primary unit battery
- `occ_tests.rs` — OCC / commit conflict pins + M13 isolation parse +
  M19-A serializable-vs-snapshot split
- `occ_tests.rs` — OCC / commit conflict pins
- `occ_conflict_tests.rs` — OCC-2 M19/M20 batteries B/C/E/F/G/H/I
  (`RowDeltaKind::Delete`, MERGE↔MERGE both orders, retry-from-original-pin,
  empty-table from-root, M15 partitioned over-rejection, M20 operation stamps,
  M14 abort-path cleanup: rejected files removed, success-path files kept,
  delete-failure does not mask OCC).
- `streaming_tests.rs` — stream write interleaving pins
- `parallel_write_tests.rs` — concurrent file write pins
- `streaming_scan_tests.rs` — streaming target-scan pins + PERF-04 residual-push
  battery + MG-1 `utf8_source_int32_target_does_not_push_residual`

## I want to…

| Task | Go to |
|---|---|
| Change MERGE execute / MoR-CoW arms | `mod.rs` |
| Change rejected-commit file cleanup | `abort.rs` + `commit_overwrite` / `commit_row_delta_kind` |
| Add a unit pin for SQL shape | `tests.rs` |
| Touch OCC commit behavior | `occ_tests.rs` / `occ_conflict_tests.rs` |

## Pointers

Up: [../map.md](../map.md). Fork contract: `docs/ENGINE_CONTRACT.md` (owned fork).

## Debug

- `--list` paths must stay `write::merge::<battery>::<test>` — identity gate for the
  declared-rename census.
- Rejected MERGE left new Parquet files: cleanup is `tx.commit` `Err` only in
  `commit_overwrite` / `commit_row_delta_kind` via [`abort.rs`](abort.rs). A catch
  that can fire after a successful commit is a HALT.
- Pub `write_data_files*` re-exported from the write module root (`../mod.rs`) and the crate
  root (`lib.rs`).
