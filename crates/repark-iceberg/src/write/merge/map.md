# map — repark-iceberg/src/write/merge/

## Purpose

Spark `MERGE INTO` adapter (copy-on-write + merge-on-read). The former `merge.rs` monolith
lives as this module directory (move-only; pub surface frozen).

## Contents

- `mod.rs` — types, `execute_merge`, plan/SQL helpers, write/commit path.
- `insert.rs` — NOT MATCHED INSERT machinery: `insert_projection` (clause→projection lowering,
  moved from `mod.rs` 2026-08-15), the source-only execution seam (`insert_stream_checked`),
  and the ANSI store-assignment gate (audit M4/M9).
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
  `occ_tests.rs` (M13 parse + M19-A snapshot split).
- `tests.rs` — primary unit battery
- `occ_tests.rs` — OCC / commit conflict pins + M13 isolation parse +
  M19-A serializable-vs-snapshot split
- `occ_tests.rs` — OCC / commit conflict pins
- `occ_conflict_tests.rs` — OCC-2 M19/M20 batteries B/C/E/F/G/H/I
  (`RowDeltaKind::Delete`, MERGE↔MERGE both orders, retry-from-original-pin,
  empty-table from-root, M15 partitioned over-rejection, M20 operation stamps,
  M14 orphan characterization). Engine frozen; no production edits.
- `streaming_tests.rs` — stream write interleaving pins
- `parallel_write_tests.rs` — concurrent file write pins
- `streaming_scan_tests.rs` — streaming target-scan pins + PERF-04 residual-push
  battery + MG-1 `utf8_source_int32_target_does_not_push_residual`

## I want to…

| Task | Go to |
|---|---|
| Change MERGE execute / MoR-CoW arms | `mod.rs` |
| Add a unit pin for SQL shape | `tests.rs` |
| Touch OCC commit behavior | `occ_tests.rs` / `occ_conflict_tests.rs` |

## Pointers

Up: [../map.md](../map.md). Fork contract: `docs/ENGINE_CONTRACT.md` (owned fork).

## Debug

- `--list` paths must stay `write::merge::<battery>::<test>` — identity gate for the
  declared-rename census.
- Pub `write_data_files*` re-exported from the write module root (`../mod.rs`) and the crate
  root (`lib.rs`).
