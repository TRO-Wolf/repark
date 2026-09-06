# map — repark-iceberg/src/write/merge/

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Wrapped-line fragments rewritten as complete sentences (D-002).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Spark `MERGE INTO` adapter (copy-on-write + merge-on-read). The former `merge.rs` monolith
lives as this module directory (move-only; pub surface frozen).
Source comments retain OCC, streaming, and cleanup invariants; implementation narration is omitted.

## Contents

- `snapshot_commit.rs` — snapshot-producing MERGE commits (`to_branch` when `MergeSpec.commit_branch` is set).
  **V3-9:** `referenced` / `abort_paths` are moved out of the prepared deletes with
  `std::mem::take` instead of deep-cloned once per row-delta commit.
  pins: v3-9-mor-predicate-dml-dv/C-009
  pins: rp-5-fork-repin/C-004
- `mod.rs` — types, `execute_merge`, plan/SQL helpers, write/commit path.
  **CTAS-VIEW-1 (2026-09-03):** `write_data_files_from_stream_with_concurrency` maps each
  batch through `conform_batch_retaining_unmapped_columns` before the fan-out (same
  `write_default_column_names` as the partitioned writer). Callers: Spark/ANSI CTAS,
  unpartitioned append, overwrite stage, MERGE insert stream (lineage extras retained).
  pins: ctas-view-1-conform-stream/C-002
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
  idempotent). V3-7: v3 MERGE carries stored `_row_id` through `row_lineage.rs`
  (`schema_with_row_lineage`); last-updated is nulled only on UPDATE rows.
  pins: v3-7-merge-lineage/C-001
- `dv_close.rs` — v3 `RowDelta` DV-container close. `prepare_row_delta_deletes` writes
  V2 parquet position deletes or calls `close_touched_dv_containers_with_partitions` on V3, then
  `apply` stamps sibling sequences. C-003 pin
  `shared_puffin_row_delta_keeps_the_untouched_sibling` calls `commit_row_delta_kind`
  on the Spark shared-Puffin fixture (id 5 must stay deleted).
  **V3-12 (2026-09-02), superseded by RP-8:** `plan_deletion_vectors` folded the superseded
  legacy positions into `new_positions` before the container close and appended the superseded
  delete files to `close.removed`, out of a RePark-owned `dv_close/legacy_deletes.rs` that walked
  the scanned snapshot's delete manifests and then its data manifests for sequence numbers.
  **RP-8 (2026-09-03):** that module and both of its walks are DELETED. At pin `c1d6c9de` the
  fork's own close (fork F-21 `#262`, F-22 `#263`) collects the live non-Puffin position deletes
  in the SAME delete-manifest pass it already made for the DVs, loads each delete file once
  through a projected `load_legacy_positions_by_path`, unions the applicable positions into the
  DV it writes, and pushes only the file-scoped sources onto `close.removed` — so
  `plan_deletion_vectors` passes STATEMENT-ONLY positions and consumes the result. The
  semantics are unchanged and still Spark's two-test rule: APPLICABILITY (`delete_seq >=
  data_seq`, unknown erring toward "applies") governs the merge, FILE SCOPE governs only the
  removal.
  **RP-9 (2026-09-03):** pin `594bdbe5` (fork F-23) restores the skip: when there are no
  legacy deletes and `known_partitions` covers every touched path the close reads ZERO data
  manifests and `data_sequence_numbers` is empty. A MoR statement with a live legacy delete
  still walks and the sequence map is total. RePark never treats that map as total on the
  pure-DV path (`apply_close` reads only `added` / `removed`). Pins:
  `a_supplied_partition_map_closes_a_fresh_partitioned_delete_with_no_data_manifest` (hide
  succeeds, map empty), `closing_a_covered_v3_delete_reads_the_data_manifest_for_sequence_numbers`
  (empty map still walks), `a_legacy_delete_fills_data_sequence_numbers_even_with_a_complete_partition_map`.
  **RP-9 r2:** `a_plain_identity_delete_closes_with_no_data_manifest` drains the production
  identity-SQL sink, hides the data manifests, and requires an empty sequence map — Spark/ANSI
  plain `DELETE WHERE` now uses that path instead of the fork delete exec's empty map.
  `plan_deletion_vectors` loads the scanned snapshot's `ManifestList` once and hands it to the
  close as `Option<&ManifestList>` so the list is not read twice.
  pins: rp-9-repin-f23/C-002, C-005
  **RP-10 (2026-09-04):** pin `85a4aaf0` (fork F-25). The production identity DELETE of the
  newest row on the 192-manifest pure-DV fixture commits with every data manifest except the
  one that holds the touched file hidden (commit-phase opens = 1). Close-phase opens stay 0.
  pins: rp-10-repin-f25/C-002
  **V3-12 C-006:** `prepare_row_delta_deletes` takes the `snapshot_id`
  `commit_target::snapshot_id_for_commit` already resolved for the target scan and
  `validate_from_snapshot`, and hands it to BOTH the legacy-delete collection and the fork
  container close. The close had always been given `None`, which the fork resolves to the CURRENT
  snapshot — so a `to_branch` merge-on-read write closed against `main`, found none of the
  branch's own deletion vectors, wrote a second DV for a data file that already had one, and the
  commit door refused. One resolved snapshot id now serves the scan, the collection, the close
  and the commit validation. Registry `V3-DV-BRANCH-1`.
  pins: v3-12-legacy-delete-merge/C-003, C-006
  **V3-9 (2026-09-02):** the position map takes `get_mut` before allocating a key and the V2
  `referenced` set allocates one `String` per distinct path, not one per row (600k rows:
  41.3 → 29.3 ms and 37.3 → 23.9 ms).
  **RP-7 (2026-09-02):** pin `ff4764d3` (fork F-18) closes registry `V3-DV-1` — only the touched
  blob is rewritten and the untouched sibling entry keeps its container and `content_offset`, so
  the C-003 pin gained that layout assertion alongside its semantic one. The
  `(spec_id, partition)` the fork needs comes from the statement's OWN target scan
  (`TargetScanStream::with_partition_sink`), which already plans every `FileScanTask` and so
  already knows each file's partition; entries are supplied only for paths that scan produced,
  which keeps the fork's "not a live file of the scanned snapshot" guard meaningful, and no
  table shape is special-cased. `plan_deletion_vectors` retains the map down to the touched
  paths. **RP-8:** F-19 (`#261`) deleted `DvContainerClose::retained_references` and collapsed
  `StampedDeleteFile` to `DataFile`, so the referenced set is `close.referenced_data_files()` —
  the replacement blobs only — and `apply_close` is one `add_deletes`. The first draft instead
  short-circuited on "every spec is unpartitioned", which was
  measurably useless — one partitioned spec anywhere in a table's history emptied the map and
  the statement paid the full lazy walk (192-partition fresh-path DELETE 2,176 ms, now 761 ms).
  The two manifest-read pins hid the live data manifests and required the close to succeed
  anyway; **RP-8** flipped both because F-22 always walked; **RP-9** restores the complete-map
  skip (`data_sequence_numbers` empty) and keeps the empty-map and legacy-delete walks.
  pins: rp-3-fork-repin/C-003
  pins: rp-8-repin-f21-f22/C-002
  pins: rp-9-repin-f23/C-002
  pins: v3-5-dv-compaction/C-005
  pins: v3-9-mor-predicate-dml-dv/C-007, C-009
  pins: rp-7-f18-repin/C-002, C-003
- `snapshot_commit.rs` — **RP-7 (2026-09-02):** `commit_row_delta_kind_with_partitions` /
  `commit_row_delta_on_ref_with_partitions` carry the scan's partition map to the DV close. The
  bare `commit_row_delta_kind` / `commit_row_delta_on_ref` wrappers have no production caller
  left and are `#[cfg(test)]`, so the OCC batteries keep their existing spellings.
  pins: rp-7-f18-repin/C-002
- `target_scan.rs` — **RP-7 (2026-09-02):** `TargetScanStream` and the partition sink, extracted
  from `mod.rs` (baseline ratcheted 1889 → 1795 in the same change). The scan takes the
  `plan_files` route whenever an allowlist OR a sink is present and `to_arrow()` otherwise; the
  two routes are byte-equivalent for this scan shape (the fork's `to_arrow` builds an
  `ArrowReaderBuilder` with the same defaults, and its within-file split expansion is a no-op
  while `_pos` is projected).
  **PERF-SCAN-1 (2026-09-03 / r2 2026-09-04):** `plan_files` + `try_collect` run once per
  stream; later `StreamingTable` re-executes reuse the cached `FileScanTask`s. That cache
  is concurrent-`execute` hardening, not a 3 × N → 1 × N drop on the production identity
  DELETE (one `execute`). Registry `PERF-SCAN-3PASS-1` stays BACKLOG. Round-2 strace at
  base `e6ebd40` and tip, N=8 and N=192: scan-to-puffin 1 × N, close 0, commit 1 × N.
  pins: rp-7-f18-repin/C-002
  pins: perf-scan-1-plan-once/C-001, C-002, C-004
- `abort.rs` — `delete_written_files_best_effort` + `written_file_paths`. Delete
  set is threaded from writer results in hand; never re-derived from the table
  or manifests. `CommitStateUnknown` errors SKIP cleanup (the commit may have
  persisted — Java's `CommitStateUnknownException` rethrow-before-cleanup rule);
  reclaim is orphan-file maintenance. Per-file `FileIO::delete` failures
  `tracing::warn` and never mask the original commit error.
- `not_matched_by_source.rs` — **DML-A:** `WHEN NOT MATCHED BY SOURCE` types, SQL
  fragments, full-snapshot path listing, MOR work SQL. COW rewrite applies the arm
  through `rewrite_column` ELSE / combined DELETE.
  pins: dml-a-merge-not-matched-by-source/C-001, C-002, C-003, C-008
- `row_lineage.rs` — V3-7: v3 MERGE write schema (`schema_with_row_lineage`), scratch
  lineage columns, rewrite SQL that keeps `_row_id` and nulls last-updated on UPDATE,
  and partitioned fanout that prefixes user columns for the partition calculator.
  pins: v3-7-merge-lineage/C-001
  V3-11: `write_partitioned_lineage_files` passes the fanout writer's close result through
  `crate::write::file_order::ascending_partition_order`, because the fork's `FanoutWriter` drains a
  `HashMap` and a MoR MERGE that updates one partition and inserts into another produced two
  files in random order — registry `V3-ROWID-3`; the general rule and its
  divergence from Spark are `V3-FILEORDER-1`.
  pins: v3-11-row-id-determinism/C-003
  V3-8: `table_carries_merge_lineage` and `scratch_schema_for_table` widen to `pub(crate)`
  so `write::predicate_dml` reuses the same scratch shape for its COW rewrite; the module is
  `pub(crate) mod`. pins: v3-8-subquery-where-lineage/C-002
  **WRITE-DISTRIBUTION-2 (2026-09-06):** MERGE inserts into a partitioned non-lineage table now
  commit one file per partition value — `write_new_data_files_from_stream` still selects this
  serial writer for V3 lineage tables and the shared partitioned stream funnel otherwise, and
  the funnel routes one value to one writer. Row semantics and `_row_id` carry are unchanged.
  pins: write-distribution-2/C-004, C-007
- `cow_scratch.rs` — COW rewrite scratch tables (file-scoped target, affected-path
  MemTable, drop guard) extracted so `mod.rs` ratchets down. Scratch providers
  register on `datafusion.public` so a session default Iceberg catalog cannot
  refuse a MemTable with rows (two-part `t.branch_b` MERGE).
  pins: rp-5-fork-repin/C-004
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
  `tests/occ.rs` (M13 parse + M19-A snapshot split + RP-1 F-0 Replace
  files-exist pin on the snapshot arm).
- [tests/](tests/map.md) — MERGE unit batteries (primary, OCC, streaming, parallel write).

## I want to…

| Task | Go to |
|---|---|
| Change MERGE execute / MoR-CoW arms | `mod.rs` |
| Change v3 MERGE `_row_id` carry | `row_lineage.rs` |
| Change MERGE snapshot commit / `to_branch` | `snapshot_commit.rs` |
| Change rejected-commit file cleanup | `abort.rs` + `commit_overwrite` / `commit_row_delta_kind` |
| Add a unit pin for SQL shape | `tests/merge.rs` |
| Touch OCC commit behavior | `tests/occ.rs` / `tests/occ_conflict.rs` |
| Touch MERGE OCC onto a named branch | `tests/occ_branch.rs` |

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
