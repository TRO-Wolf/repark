# map — repark-iceberg/src/write/merge/

ICE-MIXED-CASE-1 (2026-09-17): the MERGE executor carries the door's case-sensitivity flag on the spec (`mod.rs`, `insert.rs`, `not_matched_by_source.rs`); UPDATE SET rejects case-insensitive duplicate targets instead of first-winning. pins: ice-mixed-case-1/C-004

ICE-MIXED-CASE-1 round 5 (2026-09-17, Q-20b-2): the four write-side twin-check sites delegate to the shared `resolve_write_column` helper (`../name_resolution.rs`); `mod.rs` 1782 → 1780. pins: ice-mixed-case-1/C-004

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Wrapped-line fragments rewritten as complete sentences (D-002).

CC-2 closing-critic remediation: review-round label narration swept from prose; safety and
accuracy contracts restored in condensed form (see the unit ledger's findings dispositions).

## Purpose

Spark `MERGE INTO` adapter (copy-on-write + merge-on-read). The former `merge.rs` monolith
lives as this module directory (move-only; pub surface frozen).
Source comments retain OCC, streaming, and cleanup invariants; implementation narration is omitted.

## Contents

- `snapshot_commit.rs` — snapshot-producing MERGE commits (`to_branch` when `MergeSpec.commit_branch` is set).
  **ICE-OCC-SCOPED-1 (2026-09-17):** the three commit sites (`commit_overwrite_on_ref`'s
  add-only and delete+add arms, `commit_row_delta_kind_on_ref`) no longer hard-code
  `Predicate::AlwaysTrue`. They take a `CommitScope { isolation, conflict_filter }` as a parameter
  (`RowDeltaPolicy` now carries `kind` + `scope`) and hand `scope.conflict_filter` to the fork's
  `conflict_detection_filter`, which (fork #291, RP-24) tests each concurrently added data file
  and delete file against it through that file's own spec's partition projection before the
  inclusive metrics. Isolation still decides only whether `validate_no_conflicting_data` /
  `validate_no_conflicting_data_files` is armed, so `snapshot` arms exactly the walks it armed
  before; the filter scopes whichever walks are armed, as Java's does (ruling Q-21a-3).
  `commit_on_ref` / `commit_row_delta_on_ref_with_partitions` take the MERGE's filter as a
  parameter; the `#[cfg(test)]` wrappers and `CommitScope::unscoped` keep `AlwaysTrue`.
  pins: ice-occ-scoped-1/C-005, C-014
  **ICE-COMMIT-UNKNOWN-1 (2026-09-14):** `commit_overwrite` and `commit_row_delta_kind` mint
  the commit's `engine.operation-id` via `write::commit_error::operation_id_and_summary` and
  route the `tx.commit` `Err` through `commit_err`, so a `CommitStateUnknown` surfaces
  stamped for `error_map`'s wrapper downcast — after `abort.rs`'s file cleanup still ran
  (the cleanup carve-out lives in `abort.rs` itself).
  pins: ice-commit-unknown-1/C-003
  **V3-9:** `referenced` / `abort_paths` are moved out of the prepared deletes with
  `std::mem::take` instead of deep-cloned once per row-delta commit.
  pins: v3-9-mor-predicate-dml-dv/C-009
  pins: rp-5-fork-repin/C-004
  **WO U5 PR2b round 2 (2026-09-25):** each `maybe_to_branch` call passes the table, so a MERGE
  into a branch of a format v1 table refuses with the v1 ref text before it commits. `mod.rs`
  answers merge-on-read MERGE on v1 with `Deletes are supported in V2 and above`.
  pins: ice-nested-evo-1/C-053, C-057
- `mod.rs` — types, `execute_merge`, plan/SQL helpers, write/commit path.
  **ICE-OCC-SCOPED-1 (2026-09-17):** `MergeTarget` carries the MERGE's `conflict_filter`,
  computed once in `execute_merge` by `merge_conflict_filter`: the target-only conjuncts of the
  `ON` condition (`../conflict_filter.rs` `from_merge_on`), or `AlwaysTrue` whenever a
  `WHEN NOT MATCHED BY SOURCE` clause is present, because that clause reads every target row
  the condition does not match. Both arms hand it to their commit. `residual_join_key_filter`
  moved to `target_scan.rs` unchanged (baseline ratcheted 1792 → 1773 in the same change).
  pins: ice-occ-scoped-1/C-004, C-006, C-009, C-010, C-011, C-012
  **ICE-PROMOTE-READ-1 (2026-09-16):** every DML target scan pins the snapshot id, so on a table
  with no write since `ALTER COLUMN … TYPE` it reads the pre-promotion types; `conform_scan_batch`
  widens those data columns through `conform::promoted_scan_column` before building the batch
  under the scratch schema (MERGE, identity DELETE/UPDATE and the COW scratch all pass here). Its
  doc line names only the `_file` / `_pos` casts; this entry is where the widening is recorded.
  pins: ice-promote-read-1/C-006, C-011
  **FNP-4B (2026-09-15):** every generated-SQL identifier quotes with backticks
  (`quote_ident`, incl. the semijoin / discovery / sentinel builders), so the rewrites parse
  under the Spark Databricks session dialect.
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
  **IPI-51 type slice (2026-09-21):** both cardinality guards
  (`fold_discovery_batch_into_affected`, `consume_matched_work_batch`) raise
  `DataFusionError::Plan` with `spark_error::message(MERGE_CARDINALITY_VIOLATION, &[])`,
  so `W-MERGE-DUP-SOURCE-ERR` surfaces as `AnalysisException` /
  `MERGE_CARDINALITY_VIOLATION` / `23K01` (A-6 substitute for Spark's
  `SparkRuntimeException`). `skip_cardinality` is unchanged.
  `CARDINALITY_VIOLATION_MSG` is gone. Baseline 1656 → 1654.
  pins: ice-error-conditions-1/C-012
  **WRITE-ORDER-DIST-1 (2026-09-06):** the unpartitioned staged-write entry delegates to the
  distribution module's `drive_unpartitioned`, so a declared default sort order sorts that path
  too; the batch-sink helpers it shares (`BatchWriter`, `ForkBatchWriter`,
  `write_stream_into`, `write_stream_into_parallel`) are `pub(crate)` for that caller.
  pins: write-order-dist-1/C-008
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
- `target_scan.rs` — **ICE-EVO-DML-1 (2026-09-17):** every execute plans the pinned
  snapshot with the fork's `project_current_schema()` and reads the planned tasks with
  `ArrowReaderBuilder` (the `to_arrow()` route is gone; plans stay cached per stream). When the
  pinned snapshot's schema is not the current schema — `ADD COLUMN`, `RENAME COLUMN`, a type
  promotion with no write since — the tasks read under the current schema by field id, so
  MERGE, identity DELETE / UPDATE, the affected-file rewrite and the COW scratch stop refusing
  `Column … not found in table` and stop reading a swapped name's other field. Round 1 did the
  re-point locally (`catalog::current_schema_scan`); round 2 (2026-09-17) deleted it for the
  fork API now that F-EVO-SCAN-1 (#289) owns the semantics. The projection also widens a
  single-era promoted column before `conform_scan_batch` sees it (measured: the
  `promoted_scan` table pin stays green with the conform widening bypassed).
  pins: ice-evo-dml-1/C-010, C-011, C-013
  **ICE-OCC-SCOPED-1 (2026-09-17):** now also holds `residual_join_key_filter`
  (PERF-04's join-key bounds pushed onto the target scan), moved verbatim from `mod.rs`. It is the
  SCAN residual, derived from the SOURCE's key range; it is never the conflict filter, which only
  ever holds target-only predicates.
  **RP-7 (2026-09-02):** `TargetScanStream` and the partition sink, extracted
  from `mod.rs` (baseline ratcheted 1889 → 1795 in the same change). The scan took the
  `plan_files` route whenever an allowlist OR a sink is present and `to_arrow()` otherwise; the
  two routes were byte-equivalent for this scan shape (the fork's `to_arrow` builds an
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
  **ICE-COMMIT-UNKNOWN-1 (2026-09-14):** the keep-set is now pinned by real-file
  pins in `tests/commit_unknown.rs` — mutation-proven (deleting the early return
  reds both).
  pins: ice-commit-unknown-1/C-003, C-007
- `not_matched_by_source.rs` — **DML-A:** `WHEN NOT MATCHED BY SOURCE` types, SQL
  fragments, full-snapshot path listing, MOR work SQL. COW rewrite applies the arm
  through `rewrite_column` ELSE / combined DELETE.
  pins: dml-a-merge-not-matched-by-source/C-001, C-002, C-003, C-008
- `row_lineage.rs` — V3-7: v3 MERGE write schema (`schema_with_row_lineage`), scratch
  lineage columns, rewrite SQL that keeps `_row_id` and nulls last-updated on UPDATE,
  and partitioned fanout that prefixes user columns for the partition calculator.
  pins: v3-7-merge-lineage/C-001
  **ICE-WRITER-METRICS-1 (2026-09-20):** the partitioned rewrite builder applies
  `MetricsConfig::for_table` (detail in [../map.md](../map.md)).
  V3-11: `write_partitioned_lineage_files` passes the fanout writer's close result through
  `crate::write::file_order::ascending_partition_order`, because the fork's `FanoutWriter` drains a
  `HashMap` and a MoR MERGE that updates one partition and inserts into another produced two
  files in random order — registry `V3-ROWID-3`; the general rule and its
  divergence from Spark are `V3-FILEORDER-1`.
  **ICE-WRITE-OPTIONS-1 (2026-09-17):** `iceberg_parquet_schema` widened
  `pub(super)` → `pub(crate)` so the options staging builders reuse the same
  row-lineage schema; no behaviour change.
  pins: ice-write-options-1/C-003
  pins: v3-11-row-id-determinism/C-003
  V3-8: `table_carries_merge_lineage` and `scratch_schema_for_table` widen to `pub(crate)`
  so `write::predicate_dml` reuses the same scratch shape for its COW rewrite; the module is
  `pub(crate) mod`. pins: v3-8-subquery-where-lineage/C-002
  **WRITE-DISTRIBUTION-2 (2026-09-06):** MERGE inserts into a partitioned non-lineage table now
  commit one file per partition value — `write_new_data_files_from_stream` still selects this
  serial writer for V3 lineage tables and the shared partitioned stream funnel otherwise, and
  the funnel routes one value to one writer. Row semantics and `_row_id` carry are unchanged.
  pins: write-distribution-2/C-004, C-007
  **ICE-SORTED-INSERT-1 (2026-09-17):** both MERGE writer sites stamp the
  table's default sort order id through `distribution::stamp` — the lineage
  fanout and the unpartitioned writer in `mod.rs`.
  pins: ice-sorted-insert-1/C-003
  **ICE-SORTED-INSERT-1 round 3 (2026-09-17):** the stamp alone was a false claim
  here. Round 3's `sorted_lineage_batches` (folded into the writer in round 4) drained the stream and, when the table declares a
  default sort order, hands it to `distribution::sort_batches_by_default_order`
  before the fanout; the writer is built after that call, so a shape that cannot
  sort (a transform order, which the shared helper refuses loud) writes nothing
  rather than stamping unsorted bytes. The sort carries whole batches, so
  `_row_id` and `_last_updated_sequence_number` travel with their rows.
  **Round 4 (V-01):** round 3 drained the stream for every table and only then
  checked for an order, so an unsorted v3 rewrite buffered the whole table. Now
  `write_partitioned_lineage_files` checks `default_sort_is_declared` first:
  unsorted tables stream batch by batch into the fanout, as before round 3, and
  only a declared order drains (`drain`) and sorts, with the writer built after
  the sort. That matches `distribution::fanout_sorted_serial` and
  `drive_unpartitioned`, which also collect only when an order is declared.
  pins: ice-sorted-insert-1/C-006, C-010
- `cow_scratch.rs` — COW rewrite scratch tables (file-scoped target, affected-path
  MemTable, drop guard) extracted so `mod.rs` ratchets down. Scratch providers
  register on `datafusion.public` so a session default Iceberg catalog cannot
  refuse a MemTable with rows (two-part `t.branch_b` MERGE).
  pins: rp-5-fork-repin/C-004
  **ICE-CATALOG-SESSION-1 S9 (2026-09-20):** `quote_scratch_name` is `pub(crate)` so
  predicate DML quotes 3-part scratch names per segment like the MERGE SQL builders.
- `insert.rs` — **WO U9-TYPES-1 r3 (2026-09-25):** the MERGE store-assignment refusal asks
  `../update_cast.rs`'s `incompatible_nested_message` first when either side is a map, so
  `UPDATE SET` and `NOT MATCHED INSERT` refuse a map key or value with Spark's
  `CANNOT_SAFELY_CAST` / `CANNOT_FIND_DATA` text, the same text as UPDATE (verifier V-002).
  pins: u9-types-1/C-011
- `insert.rs` — **U8 WRITE-SQL PR2 (2026-09-25):** `store_assignment_then_sql` delegates to
  `../update_cast.rs`'s `store_assignment_cast_sql`, so a struct target casts to its type
  without Iceberg field ids. With the struct-aware gate in `../store_assign.rs`, whole-struct
  SET, `UPDATE SET *` and struct INSERT values write. pins: u8-write-sql/C-030
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
  `residual_join_key_filter` (now in `target_scan.rs`) is a thin caller of
  `scan_prune::residual_bounds_predicate`.
  `commit_overwrite` / `commit_row_delta_kind` are `pub(super)` so identity DML
  (`../predicate_dml.rs`) reuses the COW/MoR commit arms without calling
  `execute_merge`. Identity UPDATE reuses `RowDeltaKind::Merge` (Java
  UPDATE/MERGE bucket). MERGE SQL still goes through
  `commit` / `commit_row_delta`, which resolve
  `write.merge.isolation-level` (default serializable; snapshot drops
  `validate_no_conflicting_data` / `validate_no_conflicting_data_files`;
  the conflict filter is the target-only predicate since ICE-OCC-SCOPED-1). Pins in
  `tests/occ.rs` (M13 parse + M19-A snapshot split + RP-1 F-0 Replace
  files-exist pin on the snapshot arm).
  **ICE-V3-WRITE-DEFAULT-1 (2026-09-17):** NOT MATCHED INSERT fills omitted
  columns from `write_default` through `../insert_defaults.rs` (`table_projection`
  carries the fill into the lowered text); explicit NULL stays NULL. The fill entry
  points carry no doc comments per the no-code-comments ruling. Pins in
  `tests/insert_fill.rs`.
  pins: ice-v3-write-default-1/C-005
  **Round 5 (2026-09-17, R-03):** `insert_sql` takes the Arrow `write_schema`
  `execute_merge` already built, so `table_projection` no longer converts the Iceberg
  schema again, and a table with no primitive `write_default` skips the
  `ColumnDefaults` build (`schema_has_primitive_fill` pre-scan).
  pins: ice-v3-write-default-1/C-019
- [tests/](tests/map.md) — MERGE unit batteries (primary, OCC, streaming, parallel write).
- `session_staging.rs` — **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** the
  session-conf-aware staged-write entry (`write_new_data_files_from_stream_with`
  over `WriterStagingOverrides`), split out of `mod.rs` so the parent stays under
  its exact size baseline; every MERGE writer site stages through it
  (comment-free per the owner ban).
- `mod.rs` — **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** MERGE staging and
  insert-stream sites take the session write conf through `session_staging`.
- `snapshot_commit.rs` — **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** the
  commit arms take the resolved session write (snapshot properties plus codec)
  and stamp it on the commit they build. **Round 1 (2026-09-19):** both arms
  build `EngineSummary::for_changes` from the files they hold — the CoW arm from
  the new and affected data files, the row-delta arm from the data files plus
  `PreparedDeletes::delete_file_changes()` — and go through `summary_with_extras`,
  so an extra that collides with an engine-computed key refuses like Spark
  instead of replacing the engine value. pins: ice-session-write-conf-1/C-041
  **Round 2 (2026-09-19):** once round 1 threaded `branch` through every
  production commit site, the `None`-branch wrappers `commit_overwrite` and
  `commit_row_delta_kind_with_partitions` lost their last production caller, so
  they join `commit_row_delta_kind` / `commit_row_delta_on_ref` and
  `CommitScope::unscoped` under `#[cfg(test)]`: the merge and predicate-DML
  batteries keep their existing spellings and the shipped lib builds neither.
- `dv_close.rs` — **ICE-SESSION-WRITE-CONF-1 round 1 (2026-09-19):**
  `delete_file_changes()` exposes the added and superseded delete files the
  row-delta summary needs, and `prepare_row_delta_deletes` takes the resolved
  `WriterStagingOverrides` so a v2 position-delete file is written with the
  commit's codec (a v3 DV is puffin and takes none). `mod.rs`'s MERGE executor
  and the row-delta commit arms thread that staging from the session resolve.
  pins: ice-session-write-conf-1/C-040
- `row_lineage.rs` — **ICE-SESSION-WRITE-CONF-1 round 8 (2026-09-20):** the lineage writer
  builds its Parquet properties through `write_options::staged_writer_properties`, the one
  staging-to-properties bridge, so it keeps Java's `parquet.enable.dictionary` default with
  every other RePark-owned write. pins: ice-session-write-conf-1/C-064
- `row_lineage.rs` — **ICE-SESSION-WRITE-CONF-1 (2026-09-19):** the lineage
  fanout writer site honours the session write conf
  (`write_partitioned_lineage_files_with`).
- `mod.rs`, `row_lineage.rs`, `session_staging.rs` — **IPI-41 WO2b (2026-09-22):**
  the MERGE builder sites route through `resolve_data_format` (partitioned lineage
  fanout, unpartitioned rewrite, staging carry), and the MERGE format gates leave,
  so copy-on-write rewrites keep the table's ORC / AVRO bytes. The format-version
  gate stays: it guards versions, not formats.
  pins: ice-orc-avro-1/C-007, C-008, C-009, C-010, C-011, C-012

## I want to…

| Task | Go to |
|---|---|
| Change MERGE execute / MoR-CoW arms | `mod.rs` |
| Change v3 MERGE `_row_id` carry | `row_lineage.rs` |
| Change MERGE snapshot commit / `to_branch` | `snapshot_commit.rs` |
| Change what a concurrent commit must touch to conflict with a DML | `../conflict_filter.rs` (derivation) + `CommitScope` in `snapshot_commit.rs` (threading) |
| Change rejected-commit file cleanup | `abort.rs` + `commit_overwrite` / `commit_row_delta_kind` |
| Add a unit pin for SQL shape | `tests/merge.rs` |
| Touch OCC commit behavior | `tests/occ.rs` / `tests/occ_conflict.rs` |
| Touch MERGE OCC onto a named branch | `tests/occ_branch.rs` |

## Pointers

Up: [../map.md](../map.md). Fork contract: `docs/ENGINE_CONTRACT.md` (owned fork).

## Debug

- `--list` paths must stay `write::merge::<battery>::<test>` — identity gate for the
  declared-rename census.
- A MERGE / UPDATE / DELETE aborts on a concurrent commit to a DIFFERENT partition: print the
  filter the commit carried (`CommitScope.conflict_filter`). `TRUE` means no target-only
  predicate converted — check `../conflict_filter.rs` (bare column in an `ON`? a function call?
  a `WHEN NOT MATCHED BY SOURCE` clause?). Spark refuses the same shapes; see the
  `ICE-OCC-SCOPED-1` rows in `docs/spark-sql-iceberg-parity.md` before "fixing" one.
- Rejected MERGE left new Parquet files: cleanup is `tx.commit` `Err` only in
  `commit_overwrite` / `commit_row_delta_kind` via [`abort.rs`](abort.rs). A catch
  that can fire after a successful commit is a HALT.
- Pub `write_data_files*` re-exported from the write module root (`../mod.rs`) and the crate
  root (`lib.rs`).

## IPI-19 + IPI-56 (2026-09-20) — the MERGE evolution flag

- `spec.rs` — `MergeSpec` and its clause types, moved here from `mod.rs` (which
  sits on an exact size baseline) with the new `schema_evolution` flag. When the
  flag is set `execute_merge` unions the source schema into the table first and
  expands `UPDATE SET *` / `INSERT *` against the evolved schema, so the single
  data commit carries the new column.
  pins: ipi-19-56-37-schema-evolution-write/C-005, C-006, C-009

## IPI-51 PR6 slice 2 (2026-09-21) — MERGE-star missing-column stamp

- `mod.rs` — `expand_star_clauses` renders a missing source column through
  `repark_common::spark_error::message` with `UNRESOLVED_COLUMN.WITH_SUGGESTION` /
  `42703`: the first missing target column is `{columnName}` and the source columns
  are `{suggestions}`. The custom `missing from the source` text and the
  case-sensitivity suffix are gone; the `SourceMatch::Ambiguous` arm keeps its own
  text. The edit is line-neutral on the 1656 baseline. Pin:
  `expand_star_clauses_errors_on_missing_source_column` in `tests/merge.rs`.
  pins: ice-error-conditions-1/C-011

## U6 WRITE-REFUSALS (2026-09-24)

- `spec.rs` — `MergeSpec::assigns_columns` is true when any clause updates or
  inserts. Spark does not evolve a `DELETE`-only MERGE, so `execute_merge` now
  unions the schema only when the flag is set and a clause assigns columns.
- `mod.rs` — `union_source_schema` calls `evolve_merge_schema`, which adds the
  type changes. The edit is line-neutral on the 1630 baseline.
  pins: u6-write-refusals/C-007, C-008
