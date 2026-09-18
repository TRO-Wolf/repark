# map — repark-iceberg/src/write/merge/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Wrapped-line fragments rewritten as complete sentences (D-002).

## Purpose

MERGE unit tests. `merge/mod.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index (rustfmt module order).
- `dv_commit_opens.rs` — **RP-10 (2026-09-04):** the 192-manifest pure-DV identity DELETE
  of the newest row commits after every data manifest except the one that holds the
  touched file is hidden (F-25 `validate_fresh_dvs_only` stops once every `added_dvs`
  key is found — commit-phase opens = 1). Hiding that last manifest too refuses.
  `execute_predicate_dml` on the same fixture deletes the newest id. Close-phase
  opens stay 0 (RP-9 hide pin). Scan stays 3×N (`PERF-SCAN-3PASS-1`).
  pins: rp-10-repin-f25/C-002
- `merge.rs` — primary unit battery. **FNP-4B (2026-09-15):** MERGE internal-SQL
  expectations in backtick form (user names via `quote_ident`, fixed engine names bare).
  pins: fnp-4b/C-002
- `merge_dialect.rs` — **FNP-4B round 6 (2026-09-15):** the four MERGE internal
  statements carry no double-quoted identifier and parse under the Spark Databricks
  dialect. pins: fnp-4b/C-024
- `lineage.rs` — V3-7 rewrite-projection and scratch-schema pins for carried `_row_id`.
  pins: v3-7-merge-lineage/C-001
  **FNP-4B (2026-09-15):** projection expectations in backtick form. pins: fnp-4b/C-002
- `lineage_stream.rs` — **ICE-SORTED-INSERT-1 round 4 (2026-09-17):** drives
  `row_lineage::write_partitioned_lineage_files` on a v3 identity-partitioned memory-catalog
  table with a probe stream that counts the parquet files under the warehouse just before it
  yields its last batch. Unsorted table: a file already exists at that point (the rewrite
  streams into the writer), files stamp 0, and each partition file keeps arrival order with
  `_row_id` / `_last_updated_sequence_number` on their rows. `WRITE ORDERED BY (id)`: no file
  exists before the stream ends (drain, sort, then write), files stamp 1, and the id order
  carries the lineage columns with it. Mutations: the unsorted arm draining first reds the
  unsorted pin; the declared arm skipping the sort reds the ordered pin.
  pins: ice-sorted-insert-1/C-006, C-010
- `nmbs.rs` — DML-A `WHEN NOT MATCHED BY SOURCE` SQL-fragment pins and skip_cardinality
  with an NMBS clause present.
  pins: dml-a-merge-not-matched-by-source/C-002, C-003, C-004, C-005
- `occ_conflict.rs` — OCC-2 M19/M20 batteries B/C/E/F/G/H/I.
  `id_batch` and `path_exists` are `pub(super)` for `commit_unknown.rs`'s
  keep-set pins (M14 `write_data_files` pattern).
  pins: ice-commit-unknown-1/C-003
- `occ.rs` — OCC / commit conflict pins + M13 isolation parse + M19-A split.
  RP-5 C-007: snapshot isolation still commits through a concurrent append.
  pins: rp-5-fork-repin/C-007
- `occ_branch.rs` — RP-5 critic OCC-on-branch: concurrent branch append is
  `DataInvalid` / not retryable / `Found conflicting files…`; concurrent main append
  does not fail the branch commit.
  pins: rp-5-fork-repin/C-004
- `parallel_write.rs` — concurrent file write pins.
- `evolved_scan.rs` — **ICE-EVO-DML-1 (2026-09-17):** a table evolved by `ADD COLUMN`,
  `RENAME COLUMN`, a key rename or a name swap after its only write, scanned by
  `TargetScanStream` at that snapshot with and without a partition sink.
  `target_scan_after_add_column_null_fills_the_added_column`,
  `target_scan_after_rename_reads_the_renamed_column_by_field_id` and
  `target_scan_after_swapping_two_names_keeps_each_value_under_its_field_id` hold the rows
  under the current schema; `target_scan_residual_on_a_renamed_key_keeps_the_matching_row` and
  `target_scan_residual_on_a_swapped_name_filters_the_current_field` hold that a residual never
  refuses and never prunes by another field's bounds;
  `target_scan_residual_on_an_unchanged_column_still_prunes_after_add_column` holds the kept
  arm on a two-file table.
  pins: ice-evo-dml-1/C-004, C-005, C-010, C-011
- `promoted_scan.rs` — **ICE-PROMOTE-READ-1 (2026-09-16):** after a legal type
  promotion with no write since, the DML target scan pins the pre-promotion snapshot and
  reads `Int32` / `Float32` / `Decimal128(9,2)`; the scratch schema is built from the
  current schema. `conform_scan_batch_widens_legally_promoted_columns` and
  `target_scan_over_a_single_era_promoted_table_yields_the_current_types` hold the widened
  values and types; `conform_scan_batch_still_refuses_an_illegal_narrowing` keeps every
  non-promotion mismatch loud.
  pins: ice-promote-read-1/C-006, C-011
- `partition_sink.rs` — **RP-7 (2026-09-02):** the identity/MERGE target scan records each
  planned `FileScanTask`'s `(spec_id, partition)`, so the v3 DV close never re-walks the data
  manifests it just read. The pin compares the drained sink to the manifest truth on a
  three-partition table; mutation (make `record_scanned_partitions` a no-op) 1 red of 1.
  **RP-9 r2:** `a_multi_manifest_identity_scan_records_the_touched_path` drains the production
  identity-SQL sink on an 8-manifest v3 table and requires the touched `_file` in the map
  (`record_scanned_partitions` and the close `retain` keep it). `execute_predicate_dml_deletes_id_zero_on_an_eight_manifest_table`
  runs the production identity DELETE on that fixture.
  **PERF-SCAN-1 (2026-09-03 / r2 2026-09-04):** that same 8-manifest drain also requires the
  drained `known_partitions` map to equal the manifest walk.
  `three_concurrent_target_scan_executes_plan_data_manifests_once` starts three
  `StreamingTable` executes together and requires one `plan_files` (hardening; mutation
  skip-cache 1 red of 1, got 3). Production-path `plan_files==1` pins were deleted: the
  identity DELETE / matched-delete MERGE call `execute` once, so those pins cannot go red.
  pins: rp-7-f18-repin/C-002
  pins: rp-9-repin-f23/C-005
  pins: perf-scan-1-plan-once/C-001, C-002
- `occ_scoped.rs` — **ICE-OCC-SCOPED-1 (2026-09-17):** the fault-injected race pins, one per
  measured Spark 4.1.2 shape, each over v2 AND v3 and (where the oracle has both) merge-on-read
  AND copy-on-write. `RaceCatalog` delegates to a memory catalog and, on the victim's FIRST
  `update_table`, lands a real concurrent commit (an identity UPDATE / DELETE through
  `execute_predicate_dml`, or a public `append`) on the inner catalog before forwarding — so the
  victim has already planned and written from the parent snapshot, its requirement fails, and
  the fork refreshes and re-validates against the concurrent snapshot exactly as a real race
  does. `fired()` is asserted so no pin passes on a race that never landed. The concurrent MERGE
  of the oracle is modelled by an identity UPDATE of the other partition (the `#[cfg(test)]`
  MERGE lock forbids a nested `execute_merge`); it commits the same file kinds (MoR: a delete
  file + a data file; COW: a removed + an added data file). Every pin also asserts the exact
  surviving rows. The UPDATE pin drives RePark's identity UPDATE executor with a convertible
  plain `WHERE`; production reaches that executor only with a bare `col IN (SELECT …)`, whose
  filter is `AlwaysTrue` (review L-05), and a plain-`WHERE` UPDATE runs the fork exec and is the OPEN registry row
  ICE-OCC-SCOPED-1-PLAIN-UPDATE. Six cells commit (C-006..C-010, one row each for MERGE / UPDATE / DELETE /
  MERGE-vs-INSERT / COW range) and three refuse with Spark's own messages: the MoR range MERGE
  (`Found new conflicting delete files that can apply to records matching id < 50`), the
  disjoint-key MERGE (`… matching TRUE`, serializable and snapshot), and a
  `WHEN NOT MATCHED BY SOURCE` MERGE (C-004). Red-first on the pre-fix head: 6 of 7 FAILED with
  `Found conflicting files that can contain records matching TRUE` naming the other
  partition's file; the disjoint-key refusal is an over-fix guard and passes on both heads. The
  mutation table is in the unit ledger.
  Round 2 (review L-03) adds the over-NARROW guard the refusal pins could not be: a concurrent
  INSERT into the victim's OWN partition of a row its own predicate matches (MERGE `ON t.k =
  'a' …` vs `(1004, 'a')`, DELETE `k = 'a' AND id > 90` vs `(1004, 'a')`, UPDATE `k = 'a' AND id
  < 8` vs `(-4, 'a')`, each v2/v3 × MoR/COW) must abort under serializable, naming the scoped
  filter. An extra conjunct smuggled into the filter (mutation M11 conjoins `id = 0`) excludes
  the new row, the victim commits, and all three pins red.
  pins: ice-occ-scoped-1/C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011, C-012, C-019
- `occ_scoped_insert.rs` — **ICE-OCC-SCOPED-1 round 2 (2026-09-18, review L-02):** a MERGE with
  `WHEN NOT MATCHED THEN INSERT *`, scoped by `ON t.k = 'a' AND t.k = s.k AND t.id = s.id`,
  racing one append through `occ_scoped.rs`'s `RaceCatalog` (its harness items are
  `pub(super)` for this sibling; `occ_scoped.rs` sits near its line ceiling). Four cells × v2/v3
  × MoR/COW; every expectation — commit or the scoped abort, the rows at or above id 1000, the
  row count — is read from Spark's deterministic recording `spark_occ_oracle3.json` under
  `python/repark-parity/fixtures/torture/data/ice_occ_scoped_1/`, and the pin first checks that
  its statement and append still match the recorded SQL.
  pins: ice-occ-scoped-1/C-020
- `occ_partitions.rs` — **RP-7 (2026-09-02):** one battery through the PRODUCTION
  `commit_row_delta_kind_with_partitions` variant on a partitioned v3 table with a real partition
  map: the commit lands, and a stale `validate_from_snapshot` pin is still rejected with the
  table unmoved. `occ.rs` / `occ_conflict.rs` keep their spellings and exercise the empty-map
  wrappers, which have no production caller left and are `#[cfg(test)]`.
  ICE-OCC-SCOPED-1: `RowDeltaPolicy` carries a `CommitScope` and is passed by reference, so the
  battery builds it with `CommitScope::unscoped` and lends it twice (as do `occ_conflict.rs`,
  `dv_commit_opens.rs` and `../dv_close.rs`'s seam tests — line-neutral, baselines held);
  `occ_branch.rs` passes `&Predicate::AlwaysTrue` to `commit_on_ref`.
  pins: rp-7-f18-repin/C-002
- `commit_unknown.rs` — **ICE-COMMIT-UNKNOWN-1 (2026-09-14):** a delegating catalog returns
  `ErrorKind::CommitStateUnknown` from `update_table` after capturing the stamped
  `engine.operation-id`; the surfaced `CommitStateUnknownError` carries exactly that id, and
  `update_table` is attempted once (the fork never retries the ambiguous kind). Covers both
  the `commit_overwrite` and `commit_row_delta` MERGE commit paths. The two
  `*_leaves_written_files_on_disk` pins stage REAL Parquet files via `write_data_files`
  and assert `path_exists` after the unknown — the `abort.rs` keep-set, mutation-proven
  (deleting the carve-out reds both).
  pins: ice-commit-unknown-1/C-003, C-007
- `streaming_scan.rs` — streaming target-scan pins + PERF-04 residual-push + MG-1.
- `streaming.rs` — stream write interleaving pins.

## Pointers

- Up: [../map.md](../map.md)
