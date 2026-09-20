# Unit ledger — ICE-CHANGELOG-1 · incremental append reads, `t.changes`, `create_changelog_view` (round 1)

**Date:** 2026-09-20 · **Branch:** `fix/ice-changelog-1` · **Base:** `e4160a58` (`origin/main`)
**Model:** Claude Opus 5 (max) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Owner direction 2026-09-18 is 1:1 parity with Spark's Iceberg integration, and
2026-09-19 gates v1.5.0 on it. Parity inventory IPI-22 is eleven cells Spark answers and RePark
refused: the reader options `start-snapshot-id` / `end-snapshot-id` refused as "a future seed",
`SELECT … FROM t.changes` failed with the 4-part-identifier error, and
`CALL <cat>.system.create_changelog_view` was not a supported procedure.

**What it is.** Three doors over one Iceberg range scan. (1) The incremental APPEND read: the
window reader options reach the fork's `IncrementalAppendScan`. (2) The `t.changes` RELATION on
the SQL door and the reader door: raw INSERT/DELETE rows plus `_change_type`, `_change_ordinal`
and `_commit_snapshot_id`. (3) The `create_changelog_view` PROCEDURE: the Spark-engine row
transforms over that relation, registered as a lazy session view.

**Not in this unit:** row-level changelog (`ChangelogTaskKind::DeletedRows`), which Iceberg
1.11.0's own `ChangelogRowReader` does not implement either — a range holding delete manifests
refuses on both engines with the same message; `remove_carryovers` as a procedure ARGUMENT
(Iceberg 1.11 does not declare it — the behaviour is implemented, the argument is not accepted);
`t.branch_b.changes`; `STATUS.md`; `Cargo.toml` / `Cargo.lock`; any size ceiling.

**Writable paths:** `crates/repark-core/src/time_travel*`, `crates/repark-iceberg/src/catalog/`,
`crates/repark-spark/src/{call,call.rs,call_args.rs,router.rs,time_travel*}`,
`crates/repark-python/src/session_sources.rs`, `python/repark/src/repark/spark/session/`,
`python/repark/tests/`, `docs/spark-sql-iceberg-parity.md`, this ledger, touched `map.md` files.

## Measured

Oracle: live PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 (`local[1]`, InMemoryCatalog
`sc`), **36** `QI-*` / `QC-*` cells recorded by the orchestrator's run-25c measurement (harness
`/tmp/oc-worker/nc-inventory/harness.py`, cells `cells_qc4.py`), committed verbatim as
`python/repark/tests/ice_changelog_1_spark_oracle.json` (SHA-256
`c2002461483cd468541f52b953d619551e34153bbad9611b4e27271adcbb76fc`). They are a SUPERSET of the
eleven inventory cells this unit closes.

Java behaviour was read off the 1.11.0 Spark runtime jar's BYTECODE, not from documentation:
`SparkReadConf.incrementalAppendScanBoundaries`, `SparkScanBuilder`'s constructor,
`SparkChangelogScanBuilder.buildChangelogScan` / `getStartSnapshotId` / `getEndSnapshotId`,
`SnapshotUtil.oldestAncestorAfter`, `CreateChangelogViewProcedure.call` /
`computeUpdateImages` / `removeCarryoverRows` / `sortSpec`, `RemoveCarryoverIterator`,
`RemoveNetCarryoverIterator` and `ComputeUpdateIterator`.

## Clauses

| Clause | Statement | Test | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | The reader options `start-snapshot-id` / `end-snapshot-id` plan the fork's `IncrementalAppendScan` over `(from, to]` and return exactly the rows the APPEND snapshots in the window added. | `test_incremental_window_between_two_snapshots`, `test_incremental_over_a_partitioned_table`, `test_incremental_over_a_v3_table` | PROVEN | Cells QI-S0-S2, QI-PARTITIONED, QI-V3. pins: ice-changelog-1/C-001 |
| C-002 | A start with no end reads to the table's current snapshot. | `test_incremental_window_open_end_runs_to_the_current_snapshot` | PROVEN | Cell QI-S0-OPEN. pins: ice-changelog-1/C-002 |
| C-003 | A non-APPEND snapshot inside the window contributes nothing and does NOT raise — a CoW delete, a MoR delete, an INSERT OVERWRITE, and an overwrite snapshot as the window's end. | `test_incremental_over_a_copy_on_write_delete_skips_it`, `test_incremental_over_a_merge_on_read_delete_skips_it`, `test_incremental_over_an_insert_overwrite_skips_it`, `test_incremental_ending_at_a_delete_snapshot_does_not_raise` | PROVEN | Cells QI-OVER-DELETE-COW, QI-OVER-DELETE-MOR, QI-OVER-OVERWRITE, QI-END-AT-DELETE. The inventory cell named `R-DF-INCREMENTAL-OVERWRITE-ERR` does NOT raise on Spark; the recorded answer is the contract. pins: ice-changelog-1/C-003 |
| C-004 | `end-snapshot-id` with no `start-snapshot-id` refuses with Java's ``Cannot set only `end-snapshot-id` for incremental scans. Please, set `start-snapshot-id` too.`` | `test_incremental_end_without_start_refuses`; `append_boundaries_refuse_an_end_without_a_start` | PROVEN | Cell QI-END-ONLY, class `IllegalArgumentException`. pins: ice-changelog-1/C-004 |
| C-005 | `start-timestamp` / `end-timestamp` on a plain table load refuse with Java's ``Only changelog scans support `start-timestamp` and `end-timestamp`. Use `start-snapshot-id` and `end-snapshot-id` for incremental scans.`` — they are changelog-only, and they stay loud on a non-Iceberg format. | `append_boundaries_refuse_a_timestamp_window`; `test_denylist_semantic_keys_fail_loud` | PROVEN | Read off `SparkReadConf.incrementalAppendScanBoundaries`'s first `checkArgument`. pins: ice-changelog-1/C-005 |
| C-006 | A time-travel pin beside a window refuses `Cannot use time travel in incremental scan`, and a legacy `snapshot-id` keeps Spark's "no longer supported" message — raised AFTER the boundary checks, the order Java's `SparkScanBuilder` constructor uses. | `test_incremental_with_version_as_of_refuses`; `a_time_travel_pin_beside_a_window_is_refused`; `a_legacy_snapshot_pin_beside_a_window_keeps_sparks_legacy_message` | PROVEN | Cell QI-WITH-VERSIONASOF. pins: ice-changelog-1/C-006 |
| C-007 | An unknown start, a start equal to the end, a start after the end and a start on another branch's lineage all refuse with Java's `Starting snapshot (exclusive) <s> is not a parent ancestor of end snapshot <e>` — the message the fork's planner already carries. | `test_incremental_unknown_start_refuses`, `test_incremental_start_equal_to_end_refuses`, `test_incremental_start_after_end_refuses`, `test_incremental_snapshot_bounds_reach_the_incremental_scan` | PROVEN | Cells QI-START-UNKNOWN, QI-S1-S1, QI-START-AFTER-END. pins: ice-changelog-1/C-007 |
| C-008 | A projection and a filter compose over the window, and the scan reads the `to` snapshot's schema — a column added inside the range is projected, null for the older rows. | `test_incremental_composes_with_projection_and_filter`, `test_incremental_reads_the_end_snapshots_schema` | PROVEN | Cells QI-PROJECT-FILTER, QI-SCHEMA-EVOLVED. pins: ice-changelog-1/C-008 |
| C-009 | `t.changes` is a RELATION, not a metadata table: the table's columns plus `_change_type` (string), `_change_ordinal` (int) and `_commit_snapshot_id` (bigint), raw INSERT/DELETE, whole history when no window is given — including a copy-on-write UPDATE's carryover rows, which the relation keeps. | `test_changes_relation_whole_history`, `test_changes_relation_schema`, `test_changes_relation_over_a_copy_on_write_update`, `test_changes_relation_over_a_whole_file_merge_on_read_delete`, `test_changes_relation_v3_deletion_vector` | PROVEN | Cells QI-CHANGES-DEFAULT, QI-CHANGES-COLS, QI-CHANGES-COW-UPDATE, QI-CHANGES-MOR-DELETE, QI-CHANGES-V3-DV. pins: ice-changelog-1/C-009 |
| C-010 | The changelog window takes snapshot ids or TIMESTAMPS, resolved as `SparkChangelogScanBuilder` resolves them: the start timestamp maps through `SnapshotUtil.oldestAncestorAfter` (its own id when the timestamp matches exactly, else its PARENT), the end timestamp to the newest ancestor at or before it, with Java's two empty-scan short-circuits. | `test_changes_relation_reader_window`, `test_create_changelog_view_snapshot_window`, `test_create_changelog_view_start_timestamp_zero` | PROVEN | Cells QI-CHANGES-READER-OPEN, QC-OPT-RANGE, QC-OPT-TS. pins: ice-changelog-1/C-010 |
| C-011 | Carryover removal is the procedure's default and is NOT optional: a row rewritten by a copy-on-write UPDATE but not changed produces no DELETE/INSERT pair. Equality is over every column but `_change_type`, so a pair must share one snapshot. | `carryover_removal_drops_the_rewritten_but_unchanged_row`, `carryover_removal_keeps_a_delete_and_insert_from_different_snapshots`, `test_create_changelog_view_default` | PROVEN | Cell QC-DEFAULT — 8 rows, not the raw 10. pins: ice-changelog-1/C-011 |
| C-012 | `net_changes` is Java's `RemoveNetCarryoverIterator`: equality over the DATA columns only, a running net count, and a zero crossing that restarts the group — so a surviving row carries the LAST snapshot that touched it, and a row deleted and re-inserted inside the window disappears. | `net_changes_keep_each_surviving_row_once_at_its_last_ordinal`, `net_changes_drop_a_row_deleted_and_reinserted_inside_the_window`, `net_changes_keep_a_row_only_deleted_inside_the_window`, `test_create_changelog_view_net_changes` | PROVEN | Cell QC-NET — `[1,a,INSERT,3]`, the last ordinal, which a "drop twins then collapse" chain gets wrong. pins: ice-changelog-1/C-012 |
| C-013 | `compute_updates` is carryover removal FIRST and then `ComputeUpdateIterator`: within the sort (identifier columns, `_change_ordinal`, `_change_type`) a DELETE pairs with the INSERT that follows it; an unpaired DELETE at a later ordinal stays a DELETE; two rows sharing an identifier raise Java's multiple-rows message; and an identifier list with no `compute_updates` still pairs. | `compute_updates_pairs_one_ordinals_delete_and_insert`, `an_unpaired_delete_at_a_later_ordinal_stays_a_delete`, `compute_updates_refuses_two_deletes_of_one_identifier`, `compute_updates_over_a_non_unique_identifier_answers_as_spark_does`, `test_create_changelog_view_compute_updates`, `test_create_changelog_view_duplicate_identifier_values` | PROVEN | Cells QC-UPDATES-IDENT and QC-SQL-UPDATE-DUP — the same 8 rows from `array('id')` and from `array('cat')`. pins: ice-changelog-1/C-013 |
| C-014 | The procedure answers Java's parameter list and Java's refusals: the default view name is `` `<table>_changes` `` WITH backticks in the returned row and without them in the registered name; `net_changes` beside update images refuses `Not support net changes with update images`; update images with no identifier columns refuse `Cannot compute the update images because identifier columns are not set`; the fallback is the table's identifier fields. | `test_create_changelog_view_default_name_is_backticked`, `test_create_changelog_view_net_changes_with_updates_refuses`, `test_create_changelog_view_without_identifier_columns_refuses`, `test_create_changelog_view_schema` | PROVEN | Cells QC-DEFAULT-NAME, QC-NET-AND-UPDATES, QC-UPDATES-NO-IDENT, QC-SCHEMA. pins: ice-changelog-1/C-014 |
| C-015 | A changelog range holding row-level delete files refuses with `Delete files are currently not supported in changelog scans` — the same string Iceberg 1.11.0 carries — and it surfaces when the VIEW IS READ, not when the procedure is called, because the registered view is lazy as Spark's is. | `test_create_changelog_view_on_a_merge_on_read_table_refuses_at_read` | PROVEN | Cell QC-MOR: `out.rows` records the CALL succeeding and `error_step` records the failure at `SELECT … FROM v_qc_mor`. pins: ice-changelog-1/C-015 |

## Rulings taken (and the measurement behind each)

1. **The packet's D-2/D-3 pipeline is wrong and was replaced (addendum A-2, confirmed here).**
   Java is an if/else, not a chain: `computeUpdateImages` OR `removeCarryoverRows(net_changes)`.
   The chain "carryovers → pairing → net" produces `[1,a,INSERT,0]` for QC-NET where Spark
   records `[1,a,INSERT,3]`.
2. **The packet's D-7 is wrong and was struck (addendum A-1).** `with_row_level_deletes(true)` is
   NOT passed: Iceberg 1.11.0 refuses a delete-manifest changelog range itself, so passing the
   flag would make RePark answer where Spark refuses. QC-MOR pins the refusal.
3. **`t.changes` is not a metadata table (D-4).** `METADATA_TABLE_NAMES` is untouched and
   `metadata_tables.rs` holds its exact 1062-line baseline.
4. **Timestamps resolve through Java's own helpers (A-3), not through `resolve_snapshot_id`.**
   `oldestAncestorAfter` returns the snapshot's own id only on an exact timestamp match and its
   PARENT otherwise; a cell using exact snapshot timestamps would pass either way, so the
   implementation follows the bytecode.
5. **`identifier_columns` parsing landed as `CallArgs::optional_string_array`** rather than
   beside `extract_option_pairs` (addendum A-9's placement): the sibling parsers
   (`optional_string`, `optional_bool`, `optional_i64`) all live on `CallArgs`, and `array(…)`
   is an argument shape, not a rewrite option.

## Residues

1. **The RePark half needs the fork pin to move.** `ChangelogTableProvider` calls the fork's new
   `iceberg::arrow::ChangelogReader` (fork branch `fix/f-changelog-reader-1`, ledger
   `task/f-changelog-reader-1-ledger.md` in the fork). Until that merges and the pin bumps, this
   branch builds only against a local path override, and CI cannot be green.
2. **`QC-UPDATES-TABLE-IDENT` is not pinned.** Spark itself errored on that cell at its `ALTER
   TABLE … ALTER COLUMN id SET NOT NULL` step (`_LEGACY_ERROR_TEMP_2330`), before the procedure
   ran, so the cell measures an ALTER divergence and not this unit's surface. The identifier
   FIELD fallback is still implemented and is covered by C-014's argument path.
3. **`Identifier field is required as table contains unorderable columns: %s`** (Java's third
   `checkArgument` in `CreateChangelogViewProcedure.call`) is not implemented: no cell has a map
   column. A table with an unorderable column and no identifier columns will sort where Spark
   refuses.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-changelog-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every Spark-visible answer is pinned cell for cell against 36 recorded live-Spark cells committed verbatim as the oracle, a superset of the eleven inventory cells the unit closes; Java's own bytecode was read for every message and every iterator rule rather than inferred.
      artifacts: [python/repark/tests/test_ice_changelog_1.py, python/repark/tests/ice_changelog_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: The row transforms are pinned on synthetic rows where a wrong rule survives the cells — a delete and insert one snapshot apart (carryover must NOT fire), a row deleted and re-inserted inside the window (net must drop it), an unpaired DELETE at a later ordinal (pairing must not fire), and two deletes of one identifier (Java's refusal). The packet's own chain design fails the net pin, which is how it was caught.
      artifacts: [crates/repark-spark/src/call/changelog/tests.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Both doors carry the relation (SQL `FROM t.changes` and `format('iceberg').load(t + '.changes')` with window options), and the procedure is exercised through the Spark CALL door with every argument Iceberg 1.11 declares.
      artifacts: [python/repark/tests/test_ice_changelog_1.py, crates/repark-spark/src/time_travel/changes.rs]
    - id: AT-4
      status: N/A
      justification: No new shared mutable state and no concurrency. The providers are per-statement and immutable; the only registry touched is the session's temp-view namespace, through the existing `PinnedViews` release path.
    - id: AT-5
      status: ATTACKED
      evidence: The unit adds no write path. The one place it could answer where Spark refuses — a merge-on-read changelog range — is pinned to Spark's refusal instead, and the fork flag that would have answered it is deliberately not passed.
      artifacts: [python/repark/tests/test_ice_changelog_1.py]
    - id: AT-6
      status: ATTACKED
      evidence: Three residues are written down rather than smoothed over: the fork pin this branch needs, the unpinned QC-UPDATES-TABLE-IDENT cell (Spark itself errored at its ALTER step), and Java's unorderable-columns refusal, which no cell measures and which is not implemented.
      artifacts: [task/ledgers/staging/ice-changelog-1-ledger.md, docs/spark-sql-iceberg-parity.md]
    - id: AT-7
      status: ATTACKED
      evidence: The before/after counts are this unit's own replay of the same cells on the same box, reported in the hand-back with the eleven inventory cell names.
      artifacts: [task/ledgers/staging/ice-changelog-1-ledger.md]
    - id: AT-8
      status: ATTACKED
      evidence: No dependency, no feature, no committed Cargo.toml or Cargo.lock change, and no ceiling moved — both `lib.rs` files stayed at 150 by nesting the new modules under `time_travel` and `call`, and `metadata_tables.rs` holds its exact 1062 baseline because `t.changes` is resolved as its own relation kind.
      artifacts: [crates/repark-core/src/lib.rs, crates/repark-spark/src/lib.rs]
    - id: AT-9
      status: ATTACKED
      evidence: Seven maps carry the change with pins citations — repark-core src and time_travel, repark-iceberg catalog, repark-spark src and call and call/changelog, the facade session map and the Python tests map — and the registry row is filed with before/after.
      artifacts: [crates/repark-core/src/map.md, crates/repark-iceberg/src/catalog/map.md, crates/repark-spark/src/call/map.md, docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: The local gate ran the comment gate, the release native, the three named crates' test suites and the unit's pytest file offline and live against real Spark; CI runs the rest.
      artifacts: [python/repark/tests/test_ice_changelog_1.py]
  complete: true
```
