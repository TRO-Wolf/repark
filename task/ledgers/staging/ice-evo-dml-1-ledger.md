# Unit ledger — ICE-EVO-DML-1 · DML after ADD / RENAME COLUMN with no write since answers Spark

**Date:** 2026-09-17 · **Branch:** `fix/ice-evo-dml-1` (stacked on `fix/ice-promote-read-1`
`75433230`; rebases onto `main` after unit 1 and its pin bump merge)
**Model:** claude-opus-5 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Resume round (2026-09-17, run 20a):** muse-spark-1.3-contributor — registry finish, gates,
hand-back; root-cause analysis of the residual UPDATE red below.
**Round 2 (2026-09-17, run 20a):** muse-spark-1.3-contributor — ruling Q-20a-1 option (b):
fork PR #289 F-EVO-SCAN-1 merged as `f7ecd855` (F-PROMOTE-READ-1 #285 merged as `96fc9f1f`
beneath it); rebased onto `fix/ice-promote-read-1`, rebuilt the release native under the run
override, re-measured the 33 UPDATE cells (green), replaced the local re-point with the fork
`project_current_schema()` API, registry + ledger close-out. No pin bump here (separate PR).
**Round 3 (2026-09-18, run 21a):** muse-spark-1.3-contributor — the logic critic's P2 cells
(L-02 shapes (1)–(4), v2/v3 × CoW/MoR, both doors where the shape has one), then ledger
close-out under ruling Q-21a-3. No Rust change; the release native is untouched.
**Path:** HIGH. **risk_tier: high** — every MERGE / UPDATE / DELETE refuses after a spec-legal
`ADD COLUMN` or `RENAME COLUMN`, and after a rename that reuses a name the same DML silently
writes one column's values under another column.
**Fork:** none. The fork's pinned scan binds names against the pinned snapshot's schema, which is
Java's `useSnapshot` contract; the re-projection onto the current schema is the engine's DML
planner (Spark's `SparkScanBuilder` does `useSnapshot(…).project(expectedSchema)`), so the fix is
RePark-side. RePark builds against unit 1's fork branch through the local override (never
committed).

**Retires:** this ledger moves to `../completed/` in the unit's last commit.

**Why now.** Run-19a report rows V2-10e and V3-11 (schema evolution): after
`ALTER TABLE t ADD COLUMN extra STRING` or `RENAME COLUMN w TO v`, with no append since, 24 of 24
DML shapes (v2/v3 × CoW/MoR) fail `DataInvalid => Column extra not found in table`, including
RePark's MERGE `*` on a Spark-created table Spark just evolved. Spark 4.1.2 runs every one.

**Not in this unit:** `STATUS.md`, `Cargo.toml`, `Cargo.lock`, `.github/`, the fork pin;
`MERGE WITH SCHEMA EVOLUTION` (parse refusal, registry §2.3); `_spec_id` / `_partition`
metadata columns on reads.

## Reproduction (step 1, release native `75433230` + fork `04a338c3`, 2026-09-17)

Probes copied to `/tmp/oc-worker/ia-build/probes2/` (RePark halves only; scratch prefix
rewritten); logs in `/tmp/oc-worker/ia-build/repro2/`.

| Probe | Result on RePark |
|---|---|
| `p_addcol_dml` (64 cases) | between=none: **24 / 24** MERGE `*`, MERGE explicit, MERGE old columns only, UPDATE new column, UPDATE old column, DELETE — v2/v3 × CoW/MoR — `DataInvalid => Column extra not found in table. Schema: table { 1: id … 2: v … }`; INSERT and INSERT OVERWRITE succeed; between=insert: all 32 succeed |
| `p_ddl_then_dml_rp` | `rename_col` then MERGE `UPDATE SET v = s.v` → `Column v not found in table`; ADD COLUMN then MERGE → `Column extra not found`; DROP COLUMN, ADD PARTITION FIELD, SET TBLPROPERTIES, promote non-key then MERGE → OK |
| `p_merge_star_rp` | MERGE `*` from a wide, exact and reordered source → `Column extra not found`; narrow source → `AnalysisException` (Spark also refuses) |
| `p_empty_merge` | no-snapshot table and no-snapshot + ADD COLUMN → OK; **DELETE-emptied + ADD COLUMN → MERGE `*` refuses**; INSERT OVERWRITE unblocks; **no-op `rewrite_data_files` does not** |
| `p_rename_swap` (new) | `RENAME v TO tmp; RENAME extra TO v; RENAME tmp TO extra`, no write since: every DML **succeeds and writes swapped values** — CoW MERGE `UPDATE SET v` on `[(1,'e1','a'), (2,'e2','b')]` → `[(1,'m1','e1'), (2,'b','e2')]` (the untouched row 2 is rewritten swapped); CoW DELETE of id 2 leaves `(1,'a','e1')`; MoR rewrites the updated row swapped |

| `p_lineage_read` (new, v3) | after ADD / RENAME: `SELECT _row_id, …` refuses `Column extra` / `Column v not found in table` (plain reads are right); after a name swap `SELECT _row_id, id, v, extra` → `[(0, 1, 'e1', 'a'), (1, 2, None, 'b')]` (swapped) and `… WHERE v = 'a'` → `[]` (silent row loss) |

## Root cause

`crates/repark-iceberg/src/write/merge/target_scan.rs:95` (`plan_file_scan_tasks`) and `:240`
(`TargetScanStream::execute`, the `to_arrow` branch) build
`table.scan().snapshot_id(pin).select(select_columns)`, where `select_columns` are the scratch
schema's names — the table's **current** schema plus `_file` / `_pos` (and `_row_id` /
`_last_updated_sequence_number` on v3). The fork's `TableScanBuilder::build`
(`crates/iceberg/src/scan/mod.rs:447-459` at fork `04a338c3`) binds those names against
`snapshot.schema(metadata)` — the schema recorded on the pinned snapshot. DDL writes no snapshot,
so until the next write that schema is the pre-DDL one:

- a name added or renamed-to since the snapshot is absent → `Column … not found in table`;
- a name that exists in both schemas under **different field ids** (a rename that reuses a name)
  binds to the old field → the scan returns the other column's values, and the residual filter
  (`residual_join_key_filter`, `identity_scan_residual`) prunes on the other column's bounds.

Every DML shape reads through `TargetScanStream`: MERGE (`merge/mod.rs:187`), identity DELETE and
UPDATE (`predicate_dml.rs:268`, `:347`), the affected-file rewrite (`predicate_dml.rs:642`) and
the COW scratch (`merge/cow_scratch.rs:131`). The v3 lineage read has the same seam: `catalog/lineage_columns.rs:226`
(`scan_lineage_batches`) builds `table.scan().select(column_names)` with current-schema names, and
the fork's unpinned scan binds them against the **current snapshot's** schema (Java's
`newScan()` binds `table.schema()`). INSERT / INSERT OVERWRITE never scan the target, and
they write a snapshot under the current schema — which is why they unblock it; a no-op
`rewrite_data_files` commits nothing.

## PROPOSITION LEDGER — ICE-EVO-DML-1 — 2026-09-17

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Spark's answers are recorded, not hand-computed: `_record_ice_evo_dml_1.py` builds 145 cases on PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 (ANSI on), records the SQL door, the DataFrame door on a twin table wherever the statement has one, and one Spark-created evolved table; the recordings agree on every shared case; the pin module asserts the recorded catalog digest equals the driver's. | `test_recorded_oracle_covers_the_driver_catalog`; recording logs. | PROVEN | `test_recorded_oracle_covers_the_driver_catalog` green offline; live cell `1 passed in 208.57s` — Spark 4.1.2 re-derives every recorded answer, no drift. |
| C-002 | After `ADD COLUMN extra` with no write since, the eight statements (MERGE `*`, MERGE explicit, MERGE old columns only, UPDATE new column, UPDATE old column, DELETE, INSERT, INSERT OVERWRITE) answer Spark row for row — v2/v3 × CoW/MoR, SQL door. | `test_sql_door_matches_spark[grid/add_column/*]`. | PROVEN | Round 2 (fork `f7ecd855`, replacement native): all 8 statements green — the fork UPDATE / DELETE execs project the current schema over the pinned snapshot by field id (F-EVO-SCAN-1), so the plain-`UPDATE` arm that stayed red in round 1 now answers. Full round-2 module `393 passed, 2 skipped, 16 xfailed`, zero failures. Round 3 re-greens every one of these cells in the `274 passed, 1 skipped, 16 xfailed` pin-module run. |
| C-003 | After `RENAME COLUMN w TO v` with no write since, the eight statements answer Spark — v2/v3 × CoW/MoR, SQL door. | `test_sql_door_matches_spark[grid/rename_column/*]`. | PROVEN | Round 2: all 8 green in the same `393 passed` run — renamed-column UPDATE reads by field id through the fork exec. Round 3 re-greens every one in the `274 passed` module run. |
| C-004 | After renaming the key every statement filters on (`RENAME COLUMN k TO id`) with no write since, the eight statements answer Spark — the MERGE key-range and DML residual pushdowns never bind the new name against the old schema. | `test_sql_door_matches_spark[grid/rename_key/*]`; `evolved_scan::target_scan_residual_on_a_renamed_key_keeps_the_matching_row`. | PROVEN | Round 2: all 8 green in the same `393 passed` run; the Rust residual pin stays green through the fork-API replacement. Round 3 re-greens every one in the `274 passed` module run. |
| C-005 | After a rename that swaps two names (`extra → tmp`, `v → extra`, `tmp → v`) with no write since, the eight statements answer Spark — no DML writes one field's values under the other's name, and no residual prunes by the other field's bounds. | `test_sql_door_matches_spark[grid/rename_swap/*]`; `evolved_scan::target_scan_after_swapping_two_names_keeps_each_value_under_its_field_id`, `evolved_scan::target_scan_residual_on_a_swapped_name_filters_the_current_field`. | PROVEN | Round 2: all 8 green in the same `393 passed` run — the swap-UPDATE arm now answers row-for-row instead of refusing (on base it committed swapped values). Both Rust swap pins green. Round 3 re-greens every one in the `274 passed` module run. |
| C-006 | The DataFrame door answers Spark's DataFrame door on every grid, emptied, rewrite and MERGE-source case that has one: facade `mergeInto` (`updateAll` / `insertAll`, `update` / `insert` column maps) and `writeTo(t).append()`. UPDATE and DELETE have only the SQL door (PySpark has no DataFrame UPDATE / DELETE). The 16 `writeTo(t).overwritePartitions()` cells are strict `xfail(raises=ParseException)` on EX-W2-4 (OPEN, unpartitioned table, own fix unit); Spark's DataFrame answer is recorded for them. | `test_dataframe_door_matches_spark[*]`. | PROVEN | Zero DataFrame FAILED (the 33 failures are all SQL-door cells); the 16 `overwritePartitions()` cells stay strict `xfail` on EX-W2-4 as specified. |
| C-007 | A table emptied by DELETE then evolved by ADD COLUMN, and an evolved table after a no-op `rewrite_data_files`, run MERGE `*` equal to Spark — v2/v3 × CoW/MoR (emptied), v2/v3 CoW (rewrite), both doors. | `test_*_door_matches_spark[emptied/*, rewrite/*]`. | PROVEN | No emptied / rewrite cell in the failure list — MERGE `*` green on the DELETE-emptied evolved table and after the no-op `rewrite_data_files`, both doors. |
| C-008 | MERGE `*` source shapes after ADD COLUMN answer Spark: a source with an extra column, an exact source and a reordered source run and match row for row; a source missing a target column refuses with `AnalysisException` on both engines. | `test_*_door_matches_spark[merge_star_source/*]`. | PROVEN | No merge_star_source cell fails — wide / exact / reordered sources run row-for-row; the narrow source refuses `AnalysisException` on both engines. |
| C-009 | RePark adopts the Spark-created table Spark evolved (`RENAME COLUMN w TO v`, `ADD COLUMN extra`, no write since) through `register_table` and runs MERGE `*` (SQL and `mergeInto`), UPDATE and DELETE equal to Spark's own run on the same bytes. | `test_adopted_spark_table_matches_spark[adopted/*]`. | PROVEN | Round 2: `adopted/update_new_col-sql` green in the same `393 passed` run — MERGE `*` both doors, UPDATE and DELETE all match Spark on the adopted bytes. Round 3 re-greens every one in the `274 passed` module run. |
| C-010 | `TargetScanStream` reads a pinned snapshot whose schema predates the current one under the current schema, on both execution branches (with and without a partition sink): an added column null-fills, a renamed column reads by field id, a swapped name reads its own field. | `evolved_scan` pins in `crates/repark-iceberg/src/write/merge/tests/`. | PROVEN | Round 1: 6 / 6 green through the local re-point, both branches (`planned=false/true`). Round 2: 6 / 6 green through the fork `project_current_schema()` API (`290cf2fb` deleted the helper) — null-fill, rename by id, swap by id unchanged. |
| C-011 | Scan filters bind the table's current schema fork-side (`project_current_schema()`): a renamed-key or swapped-name residual still returns the matching rows, an unchanged-column residual still prunes, and the unevolved path plans exactly as before. (Round-1 wording — keep the filter only when every name binds the same field id in both schemas, else drop it — retired with the local helper; the fork binds by field id against the current schema instead.) | `evolved_scan::target_scan_residual_*` (incl. `target_scan_residual_on_an_unchanged_column_still_prunes_after_add_column` for the kept arm); `partition_sink`, `streaming_scan` batteries. | PROVEN | Round 2: all four residual pins green through the fork API, plus the 2 lineage pins (8 / 8 `evolved*`). Mutation: deleting `.project_current_schema()` from `plan_file_scan_tasks` reds all 6 `evolved_scan` pins (`0 passed; 6 failed`); restore greens 8 / 8. The round-1 always-keep mutation retired with `binds_the_same_fields`. `partition_sink` / `streaming_scan` green inside the 445. |
| C-012 | Under `REPARK_PARITY_LIVE=1` live Spark re-derives every recorded answer on both doors (no golden drift) while the offline cells hold RePark equal to the recording. | `test_live_spark_rederives_every_recorded_answer`. | PROVEN | Round 3: live leg `1 passed in 431.62s` re-derives all 183 answers on both doors (no drift), and the offline leg holds RePark equal to the recording on every one (`274 passed, 1 skipped, 16 xfailed`). |
| C-013 | Unit 1's promotion pins stay green through the re-point: a single-era promoted table still reads widened types, and `conform_scan_batch` still refuses an illegal narrowing. | `promoted_scan` pins; `test_ice_promote_read_1.py`. | PROVEN | Round 1: `test_ice_promote_read_1.py` `169 passed, 1 skipped`; `promoted_scan` pins green inside the 445-lib run. Round 2: `173 passed, 1 skipped` (the rebase added 4 inspect-table cells), `promoted_scan` pins green inside the round-2 445-lib run. |
| C-014 | Registry rows land in `docs/spark-sql-iceberg-parity.md` — FIXED with pin names — for the DML-after-ADD/RENAME refusal and the rename-swap silent wrong write; maps move in lockstep. | Registry diff; `make check-map-sync`. | PROVEN | Rows ICE-EVO-DML-1, ICE-EVO-SWAP-1, ICE-EVO-LINEAGE-READ-1 committed (`aa62f42f`), each FIXED 2026-09-17 with pin names and the F-PROMOTE-READ-1 fork-pin note; tests map and merge-tests map in lockstep; commit hook printed `map-sync: 283 maps clean`. |
| C-016 | The v3 lineage read (`SELECT _row_id, _last_updated_sequence_number, …`, served by `LineageColumnsTableProvider`, which scans the current snapshot the same way) answers Spark after each of the four evolutions with no write since, projection and a filter on a renamed column. SQL door only: RePark's DataFrame door does not resolve `_row_id` on any table (not an evolution defect). | `test_lineage_read_matches_spark[lineage_read/*]`; `evolved_lineage_read` pins in `crates/repark-iceberg/src/catalog/tests/`. | PROVEN | All four `lineage_read/*` cells green (no lineage failure in the run); `evolved_lineage_read` 2 / 2 green. |
| C-015 | Gates: `cargo test -p repark-iceberg --lib` under the override; release native; the pin module offline and live; the `*alter*`, `*evo*`, `*merge*`, `*dml*`, `*v3_*`, `*ice_*` modules; `make verify`; comment and override greps on the branch diff. | Command → result table. | PROVEN | Orchestrator run on `a0b40af2` (release native, codegen-units 16): facade `9904 passed, 3 failed` — the 3 are pre-existing local-only reds (`test_spark_sql_grammar_1.py::test_q14_current_date_bare_and_paren` ×2, a session-UTC-date vs local-date check that fails between 20:00 and 24:00 EDT, and `test_csv_infer_perf_1` timing under box load); parity `755 passed`; `cargo test -p repark-iceberg --lib` green; comment ban `hits=0`. Round 3: pin module `274 passed, 1 skipped, 16 xfailed`; live cell `1 passed in 431.62s`; `make verify` rc 0; comment ban `hits=0`. |
| C-017 | The critic's unmeasured evolutions answer Spark: DROP COLUMN then ADD COLUMN of the same name reads NULL on old rows, never the dropped values; RENAME onto a dropped name reads the renamed field; v3 `ADD COLUMN … DEFAULT` is refused `UnsupportedOperationException` on both engines; the new column works in an UPDATE predicate, a MERGE key and a `NOT MATCHED BY SOURCE` arm — v2/v3 × CoW/MoR, both doors where the shape has one. | `test_sql_door_matches_spark[drop_add/*, rename_onto_dropped/*, add_default/*, new_col_predicate/*]`; `test_dataframe_door_matches_spark[drop_add/*/merge_star, rename_onto_dropped/*/merge_star, new_col_predicate/*/merge_on_extra, new_col_predicate/*/merge_nmbs]`. | PROVEN | Round 3: 38 new cells recorded on Spark 4.1.2 with the old 145 byte-identical; all green in the `274 passed, 1 skipped, 16 xfailed` module run with zero new xfails, and the live cell re-derives all 183 answers. |

## Fix (step 5, `beaebef7` after the rebase onto `main` `79e328f2`)

- `crates/repark-iceberg/src/catalog/current_schema_scan.rs` (new) —
  `plan_under_current_schema(table, snapshot_id, select_columns, filter, concurrency_limit)`:
  when the snapshot's schema id equals the current schema id it plans exactly as before;
  otherwise it translates each select name to its current field id, plans the snapshot selecting
  the snapshot-schema name of every field that existed then (metadata columns pass through; a
  column added since is left out of planning), keeps the residual filter only when every name it
  references binds the same field id in both schemas, and re-points each planned `FileScanTask`
  at the current schema with the full current projection (`task.schema`,
  `task.project_field_ids` — the RDF-SCHEMA-EVO-1 recipe). The fork's Arrow reader then null-fills
  added columns, reads renamed columns by id and widens legal promotions.
  `plan_current_snapshot_under_current_schema` is the unpinned twin (empty when there is no
  snapshot).
- `write/merge/target_scan.rs` — `plan_file_scan_tasks` calls the helper; `execute` always plans
  (cached per stream) and reads the tasks with `ArrowReaderBuilder` — the `to_arrow()` branch,
  which bound names the same way, is gone. Production DML always took the planned branch (every
  caller passes a partition sink or an allowlist).
- `catalog/lineage_columns.rs` — `scan_lineage_batches` plans through the unpinned twin and reads
  the tasks with `ArrowReaderBuilder`.

Why RePark and not the fork: with a pin, the fork binds names against the pinned snapshot's
schema exactly as Java's `useSnapshot` does; Spark's own DML scan re-projects with
`.project(expectedSchema)`, which is engine code. The evolution itself (null-fill, rename by id,
promotion) stays in the fork's reader. The unpinned divergence (fork `table.scan()` binds the
current snapshot's schema, Java `newScan()` binds `table.schema()`) is named in the hand-back as a
fork follow-up; the lineage read no longer depends on it.

Measured: with the re-point in place, reverting `conform_scan_batch`'s widening to
`column.clone()` leaves `promoted_scan::target_scan_over_a_single_era_promoted_table_yields_the_current_types`
green (only the direct unit pin `conform_scan_batch_widens_legally_promoted_columns` goes red) —
the re-point subsumes the widening on the target-scan path; the conform contract and its
narrowing refusal stay.

## Fix — round 2 (step 3, `290cf2fb`, fork `f7ecd855`)

Ruling Q-20a-1 option (b) moved the seam fork-side (F-EVO-SCAN-1 #289: unpinned scans and
`use_ref("main")` bind the current schema; the DataFusion UPDATE / DELETE execs project the
current schema over the pinned snapshot by field id), so the round-1 helper became dead weight.
`write/merge/target_scan.rs` (`plan_file_scan_tasks`) now builds
`table.scan().snapshot_id(pin).select(<current names>).project_current_schema()` with the same
`with_filter` / `with_concurrency_limit` arms, and `catalog/lineage_columns.rs`
(`scan_lineage_batches`) builds the unpinned twin; `catalog/current_schema_scan.rs` is deleted
and the `catalog/map.md` + `write/merge/map.md` entries name the fork API. Measurement ordered
the decision: full `repark-iceberg` lib `445 passed; 0 failed` with the replacement,
`evolved_scan` 6 / 6, both Python pin modules `393 passed, 2 skipped, 16 xfailed` (identical
counts to the pre-replacement run on the same fork) — and deleting `.project_current_schema()`
from `plan_file_scan_tasks` reds all 6 `evolved_scan` pins, so the pins still guard the seam.
No fork-side defect surfaced; no RePark-side fix beyond the deletion was needed. The pin bump
to `f7ecd855` stays a separate PR (another run).

## Red evidence

### Python pins — release native `75433230` + fork `04a338c3` (unfixed), 2026-09-17 00:14–00:19

`TMPDIR=… .venv/bin/python -m pytest python/repark/tests/test_ice_evo_dml_1.py -q -p no:cacheprovider --tb=line -rA`

```
160 failed, 56 passed, 1 skipped, 16 xfailed in 312.45s (0:05:12)
```

Outcome by test and group (`sort | uniq -c`):

```
     24 FAILED test_sql_door_matches_spark grid add_column
     24 FAILED test_sql_door_matches_spark grid rename_column
     24 FAILED test_sql_door_matches_spark grid rename_key
     20 FAILED test_sql_door_matches_spark grid rename_swap
      4 FAILED test_sql_door_matches_spark emptied
      2 FAILED test_sql_door_matches_spark rewrite
      3 FAILED test_sql_door_matches_spark merge_star_source (wide, exact, reordered)
     12 FAILED test_dataframe_door_matches_spark grid add_column
     12 FAILED test_dataframe_door_matches_spark grid rename_column
     12 FAILED test_dataframe_door_matches_spark grid rename_key
     10 FAILED test_dataframe_door_matches_spark grid rename_swap
      4 FAILED test_dataframe_door_matches_spark emptied
      2 FAILED test_dataframe_door_matches_spark rewrite
      3 FAILED test_dataframe_door_matches_spark merge_star_source (wide, exact, reordered)
      4 FAILED test_adopted_spark_table_matches_spark adopted (merge_star sql + dataframe, update_new_col, delete)
     32 PASSED test_sql_door_matches_spark grid */insert_new_col, */insert_overwrite
     12 PASSED test_sql_door_matches_spark grid rename_swap mer/merge_star, mer/delete (+ inserts above)
     16 PASSED test_dataframe_door_matches_spark grid */insert_new_col
      2 PASSED test_dataframe_door_matches_spark grid rename_swap mer/merge_star
      1 PASSED test_sql_door_matches_spark merge_star_source narrow
      1 PASSED test_recorded_oracle_covers_the_driver_catalog
     16 XFAIL  test_dataframe_door_matches_spark grid */insert_overwrite (EX-W2-4)
```

Failure messages (counted):

```
     54 E   repark.errors.PySparkException: DataInvalid => Column extra not found in table. Schema: table {
     40 E   repark.errors.PySparkException: DataInvalid => Column v not found in table. Schema: table {
     36 E   repark.errors.PySparkException: DataInvalid => Column id not found in table. Schema: table {
     30 E   AssertionError: ('grid/rename_swap/…', <RePark rows>, <Spark rows>)
```

The 30 `rename_swap` assertion failures are the silent shape — the statement succeeds and commits
swapped values, e.g. `grid/rename_swap/v3/cop/update_new_col [sql]`: RePark
`[[1, 'e1', 'u'], [2, None, 'b']]`, Spark `[[1, 'a', 'u'], [2, 'b', None]]`. The swap cells that
pass on the base are the ones whose write never carries an old value: INSERT / INSERT OVERWRITE,
merge-on-read MERGE `*` (every matched column comes from the source) and merge-on-read DELETE
(position deletes only). The 16 `xfail` cells are `writeTo().overwritePartitions()` on an
unpartitioned table — EX-W2-4, a separate OPEN defect (`PARTITION ()` reaches the parser) with its
own fix unit; they are strict, so they flip red when that unit lands.

### Lineage-read pins (second red set) — unfixed native and source, 2026-09-17 00:22–00:26

`TMPDIR=… .venv/bin/python -m pytest python/repark/tests/test_ice_evo_dml_1.py -q -p no:cacheprovider --tb=line -rA -k "lineage_read or covers_the_driver"`

```
E   repark.errors.PySparkException: External error: DataInvalid => Column extra not found in table. Schema: table {
E   repark.errors.PySparkException: External error: DataInvalid => Column v not found in table. Schema: table {
E   repark.errors.PySparkException: External error: DataInvalid => Column id not found in table. Schema: table {
E   AssertionError: ('lineage_read/rename_swap row_ids', [[0, 1, 1, 'e1', 'a'], [1, 1, 2, None, 'b']], [[0, 1, 1, 'a', 'e1'], [1, 1, 2, 'b', None]])
PASSED …::test_recorded_oracle_covers_the_driver_catalog
FAILED …::test_lineage_read_matches_spark[lineage_read/add_column]
FAILED …::test_lineage_read_matches_spark[lineage_read/rename_column]
FAILED …::test_lineage_read_matches_spark[lineage_read/rename_key]
FAILED …::test_lineage_read_matches_spark[lineage_read/rename_swap]
4 failed, 1 passed, 232 deselected in 0.35s
```

`cargo --config <override> test -p repark-iceberg --lib -- catalog::tests::evolved_lineage_read catalog::tests::lineage_columns`

```
test catalog::tests::lineage_columns::try_new_with_snapshot_is_removed ... ok
test catalog::tests::evolved_lineage_read::row_id_read_after_add_column_null_fills_the_added_column ... FAILED
test catalog::tests::lineage_columns::stored_row_id_wins_over_first_row_id_plus_position ... ok
test catalog::tests::evolved_lineage_read::row_id_read_after_swapping_two_names_reads_each_field_by_id ... FAILED
test catalog::tests::lineage_columns::filter_on_id_keeps_matching_lineage_row ... ok
the lineage read runs under the current schema: External(DataInvalid => Column extra not found in table. Schema: table {
  left: [["0", "1", "e1", "a"], ["1", "2", "NULL", "b"]]
 right: [["0", "1", "a", "e1"], ["1", "2", "b", "NULL"]]
test result: FAILED. 3 passed; 2 failed; 0 ignored; 0 measured; 439 filtered out; finished in 0.05s
```

### repark-iceberg pins — `75433230` (unfixed), 2026-09-17 00:10

`CARGO_BUILD_JOBS=10 cargo --config <override> test -p repark-iceberg --lib -- write::merge::tests::evolved_scan write::merge::tests::promoted_scan`

```
test write::merge::tests::promoted_scan::conform_scan_batch_widens_legally_promoted_columns ... ok
test write::merge::tests::promoted_scan::conform_scan_batch_still_refuses_an_illegal_narrowing ... ok
test write::merge::tests::evolved_scan::target_scan_after_rename_reads_the_renamed_column_by_field_id ... FAILED
test write::merge::tests::evolved_scan::target_scan_residual_on_a_swapped_name_filters_the_current_field ... FAILED
test write::merge::tests::evolved_scan::target_scan_residual_on_a_renamed_key_keeps_the_matching_row ... FAILED
test write::merge::tests::evolved_scan::target_scan_after_add_column_null_fills_the_added_column ... FAILED
test write::merge::tests::promoted_scan::target_scan_over_a_single_era_promoted_table_yields_the_current_types ... ok
test write::merge::tests::evolved_scan::target_scan_after_swapping_two_names_keeps_each_value_under_its_field_id ... FAILED
the pinned target scan reads under the current schema: ArrowError(ExternalError(DataInvalid => Column extra not found in table. …))
the pinned target scan reads under the current schema: ArrowError(ExternalError(DataInvalid => Column v not found in table. …))
the pinned target scan reads under the current schema: ArrowError(ExternalError(DataInvalid => Column id not found in table. …))
  left: [(1, Some("e1"), Some("a")), (2, None, Some("b"))]
 right: [(1, Some("a"), Some("e1")), (2, Some("b"), None)]
planned=false []
test result: FAILED. 3 passed; 5 failed; 0 ignored; 0 measured; 434 filtered out; finished in 0.06s
```

Two of the five are refusals turned wrong answers the moment the name exists in both schemas: the
swap pin reads `extra`'s values under `v`, and the residual `v = 'a'` binds the old `v` field and
prunes the matching row (`[]`).

## Green evidence (resume round, run 20a, 2026-09-17)

Release native built with the run override (`maturin develop --release
--config /tmp/oc-worker/run20a/fork-override.toml`, 6m08s, `repark-1.4.2` installed;
`Cargo.lock` restored after every override build — tree clean at each commit).

`.venv/bin/python -m pytest python/repark/tests/test_ice_evo_dml_1.py python/repark/tests/test_ice_promote_read_1.py -q -p no:cacheprovider`

```
33 failed, 356 passed, 2 skipped, 16 xfailed in 155.28s (0:02:35)
```

Split: `test_ice_evo_dml_1.py` → `33 failed, 187 passed, 1 skipped, 16 xfailed`;
`test_ice_promote_read_1.py` → `169 passed, 1 skipped`. Every failure is a plain-`UPDATE`
cell (see Residual red); MERGE / DELETE / INSERT / lineage / DataFrame / emptied / rewrite /
merge-source / adopted-MERGE arms are all green.

## Green evidence — round 2 (fork `f7ecd855`, 2026-09-17)

Step-2 re-measurement on the rebased tree before the replacement (release native rebuilt with
the run override; `Cargo.lock` restored after every override build):

`.venv/bin/python -m pytest python/repark/tests/test_ice_evo_dml_1.py python/repark/tests/test_ice_promote_read_1.py -q -p no:cacheprovider`

```
393 passed, 2 skipped, 16 xfailed in 108.01s (0:01:48)
```

The 33 UPDATE cells pass on the merged fork fix — no RePark-side defect, no HALT. Split
(`--collect-only` 237 + 174): `test_ice_evo_dml_1.py` → `220 passed, 1 skipped, 16 xfailed`;
`test_ice_promote_read_1.py` → `173 passed, 1 skipped` (4 more than round 1: the rebase onto
`fix/ice-promote-read-1` brought its inspect-table cells).

Step-3 replacement run (native rebuilt with `290cf2fb` on top, same override):

```
393 passed, 2 skipped, 16 xfailed in 117.83s (0:01:57)
```

Counts identical to the pre-replacement run: the fork API keeps every pin green, so the local
helper stays deleted.

`CARGO_BUILD_JOBS=10 RUST_TEST_THREADS=8 cargo --config
/tmp/oc-worker/run20a/fork-override.toml test -p repark-iceberg --lib` with the replacement:

```
test result: ok. 445 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 19.84s
```

`evolved*` alone after restore: 9 / 9 (`evolved_scan` 6, `evolved_lineage_read` 2,
`evolved_unpartitioned_spec_position_delete` 1).

Round-2 mutation: `.project_current_schema()` deleted from `plan_file_scan_tasks` reds all 6
`evolved_scan` pins (`0 passed; 6 failed`); restore greens 8 / 8 with the lineage pins. The
round-1 always-keep mutation retired with `binds_the_same_fields`.

`CARGO_BUILD_JOBS=10 RUST_TEST_THREADS=8 cargo --config
/tmp/oc-worker/run20a/fork-override.toml test -p repark-iceberg --lib`

```
test result: ok. 445 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 36.80s
```

`evolved_scan` alone: 6 / 6 green, including the new kept-arm pin
`target_scan_residual_on_an_unchanged_column_still_prunes_after_add_column`.

Sweep (25 modules matching `*alter*`, `*evo*`, `*merge*`, `*dml*`, `*v3_*`, `*ice_*`,
unit module excluded — its counts are above):

```
416 passed, 97 skipped in 102.56s (0:01:42)
```

Zero failures; the skips are JVM-gated live cells (routine run is JVM-free).

`make verify` without the override (pinned fork, `Cargo.lock` restored first):
`MAKE_VERIFY_EXIT:0`, `3715 passed, 0 failed, 7 ignored` over the whole workspace log.

C-011 mutation: `_ => return false` → `_ => {}` in `binds_the_same_fields`
(`catalog/current_schema_scan.rs`) reds exactly the renamed-key and swapped-name residual
pins (`4 passed; 2 failed`); `git checkout --` restore greens 8 / 8 with the lineage pins.

Live: one full-module live run without a pyspark path failed the live cell on
`ModuleNotFoundError: No module named 'pyspark'` (environment, not the oracle — this
clone's `.venv` never synced the `record` extra). Re-run with
`PYTHONPATH=/tmp/sparkenv/lib/python3.12/site-packages` (pyspark 4.1.2, same 3.12.3):

```
test_live_spark_rederives_every_recorded_answer — 1 passed in 208.57s (0:03:28)
```

## Residual red — plain `UPDATE` after evolution stays red on a fork seam (2026-09-17)

The 33 failing cells are exactly `grid/*/update_new_col` + `grid/*/update_old_col`
(4 evolutions × v2/v3 × CoW/MoR) plus `adopted/update_new_col-sql`, e.g.

```
E   repark.errors.PySparkException: DataInvalid => Column extra not found in table. Schema: table {
E     1: id: optional long
E     2: v: optional string
```

Routing: the unit's `UPDATE {t} SET … WHERE id = 1` is an equality predicate, and the
identity-UPDATE hole admits uncorrelated positive `IN` only (`predicate_dml.rs:202`), so
RePark declines it and DataFusion plans the UPDATE onto the fork's `iceberg-datafusion`
UPDATE exec (`physical_plan/update.rs`), which reads the target through the fork
`TableProvider` scan selecting the full current projection. The fork binds those names
against the snapshot schema — fork-owned code, the same refusal as on the base tree
(identical message; on base the UPDATE arms were among the 24 / 24). DELETE passes because
a position delete only binds columns that predate the DDL. Widening RePark's identity hole
to equality predicates would re-route a DML shape on the sensitive write path, and patching
the fork exec breaks the fork-owns-semantics rule with no fork lane in this unit — both
declined here (Fixes stay narrow). Disposition is Q1 below.

## Gates (C-015, run 20a)

| Gate | Result |
|---|---|
| `cargo --config <override> test -p repark-iceberg --lib` | 445 passed; 0 failed |
| `evolved_scan` pins (incl. the new kept-arm pin) | 6 / 6 |
| release native (`maturin develop --release --config <override>`) | built 6m08s, installed |
| `pytest test_ice_evo_dml_1.py test_ice_promote_read_1.py` offline | 33 failed, 356 passed, 2 skipped, 16 xfailed |
| `pytest test_ice_evo_dml_1.py` with `REPARK_PARITY_LIVE=1` (one JVM) | 33 UPDATE red + live cell red on missing pyspark path (34 failed, 187 passed, 16 xfailed in 34.27s); live cell alone with the sparkenv path `1 passed in 208.57s` |
| sweep of the 25 related modules offline | 416 passed, 97 skipped, 0 failed |
| `make verify` (pinned fork, no override) | exit 0; 3715 passed, 0 failed, 7 ignored |
| C-011 mutation (always-keep residual) | red `4 passed; 2 failed`, restore green 8 / 8 |
| comment grep on `git diff origin/main..HEAD` | pending at hand-back (step 9) |
| override grep on `git diff origin/main..HEAD` | pending at hand-back (step 9) |

Round 3 files the attestation: every clause is PROVEN, the L-01 race stays a residue
outside the clause table, and the critic's P3 items are dispositions, not clauses.

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ice-evo-dml-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every DML arm after every evolution measured on Spark 4.1.2 and pinned on RePark — ADD, RENAME, key-rename, swap, DROP-then-ADD, RENAME-onto-dropped × v2/v3 × CoW/MoR, the added column in an UPDATE predicate, a MERGE key and NMBS, plus the v3 default-DDL refusal on both engines. Residuals bind the current schema by field id; NMBS disables the residual and still answers.
      artifacts: [python/repark/tests/test_ice_evo_dml_1.py, python/repark-parity/fixtures/torture/data/ice_evo_dml_1/truth.json]
    - id: AT-2
      status: ATTACKED
      evidence: SQL and DataFrame doors pinned wherever the shape has one (facade mergeInto with updateAll/insertAll, column maps, append, by-source update; UPDATE/DELETE are SQL-only on both engines); lineage reads are SQL-only (the DataFrame door resolves no _row_id on any table). Four recordings agree on every shared case; the live cell re-derives all 183 answers on both doors.
      artifacts: [python/repark/tests/test_ice_evo_dml_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Round 3 adds no Rust and no new failure path; the round-2 engine change rides the fork project_current_schema API with clippy pedantic and the panic/async bans green under make verify.
      artifacts: [crates/repark-iceberg/src/write/merge/target_scan.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Single-writer path only; the plan-vs-commit schema race (L-01) is recorded as an OPEN residue under ruling Q-21a-3, never absorbed into a passing clause.
      artifacts: [task/ledgers/staging/ice-evo-dml-1-ledger.md]
    - id: AT-5
      status: N/A
      justification: Local scratch catalogs only; no credentials, no network, no privileged action. Spark ran on loopback with a warm Ivy cache.
    - id: AT-6
      status: ATTACKED
      evidence: Scan-by-id then name-based conform/SET/write-back composition attacked by the swap cells (untouched rows keep field-id values end to end, CoW and MoR); the critic's composition null report holds.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/evolved_scan.rs]
    - id: AT-7
      status: ATTACKED
      evidence: Facade 9904 passed with 3 pre-existing local-only reds, parity 755 passed, repark-iceberg lib green, pin module 274 passed with the live re-derivation green; make verify rc 0; comment ban hits 0.
      artifacts: [python/repark/tests/test_ice_evo_dml_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: Spark contracts read from the oracle, never assumed: old-row NULL fill after DROP-then-ADD, w-values after RENAME-onto-dropped, the 4.1.2 default-DDL refusal recorded verbatim, NMBS and merge-on-new-column row shapes recorded on both doors.
      artifacts: [python/repark-parity/fixtures/torture/data/ice_evo_dml_1/truth.json]
    - id: AT-9
      status: ATTACKED
      evidence: Every refusal pins its class: narrow MERGE source AnalysisException on both engines, default DDL UnsupportedOperationException on both engines, overwritePartitions strict-xfail on EX-W2-4.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Mutation guards run — deleting .project_current_schema() from plan_file_scan_tasks reds all 6 evolved_scan pins; every pin compares row equality against truth.json, never is_ok.
      artifacts: [crates/repark-iceberg/src/write/merge/tests/evolved_scan.rs]
  complete: true
```

## Round 3 — the critic's P2 cells (run 21a, 2026-09-18)

Recorder: `l02_evolution_cases` (DROP-then-ADD and RENAME-onto-dropped × MERGE `*` / UPDATE /
DELETE), `add_default_cases` (v3 `ADD COLUMN … DEFAULT`, both modes), `new_col_predicate_cases`
(UPDATE … WHERE the new column IS NULL, MERGE ON the new column, MERGE … WHEN NOT MATCHED BY
SOURCE THEN UPDATE) — v2/v3 × CoW/MoR, DataFrame door on every MERGE shape. 38 new answers
recorded on Spark 4.1.2 in one short-lived JVM; the old 145 answers are byte-identical
(parsed-JSON equality over every old key); `catalog_sha256` matches the extended driver.

Spark 4.1.2 refuses `ALTER TABLE t ADD COLUMN extra STRING DEFAULT 'd'` on v2 and v3 alike
(`UnsupportedOperationException: Cannot add column extra since setting default values in Spark
is currently unsupported`); RePark refuses the same DDL with `UnsupportedOperationException`.
The initial-default DML read is untestable on this Spark, so the cell pins the refusal on both
engines instead.

### L-01 residue (OPEN, ruling Q-21a-3)

A schema-only ALTER committed between a DML's plan and its commit is not guarded by a
current-schema assertion: the fork's snapshot producer requires UUID + ref-snapshot match only,
DDL writes no snapshot so the ref still matches, and `do_commit` rebases the already-written
files onto the refreshed table. The orchestrator rules this Iceberg-wide (Java's snapshot
producer carries the same requirements), not a Spark divergence of this unit: no code change
here; the race stays an OPEN residue with the critic's reasoning.

### P3 dispositions

- L-03: the swap / renamed-key residual pins are fail-open (extra I/O, never wrong rows —
  the MERGE join still filters); only the kept-arm pin is an exact prune. Recorded; no change.
- L-04: `planned=false/true` no longer covers two scan APIs (round 2 always plans; `planned`
  only attaches the partition sink). Wording stale; recorded; no change.
- L-05: the lineage `.project_current_schema()` is redundant on the unpinned scan (the fork
  binds the current schema there since F-EVO-SCAN-1) but load-bearing on the pinned DML scan.
  Kept as the explicit guard; recorded; no change.

## Open questions

- Q1 (RULING): the 33 plain-UPDATE cells stay red on the fork UPDATE-exec seam above.
  (a) Narrow C-002..C-005, C-009 and C-012 to the DML the RePark fix owns (MERGE, DELETE,
  INSERT, lineage read), record the UPDATE arm as a dated registry residue, and file the fork
  UPDATE-exec re-projection as its own fork follow-up unit; or (b) keep the clauses OPEN and
  extend this unit with a fork lane. Lean: (a) — every path this unit owns is green and
  mutation-proven; the alternatives widen the write-path hole or patch fork-owned exec code
  with no fork lane chartered.
