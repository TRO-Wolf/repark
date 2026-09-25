# Charter ledger — U7-WRITE-DF-2 · WO U7 PR2: DataFrame writer semantics

**Date:** 2026-09-24 (round 2: 2026-09-25; slice 2 rebased 2026-09-25) · **Branch:** `feat/u7-write-df-2b` (slice 1 as `feat/u7-write-df-2a`, repark#835) · **Base:** `9e3bf2dd` (`origin/main`, U7 PR1 merged); slice 2 rebased onto `e97682ed` (U8 PR1 repark#833 and slice 1 merged) · **Model:** Claude Opus (`claude-opus-5-5`), per each commit's `Authored-By` trailer · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.** **Registry:** `docs/spark-sql-iceberg-parity.md` — rows `ICE-V3-WRITE-DEFAULT-1-SAVEAS-OVERWRITE`, `EX-W2-3` and `EX-W2-1` FIXED (EX-W2-1 lists residues R-2..R-8), row `EX-W2-5` added (BACKLOG, residue R-1), the ICE-OVERWRITE-MODE-1 "Before/After" note on the two `saveAsTable(overwrite)` shapes and the ICE-WRITE-OPTIONS-1 P2-14 sentence dated. Round 2 (critic r1 V-001..V-005): the SAVEAS-OVERWRITE row states the by-name field ids and residues R-9/R-10; EX-W2-3 names the doors slice 1 pins. Slice-2 round 2 (2026-09-25, critic r4 V-001..V-007, fixer `claude-fable-5-1`): EX-W2-1 states the by-name binding in Rust, the `EXTRA_COLUMNS` refusal, the pin file and residues R-11..R-14; ICE-WRITE-OPTIONS-1 no longer lists `none` as an accepted isolation level.
**Stack:** slice 2 was built on U8 PR1 (repark#833) before it merged. After #833 (`e97682ed`) and slice 1 (repark#835, `f3242566`) landed, the slice-2 commit was rebased onto `origin/main` with `git rebase --onto origin/main f405e5b2` (the old slice-1 commit is the cut, so only the slice-2 commit replays); U8's kernel is main's, `FilterValidation` replaces its `isolation_override` parameter as designed, and the slice-2 shapes were re-recorded with the field-id observations round 2 added (M-5).

**Retires:** in flight.

**Scope:** four scoreboard cells (`/tmp/oc-worker/scoreboard/2026-09-24`, Spark answers in
`out/spark-core.json`, bodies in `cells_write.py`): `W-DF-SAVEASTABLE-OVERWRITE`,
`W-DF-V2-OPTION-BRANCH`, `W-DF-V2-OVERWRITE-COND-PART`, `W-DF-V2-OVERWRITE-COND-ROWS`.
Slice 1 (this ledger's C-001..C-005) covers the first two:
- `crates/repark-iceberg/src/write/writer_plan.rs`: every `saveAsTable` overwrite plans
  `rtas` (Spark's `ReplaceTableAsSelect(orCreate = true)`).
- `python/repark/src/repark/spark/dataframe/writer_readwriter.py`: `DataFrameWriterV2.option`
  no longer refuses a `branch` or `tag` key.
- Round 2: `crates/repark-iceberg/src/write/replace_schema.rs` (`replacement_schema`, Java's
  by-name `assignFreshIds` through the fork's `assign_fresh_ids_with_base`), called by
  `crates/repark-spark/src/ctas.rs` before the replace's partition spec is built.
- Round 3 (critic r2 V-001): the other two `begin_replace` callers, the column-def
  `CREATE OR REPLACE` / `REPLACE TABLE` doors `crates/repark-spark/src/create_table.rs` and
  `crates/repark-sql/src/create_table.rs` (the native door, CTAS and column-def), call
  `replacement_schema` before their partition spec is built.

Slice 2 (C-006..C-010, pinned in `test_ice_write_df_2_overwrite.py` on the slice-1 file's helpers
since the rebase, when the merged pin file crossed the 1000-line ceiling) covers the other two, on
U8's REPLACE WHERE door:
- `python/repark/src/repark/spark/dataframe/writer_schema.py`: `replace_where_statement`, the
  `INSERT INTO t REPLACE WHERE <condition> SELECT …` for `DataFrameWriterV2.overwrite`.
  Slice-2 round 2 (critic V-001/V-002): the statement names the frame's columns in frame order
  and the facade decides nothing else; `writer_layout.run_overwrite_condition` raises a typed
  `source_by_name` flag that rides the native call (`session_sql_with_write_options` →
  `ReparkSession::sql_with_write_options` → `EngineContext::source_by_name` →
  `StatementWriteOptions::source_by_name`), and U8's door binds the source to the table by
  name through `insert_by_name::by_name_source_query` (the BY NAME door's resolution, shared).
- `crates/repark-spark/src/write_options.rs` and `crates/repark-iceberg/src/write/write_options.rs`
  (round 2, critic V-003): the `isolation-level` validators accept `snapshot` and
  `serializable` only; `none` refuses `Invalid isolation level: none`.
- `crates/repark-iceberg/src/write/commit_target.rs`: `FilterValidation`, the level and the
  start of the conflict validation (`validate-from-snapshot-id`), read by U8's
  `overwrite_filter.rs` commit; `crates/repark-spark/src/write_options.rs` keeps the option
  raw and U8's `replace_where.rs` passes it.

The fork, every `Cargo.toml`, `Cargo.lock` and `STATUS.md` are untouched, and no code comment
is added.

## Measurements (decide-then-build evidence)

**M-1 — the record.** On Spark 4.1.2 + Iceberg 1.11.0 (`dfw` seeds three rows with one
`INSERT … VALUES`, then writes `[(7,'g','x'),(8,'h','w')]`):
- `W-DF-SAVEASTABLE-OVERWRITE`: rows 7,8; snapshots `append` (3 records) then `overwrite`
  with `added-records=2`, `total-records=2` and no `deleted-*` counter; `main → S1`.
- `W-DF-V2-OPTION-BRANCH`: main holds rows 1,2,3,7,8, `branch_b1` holds 1,2,3; refs
  `b1 → S0`, `main → S1`. The write lands on main; the brief's caution holds.

RePark before this unit: the first cell answered the rows with an in-place `INSERT OVERWRITE`
(`deleted-records=3`, `deleted-data-files=1`); the second refused
`UnsupportedOperationException` ("writing to an Iceberg branch is not supported").

**M-2 — step-1 probes on Spark 4.1.2** (`target/probe-u7-pr2/cells_pr2.py` through a copy of
the scoreboard harness, and `target/probe-u7-pr2/record.py`, under `jvm-lock.sh`, JDK 17,
InMemoryCatalog `sc`). The recorder's shapes are committed under `measured` in
`python/repark/tests/ice_write_df_1_spark_oracle.json`. Findings:

`saveAsTable(mode="overwrite")`:
- It is an RTAS on every table: the uuid stays; properties stay (`k1`, and
  `write.distribution-mode=range` left by `WRITE ORDERED BY`); other refs stay (`b1 → S0`).
- The frame's schema replaces the table's; the `partitionBy` spec replaces the table's (none
  without `partitionBy`); the sort order becomes empty; the format version stays (3 stays 3).
- The new snapshot is an `overwrite` with no parent: `.history` shows the old snapshots as
  not current ancestors. An empty frame writes a `delete` snapshot with `total-records=0`.
- A missing table is created by the same statement (`overwrite`, not CTAS's `append`).
- The session `partitionOverwriteMode=dynamic` and the `overwrite-mode=dynamic` option change
  nothing (the option measured and pinned in round 2), and a `snapshot-property.k` option
  lands in the summary.
- Without `format(...)` the table gets `write.format.default=parquet` on the CTAS and on the
  RTAS, even over an existing `orc` (residue R-1).

The `branch` option:
- `branch` and `tag` keys are ignored on `writeTo` append, `overwritePartitions`,
  `overwrite(condition)` and `create`, on `options(BRANCH=…)`, on a branch that does not
  exist, and on `DataFrameWriter` append, overwrite and `insertInto`. The write lands on main;
  the named ref keeps its snapshot. (Spark's answers; in slice 1 RePark's
  `overwrite(condition)` still refuses, so C-005 covers the other doors.)

RePark after slice 1 (`target/probe-u7-pr2/oracle-repark-s1.json`): 21 of the 23 slice-1
shapes equal Spark's on every observation; the two others are residue R-1.

**M-4 — round 3 (critic r2), Spark 4.1.2 + Iceberg 1.11.0** (`target/probe-u7-pr2a-r2fix/
record_doors.py` under `jvm-lock.sh`): the five replace doors, each reading branch b1 after
the replace — `CREATE OR REPLACE TABLE t (cat, data, id)` keeps ids 3, 2, 1 and the branch
reads `['x', 'a', 1]`; `REPLACE TABLE t (cat, payload, id)` gives 3, 4, 1, `last-column-id` 4,
rows `['x', null, 1]`; SQL RTAS, `createOrReplace()` and `replace()` likewise. Before round 3
the two column-def doors numbered by position (`[null, 'a', null]`). After: 5 of 5 equal
Spark on every observation (the critic's r2 probe measured the same Spark answers).

**M-3 — round 2 (critic r1), Spark 4.1.2 + Iceberg 1.11.0** (`target/probe-u7-pr2a-r1fix/
record_ids.py` under `jvm-lock.sh`, results `spark.json` / `repark.json`): the replace keeps
field ids by name. A reordered frame `(cat, data, id)` keeps ids 3, 2, 1, and `branch_b1`
reads the seed rows (RePark before: ids 1, 2, 3 by position; `branch_b1` read `[null, 'a',
null]`). A renamed or added column takes a fresh id above `last-column-id` (4). A dropped
column's id is not reused by a later re-add (5). Nested struct fields keep their ids by dotted
name. A type-changed kept name keeps its id. The identifier column keeps field id 1; the replace
clears `identifier-field-ids`, as Spark does (`ids_identifier`). A reordered
`partitionBy('cat')` keeps source id 3. RePark after the fix: 34 of the 37 recorded shapes
equal Spark's, field ids included (the raw JSON of `ids_nested_reorder_branch` differs only in
how the recorder renders a struct value, a Spark `Row` list against a RePark dict; the pin
normalizes both). The three others: R-1 twice (no-format property), and R-9
(Spark's type-changed branch read fails with `ClassCastException`, RePark reads NULL). The
metadata JSON's `partition-specs` order is hash order on RePark and ascending on Spark (R-10,
not compared).

**M-5 — overwrite(condition) on Spark 4.1.2** (`record.py` `oc_*`/`vf_*`, committed as
`measured`): the two cells are Iceberg's `overwriteByRowFilter` with no added-file validation:
- `W-DF-V2-OVERWRITE-COND-PART`: rows 2,7,8; one `overwrite` (`deleted-data-files=1`,
  `deleted-records=2`, two added files, the `w` row outside the filter kept).
- `W-DF-V2-OVERWRITE-COND-ROWS`: the named seed writes one file per row, so the `id = 1` file
  is replaced; with one seed file (`values`) the same call refuses `ValidationException:
  Cannot delete file where some, but not all, rows match filter ref(name="id") == 1: <file>`.
  The cell title's "Spark refuses non-aligned" is right for that layout only.
- `false` commits `append`, an empty frame `delete` (also on a table with no snapshot), a
  frame is written by name, a missing frame column is NULL, an extra one refuses
  `TOO_MANY_DATA_COLUMNS` unless `mergeSchema` on an accept-any table evolves the schema, a
  `str` names a column, a non-Column refuses `NOT_COLUMN_OR_STR`, `UPPER(cat) = 'X'` refuses
  `Cannot convert Spark predicate …`, `CAT` refuses `Cannot find field 'CAT' …`.
- Validation happens only with an `isolation-level` option: from `validate-from-snapshot-id`,
  else from the start of history; `serializable` checks data and deletes, `snapshot` deletes;
  a non-long id is `NumberFormatException`, an unknown one `Cannot determine history between
  starting snapshot <id> and the last known ancestor <oldest>`.

RePark after slice 2 at hand-back (`target/probe-u7-pr2/oracle-repark-final.json`): 48 of the
66 shapes recorded then equal Spark's on every observation; the other 18 are residues R-1..R-8.
Rebase (2026-09-25, same Spark and Iceberg): the 43 slice-2 shapes (`oc_*`, `vf_*`,
`v2_option_branch_overwrite_condition`) re-recorded through `record.py`'s shapes under a recorder
that adds M-3's field-id observations; every earlier value unchanged (the random snapshot id of
`vf_unknown_snapshot_id` aside, which its pin normalizes; the pins sort rows only inside runs of
equal ids, so the `ORDER BY id` order itself is compared — round 2, critic V-007).

**M-6 — slice-2 round 2 (critic r4), Spark 4.1.2 + Iceberg 1.11.0** (the critic's
`scratchpad/xr/xrec.py` shapes re-recorded by `xr/xrec2.py` — `record.py`'s session and state
plus M-3's field-id observations — under `jvm-lock.sh`, JDK 17; committed under `measured`):
- A frame with one extra column and one missing (`df.drop('data').withColumn('extra', …)`,
  bigint or string extra, reordered or not) refuses `AnalysisException`
  `[INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS] Cannot write incompatible data for the table
  `sc`.`u7`.`t`: Cannot write extra columns `extra`. SQLSTATE: KD000`, table unchanged.
  RePark before round 2 (critic's `repark-x.json`, the lane wheel): the facade projected the
  frame by position at matching width and the string shape COMMITTED `[7, 'x', 'e'], [8, 'w',
  'e']` (cat in data, extra in cat), the bigint shape likewise, the reordered one refused a
  leaked store-assign text. After: all three refuse Spark's condition, SQLSTATE and text (the
  R-4 `Error during planning: ` prefix aside), table unchanged.
- `isolation-level=none`: Spark `IllegalArgumentException: Invalid isolation level: none`
  (`vf_bad_id_level_none`, table unchanged). RePark before: accepted, skipped the validation
  (the shape then refused the bad id, `NumberFormatException`). After: Spark's text (R-4 class).
- EQUAL on every observation, before and after: `id.isin(1, 2)`, `data == 'a'`, `cat.isNull()`
  and `NOT (cat = 'x')` / `NOT cat IN ('x')` over a NULL partition row (the NULL file kept),
  an upper-cased frame (`ID`, `DATA`), a condition on a column the frame dropped (NULL written),
  `validate-from-snapshot-id` `abc` (`NumberFormatException` even without a level) and `12345`
  without a level (ignored), `12345` + `serializable` on an unsnapshotted table (commits), and
  `writeTo('t.branch_b1').overwrite` (commits to the branch, main untouched).
- Residues, dated 2026-09-25: R-11 fractional literals (`between(0.5, 1.5)`, `id <= 1.5`:
  Spark refuses `Cannot convert Spark predicate to Iceberg expression: CAST(id AS double) >=
  0.5` / `<= 1.5`; RePark commits the `id = 1` replacement); R-12 `cat.startswith('x')` (Spark
  commits, `STARTS_WITH` is an Iceberg predicate; RePark refuses `Cannot convert Spark
  predicate to Iceberg expression: starts_with(`cat`, 'x')`); R-13 `lit(None)` (Spark
  `AnalysisException` `_LEGACY_ERROR_TEMP_1109` `Exec update failed: cannot translate
  expression to source filter: null.`; RePark `IllegalArgumentException` `Cannot convert Spark
  predicate to Iceberg expression: null`); R-14 a struct-field condition `s.a == 1` (Spark
  converts it and refuses the partial file, `ValidationException: Cannot delete file where some,
  but not all, rows match filter ref(name="s.a") == 1: <file>`; RePark refuses at conversion,
  `` `s`.`a` = 1 ``); a validated overwrite on a branch target refuses in R-2's class.

## Clauses

| ID | Proposition | Evidence required | Status | Evidence |
|---|---|---|---|---|
| C-001 | `format("iceberg").mode("overwrite").saveAsTable(t)` on an existing table replaces it exactly as cell `W-DF-SAVEASTABLE-OVERWRITE` records (rows, `overwrite` summary without `deleted-*`, refs). | `test_save_as_table_overwrite_replaces_like_the_recorded_cell`; replay. | **PROVEN** | Replay EQUAL on every observation. |
| C-002 | Every `saveAsTable` overwrite is Spark's RTAS: uuid, properties and other refs kept; schema, `partitionBy` spec and sort order replaced; no parent on the replace snapshot; a missing table created with an `overwrite` snapshot; the session `partitionOverwriteMode` and the `overwrite-mode=dynamic` option ignored; an empty frame writes `delete`. | `test_save_as_table_overwrite_shapes_match_spark` (16 shapes, `sat_overwrite_dynamic_option` among them, each also comparing field ids, schema ids and spec ids); Rust `save_as_table_statements_follow_spark_save_modes`; `test_ice_overwrite_mode_1.py::test_save_as_table_history_matches_spark` passing plainly; `test_ice_hadoop_vn_1.py::test_stale_overwrite_doors_raise` (the stale replace, full message). | **PROVEN** | M1 red. The ICE-OVERWRITE-MODE-1 strict xfail on the RTAS history retired. |
| C-003 | Residue R-1: a format-less `saveAsTable` (CTAS or RTAS) stores no `write.format.default`, where Spark stores `parquet`; every other observation equals Spark's. | `test_save_as_table_without_a_format_writes_no_format_property_divergence`; registry `EX-W2-5`. | **PROVEN** | Divergence pin, BACKLOG row. |
| C-004 | `writeTo(t).option("branch", "b1").append()` writes main and leaves `b1` at S0, as cell `W-DF-V2-OPTION-BRANCH` records. | `test_writer_v2_branch_option_writes_main_like_the_recorded_cell`, `test_examples_window_catalog.py::test_writerv2_option_branch_writes_the_default_branch`; replay. | **PROVEN** | M2 red. |
| C-005 | A `branch` or `tag` key, in any case, is ignored on `writeTo` `append`, `overwritePartitions`, `overwrite(condition)` (slice 2) and `create`, and on `DataFrameWriter` `saveAsTable` append, `saveAsTable` overwrite and `insertInto` overwrite, even for a missing ref: the write lands on main and the ref keeps its snapshot. | `test_branch_and_tag_options_are_ignored_like_spark` (10 shapes, one per door plus tag, upper case and missing ref), `test_time_travel.py::test_write_to_branch_option_writes_main`, `::test_write_to_tag_option_writes_main`. | **PROVEN** | M2 red on six of them. Narrowed 2026-09-25 (critic V-004): `overwrite(condition)` is not a slice-1 door. |
| C-006 | `writeTo(t).overwrite(condition)` answers cells `W-DF-V2-OVERWRITE-COND-PART` and `-ROWS` (rows, summaries, refs) through U8's REPLACE WHERE door, and carries its writer options. | `test_overwrite_condition_replaces_like_the_recorded_cells`; `test_examples_window_catalog.py::test_writerv2_overwrite_condition_replaces_the_matching_files`; `test_writer_v2.py::test_write_to_overwrite_condition_replaces_the_matching_file`; `test_ice_write_options_1.py::test_overwrite_condition_carries_options`; replay. | **PROVEN** | M8 red. |
| C-007 | The overwrite-by-filter shapes equal Spark on every observation: partitioned, unpartitioned and bucket-aligned filters, `OR`/`AND`/`IN`/`IS NULL`/`BETWEEN`/`!=`, `expr`, `true`/`false`, empty frames, by-name (reordered, narrower) frames, a non-Column condition, a snapshot property, a repeat. | `test_overwrite_condition_shapes_match_spark` (29 shapes since round 2). | **PROVEN** | M9 and M12 red. |
| C-008 | Refusals on this path answer Spark's condition and SQLSTATE and leave the table as Spark leaves it; their class or text differs by the stated residue rule (R-2, R-4, R-5). | `test_overwrite_condition_residues` (7 shapes); registry EX-W2-1 Residual. | **PROVEN** | Rules `_data_invalid`, `_planning`, `_suggestions_in_table_order`, `_analysis_class`, `_facade_spelling`. |
| C-009 | `validate-from-snapshot-id` beside an explicit `isolation-level` starts the conflict validation there: a later matching delete (both levels) or append (`serializable`) refuses, a later snapshot or a change to other rows passes, no level ignores it, a non-long refuses `NumberFormatException`, a non-ancestor refuses Java's `Cannot determine history …` text. | `test_validate_from_snapshot_shapes_match_spark` (8 since round 2), `test_validate_from_snapshot_residues` (6 since round 2), `test_an_unknown_validation_start_refuses_with_java_text`; Rust `tests/filter_validation.rs` (6), `write_options::tests::validate_from_snapshot_id_stays_raw_until_an_overwrite_commit_reads_it`; scoreboard `W-CONCURRENT-VALIDATE-ERR`. | **PROVEN** | M10, M11 and M13 red. |
| C-010 | Where one engine answers and the other refuses, RePark's answer is pinned beside Spark's: R-3 (`mergeSchema` refuses), R-6 (no start: loaded snapshot), R-7 (a frame-bound column is served), R-8 (the writer's not-found text). | `test_overwrite_condition_divergences_where_one_engine_answers`. | **PROVEN** | Residues listed below and in registry EX-W2-1. |
| C-011 | A replace keeps each column's field id by name (Java `TypeUtil.assignFreshIds(schema, base, nextId)`): reordered, swapped and renamed columns, added and re-added ones (fresh ids above `last-column-id`, a dropped id never reused), nested struct fields by dotted name, the identifier column (the replace clears `identifier-field-ids`, as Spark does) and a reordered `partitionBy` source; a branch on a pre-replace snapshot reads its rows. Round 3: on every replace door — `saveAsTable` overwrite, column-def `CREATE OR REPLACE`, `REPLACE TABLE`, SQL RTAS, `createOrReplace()`, `replace()` and the native door's CTAS and column-def replace. | `test_save_as_table_overwrite_keeps_field_ids_by_name_like_spark` (9 shapes), `test_every_replace_door_keeps_field_ids_by_name_like_spark` (5 doors); Rust `repark-iceberg tests/replace_schema.rs` (4), `repark-spark tests/replace_table.rs::column_def_replace_keeps_field_ids_by_name`, `::replace_table_takes_a_fresh_id_above_the_last_column_id`, `repark-sql create_table/rtas_ops_tests.rs::native_column_def_replace_keeps_field_ids_by_name`, `::native_rtas_keeps_field_ids_by_name`, `::native_partitioned_rtas_keys_the_spec_by_name_and_keeps_the_old_branch`, `::native_partitioned_column_def_replace_keys_the_spec_by_name_and_keeps_the_old_branch` (spec source id 4 on the re-keyed added column; branch `b1` reads `[NULL, 1, x]` after the replace; the native replace loads the table once — critic r3 [S2]/[S3]). | **PROVEN** | M3..M7 red. |
| C-012 | Residue R-9: after a replace that changes a kept column's type, the old branch reads NULL for it where Spark fails the read (`ClassCastException`); every other observation, the kept id included, equals Spark's. | `test_a_type_change_on_a_kept_name_reads_the_old_branch_as_null_divergence`. | **PROVEN** | Divergence pin; registry SAVEAS-OVERWRITE Residual. |
| C-013 | `overwrite(condition)` binds the frame to the table by name in the Rust door, as Spark's `OverwriteByExpression.byName`: the facade names the frame's columns and raises a typed `source_by_name` flag; a wider frame refuses `TOO_MANY_DATA_COLUMNS`, a frame column the table lacks at matching width refuses `INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_COLUMNS` with Spark's text naming every extra column, a column the frame lacks is NULL, names fold by `spark.sql.caseSensitive`; the SQL door stays positional. | Rust `tests/replace_where.rs::a_by_name_source_resolves_against_the_table_like_the_v2_writer`; `test_overwrite_condition_residues[oc_extra_and_missing_bigint / _string / _reordered]` (Spark's class, condition, SQLSTATE and text under the R-4 prefix, table unchanged); `[oc_frame_upper_case]`, `[oc_cond_on_dropped_frame_column]`, `[oc_missing_frame_column]` EQUAL; `repark-python tests.rs` intent pins pass `false`. | **PROVEN** | M14 red (the string shape committed shifted columns on the lane wheel before the fix, critic V-001). |
| C-014 | `isolation-level=none` refuses `Invalid isolation level: none` on every door that validates the option, as Spark's `IsolationLevel.fromName` (before: accepted, validation skipped). | `write_options::tests::isolation_level_none_refuses_like_spark`; `repark-iceberg writer_props.rs::isolation_override_none_refuses_like_spark`; `test_validate_from_snapshot_residues[vf_bad_id_level_none]` (Spark's text, R-4 class); registry ICE-WRITE-OPTIONS-1 wording. | **PROVEN** | The two validators' `none` arms removed; the table-property parser keeps the fork sentinel (out of scope). |
| C-015 | The 21 shapes critic r4 measured are pinned: the EQUAL ones on every observation, the rest as residues R-2/R-4 (rules) or R-11..R-14 (each beside Spark's recorded answer). | `test_overwrite_condition_shapes_match_spark` (+8), `test_validate_from_snapshot_shapes_match_spark` (+3), `test_overwrite_condition_residues` (+5), `test_validate_from_snapshot_residues` (+2), `test_overwrite_condition_divergences_on_the_row_filter`; oracle `measured` +21 (M-6). | **PROVEN** | Measured on both engines (M-6); no expected value typed in. |

## Pre-existing pins changed (slice 1)

- `test_ice_v3_write_default_1.py::test_saveastable_overwrite_is_insert_overwrite_not_replace`
  flipped red as its row promised; it is now
  `test_saveastable_overwrite_replaces_the_table_like_spark` against the same recorded cell
  and schema.
- `test_examples_window_catalog.py::test_writerv2_option_branch_refuses` is now
  `test_writerv2_option_branch_writes_the_default_branch` (EX-W2-3's measured Spark answer).
- `test_ice_hadoop_vn_1.py::test_stale_overwrite_doors_raise`: a stale-pointer
  `saveAsTable(overwrite)` now fails on the replace door, `CatalogCommitConflicts => Cannot
  stage replace to …`, as the SQL `CREATE OR REPLACE` does in
  `test_stale_replace_raises_conflict`. Round 2 (critic V-002): `_assert_stale_write_raises`
  takes the message start as a parameter, so this door keeps every other observation (exact
  class, `version file already exists`, `v3.metadata.json`, the metadata names, v3's bytes),
  and the full message is pinned with `==`.
- `test_time_travel.py::test_write_to_branch_unsupported` / `test_write_to_tag_unsupported`
  (the V2 refusal slice 1 removed) are `test_write_to_branch_option_writes_main` /
  `test_write_to_tag_option_writes_main` (round 2; the round-1 gate list missed this file).
- `test_ice_overwrite_mode_1.py::test_save_as_table_history_matches_spark` loses its strict
  xfail.

## Pre-existing pins changed (slice 2)

- `test_examples_window_catalog.py::test_writerv2_overwrite_condition_refuses` →
  `test_writerv2_overwrite_condition_replaces_the_matching_files` (one file per row; a
  one-file seed refuses on both engines, M-3).
- `test_writer_v2.py::test_write_to_overwrite_condition_loud_reject` →
  `test_write_to_overwrite_condition_replaces_the_matching_file`.
- `test_ice_write_options_1.py::test_overwrite_condition_refusal_with_options` →
  `test_overwrite_condition_carries_options` (the recorded P2-14 Spark cell).
- `test_ice_hadoop_vn_1.py::test_stale_overwrite_doors_raise`: `writeTo().overwrite("true")`
  (a column named `true` now) becomes `overwrite(lit(True))` on the stale pointer, which
  raises the recorded conflict contract.
- Round 2: `repark-iceberg writer_props.rs::isolation_override_none_disables_validations` →
  `isolation_override_none_refuses_like_spark` (C-014); `test_ice_write_df_2.py::_sorted_rows`
  sorts only inside runs of equal ids (critic V-007), so the slice-1 pins compare the
  `ORDER BY id` order again; `repark-python tests.rs` and `repark-core dialect/tests.rs` pass
  the new `source_by_name` argument / field.

## Residues

- **R-1** — the format-less `write.format.default` (C-003, registry EX-W2-5).
- **R-2** — Iceberg validation refusals (a file matching in part, conflicting files or
  deletes, an unknown start) raise `PySparkException` `DataInvalid => …` with `id = 1` and a
  bare path; Spark raises `ValidationException` with `ref(name="id") == 1` and `[<path>]`
  (U8's R-1 class).
- **R-3** — `mergeSchema` on an accept-any table refuses `TOO_MANY_DATA_COLUMNS`; Spark adds
  the column (REPLACE WHERE has no by-name evolving source).
- **R-4** — analysis refusals carry the engine's `Error during planning: ` prefix; `CAT`
  refuses as `AnalysisException` (Spark `ValidationException`); `isolation-level=bogus` as
  `AnalysisException` (Spark `IllegalArgumentException`); unresolved-column suggestions keep
  table order (Spark: closest first).
- **R-5** — an untranslatable condition quotes the facade's SQL (``upper(`cat`) = 'X'``;
  Spark `UPPER(cat) = 'X'`; U8's R-2 class).
- **R-6** — an explicit `isolation-level` without `validate-from-snapshot-id` validates from
  the loaded snapshot; Spark validates the whole history and refuses a matching earlier file
  (the fork has no whole-history mode).
- **R-7** — `overwrite(df["cat"] == "x")` is served; Spark refuses
  `MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_APPEAR_IN_OPERATION`.
- **R-8** — a missing table answers the V2 writer's own `TABLE_OR_VIEW_NOT_FOUND` text, as
  `append()` already does.
- **R-11** (2026-09-25) — a fractional literal on a long column (`between(0.5, 1.5)`,
  `id <= 1.5`) commits the `id = 1` replacement; Spark refuses `IllegalArgumentException:
  Cannot convert Spark predicate to Iceberg expression: CAST(id AS double) >= 0.5` (`<= 1.5`).
  RePark's row filter folds the literal into the long column. Pinned beside Spark's answer.
- **R-12** (2026-09-25) — `cat.startswith('x')` on the partition column refuses
  `IllegalArgumentException: Cannot convert Spark predicate to Iceberg expression:
  starts_with(`cat`, 'x')`; Spark converts it (`STARTS_WITH`) and commits `[2, 7, 8]`.
- **R-13** (2026-09-25) — `lit(None)` refuses `IllegalArgumentException: Cannot convert Spark
  predicate to Iceberg expression: null`; Spark raises `AnalysisException`
  `_LEGACY_ERROR_TEMP_1109` `Exec update failed: cannot translate expression to source filter:
  null.`. Both leave the table unchanged.
- **R-14** (2026-09-25) — a struct-field condition (`s.a == 1`) refuses at conversion
  (`Cannot convert Spark predicate to Iceberg expression: `s`.`a` = 1`); Spark converts it and
  refuses the partial file (`ValidationException`, R-2's class). Both leave the table unchanged.
- The R-4 class also covers `isolation-level=none` and the `EXTRA_COLUMNS` refusal's prefix
  (round 2).

## Mutation (step 6, `target/probe-u7-pr2/mutation-*.txt`)

Each load-bearing line was broken, its named test run, and the source restored.
- **M1:** `plan_save_as_table` answers `("overwrite", true) => Overwrite` again.
  `save_as_table_statements_follow_spark_save_modes` goes red: `left: ("overwrite", false)
  right: ("rtas", false)`.
- **M2:** `DataFrameWriterV2.option` raises on a `branch`/`tag` key again.
  `test_writer_v2_branch_option_writes_main_like_the_recorded_cell` and six
  `test_branch_and_tag_options_are_ignored_like_spark` shapes go red.
- **M3** (round 2, `target/probe-u7-pr2a-r1fix/mutation-M3.txt`): `replacement_schema`
  returns the frame's schema unchanged. All three `tests/replace_schema.rs` tests go red
  (`left: [(1, "id"), (2, "new")] right: [(1, "id"), (4, "new")]`).
- **M4** (`mutation-M4.txt`, rebuilt wheel): `ctas.rs` skips `replacement_schema`. Six
  `test_save_as_table_overwrite_keeps_field_ids_by_name_like_spark` shapes go red (reorder,
  swap, rename, nested, reordered `partitionBy`, re-add).
- **M5** (round 3, `target/probe-u7-pr2a-r2fix/mutation-M5.txt`): the counter starts at the
  current schema's highest id. `fresh_ids_start_above_the_last_column_id_not_the_highest_current_id`
  goes red (`left: [(1, "id"), (2, "data"), (3, "cat")] right: [… (4, "cat")]`).
- **M6** (`mutation-M6.txt`): the Spark column-def door skips `replacement_schema`. Both new
  `tests/replace_table.rs` pins go red (`left: ([(1, "cat"), (2, "data"), (3, "id")], 3)`).
- **M7** (`mutation-M7.txt`): the native door skips it. Both new `rtas_ops_tests.rs` pins go
  red.
- **M8:** `replace_where_statement` writes `REPLACE WHERE TRUE`.
  `test_overwrite_condition_replaces_like_the_recorded_cells`, `[oc_partition]` and
  `[oc_rows]` go red.
- **M9:** a column the frame lacks is named instead of `NULL AS`. `[oc_missing_frame_column]`
  goes red (an error where Spark answers).
- **M10:** `FilterValidation::start` returns the loaded snapshot for a requested start.
  `an_explicit_isolation_validates_from_the_requested_snapshot` and
  `serializable_refuses_a_matching_append_and_snapshot_does_not` go red.
- **M11:** the ancestry check is skipped.
  `the_requested_snapshot_parses_like_java_long_and_must_be_an_ancestor` goes red.
- **M12:** the `NOT_COLUMN_OR_STR` text is unrendered. `[oc_bool_condition]` goes red.
- **M13:** the option key is misspelled in `StatementWriteOptions::validate`.
  `validate_from_snapshot_id_stays_raw_until_an_overwrite_commit_reads_it` goes red:
  `left: None right: Some("abc")`.
- **M14** (round 2): `by_name_source` in `replace_where.rs` never binds (`&& false`).
  `a_by_name_source_resolves_against_the_table_like_the_v2_writer` goes red at its first
  `expect_err` — the extra-column source commits by position, the columns shifted.

## Out of scope (observed, not worked)

- Residue R-1 (C-003, registry `EX-W2-5`): the format-less provider property on CTAS and RTAS.
- Residue R-9 (C-012): a type-changed kept column reads NULL on a pre-replace branch; Spark
  fails the read.
- Residue R-10 (dated 2026-09-25, critic V-005): the fork's `TableMetadata` serializer writes
  `schemas`, `partition-specs` and `sort-orders` in `HashMap` order. After
  `partitionBy('data')` over a `cat`-partitioned table, RePark wrote `[spec 1, spec 0]` and
  Spark `[spec 0, spec 1]`. Ids, fields, `default-spec-id` and `last-partition-id` are equal.
  The order varies run to run and SQL cannot observe it, so the pins compare the sorted ids.
  This is a fork change (hand-back Q1).
- The fork's `StagedTableTransaction::begin_replace` does not run Java's `assignFreshIds`
  itself; RePark calls the fork's port before staging (hand-back Q1).
- A frame missing a column that carries a `write-default` writes NULL through
  `overwrite(condition)` (the append path fills the default); Spark's answer there is
  unmeasured, and RePark cannot create such a column through SQL.
- U8's REPLACE WHERE door reads the table's `write.overwrite.isolation-level` default when no
  option is set; Spark's `OverwriteByFilter` reads only the option. Unchanged here.
- The table-property parser (`overwrite.rs::parse_overwrite_isolation`) keeps the fork's
  `none` sentinel; only the `isolation-level` option validators refuse it (round 2, C-014).
- The isolation refusal's class (`AnalysisException`, Spark `IllegalArgumentException`) stays
  R-4 for `bogus` and `none` alike; the validators' error constructor is the same line.
- `mergeSchema` on an accept-any table still refuses `TOO_MANY_DATA_COLUMNS` (R-3): the by-name
  binding runs the resolution, not the schema evolution.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: u7-write-df-2
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: The four recorded cells are pinned from the scoreboard record and every
        measured shape is compared on rows, operations, summary counters, `.files` specs,
        partitioning, schema, refs, history, selected properties, format version, sort
        fields, the uuid, field ids by name, identifier ids, last-column-id, schema ids, spec
        source/field ids, and the refusal class, condition, SQLSTATE and text; residues R-1..R-9
        are divergence pins.
      artifacts: [python/repark/tests/test_ice_write_df_2.py, python/repark/tests/test_ice_write_df_2_overwrite.py, python/repark/tests/ice_write_df_1_spark_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: The oracle is live Spark 4.1.2 + Iceberg 1.11.0 (scoreboard record plus
        `target/probe-u7-pr2/record.py` under jvm-lock.sh); no expected value is derived from
        RePark output or typed in beside the oracle.
      artifacts: [python/repark/tests/ice_write_df_1_spark_oracle.json]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal on the new path leaves the table as Spark leaves it (rows,
        snapshots) and is pinned with its class and full text; the stale-pointer replace and
        overwrite keep the metadata files unchanged.
      artifacts: [python/repark/tests/test_ice_hadoop_vn_1.py]
    - id: AT-4
      status: N/A
      justification: No shared mutable state is added.
    - id: AT-5
      status: ATTACKED
      evidence: Memory catalogs under temp dirs; the JVM ran only for the probes under the lock;
        no network or credentials.
      artifacts: [task/ledgers/staging/u7-write-df-2-ledger.md]
    - id: AT-6
      status: ATTACKED
      evidence: One mutation per new path (M1..M14), each red on its named test (section Mutation).
      artifacts: [crates/repark-iceberg/src/tests/writer_plan.rs, crates/repark-iceberg/src/tests/replace_schema.rs, crates/repark-iceberg/src/tests/filter_validation.rs, python/repark/tests/test_ice_write_df_2.py]
    - id: AT-7
      status: N/A
      justification: No performance claim.
    - id: AT-8
      status: ATTACKED
      evidence: No Cargo.toml, lockfile, fork or workflow change; `writer_readwriter.py`
        ratchets 1033 to 1031, then 1029 in round 2, with its CAP-1 mirror row (1039 before
        slice 1's rounds); `writer_props.rs` stays at the 1000 default ceiling; the docs-links
        allowlist loses one row the guide fix repaired.
      artifacts: [scripts/check_lib_py.py, python/repark-parity/tests/test_cap_1_source_file_line_cap.py]
    - id: AT-9
      status: ATTACKED
      evidence: Registry rows ICE-V3-WRITE-DEFAULT-1-SAVEAS-OVERWRITE, EX-W2-3 and EX-W2-1
        FIXED, EX-W2-5 added; the guide and the touched map.md files in lockstep.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: The writer facade sweep (1327 passed over 40 files) and the touched Rust
        suites stay green; the 62 scoreboard W-DF cells plus W-CONCURRENT-VALIDATE-ERR
        replay 63 of 63 EQUAL (53 on the scoreboard's older RePark build); the pre-existing
        pins changed are listed with their reason.
      artifacts: [python/repark/tests/test_ice_overwrite_mode_1.py, python/repark/tests/test_ice_v3_write_default_1.py]
  complete: true
```
