# Unit ledger — U11-EDGE-1 · output-name spelling, case-sensitive partition sources, the v1 DELETE record

**Date:** 2026-09-26 · **Branch:** `feat/u11-edge-1` · **Base:** `4ae73c2c` (`origin/main`)
**Model:** Claude Opus 5.5 (`claude-opus-5-5`), triage-and-salvage of a Muse round ·
**Policy:** [../../../AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** Four scoreboard cells of the 2026-09-25 run: `E-CASE-SELECT` answered the stored
column case where Spark answers the query's spelling, `E-CASE-PARTITION-FIELD` added a partition
field that Spark refuses, `TP-FORMAT-V1-DELETE` was to be replayed first, and
`E-CATALOG-LISTDATABASES` listed the Iceberg catalog where Spark lists the session catalog.

**Orchestrator rulings (2026-09-25).** `E-CATALOG-LISTDATABASES` is OUT of this unit (moved to the
catalog unit): the Muse round's builder "single-catalog current-catalog default" removal and its
`catalog.py`, `session/catalog_resolution.py`, `session_core.py`, `writer_readwriter.py`,
`test_catalog_surface.py`, `test_e2_readwriter.py`, `test_production_file_size.py` and
`test_u11_edge_catalog.py` edits are reverted; the builder default is main's. No ceiling grows:
the round's `dataframe/core.py` (+71), `writer_readwriter.py` (+9) and `alter.rs` (+1) growth is
refused. The DataFrame-door case binding that `core.py` carried moved into Rust
(`df_guards/case_bind.rs`, C-015). The round's `count(*)` → `count(1)` rename and its pin
edits (`test_ice_count_fold_1.py`, `test_ice_metadata_cols_1.py`, `test_range_tvf_id_2.py`,
`metadata_columns_deleted.rs`, the `count` legs of `metadata_columns_reserved.rs` and
`column_resolution/tests.rs`) are reverted as out of the unit (residue R-3).

**What Spark does (measured 2026-09-25/26 on PySpark 4.1.2 + Iceberg 1.11 through
`jvm-lock.sh`; probes under `target/probe-u11-edge-1/`: `spark_case.json` from the Muse round,
`spark_r2.json` and `spark_r3.json` from this round).** Under the default
`caseSensitive=false` an output column keeps the spelling the query wrote: `SELECT ID, data` →
`ID`, `data`; `SELECT Data` → `Data`; `SELECT id AS X` → `X`; `SELECT *` → `id`, `Data`;
`SELECT t.ID` → `ID`; `SELECT s.A` → `A`; `GROUP BY ID` with `count(*) AS c` → `ID`, `c`; a
`UNION` takes the left spelling; `SELECT ID, id` → `ID`, `id`; `df.select('ID')` → `ID`; and a
`F.col` of either case filters a spelled frame. Under `caseSensitive=true`, `SELECT ID` refuses
`[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `ID`
cannot be resolved. Did you mean one of the following? [`id`, `Data`]. SQLSTATE: 42703`.
Partition sources bind through Iceberg's `NamedReference.bind`, case-sensitively under either
setting: `ADD PARTITION FIELD CAT`, `bucket(4, ID)` and `REPLACE PARTITION FIELD cat WITH CAT`
raise `Py4JJavaError` wrapping `org.apache.iceberg.exceptions.ValidationException: Cannot find
field 'CAT' in struct: struct<1: id: optional int, 2: cat: optional string>`; `DROP PARTITION
FIELD CAT` raises `IllegalArgumentException: Cannot find partition field to remove: CAT` whether
or not an identity field `cat` exists; `WRITE ORDERED BY CAT` answers under `false` and refuses
the same `ValidationException` under `true`.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | `SELECT ID, data FROM t WHERE DATA = 'a'` on `(id INT, Data STRING)` answers columns `ID` (int), `data` (string) and rows `[[1, a]]`. | Names and rows on the facade and the kernel; the scoreboard cell. | PROVEN | `test_select_keeps_written_spelling`; Rust `display_rewrite_keeps_the_written_spelling`, `wrong_case_select_filters_orders_and_reads_rows`, `quoted_wrong_case_and_qualified_resolve`. Cell `E-CASE-SELECT` replays EQUAL on `rows` and `cols` (`target/probe-u11-edge-1/replay-r2-case.json`). `test_ice_mixed_case_1.py` now compares the recorded Spark names exactly (`userid`, `EVENTNAME`, `USERID`). Probe `spark_case.json` `sel_ID_data_*`. |
| C-002 | `SELECT Data` answers `Data`. | Name pin. | PROVEN | `test_select_mixed_case_single_column`. Probe `sel_Data_cols`. |
| C-003 | An unquoted alias keeps its case: `SELECT id AS X` → `X` (main answered `x`). | Name pin on both doors. | PROVEN | `test_select_alias_wins`; Rust `display_rewrite_keeps_the_written_spelling` (`userId AS ID` → `ID`). Probe `sel_alias`. |
| C-004 | `SELECT *` answers the stored names `id`, `Data`. | Name pin on both doors. | PROVEN | `test_select_star_keeps_stored_names`; Rust `display_rewrite_keeps_the_written_spelling`. Probe `sel_star`. |
| C-005 | A qualified reference names its column: `SELECT t.ID FROM t t` → `ID`; a struct field `SELECT s.A` → `A` with row `[5]`; `SELECT s._deleted` → `_deleted`; unaliased `s.a` → `a` (EX-COL-2's SQL arm). | Name and row pins. | PROVEN | `test_select_qualified_and_struct_field`; Rust `quoted_wrong_case_and_qualified_resolve`, `metadata_columns_reserved::reserved_word_positions_follow_spark` (B7); `test_ice_nested_evo_1.py::test_unaliased_nested_projection_names_like_spark` (strict xfail retired). Probes `sel_qual`, `sel_struct`, `spark_r2.json` `sv_struct`. |
| C-006 | `SELECT ID, count(*) AS c … GROUP BY ID` → `ID`, `c` with `[[1, 1]]`; `SELECT ID … UNION SELECT id …` → `ID` with `[[1]]`. | Names and rows. | PROVEN | `test_select_group_by_and_union`; Rust `group_by_wrong_case_groups`, `display_rewrite_keeps_the_written_spelling` (UNION). Probes `spark_r2.json` `group_alias`, `spark_case.json` `sel_union`. |
| C-007 | Under `caseSensitive=true` a wrong-case reference on a single-table query refuses Spark's `UNRESOLVED_COLUMN.WITH_SUGGESTION` text naming the reference as written and the stored names as suggestions; the exact spelling answers. | Message pins on both doors. | PROVEN | `test_case_sensitive_select_refuses`; Rust `sensitive_session_refuses_folded_names_and_keeps_backticks`, `display_rewrite_keeps_the_written_spelling` (`T.USERID`). Probes `spark_r2.json` `sens_ID`, `sens_v_ID`, `sens_id`. |
| C-008 | `spark.table(t).select('ID')` answers `ID` with `[[1]]`. | Name and row. | PROVEN | `test_dataframe_select_keeps_spelling` (already true on main, measured on a main build 2026-09-26; pinned). Probe `df_select`. |
| C-009 | `ALTER TABLE t ADD PARTITION FIELD CAT` on `(id INT, cat STRING)` refuses with the text ending `org.apache.iceberg.exceptions.ValidationException: Cannot find field 'CAT' in struct: struct<1: id: optional int, 2: cat: optional string>` and the default spec stays empty. | Full text and the metadata spec on the facade; the kernel pin; the cell. | PROVEN | `test_add_partition_field_wrong_case_refuses`; Rust `partition_spec_add_wrong_case_source_refuses_like_spark` (replaces main's I7 accept pin). Cell `E-CASE-PARTITION-FIELD`: both engines refuse (`replay-r2-part.json`). Probe `spark_r2.json` `add_CAT`. |
| C-010 | `ADD PARTITION FIELD bucket(4, ID)` refuses `Cannot find field 'ID' in struct`; spec unchanged. | Text and spec. | PROVEN | `test_add_partition_field_bucket_wrong_case_refuses`. Probe `add_bucket_ID`. |
| C-011 | `DROP PARTITION FIELD CAT` on a table with no partition field refuses `Cannot find partition field to remove: CAT`; spec unchanged. | Text and spec. | PROVEN | `test_drop_partition_field_missing_refuses` (RePark's class, R-1 note). Probe `drop_CAT_missing`. |
| C-012 | `REPLACE PARTITION FIELD cat WITH CAT` refuses `Cannot find field 'CAT' in struct`. | Text. | PROVEN | `test_replace_partition_field_wrong_case_refuses`. Probe `replace_cat_WITH_CAT`. |
| C-013 | The partition-source binding ignores `spark.sql.caseSensitive`: `ADD PARTITION FIELD CAT` refuses under `true` and `false`; `ADD PARTITION FIELD cat` answers under `true`. | Text and spec per setting. | PROVEN | `test_partition_field_case_rules_ignore_case_sensitive_conf`. Probes `sens_add_CAT`, `sens_add_cat`, `add_CAT`. |
| C-014 | `WRITE ORDERED BY CAT` answers under the default `caseSensitive=false`. | The statement succeeds. | PROVEN | `test_write_ordered_by_accepts_any_case`. Probe `write_ordered_CAT`. The `true` refusal is R-2. |
| C-015 | The DataFrame door binds an unqualified `F.col` to the single frame field that matches it case-insensitively when the frame lacks the exact name: `spark.sql("SELECT ID, data …").filter(F.col("id") > 1)` and `F.col("ID")` → `ID`, `data`, `[[2, b]]`; `select(F.col("id") + 1)` → `(id + 1)`; `SELECT id AS Id` filtered by `F.col("Id")` → `Id`, `[[2]]`; `createDataFrame(…, ["Id"]).filter(F.col("id") > 1)` → `[[2]]` (main refused this one too). An exact hit, a case twin or a qualified column is left as it is. | Facade rows and names; kernel pins. | PROVEN | `test_dataframe_door_binds_spelled_sql_columns`; Rust `case_bind::tests::folded_reference_binds_the_single_spelled_field`, `exact_ambiguous_and_qualified_references_stay`; `test_perf_facade_logical_names.py` green again. Probe `spark_r3.json`. |
| C-016 | `TP-FORMAT-V1-DELETE` replays EQUAL on every observation with no code change in this unit (the work is D-CREATE-V1, WO U5 PR2b). | Replay against the record. | PROVEN | `target/probe-u11-edge-1/replay-v1.json` (Muse round) and `replay-r2-v1.json` (this tree): every `obs` key equal to `spark-props.json`. The standing pins are `create_format_version_one.rs`. |

VERDICT (2026-09-26): 16 clauses, 16 PROVEN, 0 OPEN, 0 REJECTED. `E-CATALOG-LISTDATABASES` is not a clause of this unit (moved, 2026-09-25).

## Mutation record (2026-09-26)

Each line was broken, the named tests ran, and the file was restored.

| # | Mutation | Red |
|---|---|---|
| M1 | `display_rewrite` always returns `None` | Rust `display_rewrite_keeps_the_written_spelling`, `quoted_wrong_case_and_qualified_resolve`, `group_by_wrong_case_groups`, `unresolved_stamp_keeps_folded_select`, `wrong_case_select_filters_orders_and_reads_rows`, `v02_outer_spelling_is_not_rewritten_into_an_inner_scope`, `s22b_thousand_branch_union_folds_wrong_case_on_a_two_mebibyte_stack` |
| M2 | `strict_case_guard` returns early | Rust `sensitive_session_refuses_folded_names_and_keeps_backticks`, `display_rewrite_keeps_the_written_spelling` |
| M3 | `bind_case_insensitive` never binds | Rust `case_bind::tests::folded_reference_binds_the_single_spelled_field` |
| M4 | the partition-source check uses `field_by_name_case_insensitive` | Rust `partition_spec_add_wrong_case_source_refuses_like_spark` |

## Coverage

```yaml
COVERAGE_ATTESTATION:
  pr_unit: u11-edge-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Every clause comes from a live Spark 4.1.2 + Iceberg 1.11 measurement under target/probe-u11-edge-1/ or the scoreboard record; the three cells replay against the records (E-CASE-SELECT and TP-FORMAT-V1-DELETE EQUAL on every obs key, E-CASE-PARTITION-FIELD both refuse).
      artifacts: [python/repark/tests/test_u11_edge_case_select.py, python/repark/tests/test_u11_edge_partition_field.py]
    - id: AT-2
      status: ATTACKED
      evidence: Plain, mixed-case, aliased, star, qualified, struct-field, grouped, union and case-sensitive spellings; the DataFrame door on SQL, aliased and created frames; partition ADD, bucket, DROP, REPLACE and WRITE ORDERED BY under both case settings.
      artifacts: [crates/repark-core/src/column_resolution/tests.rs, crates/repark-core/src/session/df_guards/case_bind.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Every partition refusal asserts the default spec unchanged from the table metadata; the source check runs before the transaction is built.
      artifacts: [crates/repark-iceberg/src/write/partition_spec.rs, python/repark/tests/test_u11_edge_partition_field.py]
    - id: AT-4
      status: N/A
      justification: No concurrency surface changes; the partition check reads the loaded table's current schema inside the existing single-transaction flow.
    - id: AT-5
      status: N/A
      justification: No privileged action, credential, deserialization or path handling; the display rewrite only re-quotes aliases of the statement already parsed.
    - id: AT-6
      status: ATTACKED
      evidence: The whole facade suite (13,580 tests) and the repark-core, repark-spark, repark-iceberg and repark-python Rust suites are green; the re-pinned names (ice-mixed-case-1, ice-nested-evo-1, metadata_columns_reserved B7, the DataFusion alias test) follow the measured Spark spelling.
      artifacts: [python/repark/tests/test_ice_mixed_case_1.py, python/repark/tests/test_ice_nested_evo_1.py, crates/repark-spark/src/tests/metadata_columns_reserved.rs]
    - id: AT-7
      status: ATTACKED
      evidence: The display rewrite re-plans only when a written spelling differs from the planned name; the repair future boxes the display and strict-guard steps so the 100-level nested-view and 5,000-branch union stacks stay within their pins (test_nested_view_depth_guard segfaulted on the unboxed draft).
      artifacts: [crates/repark-core/src/column_resolution.rs, python/repark/tests/test_ice_views_1.py]
    - id: AT-8
      status: ATTACKED
      evidence: Every decision is in Rust (column_resolution/display.rs, df_guards/case_bind.rs, write/partition_spec.rs); no Python source file changes; no ceiling moves (alter.rs stays at 1607, router.rs untouched); no code comments.
      artifacts: [crates/repark-core/src/column_resolution/display.rs, crates/repark-iceberg/src/write/alter.rs]
    - id: AT-9
      status: ATTACKED
      evidence: The registry gains E-CASE-SELECT, E-CASE-PARTITION-FIELD, TP-FORMAT-V1-DELETE and the moved E-CATALOG-LISTDATABASES rows and an EX-COL-2 note.
      artifacts: [docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: Each load-bearing line was broken and its named tests ran red (mutation table above).
      artifacts: [crates/repark-core/src/column_resolution/tests.rs, crates/repark-iceberg/src/write/alter.rs]
  complete: true
```

## Residues

| # | Residue |
|---|---|
| R-1 | Dated 2026-09-26 (probe `spark_r2.json` `drop_CAT_present`). `DROP PARTITION FIELD CAT` on a table whose identity field is `cat`: Spark refuses `IllegalArgumentException: Cannot find partition field to remove: CAT`; RePark drops the field (`partition_spec.rs` resolves partition-field names case-insensitively, pinned by main's `partition_spec_drop_replace_field_name_case_insensitive`). The refusal class also differs where both refuse: RePark raises `PySparkException` `DataInvalid => …`, Spark `IllegalArgumentException` / `Py4JJavaError`. Needs a measurement of `RENAME` and by-name `REPLACE` before the lookup changes. |
| R-2 | Dated 2026-09-26 (probe `spark_r2.json` `sens_write_ordered_CAT`). `WRITE ORDERED BY CAT` under `caseSensitive=true`: Spark refuses `ValidationException: Cannot find field 'CAT' in struct: struct<1: id: optional int, 2: cat: optional string>`; RePark answers (`sort_order.rs` resolves case-insensitively under both settings). |
| R-3 | Dated 2026-09-26 (probe `spark_case.json` `sel_group`). `SELECT ID, count(*) … GROUP BY ID` names the aggregate `count(*)` on the SQL door; Spark names it `count(1)`. Out of this unit by the orchestrator's scope; the Muse round's rename and its pin edits were reverted. |
| R-4 | Dated 2026-09-26 (probe `spark_r2.json` `dup`). `SELECT ID, id` refuses DataFusion's `unique expression names` (registry ID-3); Spark answers `ID`, `id`. |
