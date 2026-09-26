# Unit ledger — U11-EDGE-1 · output-name spelling, case-sensitive partition sources, the v1 DELETE record

**Date:** 2026-09-26 · **Branch:** `feat/u11-edge-1` · **Base:** `4ae73c2c` (`origin/main`)
**Model:** Claude Opus 5.5 (`claude-opus-5-5`), triage-and-salvage of a Muse round ·
**Policy:** [../../../AGENTS.md](../../../AGENTS.md). **Path:** STANDARD. **risk_tier: standard.**

**Round 2 (2026-09-26, fixer, Claude Opus 5.5):** the verifier measured that the respelled SQL
frame broke four DataFrame-door operations that worked on main (V-001 drop, V-002 qualified
references, V-003 join on a name, V-004 unionByName). The fix is one Rust binder
(`df_guards/case_bind.rs`, re-exported as `repark_core::frame_names`) that every one of those
paths calls, plus the relation kept on respelled references (`column_resolution/display.rs`);
C-017…C-020. Output names are not re-lower-cased: `E-CASE-SELECT` replays EQUAL on every `obs`
key (`target/probe-u11-edge-1/replay-r3-case.json`).

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
| C-012 | `REPLACE PARTITION FIELD cat WITH CAT` refuses `Cannot find field 'CAT' in struct`. | Text; round 2 (V-012) adds one Rust pin per replace arm. | PROVEN | `test_replace_partition_field_wrong_case_refuses`; Rust `partition_spec::tests::replace_field_wrong_case_source_refuses_like_spark`, `replace_by_transform_wrong_case_source_refuses_like_spark` (spec keeps `cat`; M10). Probe `replace_cat_WITH_CAT`. |
| C-013 | The partition-source binding ignores `spark.sql.caseSensitive`: `ADD PARTITION FIELD CAT` refuses under `true` and `false`; `ADD PARTITION FIELD cat` answers under `true`. | Text and spec per setting. | PROVEN | `test_partition_field_case_rules_ignore_case_sensitive_conf`. Probes `sens_add_CAT`, `sens_add_cat`, `add_CAT`. |
| C-014 | `WRITE ORDERED BY CAT` answers under the default `caseSensitive=false`. | The statement succeeds. | PROVEN | `test_write_ordered_by_accepts_any_case`. Probe `write_ordered_CAT`. The `true` refusal is R-2. |
| C-015 | The DataFrame door binds an unqualified `F.col` to the single frame field that matches it case-insensitively when the frame lacks the exact name: `spark.sql("SELECT ID, data …").filter(F.col("id") > 1)` and `F.col("ID")` → `ID`, `data`, `[[2, b]]`; `select(F.col("id") + 1)` → `(id + 1)`; `SELECT id AS Id` filtered by `F.col("Id")` → `Id`, `[[2]]`; `createDataFrame(…, ["Id"]).filter(F.col("id") > 1)` → `[[2]]` (main refused this one too). An exact hit, a case twin or a qualified column is left as it is. | Facade rows and names; kernel pins. | PROVEN | `test_dataframe_door_binds_spelled_sql_columns`; Rust `case_bind::tests::folded_reference_binds_the_single_spelled_field`, `exact_ambiguous_and_qualified_references_stay`; `test_perf_facade_logical_names.py` green again. Probe `spark_r3.json`. |
| C-016 | `TP-FORMAT-V1-DELETE` replays EQUAL on every observation with no code change in this unit (the work is D-CREATE-V1, WO U5 PR2b). | Replay against the record. | PROVEN | `target/probe-u11-edge-1/replay-v1.json` (Muse round) and `replay-r2-v1.json` (this tree): every `obs` key equal to `spark-props.json`. The standing pins are `create_format_version_one.rs`. |

| C-017 | Round 2 (V-001). On `f = spark.sql("SELECT ID, data FROM t")` over rows `(1,'a'),(2,'b')`: `f.drop("ID")`, `f.drop("id")` and `f.drop(F.col("ID"))` answer `data` with `[[a],[b]]`; `f.drop("ID", "data")` answers no columns and two empty rows; `f.drop("data")` answers `ID`. The PR head dropped nothing for the first three and left `ID` for the pair. | Facade names and rows; the kernel pin. | PROVEN | `test_drop_binds_spelled_names`; Rust `case_bind::tests::drop_removes_every_folded_match`. Spark: `vx2-spark.json` `s_drop_colID`, `s_drop_two`, `s_drop_data`; `vx-spark.json` `sql_drop_ID`, `sql_drop_id`. |
| C-018 | Round 2 (V-002). On `q = spark.sql("SELECT ID, data FROM t t")`: `q.select(F.col("t.ID"))` → `ID`, `[[1],[2]]`; `q.select(F.col("t.id"))` → `id`; `q.filter(F.col("t.id") > 1)` → `ID`, `data`, `[[2, b]]`; `spark.sql("SELECT id FROM t t").select(F.col("t.id"))` → `id`; `spark.table(t).alias("t").select(F.col("t.ID"))` → `ID`. The respelled reference keeps its relation on the SQL frame and a qualified column binds case-insensitively within it; the output is named by the written segment. | Facade names and rows; kernel pins for the qualifier, the qualified bind, the written-segment alias and the projection spelling. | PROVEN | `test_qualified_references_bind_spelled_names`; Rust `respelled_plain_references_keep_their_relation`, `case_bind::tests::qualified_reference_binds_through_its_relation`, `qualified_alias_names_the_written_segment`, `projection_keeps_the_written_spelling`. Spark: `vx2-spark.json` `s_q_col_tID`, `s_q_filter_tid`, `e_q_col_tid`; `vx-spark.json` `qual_t_ID`, `qual_t_id`, `alias_qual` (the PR head named the last one `t.ID`). |
| C-019 | Round 2 (V-003). `spark.sql("SELECT ID, data FROM t").join(spark.sql("SELECT 1 AS ID, 'q' AS w"), "ID")` → `ID`, `data`, `w` with `[[1, a, q]]`: each key binds on each side case-insensitively and one key column stays. | Facade names and rows; the kernel pin. | PROVEN | `test_join_on_a_spelled_key`; Rust `case_bind::tests::join_binds_each_side_and_keeps_one_key`. Spark: `vx2-spark.json` `s_join_ID`. |
| C-020 | Round 2 (V-004). `spark.sql("SELECT ID, data FROM t").unionByName(spark.sql("SELECT id, Data FROM t"))` → `ID`, `data` with four rows: names pair case-insensitively and the left spelling wins. The column match moved from `core.py` into Rust. | Facade names and rows; the kernel pin (respelling, strict mismatch, allow-missing). | PROVEN | `test_union_by_name_matches_names_case_insensitively`; Rust `case_bind::tests::union_respells_the_right_and_refuses_a_mismatch`. Spark: `vx2-spark.json` `s_union`. |
| C-021 | Round 2 (V-005). The partition-source refusal renders the schema as Java's `StructType.toString`: a field with a doc carries ` (doc)` after its type and a decimal reads `decimal(10, 2)`; Spark printed `struct<1: id: required long (the key), 2: d: optional decimal(10, 2), 3: l: optional list<string>, 4: m: optional map<string, int>, 5: s: optional struct<15: a: optional int, 16: b: optional string>, 6: tn: optional timestamp, 7: bi: optional binary, 8: f: optional float, 9: db: optional double, 10: dt: optional date, 11: bo: optional boolean>`. | The exact text on the kernel. | PROVEN | Rust `partition_spec::tests::struct_text_renders_docs_and_decimal_like_java` (red on the PR head's rendering, which the verifier measured as `required long, 2: d: optional decimal(10,2)`). Spark: `vx3-spark.json` `add`; RePark before: `vx3-repark-102ab9d0.json`. |
| C-022 | Round 4 (verifier round 2 V-001, V-003). On `q = spark.sql("SELECT ID, data FROM t t")` over rows `(1,'a'),(2,'b')`: `q.drop(F.col("t.ID"))` and `q.drop(F.col("t.id"))` → `data`, `[[a],[b]]`; `q.drop("t.ID")`, `q.drop("u.id")` and `q.drop(F.col("u.id"))` → `ID`, `data` with both rows (a string names a column by its whole text; a target matching nothing is a no-op). On `j = spark.sql("SELECT a.id, b.ID FROM t a JOIN t b ON a.id = b.id")`: `j.drop(F.col("b.id"))` → `id`, `[[1],[2]]`; `j.drop(F.col("A.ID"))` → `ID`. The round-2 head refused the qualified shapes `Schema error: No field named t.id …` / `No field named b.id … Did you mean 'a.id'?`. | Facade names and rows; the kernel pin. | PROVEN | `test_qualified_drop_binds_through_its_relation` (red on 5e2a68b8, `target/probe-u11-edge-1/red-r4-facade.txt`); Rust `case_bind::tests::qualified_drop_binds_through_its_relation`. Spark: `target/probe-u11-edge-1/vz-spark.json` `q_drop_col_t_ID`, `q_drop_col_t_id`, `q_drop_str_t_ID`, `q_drop_str_u_id`, `q_drop_col_u_id`, `j_drop_col_b_id`, `j_drop_col_A_ID`; the verifier's `vy-spark.json` `drop_qual_str`, `drop_qual_col`, `jj_drop_b_id`. |
| C-023 | Round 4 (verifier round 2 V-002, V-008). Under `caseSensitive=false` a bare reference that matches two or more fields ignoring case refuses `[AMBIGUOUS_REFERENCE] Reference <ref> is ambiguous, could be: [<candidates>]. SQLSTATE: 42704`, an exact spelling among them included; the candidates are each field's relation parts plus the reference as written, sorted. Measured on `t_vz_1` (`vz-spark.json`): `j = spark.sql("SELECT a.id, b.ID FROM t a JOIN t b ON a.id = b.id")`: `j.select("id")` → ``Reference `id` … [`a`.`id`, `b`.`id`]``; `j.select(F.col("ID"))` → ``Reference `ID` … [`a`.`ID`, `b`.`ID`]``; the frame `SELECT b.id, a.ID …` gives the same sorted list; `j.select(F.col("a.Id"))` → `Id`, `[[1],[2]]`. `a = spark.sql("SELECT ID, data FROM t")`, `b = spark.sql("SELECT 1 AS id, 'q' AS w")`: `a.join(b, a["ID"] == b["id"]).select("id")` → ``Reference `id` … [`id`, `sc`.`ns`.`t_vz_1`.`id`]``; `a.join(b, F.col("ID") == F.col("id"))` refuses at build ``Reference `ID` … [`ID`, `sc`.`ns`.`t_vz_1`.`ID`]``; `b.join(a, F.col("id") == F.col("ID"))` ``Reference `id` … [`id`, `sc`.`ns`.`t_vz_1`.`id`]``. On `createDataFrame([(1, 2, 3)], ["id", "ID", "other"])` (`vz2-spark.json`): `df.filter(df["id"] > 1)` → ``Reference `id` … [`id`, `id`]``, `df["ID"]` likewise. RePark answers each with the same text behind DataFusion's `Error during planning: ` prefix (the SQL door's L-08 precedent). The round-2 head answered all of the select shapes and built both joins. | Facade texts; kernel pins for the rule, the sorted rendering, the unqualified and catalog candidates, the scratch relation, the condition check and the requalified join. | PROVEN | `test_bare_reference_matching_two_fields_is_ambiguous` (red on 5e2a68b8, `red-r4-facade.txt`), `test_filter_predicate_rewrite.py::test_column_entry_point_refuses_the_ambiguity_like_spark`, live leg `filter_case_collision_bypasses`; Rust `case_bind::tests::exact_and_ambiguous_references_stay` (flipped, V-008), `ambiguous_candidates_render_sorted_like_spark`, `unqualified_and_catalog_candidates_render_like_spark`, `requalified_join_carries_each_side_relation`. Spark: `target/probe-u11-edge-1/vz-spark.json`, `vz2-spark.json`; the verifier's `vy-spark.json` `jj_sel_id`, `jj_sel_col_ID`, `twin2_sel_id`, `twin_build`. |

VERDICT (2026-09-26, round 4): 23 clauses, 23 PROVEN, 0 OPEN, 0 REJECTED. `E-CATALOG-LISTDATABASES` is not a clause of this unit (moved, 2026-09-25).

## Mutation record (2026-09-26)

Each line was broken, the named tests ran, and the file was restored.

| # | Mutation | Red |
|---|---|---|
| M1 | `display_rewrite` always returns `None` | Rust `display_rewrite_keeps_the_written_spelling`, `quoted_wrong_case_and_qualified_resolve`, `group_by_wrong_case_groups`, `unresolved_stamp_keeps_folded_select`, `wrong_case_select_filters_orders_and_reads_rows`, `v02_outer_spelling_is_not_rewritten_into_an_inner_scope`, `s22b_thousand_branch_union_folds_wrong_case_on_a_two_mebibyte_stack` |
| M2 | `strict_case_guard` returns early | Rust `sensitive_session_refuses_folded_names_and_keeps_backticks`, `display_rewrite_keeps_the_written_spelling` |
| M3 | `bind_case_insensitive` never binds | Rust `case_bind::tests::folded_reference_binds_the_single_spelled_field` |
| M4 | the partition-source check uses `field_by_name_case_insensitive` | Rust `partition_spec_add_wrong_case_source_refuses_like_spark` |
| M5 | round 2: `same_relation` always false | Rust `qualified_reference_binds_through_its_relation`, `qualified_alias_names_the_written_segment` |
| M6 | round 2: `written_segment` always `None` | Rust `qualified_alias_names_the_written_segment` |
| M7 | round 2: `union_by_folded_name` never respells the right frame | Rust `union_respells_the_right_and_refuses_a_mismatch` |
| M8 | round 2: the join key projection compares keys case-sensitively | Rust `join_binds_each_side_and_keeps_one_key` |
| M9 | round 2: `requalify_alias` leaves the relation off | Rust `respelled_plain_references_keep_their_relation` |
| M10 | round 2: `bound_source` returns `None` for `ReplaceField` and `ReplaceFieldByTransform` | Rust `replace_field_wrong_case_source_refuses_like_spark`, `replace_by_transform_wrong_case_source_refuses_like_spark` |

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
      evidence: Every decision is in Rust (column_resolution/display.rs, df_guards/case_bind.rs, write/partition_spec.rs); round 2 moves the unionByName column match out of core.py into the Rust binder (the only Python source change, a shrink); ceilings only ratchet down (dataframe.rs 1017 → 1016, core.py 3991 → 3981, mirrored in CAP-1; alter.rs stays at 1607, router.rs untouched); no code comments.
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
| R-5 | Dated 2026-09-26, round 2 (V-006; probes `vx-spark.json` / `fx-repark.json`, cell `VX-SQL`, keys `subq_ID`, `cte_ID`, `catalog_view_ID`). A subquery, CTE or catalog view keeps the stored spelling where Spark keeps the written one: `subq_ID` Spark `ID` `[[1],[2]]`, RePark `id`; `cte_ID` Spark `ID`, `DATA`, RePark `id`, `Data`; `catalog_view_ID` Spark `ID`, `DATA` with two rows, RePark refuses `[UNRESOLVED_COLUMN.WITH_SUGGESTION] … name `data` cannot be resolved. Did you mean one of the following? [`id`, `Data`, `id`, `Data`, `s`]`. (`temp_view_ID` and `subq_ID_outer_id` are EQUAL.) Candidate for the case-sensitivity follow-up unit CASESENS-1. |
| R-6 | Dated 2026-09-26, round 2 (V-007; `VX-SQL` `upper_Data`, `upper_data_lc`, `cast_ID`; `VX-DF` `selectExpr_ID`). Expression output names carry the relation qualifier and DataFusion's spelling: Spark `upper(Data)`, RePark `upper(sc.ns.t_vx_sql.Data)`; Spark `upper(DATA)`, `(ID + 1)`, `count(ID) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`, RePark `upper(sc.ns.t_vx_sql.Data)`, `sc.ns.t_vx_sql.id + Int64(1)`, `count(sc.ns.t_vx_sql.id) ROWS BETWEEN …`; Spark `ID`, `(ID IS NULL)`, `coalesce(ID, 0)`, RePark `sc.ns.t_vx_sql.id`, `sc.ns.t_vx_sql.id IS NULL`, `coalesce(sc.ns.t_vx_sql.id,Int64(0))`; `selectExpr("ID", "upper(DATA)")` Spark `upper(DATA)`, RePark `upper(Data)`. Rows equal throughout. Candidate for CASESENS-1. |
| R-7 | Dated 2026-09-26, round 2 (V-008; `VX-DF` `withColumn_ID`, `tbl_describe_ID`; `VX2-DF` `s_join_self`). On the stored table `(id, Data, s)`: `withColumn("ID", lit(9))` Spark replaces `id` → `ID`, `Data`, `s` with `ID` = 9; RePark appends → `id`, `Data`, `s`, `ID`. `describe("ID")` Spark answers `summary`, `ID` (count 2, max 2, mean 1.5, min 1, stddev 0.7071067811865476); RePark refuses `PySparkValueError: describe/summary columns must be numeric or string; refused: ['ID']`. A self-join `s.alias("l").join(s.alias("r"), F.col("l.ID") == F.col("r.ID")).select("l.ID", "r.data")` Spark answers `ID`, `data` with `[[1, a], [2, b]]`; RePark refuses `[UNRESOLVED_COLUMN.WITH_SUGGESTION] … name `ID` cannot be resolved`. The fourth V-008 shape, `spark.table(t).alias("t").select(F.col("t.ID"))` (was `t.ID`), answers `ID` since round 2 (C-018, `alias_qual` EQUAL). Candidate for CASESENS-1. |
| R-8 | Dated 2026-09-26, round 2 (V-009; `VX-SQL` `id_as_Id_ID`, `star_plus`; `VX-TWIN` `twin_shape`, `twin_col_a`, `twin_col_A`, `twin_col_x`, `twin_filter_a`, `twin_str_a`, `twin_sql_a`). Case-twin outputs refuse under registry ID-3 where Spark answers: `SELECT id AS Id, ID` Spark `Id`, `ID` with `[[1,1],[2,2]]`; `SELECT *, ID` Spark `id`, `Data`, `s`, `ID`; `SELECT 1 AS a, 2 AS A` Spark `a`, `A` with `[[1, 2]]`; RePark refuses `Projections require unique expression names …` for each. References into the twin frame: Spark raises `[AMBIGUOUS_REFERENCE] Reference `a` is ambiguous, could be: [`a`, `a`]. SQLSTATE: 42704` (`A` likewise; the SQL door appends `; line 1 pos 7`) and, for `x`, `[UNRESOLVED_COLUMN.WITH_SUGGESTION] … Did you mean one of the following? [`A`, `a`]`; RePark refuses at the twin frame's creation with the unique-names text. Candidate for CASESENS-1. |
| R-9 | Dated 2026-09-26, round 2 (V-010; `VX-TWIN` `missing_x`). `spark.sql("SELECT ID, data FROM t t").select(F.col("x"))`: Spark `[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `x` cannot be resolved. Did you mean one of the following? [`ID`, `data`]. SQLSTATE: 42703` plus the plan; RePark `Schema error: No field named x. Valid fields are t."ID", t.data, t.id, t."Data".` (round 2 qualifies the respelled fields; before it read `"ID", data, t.id, t."Data"`). Candidate for CASESENS-1. |
| R-10 | Dated 2026-09-26, round 2 (V-012 part 1; `vx3-spark.json` / `vx3-repark-102ab9d0.json`, cell `VX3-TEXT`, keys `add_nested`, `add_nested_ok`, `replace_T`). Nested partition sources do not parse: `ADD PARTITION FIELD s.a` Spark answers (spec `s.a` identity); `ADD PARTITION FIELD s.A` Spark refuses `ValidationException: Cannot find field 's.A' in struct: …`; `REPLACE PARTITION FIELD s.a WITH bucket(2, ID)` Spark refuses `Cannot find field 'ID' in struct: …`. RePark refuses all three at parse: `trailing tokens after ADD PARTITION FIELD (starting at `.`)` and `ALTER TABLE REPLACE PARTITION FIELD expects `WITH <transform>(col) [AS name]``. Candidate for CASESENS-1. |
| R-11 | Dated 2026-09-26, round 4 (verifier round 2 V-004; `vy-spark.json` / `vy-base.json` / `vy-repark.json` and `target/probe-u11-edge-1/vy-r4-repark.json`, cell `VY-B`, keys `q_sel_str_t_id`, `q_sel_str_t_ID`, `tb_alias_sel_str`, `q_sel_t_star`). Qualified names as STRINGS in `select`: on `q = spark.sql("SELECT ID, data FROM t t")`, `q.select("t.id")` Spark `id` `[[1],[2]]`, `q.select("t.ID")` Spark `ID` `[[1],[2]]`, `q.select("t.*")` Spark `ID`, `data` with both rows; `spark.table(t).alias("t").select("t.ID")` Spark `ID` `[[1],[2]]`. RePark refuses each on main and on this head with the facade's `A column with name `t.id` cannot be resolved; available columns: ['ID', 'data']` (`t.ID`, `t.*` likewise; the aliased table lists `['id', 'Data']`). Candidate for CASESENS-1. |
| R-12 | Dated 2026-09-26, round 4 (V-005; `VY-B` `q_expr_t_id`). `q.select(F.col("t.id") + 1)` names the output `(t.id + 1)` on main and this head; Spark names it `(id + 1)`. Rows equal (`[[2],[3]]`, `int`). Candidate for CASESENS-1. |
| R-13 | Dated 2026-09-26, round 4 (V-006; `VY-B` `union_pos`, control `union_pos2`). Positional `union` of an INT column with a STRING column under ANSI: `spark.sql("SELECT ID, data FROM t").union(spark.sql("SELECT Data, id FROM t"))` Spark raises `NumberFormatException` `[CAST_INVALID_INPUT] The value 'a' of the type "STRING" cannot be cast to "BIGINT" because it is malformed. … SQLSTATE: 22018`; RePark answers both columns as strings (`ID` string, `data` string, rows `["1","a"]`, `["2","b"]`, `["a","1"]`, `["b","2"]`; main names them `id`, `Data`). The INT-with-INT control `union_pos2` is EQUAL. The same coercion is the standing disclosure `int_union_string`. Candidate for CASESENS-1. |
| R-14 | Dated 2026-09-26, round 4 (V-007; `VY-B` `q_sel_wrong_rel`). A reference to a missing relation, `q.select(F.col("u.id"))`: Spark `[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `u`.`id` cannot be resolved. Did you mean one of the following? [`t`.`ID`, `t`.`data`]. SQLSTATE: 42703` plus the plan; RePark `Schema error: No field named u.id. Valid fields are t."ID", t.data.` on this head (main: `Schema error: No field named u.id. Did you mean 't.id'?.`). Candidate for CASESENS-1. |
| R-15 | Dated 2026-09-26, round 4 (found by the fixer; `target/probe-u11-edge-1/vz-spark.json` / `vz-fixed.json` `j_filter_col_Id`). On `j = spark.sql("SELECT a.id, b.ID FROM t a JOIN t b ON a.id = b.id")`, `j.filter(F.col("Id") > 1)`: Spark `[AMBIGUOUS_REFERENCE] Reference `Id` is ambiguous, could be: [`a`.`Id`, `b`.`Id`]. SQLSTATE: 42704`; RePark refuses the same class but spells the reference as DataFusion's `col()` folds it, `Reference `id` … [`a`.`id`, `b`.`id`]`, because a compound Column (`F.col(...) > 1`) reaches the binder with the folded name (`select` of a bare `F.col` keeps the written spelling). Every door's refusal also carries DataFusion's `Error during planning: ` prefix ahead of Spark's text (C-023). Candidate for CASESENS-1. |
