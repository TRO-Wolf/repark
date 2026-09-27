# Unit ledger — WO CASESENS-1 · names resolve the way Spark resolves them under `spark.sql.caseSensitive` false and true

**Date:** 2026-09-27 · **Branch:** `feat/casesens-1-s1` · **Base:** `707e8a52`
(`origin/main`) · **Model:** Muse Spark (`muse-spark-1.3-contributor`) · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** GUIDED. **risk_tier: high.**

**Retires:** this ledger moves to `../completed/` when the unit's last commit lands.

**Why now.** RePark answers a nested query's inner scopes with the stored spelling
where Spark keeps the written one (u11 R-5: derived tables, CTEs, set-operation
branches, column-alias lists, catalog views), and a MERGE whose derived source
spells a column in another case refuses `No field named data` where Spark
answers (u11 R-30). Under `caseSensitive=true` RePark folds where Spark matches
exactly (N-1, N-2), case twins refuse where Spark answers (R-4, R-8, N-3), and
Iceberg DDL binds names case-insensitively where Iceberg binds exactly (R-1,
R-2, N-4). This unit adopts Spark's resolver per setting in five slices; slice 1
is the `false`-mode nested scopes plus the MERGE derived source.

**What Spark does (measured 2026-09-27; Spark 4.1.2 + Iceberg 1.11.0, probes
`p1-spark.json` … `p4-spark.json`, oracle `casesens_1_spark_oracle.json`, 210
steps; RePark baseline `p1-repark.json` … `p4-repark.json`, same day).** A
derived table's, a CTE body's and a nested derived table's projection keeps the
written spelling of each plain reference and unquoted alias, and a derived
table's column-alias list keeps its spelling. A catalog view whose body spells
columns in another case answers the written names. A MERGE whose derived source
or clauses spell columns in another case answers, as do UPDATE / DELETE /
INSERT column lists spelled in another case. Under `caseSensitive=true` every
wrong-case reference refuses exactly; case-twin outputs answer while
case-twin columns refuse at creation; partition field names, transform sources
and identifier fields bind exactly under both settings and the write order
follows the session.

## Slice 1 (2026-09-27, this round; landed without C-003 per ruling (b))

S1-0 baseline (before any edit): p1 16/84, p2 15/63, p3 11/34, p4 7/15 EQUAL
(`target/casesens-1/baseline-pN.txt`). S1-1 new
`crates/repark-core/src/column_resolution/inner_scopes.rs`
(`respell_inner_scopes`, R4) wired into `finish_with_display` (+10 lines in
`column_resolution.rs`, budget +30 for the unit): on a successful plan the
folded statement's inner scopes are respelled and re-planned once. S1-2 new
`column_resolution/fold_text.rs` (`fold_query_text`, R5) called from
`maybe_rewrite_merge_fragments` for a parenthesized MERGE source before the
fragment rewrite. Pins: `crates/repark-spark/src/tests/casesens_scopes.rs` (5
tests), `python/repark/tests/test_casesens_1.py` (17 nested + merge legs) over
the copied oracle (unchanged bytes). The S1-3 catalog-view halt fired (record
in C-003); per the 2026-09-27 ruling (b) the view legs were dropped from the
commit, C-003 stays OPEN, and C-003 with R-CS1-1 is homed to CASESENS-1 S1b,
the repair-loop round next after Slice 1, before Slice 2. D1 is accepted in the
same ruling.

Deviation D1 (2026-09-27, forced by a regression the order did not foresee):
the re-plan in `finish_with_display` falls back to the pre-respell plan when
the respelled statement fails to plan, instead of propagating the error. Without
the fallback, `SELECT ID FROM (SELECT ID FROM t)` (`p1/r5_subq_both`, EQUAL on
main, a C-002 key) refuses: the respelled inner output `"ID"` leaves the outer
folded `id` with no hit, and the fold loop cannot see an unaliased derived
table's fields. The fallback keeps the refinement total: a naming pass never
breaks a successful plan. `plan_with_repair` itself is untouched.

Baseline vs inferred (all three go to the hand-back): `p1/r5_cte_outer` refuses
on main (`UNRESOLVED_COLUMN` name `data`, 42703) where C-002 placed it among the
already-equal shapes — dropped from C-002 and the pin per the 2026-09-27
orchestrator ruling, recorded as R-CS1-1; `p1/tw_create` refuses
(`DataInvalid => Cannot build lower case index: a and A collide`) where the
order inferred RePark may create the twins (S4 input); `p1/r5_view_describe`
is EQUAL at baseline (better than C-003 implies). `p1/cs_insert_cols` refuses
on main too (naming `data` where Spark names `ID`), against the N-1 "answers
(inferred, not run)" — S2 input. The DML/DDL `[[]]`-vs-`[]` framing is
pre-existing and out of this unit's fence.

## Slice 1b (2026-09-27, this round; Devin SWE-2)

S1b-0 trace (before any edit; instrumentation removed before the commit): all
four keys left `plan_with_repair` through `seen`-repeat. `r5_cte_outer`: the
first miss is inside the CTE body (`data` against `ice.sales.t`'s `id`,
`Data`); the fold respells the body to `` `Data` ``, the outer `ID`, `DATA`
look up `Unknown` (relation `c` carries no fields), the re-plan fails on
`data` against `c.id`, `c.Data` — the same `(None, "data")` miss repeats. The
view keys plan `SELECT * FROM (SELECT ID, DATA FROM ice.sales.t) AS
_repark_view("ID", "DATA")`: the same inner miss, then DataFusion's own
`apply_expr_alias` (`col(field.name())` zipped onto the column-alias list)
normalizes `Data` to `data`, which cannot resolve — the same miss repeats.
S1b-1 lands in `column_resolution/`: new `scope_fields.rs` (`query_outputs`
reads a query's syntactic output names the way DataFusion names them — the
written segment for a plain reference, the normalizer's rendering for `AS`
aliases and column-alias lists, the left branch of a set operation, `*`
expanded from the statement's own relations, `None` where uncomputable) and
`InjectAliases` (moves `x(col, …)` alias lists into the body as `AS` items
when no clause can reference them); `fold.rs` carries each CTE's outputs on
its `Level` and gives a CTE-shadowed `FROM` name and a derived-table alias
those fields; `plan_with_repair` is unchanged (zero net lines). Pins: three
new tests in `casesens_scopes.rs`, `r5_cte_outer` re-added to
`already_equal_nested_shapes_stay` (EQUAL), and in `test_casesens_1.py` the
`r5_cte_outer` nested leg plus `test_s1_catalog_view_keeps_its_spelling`.
Probe `after-s1b`: exactly the four target keys changed (`r5_cte_outer`,
`r5_view_star`, `r5_view_lower`, `r5_view2_upper` DIFF→EQUAL),
`r5_view_describe` stays EQUAL. Mutation (the S1b order's M3, ledger M13):
`query_outputs` returns `None` and `InjectAliases` is skipped — both new pins
red, restored byte-identical. The twin shape `WITH c AS (SELECT 1 AS a, 2 AS
A) SELECT A FROM c` is unmeasured in `p1-spark.json`; the pin asserts only the
kept refusal class (`unique expression names`), recorded as unmeasured.

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Under `caseSensitive=false` a derived table's, a CTE body's and a nested derived table's projection keeps the written spelling of each plain reference and unquoted alias, and a derived table's column-alias list keeps its spelling: `p1/r5_subq_inner_ID`, `r5_cte_ID`, `r5_cte_mixed`, `r5_union_subq`, `p3/n_nested2`, `n_join_derived`, `n_col_alias_list`, `n_df_sql_subq` answer Spark's recorded names and rows. | Rust door pins plus the facade replay assert names and rows per key; M1 breaks the pass. | PROVEN | `casesens_scopes.rs` `derived_and_cte_projections_keep_the_written_spelling`, `column_alias_list_keeps_its_spelling`; `inner_scopes.rs` unit tests; `test_casesens_1.py` `test_s1_nested_scopes_keep_the_written_spelling`; M1. After-S1 probe: the four p1 keys EQUAL. Fold round 2026-09-27: `leaves_a_spelling_written_twice_in_another_case` (M14), `refuses_unequal_scope_walks` (M15), `keeps_a_quoted_value` (M16), the `SELECT 1 AS ID` alias fixtures in `respells_derived_and_cte_items_written_in_another_case` and `derived_and_cte_projections_keep_the_written_spelling` (M17; the leg asserts only RePark's `ID` — no probe key records an expression-alias output name, unmeasured). |
| C-002 | The nested shapes Spark answers that RePark already answers stay equal, and expression subqueries are not respelled: `p1/r5_subq_ID`, `r5_subq_both`, `r5_subq_alias_qual`, `r5_in_subq`, `r5_temp_view_star`, `r5_temp_view_lower`, `p3/n_group_having`, `n_cte_star_outer_upper`, `n_exists`. | Same doors; the pin fails a pass that respells expression subqueries or breaks an outer reference. | PROVEN (clause rewritten 2026-09-27 per orchestrator ruling: `p1/r5_cte_outer` moved to R-CS1-1; regained 2026-09-27 under S1b, EQUAL) | `casesens_scopes.rs` `already_equal_nested_shapes_stay` (`r5_cte_outer` re-added under S1b); `test_casesens_1.py` nested legs; D1 keeps `r5_subq_both` EQUAL. Fold round 2026-09-27: the scalar-subquery fixture in `inner_scopes.rs` `leaves_expression_subqueries_alone` (M18). |
| C-003 | A catalog view whose body spells columns in another case answers Spark's names and rows: `p1/r5_view_star`, `r5_view_lower`, `r5_view_describe`, `r5_view2_upper`. | Same doors once the S1-3 ruling lands. | PROVEN (2026-09-27, S1b) | `casesens_scopes.rs` `catalog_view_body_keeps_its_spelling` (names and rows for `r5_view_star`, `r5_view_lower`, `r5_view2_upper`; `r5_view_describe` unchanged); `test_casesens_1.py` `test_s1_catalog_view_keeps_its_spelling` over the `vc` oracle keys; probe `after-s1b` all four EQUAL (`describe` stays). S1-3 record 2026-09-27 kept: stored text `CREATE VIEW ice.sales.v (ID, DATA) … AS SELECT ID, DATA FROM ice.sales.t`; stored columns `["ID", "DATA"]`; the SQL `execute_view_body_query` plans is `SELECT * FROM (SELECT ID, DATA FROM ice.sales.t) AS _repark_view("ID", "DATA")` — S1b's `InjectAliases` moves the list into the body as `AS` items so DataFusion's normalizing `col(field.name())` path never runs. |
| C-004 | A MERGE whose derived source or clauses spell columns in another case answers as Spark: `p1/r30_merge_derived`, `r30_merge_upper_set` (tables after each as recorded), and UPDATE / DELETE / INSERT column lists spelled in another case answer (`dml_*`). | Rust door pins plus the facade merge replay assert the tables after each statement; M2 breaks the fold. | PROVEN | `casesens_scopes.rs` `merge_with_a_derived_source_spelled_in_another_case` is the red-on-main proof (baseline fails `No field named data`); `dml_spelled_in_another_case_answers` stays as a regression pin and is not cited as red-on-main evidence; `test_casesens_1.py` `test_s1_merge_derived_source`; M2. Probe `r30_after` / `dml_after` still differ downstream of S2 scope: the `true`-mode `cs_*` DML steps mutate `u` on RePark (C-008) while Spark refuses them, so the probe's `u` is `(1,m)` at `r30` time; the hermetic pins assert Spark's recorded tables. |
| C-005 | Under `caseSensitive=true` a reference whose exact spelling is not held refuses `UNRESOLVED_COLUMN.WITH_SUGGESTION` (42703) naming the reference as written, qualified as written, in every scope (select list, join condition, derived table, CTE, WHERE, ORDER BY, GROUP BY, alias, backticks), and a wrong-case struct field refuses (`p1/cs_struct_A`: Spark `FIELD_NOT_FOUND` 42704). | Slice 2. | OPEN | Slice 2. |
| C-006 | Under `caseSensitive=true` an exact mixed-case spelling answers and is named as written (`p1/cs_sel_Data`, `p3/cs_backtick_Data`, `cs_temp_view_exact`, `p1/cs_tw_ID_id`, `cs_tw_lit_ref`, `p3/cs_star`, `cs_func_upper`), and function names stay case-insensitive. | Slice 2. | OPEN | Slice 2. |
| C-007 | Under `caseSensitive=true` relation aliases, CTE names and temp-view names match exactly (`p1/cs_table_T`, `cs_ns_NS`, `p3/cs_cte_name`, `cs_temp_view_upper`, `cs_rel_alias_upper` refuse); under `false` they fold (`p3/rel_alias_upper`, `cte_name_upper`, `temp_view_upper` answer); metadata-table suffixes fold under both (`p4/mt_upper_true`, `mt_upper_false`). | Slice 2. | OPEN | Slice 2. |
| C-008 | Under `caseSensitive=true` INSERT column lists, UPDATE, DELETE and MERGE fragments with a wrong-case column refuse and leave the table unchanged (`p1/cs_insert_cols`, `cs_update_where`, `cs_delete_where`, `cs_merge_on` with Spark's full text); the exact column list answers (`p4/ins_exact_true`). | Slice 2. | OPEN | Slice 2. Baseline note: `cs_insert_cols` already refuses on main (naming `data`, not Spark's `ID`). |
| C-009 | Under `caseSensitive=true` the DataFrame door's `F.col`, string names, `filter`, `orderBy`, `groupBy`, `selectExpr`, string `filter`, `describe`, `join(on=)`, `unionByName` and `drop` resolve exactly with Spark's measured refusal texts (`p1/cs_df_*` listed in S3, `p4/df_window_ID_true`); under `false` `F.col("ID")`, `select("ID")`, `F.col("ID") + 1` and a window ordered by `F.col("ID")` answer and are named as written (`p4/df_col_ID_false`, `df_select_str_ID_false`, `df_expr_ID_false`, `df_window_ID_false`). | Slice 3. | OPEN | Slice 3. |
| C-010 | `describe` / `summary` columns bind by the session rule in Rust and name the output with the written spelling (`p1/r7_describe_ID`, `p3/n_df_describe_two`). | Slice 3. | OPEN | Slice 3. |
| C-011 | Under `caseSensitive=false` case-twin outputs answer with both spellings (`p1/tw_ID_id`, `tw_Id_ID`, `tw_star_ID`, `tw_lit`, `p3/tw_subq_star`); a reference into the twins refuses `AMBIGUOUS_REFERENCE` (42704) (`p1/tw_lit_ref`); exact duplicates keep ID-3's refusal. | Slice 4. | OPEN | Slice 4. |
| C-012 | Under `caseSensitive=false` `CREATE TABLE`, CTAS, `CREATE VIEW` and `CREATE TEMPORARY VIEW` with case-twin columns refuse `[COLUMN_ALREADY_EXISTS] The column `a` already exists. Choose another name or rename the existing column. SQLSTATE: 42711` and create nothing (`p1/tw_ctas`, `tw_create`, `p3/tw_view`, `tw_temp_view`, `tw_temp_view_read`); a positional INSERT from twins answers (`p3/tw_insert_into_run`, `tw_insert_after`). | Slice 4. | OPEN | Slice 4. Baseline note: `tw_create` already refuses on main (`Cannot build lower case index: a and A collide`). |
| C-013 | Partition field names bind exactly under both settings: `DROP` / `REPLACE PARTITION FIELD` by a name that differs in case refuses `Cannot find partition field to remove: <written>` with the spec unchanged (`p2/pt_drop_CAT_present`, `pt_replace_CAT_by_name`, `pt_drop_named_lower`), and the exact name answers (`pt_drop_cat_present`). | Slice 5. | OPEN | Slice 5. |
| C-014 | Partition transform sources bind exactly under both settings on ADD, DROP and REPLACE: `bucket(4, ID)` refuses `ValidationException: Cannot find field 'ID' in struct: …` with the spec unchanged (`p2/pt_drop_bucket_ID`, `pt_replace_bucket_ID`, `pt_true_drop_bucket_ID2`). | Slice 5. | OPEN | Slice 5. |
| C-015 | `WRITE ORDERED BY` / `WRITE LOCALLY ORDERED BY` / `WRITE DISTRIBUTED BY PARTITION … ORDERED BY` bind by the session rule: under `false` any case answers with the measured source ids; under `true` a wrong-case column, nested field or transform source refuses `ValidationException: Cannot find field '<written>' in struct: …`; an unknown column refuses that text under either setting (`p2/so_*`, `p3/so_false_missing`). | Slice 5. | OPEN | Slice 5. |
| C-016 | `SET` / `DROP IDENTIFIER FIELDS` bind exactly under both settings (`p2/id_false_ID`, `id_true_ID`, `id_true_id`, `p3/id_drop_ID`). | Slice 5. | OPEN | Slice 5. |
| C-017 | One rule: every site this unit touches compares names through `repark_common::names::NameRule`; no `eq_ignore_ascii_case` / `to_ascii_lowercase` name comparison is added (grep of the unit's diff); the SQL door's `true` path turns identifier normalization off per statement only. | Last slice that lands. | OPEN | Partial 2026-09-27: S1 adds no session-rule name matching (`plan_with_repair`, the fold and the strict guard untouched; D1 changes only error propagation). The unit diff's single `to_ascii_lowercase` is `inner_scopes.rs` `planned_name`, R4's fixed DataFusion-normalization rendering (a written-vs-rendered identity check on one name: quoted value, else ASCII lower case) — not session-rule matching, which `NameRule` (S2) will own. The final grep reading must exclude R4's rendering or S2 must migrate it; flagged in the hand-back. S1b adds `eq_ignore_ascii_case` uses inside the fold's own CI machinery (`table_relation`'s CTE shadow lookup — the reformulated `shadowed` check — and `scope_fields`' relation matching), the same mechanism the fold already runs under `false`, plus `normalized_ident`'s `to_lowercase` rendering DataFusion's normalizer for `AS`/alias-list names (the `planned_name` class, not session-rule matching); flagged for the final grep. |
| C-018 | Nothing regresses: the U11-EDGE-1 V-001 … V-004 pins, the `case_bind` and `column_resolution` batteries, the U8 C-033 case-sensitive oracle keys and the ANSI door stay green; cells `E-CASE-SELECT`, `E-CASE-ALTER`, `E-CASE-INSERT-BY-NAME`, `E-CASE-MERGE`, `R-MT-CASE` replay EQUAL and `E-CASE-PARTITION-FIELD`, `E-CASE-TABLE-NAME` both-refuse. | Last slice that lands. | OPEN | Partial 2026-09-27: zero existing pins changed. `column_resolution` battery 43/43 incl. `s22b_*`; `casesens_scopes merge view` 363/363; facade sweep 944 passed exit 0 (`test_u11_edge_case_select` incl. V-001…V-004, `test_ice_views_1` incl. `test_nested_view_depth_guard`, `test_ice_mixed_case_1`, `test_ice_write_sql_1`, `test_select_naming`, `test_case_insensitive_conform`, `test_ice_views_2_describe`, `test_perf_facade_logical_names`, `test_merge_into`, `test_u11_edge_partition_field`). Scoreboard replay 2026-09-27 (`target/casesens-1/replay-s1.json`): all 8 cells byte-equal to Spark's recorded `obs`/status, matrix outcomes unchanged (6 EQUAL, `E-CASE-PARTITION-FIELD` / `E-CASE-TABLE-NAME` both-refuse). S1b 2026-09-27: `repark-core` lib 826/826 (`column_resolution` 43/43 incl. `s22b_*`), `repark-spark` lib 2406/2406, facade 194 passed (`test_casesens_1` incl. the S1b legs, `test_ice_views_1` incl. `test_nested_view_depth_guard`, `test_ice_mixed_case_1`, `test_select_naming`); the `after-s1b` probe changes exactly the four target keys to EQUAL. |

## Mutation record (2026-09-27)

Each line was broken, the named tests ran red, and the file was restored byte-identical.

| # | Mutation | Red |
|---|---|---|
| M1 | `respell_inner_scopes` returns `false`. | `inner_scopes.rs` `respells_derived_and_cte_items_written_in_another_case`, `quotes_an_upper_case_column_alias_list` red; `casesens_scopes.rs` `derived_and_cte_projections_keep_the_written_spelling` red; restored green. |
| M2 | `fold_query_text` not called (`merge_fragments.rs` keeps the inner text). | `casesens_scopes.rs` `merge_with_a_derived_source_spelled_in_another_case` red; restored green. |
| M3 | Slice 2: normalization left on in `plan_case_sensitive`. | Pending slice 2. |
| M13 | S1b (the S1b order's "M3"): `query_outputs` returns `None` and `InjectAliases` is skipped. | `casesens_scopes.rs` `cte_outer_reference_in_another_case_binds` and `catalog_view_body_keeps_its_spelling` red with the recorded refusals; restored byte-identical, green again. |
| M4 | Slice 2: normalization off for `false` too. | Pending slice 2. |
| M5 | Slice 3: `bind_names` ignores the rule. | Pending slice 3. |
| M6 | Slice 3: `written_column` folds. | Pending slice 3. |
| M7 | Slice 4: `respell_case_twins` returns `false`. | Pending slice 4. |
| M8 | Slice 4: `folded_duplicate` returns `None`. | Pending slice 4. |
| M9 | Slice 5: `resolve_field_name` back to `eq_ignore_ascii_case`. | Pending slice 5. |
| M10 | Slice 5: the transform-source pre-check removed. | Pending slice 5. |
| M11 | Slice 5: `resolve_sort_field` ignores the rule. | Pending slice 5. |
| M12 | Slice 5: `struct_child` back to ignore-case. | Pending slice 5. |
| M14 | Fold round 2026-09-27: the twice-written counts guard `> 1` changed to `> 2`. | `inner_scopes.rs` `leaves_a_spelling_written_twice_in_another_case` red; restored green. |
| M15 | Fold round 2026-09-27: the scope-count length check removed (zip to the shorter list). | `inner_scopes.rs` `refuses_unequal_scope_walks` red; restored green. |
| M16 | Fold round 2026-09-27: `planned_name` always lower-cases. | `inner_scopes.rs` `keeps_a_quoted_value` red; restored green. |
| M17 | Fold round 2026-09-27: the unquoted upper-case alias arm removed. | `inner_scopes.rs` `respells_derived_and_cte_items_written_in_another_case` and `casesens_scopes.rs` `derived_and_cte_projections_keep_the_written_spelling` red; restored green. |
| M18 | Fold round 2026-09-27: the `Subquery` arm removed from `is_expression_subquery`. | `inner_scopes.rs` `leaves_expression_subqueries_alone` red; restored green. |

## Tests rewritten

None in slice 1: every existing pin keeps its answer (sweep above).

## Residues

| # | Residue |
|---|---|
| R-CS1-1 | **CLOSED 2026-09-27** (S1b commit `fix(casesens-1): CTE outputs and catalog view bodies bind a reference written in another case (S1b)`): `p1/r5_cte_outer` (`WITH c AS (SELECT id, Data FROM sc.ns.t) SELECT ID, DATA FROM c`) now answers Spark's columns `ID`, `DATA` rows `[[1, "a"], [2, "b"]]` — `Level` carries the CTE's syntactic outputs so the outer reference binds across case; EQUAL on the `after-s1b` probe, pinned by `cte_outer_reference_in_another_case_binds` and the facade nested leg. Prior record (dated 2026-09-27, dropped from C-002 per the orchestrator ruling): RePark refused `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` name `data`, [`id`, `Data`], 42703 — the refusal came from the repair loop in `plan_with_repair`, which S1's success-path respell could not reach. |
