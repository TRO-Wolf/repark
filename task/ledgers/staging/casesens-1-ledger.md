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

## Slice 2 (2026-09-27, this round; `spark_name_rule` and `cs_temp_view_upper` blocked on hand-back Q1 + Q2)

S2-1 new `crates/repark-common/src/names.rs` (`NameRule { Exact, IgnoreCase }`
with `from_case_sensitive` / `matches` / `lookup` → `NameHit::{One, Many,
CaseOnly, None}`, inline table test over `["id", "Data", "ID"]` under both
rules; `pub mod names;` in `lib.rs`, 92 → 93). `spark_name_rule` is NOT
placed: `repark-functions` carries no `repark-common` edge (the DAG table has
no such row; the crate is dependency-isolated) and both the `Cargo.toml` edit
and a new DAG edge are outside this lane's authority (hand-back Q1). Nothing
in S2 calls it (S2-2 plans normalization-off; S2-3 matches
`NameRule::Exact` literally); its only WO consumer is S5's
`alter_write_order.rs`. S3's `frame_rule` will hit the same wall
(`repark-python` → `repark-common` is DEV-ONLY): flagged in the hand-back.

S2-2 `plan_case_sensitive` plans on a cloned state with
`enable_ident_normalization = false` (the guard stays first);
`stamp_unresolved_column` renders a DataFusion-reported qualified miss as its
backticked parts through the existing `quoted` / `reference_parts` helpers
(`column_resolution.rs` 931 → 936, inside the unit's +30 budget). S2-3 new
`crates/repark-spark/src/merge_fragments/exact.rs` (`check_fragment_exact`,
R6 byte-exact on `p1/cs_merge_on`; `check_identity_exact` shares the walker
with bare candidates for the identity-DML selection);
`rewrite_merge_fragments` takes a `NameRule` over the same seven seats; the
derived-source probe plans normalization-off under `true` so scope fields
keep their written case. `spark_ast.rs` `try_execute_identity_dml` checks the
selection and SET values under exact (adaptation the WO did not foresee: the
WO's "statement path" does not cover plain UPDATE/DELETE — they commit
through the owned identity route whose `ctx.sql` scan folds `WHERE ID`, so
without the check `cs_update_where` / `cs_delete_where` answer; SET targets
stay with `validate_update_assignments`, which already reads the flag).

Pins: `casesens_true.rs` (6 tests; `enable_case_sensitive` moved from
`create_typed_partition.rs` to `common.rs`),
`test_casesens_1.py::test_s2_sql_door_is_exact_under_case_sensitive` (28
legs) + `::test_s2_default_session_unchanged`. `p3/cs_temp_view_upper` stays
unpinned (R-CS1-8, hand-back Q2): the temp-home probe folds before planning
and every placement for its fix breaks a hard rule. Registry
`E-CASE-SENSITIVE-SQL` added; `E-CASE-SELECT` points at it.

Baseline vs inferred (S2 observations, 2026-09-27): the single-table
true-mode shapes already refuse on main via the strict guard (`cs_sel_ID`,
`cs_where_ID`, `cs_group_ID`, `cs_order_ID`, `cs_rel_alias_upper`), against
N-1's "answers (inferred, not run)"; only the join / subquery / CTE / DML
shapes flip from answer to refusal. `SELECT Data` answers (the inferred
refusal is gone). `cs_insert_cols` refused on main naming `data`; it now
refuses naming `ID` (Spark's text byte-exact).

## Clauses

| Clause | Statement | Proof obligation | Verdict | Evidence |
|---|---|---|---|---|
| C-001 | Under `caseSensitive=false` a derived table's, a CTE body's and a nested derived table's projection keeps the written spelling of each plain reference and unquoted alias, and a derived table's column-alias list keeps its spelling: `p1/r5_subq_inner_ID`, `r5_cte_ID`, `r5_cte_mixed`, `r5_union_subq`, `p3/n_nested2`, `n_join_derived`, `n_col_alias_list`, `n_df_sql_subq` answer Spark's recorded names and rows. | Rust door pins plus the facade replay assert names and rows per key; M1 breaks the pass. | PROVEN | `casesens_scopes.rs` `derived_and_cte_projections_keep_the_written_spelling`, `column_alias_list_keeps_its_spelling`; `inner_scopes.rs` unit tests; `test_casesens_1.py` `test_s1_nested_scopes_keep_the_written_spelling`; M1. After-S1 probe: the four p1 keys EQUAL. Fold round 2026-09-27: `leaves_a_spelling_written_twice_in_another_case` (M14), `refuses_unequal_scope_walks` (M15), `keeps_a_quoted_value` (M16), the `SELECT 1 AS ID` alias fixtures in `respells_derived_and_cte_items_written_in_another_case` and `derived_and_cte_projections_keep_the_written_spelling` (M17; the leg asserts only RePark's `ID` — no probe key records an expression-alias output name, unmeasured). |
| C-002 | The nested shapes Spark answers that RePark already answers stay equal, and expression subqueries are not respelled: `p1/r5_subq_ID`, `r5_subq_both`, `r5_subq_alias_qual`, `r5_in_subq`, `r5_temp_view_star`, `r5_temp_view_lower`, `p3/n_group_having`, `n_cte_star_outer_upper`, `n_exists`. | Same doors; the pin fails a pass that respells expression subqueries or breaks an outer reference. | PROVEN (clause rewritten 2026-09-27 per orchestrator ruling: `p1/r5_cte_outer` moved to R-CS1-1; regained 2026-09-27 under S1b, EQUAL) | `casesens_scopes.rs` `already_equal_nested_shapes_stay` (`r5_cte_outer` re-added under S1b); `test_casesens_1.py` nested legs; D1 keeps `r5_subq_both` EQUAL. Fold round 2026-09-27: the scalar-subquery fixture in `inner_scopes.rs` `leaves_expression_subqueries_alone` (M18). |
| C-003 | A catalog view whose body spells columns in another case answers Spark's names and rows: `p1/r5_view_star`, `r5_view_lower`, `r5_view_describe`, `r5_view2_upper`. | Same doors once the S1-3 ruling lands. | PROVEN (2026-09-27, S1b) | `casesens_scopes.rs` `catalog_view_body_keeps_its_spelling` (names and rows for `r5_view_star`, `r5_view_lower`, `r5_view2_upper`; `r5_view_describe` unchanged); `test_casesens_1.py` `test_s1_catalog_view_keeps_its_spelling` over the `vc` oracle keys; probe `after-s1b` all four EQUAL (`describe` stays). S1-3 record 2026-09-27 kept: stored text `CREATE VIEW ice.sales.v (ID, DATA) … AS SELECT ID, DATA FROM ice.sales.t`; stored columns `["ID", "DATA"]`; the SQL `execute_view_body_query` plans is `SELECT * FROM (SELECT ID, DATA FROM ice.sales.t) AS _repark_view("ID", "DATA")` — S1b's `InjectAliases` moves the list into the body as `AS` items so DataFusion's normalizing `col(field.name())` path never runs. |
| C-004 | A MERGE whose derived source or clauses spell columns in another case answers as Spark: `p1/r30_merge_derived`, `r30_merge_upper_set` (tables after each as recorded), and UPDATE / DELETE / INSERT column lists spelled in another case answer (`dml_*`). | Rust door pins plus the facade merge replay assert the tables after each statement; M2 breaks the fold. | PROVEN | `casesens_scopes.rs` `merge_with_a_derived_source_spelled_in_another_case` is the red-on-main proof (baseline fails `No field named data`); `dml_spelled_in_another_case_answers` stays as a regression pin and is not cited as red-on-main evidence; `test_casesens_1.py` `test_s1_merge_derived_source`; M2. Probe `r30_after` / `dml_after` still differ downstream of S2 scope: the `true`-mode `cs_*` DML steps mutate `u` on RePark (C-008) while Spark refuses them, so the probe's `u` is `(1,m)` at `r30` time; the hermetic pins assert Spark's recorded tables. |
| C-005 | Under `caseSensitive=true` a reference whose exact spelling is not held refuses `UNRESOLVED_COLUMN.WITH_SUGGESTION` (42703) naming the reference as written, qualified as written, in every scope (select list, join condition, derived table, CTE, WHERE, ORDER BY, GROUP BY, alias, backticks), and a wrong-case struct field refuses (`p1/cs_struct_A`: Spark `FIELD_NOT_FOUND` 42704). | Slice 2. | PROVEN | `casesens_true.rs` `wrong_case_refuses_in_every_scope_under_case_sensitive` (11 legs + struct; join legs name `` `a`.`ID` `` per R3; M3 red) + `test_casesens_1.py` `test_s2_sql_door_is_exact_under_case_sensitive`. Rendering residues where RePark's text differs from Spark's for a registered reason: R-CS1-2 (`cs_order_ID` whole-table list), R-CS1-3 (`` `T.id` `` vs `` `T`.`id` ``), R-CS1-4 (`cs_struct_A` raw text vs `FIELD_NOT_FOUND`), R-CS1-5 (`cs_order_alias` wide list); join candidates stay bare (WO out of scope). Final fold (2026-09-28): `same_session_toggle_true_false_true_keeps_folding` (true refuses, false folds, true refuses again in one session — the per-statement clone never leaks). |
| C-006 | Under `caseSensitive=true` an exact mixed-case spelling answers and is named as written (`p1/cs_sel_Data`, `p3/cs_backtick_Data`, `cs_temp_view_exact`, `p1/cs_tw_ID_id`, `cs_tw_lit_ref`, `p3/cs_star`, `cs_func_upper`), and function names stay case-insensitive. | Slice 2. | PROVEN | `casesens_true.rs` `exact_mixed_case_answers_under_case_sensitive` (7 legs; star + exact-insert legs read the struct-less table; M3 red on a fold) + the facade S2 legs. `cs_func_upper` binds exactly with Spark's rows; its output name keeps DataFusion's qualification (R-CS1-6, OD-1 expression-naming follow-up). Unquoted exact `userId` answers (the ID-1 declared refusal is overturned; `tests.rs` leg rewritten, MC-SEL-01/MC-UPD-01/MC-MRG-01 flipped to the recorded `true` oracle rows — see Tests rewritten). |
| C-007 | Under `caseSensitive=true` relation aliases, CTE names and temp-view names match exactly (`p1/cs_table_T`, `cs_ns_NS`, `p3/cs_cte_name`, `cs_temp_view_upper`, `cs_rel_alias_upper` refuse); under `false` they fold (`p3/rel_alias_upper`, `cte_name_upper`, `temp_view_upper` answer); metadata-table suffixes fold under both (`p4/mt_upper_true`, `mt_upper_false`). | Slice 2. | PROVEN | S2 proved every leg except the temp-view-name read; S3 lands it (Q2): `resolve_temp_view_home_ref_exact` + the `temp_view_names` door module refuse `SELECT * FROM TV` over registered `tv` under `true` (unstamped `not found`, R-CS1-7 class; R-CS1-8 closed), while `tv` under `true` and `TV` under `false` answer. Pinned by the facade S3 temp-view legs. |
| C-008 | Under `caseSensitive=true` INSERT column lists, UPDATE, DELETE and MERGE fragments with a wrong-case column refuse and leave the table unchanged (`p1/cs_insert_cols`, `cs_update_where`, `cs_delete_where`, `cs_merge_on` with Spark's full text); the exact column list answers (`p4/ins_exact_true`). | Slice 2. | PROVEN | `casesens_true.rs` `dml_wrong_case_refuses_and_leaves_the_table_under_case_sensitive` (INSERT/UPDATE/DELETE byte-exact, MERGE R6 byte-exact, `u` snapshotted unchanged after each, exact INSERT answers) + the facade S2 DML legs with the same unchanged checks. UPDATE/DELETE refuse through the new identity-DML exact check (the WO's "statement path" does not cover them). Final fold (2026-09-28): `exact_fragment_identifiers_quote_and_plan_under_case_sensitive` (exact MERGE/identity hits backtick-quoted, wrong-case hits refuse, a quoted no-op MERGE plans with `u` unchanged), `missing_fragment_identifiers_pass_through_to_the_planner`, `update_set_targets_match_exactly_under_case_sensitive` (wrong-case SET refuses UNRESOLVED naming `` `DATA` `` with `u` unchanged, exact SET answers and lands; unmeasured — no oracle key covers a wrong-case SET target). |
| C-009 | Under `caseSensitive=true` the DataFrame door's `F.col`, string names, `filter`, `orderBy`, `groupBy`, `selectExpr`, string `filter`, `describe`, `join(on=)`, `unionByName` and `drop` resolve exactly with Spark's measured refusal texts (`p1/cs_df_*` listed in S3, `p4/df_window_ID_true`); under `false` `F.col("ID")`, `select("ID")`, `F.col("ID") + 1` and a window ordered by `F.col("ID")` answer and are named as written (`p4/df_col_ID_false`, `df_select_str_ID_false`, `df_expr_ID_false`, `df_window_ID_false`). | Slice 3. | PROVEN (partial: the Rust-reached legs; the six core.py-pre-bound legs are residue R-CS1-10, CASESENS-2) | S3 proves: `filter(F.col)` (`cs_df_filter_ID`, R12-only), `describe` (`cs_df_describe_ID`, R12-only), `join(on=)` (`cs_df_join_on_ID`, byte-exact), `unionByName` (`cs_df_unionByName`, message byte-exact, R-CS1-9), `drop` no-op (`cs_df_drop_ID`), exact-hit select (`cs_df_select_Data`), window (`df_window_ID_true`), `selectExpr` (R12-only, via the SQL door), and all four `false` name legs. Descoped to CASESENS-2 per the S3 ruling (R-CS1-10, unpinned): select string, select `F.col`, select lowercase-data, `orderBy`, `groupBy`, string `filter`. Final fold (2026-09-28): `door_parity_tests.rs` `exact_predicate_refuses_a_case_only_match` pins `parse_canonical_predicate_exact` (`ID > 1` refuses UNRESOLVED naming `` `ID` ``, `Data = 'a'` binds — red if the predicate folds). |
| C-010 | `describe` / `summary` columns bind by the session rule in Rust and name the output with the written spelling (`p1/r7_describe_ID`, `p3/n_df_describe_two`). | Slice 3. | PROVEN | S3 (R8): explicit columns resolve through `resolve_frame_names` — `r7_describe_ID` and `n_df_describe_two` answer with written-spelling labels under `false`, `cs_df_describe_ID` refuses `UNRESOLVED_COLUMN.WITH_SUGGESTION` under `true` (R12-only). Pinned by the facade S3 describe legs. Final fold (2026-09-28): `describe_with_twin_columns_refuses_ambiguous` (the `Many` branch) and `describe_with_missing_column_refuses_unresolved` (the `None` branch under both rules) pin the remaining `resolve_written_names` arms. |
| C-011 | Under `caseSensitive=false` case-twin outputs answer with both spellings (`p1/tw_ID_id`, `tw_Id_ID`, `tw_star_ID`, `tw_lit`, `p3/tw_subq_star`); a reference into the twins refuses `AMBIGUOUS_REFERENCE` (42704) (`p1/tw_lit_ref`); exact duplicates keep ID-3's refusal. | Slice 4. | PROVEN | S4 (backfilled in the S5 round): `casesens_twins.rs` `case_twin_outputs_answer_with_both_spellings` (5 legs, names and rows), `reference_to_a_case_twin_is_ambiguous` (`tw_lit_ref` 42704, the same shape answers under `true`), `exact_duplicates_still_refuse` (ID-3 kept); `test_casesens_1.py` `test_s4_case_twins`; M7. `p3/tw_order_by` stays a dated residue (R-CS1-11). |
| C-012 | Under `caseSensitive=false` `CREATE TABLE`, CTAS, `CREATE VIEW` and `CREATE TEMPORARY VIEW` with case-twin columns refuse `[COLUMN_ALREADY_EXISTS] The column `a` already exists. Choose another name or rename the existing column. SQLSTATE: 42711` and create nothing (`p1/tw_ctas`, `tw_create`, `p3/tw_view`, `tw_temp_view`, `tw_temp_view_read`); a positional INSERT from twins answers (`p3/tw_insert_into_run`, `tw_insert_after`). | Slice 4. | PROVEN | S4 (backfilled in the S5 round): `creating_case_twin_columns_refuses` (all four creations refuse 42711 with nothing created) and `positional_insert_from_case_twins_answers`; facade `test_s4_case_twins` over the same keys plus `tw_temp_view_read`, `tw_df_write_create` (routes through CTAS into the same refusal); M8. `tw_create` refused on main with the lower-case-index text (baseline note); it now refuses Spark's `COLUMN_ALREADY_EXISTS`. Final fold (2026-09-28): the true-mode twin-creation probe contradicts R10's legality premise — CREATE TABLE, CTAS and CREATE VIEW with `a`/`A` under `true` refuse `DataInvalid => Cannot build lower case index: a and A collide` (the fork's `Schema::build` parity check, reached past the session-gated twin check); only CREATE TEMPORARY VIEW succeeds. No success pins landed; the ruling is open (R-CS1-12). |
| C-013 | Partition field names bind exactly under both settings: `DROP` / `REPLACE PARTITION FIELD` by a name that differs in case refuses `Cannot find partition field to remove: <written>` with the spec unchanged (`p2/pt_drop_CAT_present`, `pt_replace_CAT_by_name`, `pt_drop_named_lower`), and the exact name answers (`pt_drop_cat_present`). | Slice 5. | PROVEN | `partition_spec/tests.rs` `drop_and_replace_by_name_are_exact` (`CAT` against `cat`, `kat` against `Kat` refuse with the spec unchanged; the exact names drop and replace) and the rewritten `alter.rs` `partition_spec_drop_replace_field_name_is_exact` (OD-3); `test_casesens_1.py` `test_s5_iceberg_ddl_binds_exactly` legs plus the `p1` / `p2` / `p4` metas; M9. `pt_drop_field_upper` refuses the same text through the same check. Final fold (2026-09-28): the missed SQL-door pin `tests/alter.rs` `alter_partition_transforms_drop_by_transform_and_replace_required_refuse` rewritten to the same refusal (see Tests rewritten). |
| C-014 | Partition transform sources bind exactly under both settings on ADD, DROP and REPLACE: `bucket(4, ID)` refuses `ValidationException: Cannot find field 'ID' in struct: …` with the spec unchanged (`p2/pt_drop_bucket_ID`, `pt_replace_bucket_ID`, `pt_true_drop_bucket_ID2`). | Slice 5. | PROVEN | `transform_sources_are_exact_on_drop_and_replace` (DROP and REPLACE refuse with the byte-exact struct text, spec unchanged; the exact source drops); facade S5 legs over the three keys plus the `p3` / `o2` metas; M10. |
| C-015 | `WRITE ORDERED BY` / `WRITE LOCALLY ORDERED BY` / `WRITE DISTRIBUTED BY PARTITION … ORDERED BY` bind by the session rule: under `false` any case answers with the measured source ids; under `true` a wrong-case column, nested field or transform source refuses `ValidationException: Cannot find field '<written>' in struct: …`; an unknown column refuses that text under either setting (`p2/so_*`, `p3/so_false_missing`). | Slice 5. | PROVEN | `casesens_ddl.rs` `write_order_follows_the_case_rule` (`false` answers with source ids 2, 4, 1, 1; `true` refusals commit no order; exact `cat` answers; `nope` refuses); facade S5 `so_*` legs plus the `o1` / `o2` metas; the two rewritten transform legs; M11. |
| C-016 | `SET` / `DROP IDENTIFIER FIELDS` bind exactly under both settings (`p2/id_false_ID`, `id_true_ID`, `id_true_id`, `p3/id_drop_ID`). | Slice 5. | PROVEN | `casesens_ddl.rs` `identifier_fields_are_exact_under_both_settings` (wrong-case SET and DROP refuse Iceberg's recorded texts with the set unchanged; exact `id` sets identifier `[1]`); facade S5 `id_*` legs plus the `o1` / `o2` metas (all three refusal legs replay EQUAL); M12. |
| C-017 | One rule: every site this unit touches compares names through `repark_common::names::NameRule`; no `eq_ignore_ascii_case` / `to_ascii_lowercase` name comparison is added (grep of the unit's diff); the SQL door's `true` path turns identifier normalization off per statement only. | Last slice that lands. | PROVEN | Final grep reading 2026-09-27 (`git diff origin/main`, S5 head): the unit diff adds 24 comparison lines, all classified — 4 in `names.rs` (the rule's own `IgnoreCase` implementation and `folded_duplicate`); the S1/S1b fixed false-path fold machinery (`inner_scopes` R4 rendering, `fold` / `scope_fields` CTE and derived lookups); the S4 twin-detection machinery (`twins.rs` / `ambiguity.rs` folded-equality detection, false-only by construction). Every session-rule dispatch site (S2 `exact.rs`, S3 `bind_names` and the frame functions, S5 `resolve_known_field`, `bound_sources`, `resolve_field_by_transform`, `resolve_sort_field`, `struct_child` / `field_child`) compares only through `NameRule::matches` / `lookup`. S5 adds 0 and removes 8. The SQL `true` path is the per-statement normalization-off clone in `plan_case_sensitive` only (M3/M4). |
| C-018 | Nothing regresses: the U11-EDGE-1 V-001 … V-004 pins, the `case_bind` and `column_resolution` batteries, the U8 C-033 case-sensitive oracle keys and the ANSI door stay green; cells `E-CASE-SELECT`, `E-CASE-ALTER`, `E-CASE-INSERT-BY-NAME`, `E-CASE-MERGE`, `R-MT-CASE` replay EQUAL and `E-CASE-PARTITION-FIELD`, `E-CASE-TABLE-NAME` both-refuse. | Last slice that lands. | PROVEN | S5 2026-09-27: facade sweep 1063 passed + 47 skipped, zero facade pins changed (V-001…V-004, U8 C-033 keys, `test_nested_view_depth_guard` in the sweep); Rust `repark-iceberg --lib` 35/35, `repark-spark --lib` 83/83 (new pins plus the rewritten transform legs), `repark-sql --lib` 57/57 partition, `cross_door` 23/23. Probes `after-s5`: p1 47/84 (= s4), p2 17/63 (+2 EQUAL: `id_false_ID`, `id_true_ID`), p3 22/34 (+1: `id_drop_ID`), p4 9/15 (= s4); every changed key is an S5 key except the `tw_order_by` rendering flake (R-CS1-11, nondeterministic on one tree). Replay `replay-s5.json`: 7/7 cells byte-identical to `replay-s2.json`, `P-CALL-UPPERCASE` ok byte-equal to Spark. Whole-unit existing-pin changes, all Spark-wins rewrites: S2 5 (ID-1), S4 1 (twin-view creation text), S5 3 (the `alter.rs` pin, 2 transform legs). Final fold (2026-09-28, correcting the S5 record): the SQL-door `tests/alter.rs` REG DROP leg asserted the pre-OD-3 case-insensitive DROP and went red at the S5 head; rewritten to Spark's refusal (see Tests rewritten), so the count grows by one — S2 5, S4 1, S5 3, final fold 1. |

## Mutation record (2026-09-27)

Each line was broken, the named tests ran red, and the file was restored byte-identical.

| # | Mutation | Red |
|---|---|---|
| M1 | `respell_inner_scopes` returns `false`. | `inner_scopes.rs` `respells_derived_and_cte_items_written_in_another_case`, `quotes_an_upper_case_column_alias_list` red; `casesens_scopes.rs` `derived_and_cte_projections_keep_the_written_spelling` red; restored green. |
| M2 | `fold_query_text` not called (`merge_fragments.rs` keeps the inner text). | `casesens_scopes.rs` `merge_with_a_derived_source_spelled_in_another_case` red; restored green. |
| M3 | Slice 2: normalization left on in `plan_case_sensitive`. | 2026-09-27: the four true-mode `casesens_true.rs` tests red, both false-mode tests green; restored identical, green again. |
| M13 | S1b (the S1b order's "M3"): `query_outputs` returns `None` and `InjectAliases` is skipped. | `casesens_scopes.rs` `cte_outer_reference_in_another_case_binds` and `catalog_view_body_keeps_its_spelling` red with the recorded refusals; restored byte-identical, green again. |
| M4 | Slice 2: normalization off for `false` too. | 2026-09-27: only `default_session_keeps_folding` red (the metadata false leg needs no fold, so it stays green); restored identical, green again. |
| M5 | Slice 3: `bind_names` ignores the rule. | 2026-09-27 (S3 round): `exact_rule_refuses_a_case_only_match` red; frame functions ignoring the rule redden `frame_functions_follow_the_rule` (M5b); restored, green. |
| M6 | Slice 3: `written_column` folds. | 2026-09-27 (S3 round): `column_keeps_the_written_spelling` red; restored, green. M6 bites only through the Rust spelling pin: the facade pre-binds names as written with or without the fold, so the WO's facade tripwire premise does not hold in this tree. |
| M7 | Slice 4: `respell_case_twins` returns `false`. | 2026-09-27 (run in the S5 round for the S4 backfill): `casesens_twins.rs` `case_twin_outputs_answer_with_both_spellings` red; restored byte-identical, green again. |
| M8 | Slice 4: `folded_duplicate` returns `None`. | 2026-09-27 (run in the S5 round for the S4 backfill): `casesens_twins.rs` `creating_case_twin_columns_refuses` red; restored byte-identical, green again. |
| M9 | Slice 5: `resolve_field_name` back to `eq_ignore_ascii_case`. | 2026-09-27: `resolve_known_field` through `IgnoreCase` reddens `drop_and_replace_by_name_are_exact`; restored, green. |
| M10 | Slice 5: the transform-source pre-check removed. | 2026-09-27: `bound_sources` without the DROP and old-source arms reddens `transform_sources_are_exact_on_drop_and_replace` (the DROP leg answers; the REPLACE leg still refuses through `resolve_field_by_transform`); restored, green. |
| M11 | Slice 5: `resolve_sort_field` ignores the rule. | 2026-09-27: the rule shadowed to `IgnoreCase` reddens `write_order_follows_the_case_rule` (the `true` legs answer); restored, green. |
| M12 | Slice 5: `struct_child` back to ignore-case. | 2026-09-27: `IgnoreCase` in `struct_child` reddens `identifier_fields_are_exact_under_both_settings`; restored, green. |
| M14 | Fold round 2026-09-27: the twice-written counts guard `> 1` changed to `> 2`. | `inner_scopes.rs` `leaves_a_spelling_written_twice_in_another_case` red; restored green. |
| M15 | Fold round 2026-09-27: the scope-count length check removed (zip to the shorter list). | `inner_scopes.rs` `refuses_unequal_scope_walks` red; restored green. |
| M16 | Fold round 2026-09-27: `planned_name` always lower-cases. | `inner_scopes.rs` `keeps_a_quoted_value` red; restored green. |
| M17 | Fold round 2026-09-27: the unquoted upper-case alias arm removed. | `inner_scopes.rs` `respells_derived_and_cte_items_written_in_another_case` and `casesens_scopes.rs` `derived_and_cte_projections_keep_the_written_spelling` red; restored green. |
| M18 | Fold round 2026-09-27: the `Subquery` arm removed from `is_expression_subquery`. | `inner_scopes.rs` `leaves_expression_subqueries_alone` red; restored green. |

## Tests rewritten

None in slice 1: every existing pin keeps its answer (sweep above).

Slice 2 (5 pins, one behavior: the ID-1 declared refusal converges per C-006 —
Spark answers unquoted exact-case under `true`):

- `crates/repark-core/src/column_resolution/tests.rs::sensitive_session_refuses_folded_names_and_keeps_backticks`
  (net-zero lines, file stays 930): `SELECT userId FROM t` under `true`
  refused `FieldNotFound userid` before, answers `["userId"]` now (Spark:
  `p1/cs_sel_Data` answers `Data`).
- `python/repark/tests/test_ice_mixed_case_1.py::test_sql_door_unquoted_exact_case_refuses_case_sensitive`
  → renamed `..._succeeds_case_sensitive`, asserting the recorded `true`
  oracle rows for MC-SEL-01, MC-UPD-01 and MC-MRG-01 (each refused before;
  the MERGE executes and its trailing SELECT matches Spark's rows now).
- `python/repark/tests/test_ice_mixed_case_1.py::test_sql_set_statement_drives_case_sensitive`:
  the `SELECT userId … ORDER BY userId` leg answers `[{userId: 1}, {userId:
  2}]` instead of raising (the wrong-case `SELECT userid … WHERE USERID`
  leg still raises; the backticked twin is unchanged).

Slice 4 (1 pin): `python/repark/tests/test_ice_mixed_case_1.py::test_star_over_a_case_twin_frame_answers_both_columns_declared`
— the N03 twin-view creation refuses Spark's `[COLUMN_ALREADY_EXISTS]` text (oracle
`p3/tw_view`) instead of `Projections require unique expression names`.

Slice 5 (3 pins, OD-3 adopted, Spark wins):
- `crates/repark-iceberg/src/write/alter.rs::partition_spec_drop_replace_field_name_case_insensitive`
  → renamed `..._is_exact`, asserting the refusal and the unchanged spec
  (`alter.rs` 1607 → 1606, EXCEPTIONS row lowered in the same commit).
- `crates/repark-spark/src/tests/alter_write_order_transform.rs::write_ordered_by_transform_refusals_match_spark_and_commit_nothing`
  (`bucket(4, nope)` leg) and `::hex_quoted_and_string_tokens_render_as_spark_does`
  (`bucket(4, 0x4)` leg): the unknown-field text is now Spark's
  `ValidationException: Cannot find field '<written>' in struct: …` (the shared
  untouched-order tail moved to `assert_order_untouched` for the line ceiling).

Final fold (2026-09-28, 1 pin, OD-3 adopted, Spark wins):
- `crates/repark-spark/src/tests/alter.rs::alter_partition_transforms_drop_by_transform_and_replace_required_refuse`
  (REG DROP leg): before, `DROP PARTITION FIELD REG` over stored `reg` succeeded
  (`.expect("DROP PARTITION FIELD name must be case-insensitive at SQL")`); after, it
  refuses `Cannot find partition field to remove: REG` with the one-field spec unchanged
  (Spark key `p2/pt_drop_CAT_present`: `Cannot find partition field to remove: CAT`). Seven
  setup legs compress `execute` to `run` (`tests/alter.rs` 1182 → 1181, EXCEPTIONS row
  lowered in the same commit).

## Residues

| # | Residue |
|---|---|
| R-CS1-1 | **CLOSED 2026-09-27** (S1b commit `fix(casesens-1): CTE outputs and catalog view bodies bind a reference written in another case (S1b)`): `p1/r5_cte_outer` (`WITH c AS (SELECT id, Data FROM sc.ns.t) SELECT ID, DATA FROM c`) now answers Spark's columns `ID`, `DATA` rows `[[1, "a"], [2, "b"]]` — `Level` carries the CTE's syntactic outputs so the outer reference binds across case; EQUAL on the `after-s1b` probe, pinned by `cte_outer_reference_in_another_case_binds` and the facade nested leg. Prior record (dated 2026-09-27, dropped from C-002 per the orchestrator ruling): RePark refused `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` name `data`, [`id`, `Data`], 42703 — the refusal came from the repair loop in `plan_with_repair`, which S1's success-path respell could not reach. |
| R-CS1-2 | **OPEN 2026-09-27** (S2): `p1/cs_order_ID` (`SELECT id FROM t ORDER BY ID` under `true`): Spark narrows the candidates to the ORDER BY scope (``[`id`]``); RePark's single-table guard lists the whole table (``[`id`, `Data`, `s`]``). Class, condition, SQLSTATE, head and written name match; the pin asserts RePark's set. Home: UNRESOLVED-TEXT. |
| R-CS1-3 | **OPEN 2026-09-27** (S2): `p3/cs_rel_alias_upper` (`SELECT T.id FROM t t` under `true`): Spark names `` `T`.`id` ``; RePark's single-table guard (kept as is per R2) names `` `T.id` `` (one backtick pair). The planner stamp renders Spark's form (R3) but the guard fires first on single-table shapes. Home: UNRESOLVED-TEXT. |
| R-CS1-4 | **OPEN 2026-09-27** (S2): `p1/cs_struct_A` (`SELECT s.A FROM t` under `true`): Spark stamps `[FIELD_NOT_FOUND] No such struct field `A` in `a`. SQLSTATE: 42704`; RePark refuses with DataFusion's raw `Field A not found in struct` (no condition row exists for it). The refusal is pinned; both texts recorded per the WO's `cs_struct_A` recipe. Home: UNRESOLVED-TEXT (missing condition row). |
| R-CS1-5 | **OPEN 2026-09-27** (S2): `p3/cs_order_alias` (`SELECT id AS X FROM t ORDER BY x` under `true`): Spark narrows the candidates to the output (``[`X`]``); DataFusion reports the output plus the inputs, so RePark lists ``[`X`, `id`, `Data`]`` (``[`X`, `id`, `Data`, `s`]`` over the struct table). Class, condition, SQLSTATE, head and written name match. Home: UNRESOLVED-TEXT. |
| R-CS1-6 | **OPEN 2026-09-27** (S2): `p3/cs_func_upper` binds exactly with Spark's rows but names the output `upper(sc.ns.t.Data)` (DataFusion's qualified rendering, pre-existing under `false` too) where Spark names it `upper(Data)`. The pin asserts rows plus RePark's name. Home: the OD-1 expression-naming follow-up. |
| R-CS1-7 | **OPEN 2026-09-27** (S2): `p1/cs_table_T`, `cs_ns_NS`, `p3/cs_cte_name` (and the planner-level `TV` read): Spark stamps `[TABLE_OR_VIEW_NOT_FOUND]` (42P01); RePark refuses with DataFusion's raw `table '…' not found` (CTE/temp names surface as `table 'datafusion.public.<name>' not found`). The refusal is pinned; both texts recorded per the WO's relation-name recipe. Home: UNRESOLVED-TEXT. |
| R-CS1-8 | **CLOSED 2026-09-27** (S3 lands the S2-Q2 leg): `p3/cs_temp_view_upper` now refuses `table 'spark_catalog.default.TV' not found` (R-CS1-7 class: unstamped, the refusal itself is pinned). The rule-aware probe (`resolve_temp_view_home_ref_exact` + `temp_view_names`) resolves exactly under `true` and folds under `false`. Prior record: the temp-home probe folded the name before planning, so normalization-off never saw `TV`; unpinned in the facade while the Rust planner-level leg refused. |
| R-CS1-9 | **OPEN 2026-09-27** (S3, needs an orchestrator ruling — see S3 halt note): `p1/cs_df_unionByName` refuses with the byte-exact Spark message (`Cannot resolve column name "ID" among (id).`) but RePark surfaces condition `None` where Spark reports `_LEGACY_ERROR_TEMP_1201` (Spark's internal fallback id for legacy errors). Minting that id would corrupt the byte-exact message, so per R14 the pin (when written) asserts type + message only. Home: UNRESOLVED-TEXT. |
| R-CS1-10 | **OPEN 2026-09-27** (S3 ruling: six C-009 legs descoped to CASESENS-2, unpinned): the facade pre-binds bare names in `dataframe/core.py` before Rust sees the written spelling, so under `true` RePark answers where Spark refuses `UNRESOLVED_COLUMN.WITH_SUGGESTION` (42703). `cs_df_select_ID`: Spark refuses naming `` `ID` `` ([`` `s` ``, `` `id` ``, `` `Data` ``]); RePark answers cols `[[ID, int]]` rows `[[1], [2]]`. `cs_df_select_col_ID`: same on both sides. `cs_df_select_data`: Spark refuses naming `` `data` `` ([`` `Data` ``, `` `id` ``, `` `s` ``]); RePark answers `[[data, string]]` `[[a], [b]]`. `cs_df_orderBy_ID`: Spark refuses naming `` `ID` ``; RePark answers `[[id, int]]` `[[1], [2]]`. `cs_df_groupBy_ID`: Spark refuses naming `` `ID` ``; RePark answers `[[ID, int], [count, bigint]]` `[[1, 1], [2, 1]]`. `cs_df_filter_str_ID`: Spark refuses naming `` `ID` ``; RePark answers `[[id, int]]` `[[2]]`. Home: CASESENS-2 (DataFrame-door names move to Rust; OD-1), 2026-09-27. |
| R-CS1-11 | **OPEN 2026-09-27** (S4 shape, recorded in the S5 round per the WO's `tw_order_by` recipe): `p3/tw_order_by` (`SELECT ID, id FROM t ORDER BY id` under `false`): Spark answers `ID`, `id` rows `[[1, 1], [2, 2]]`; RePark refuses `[AMBIGUOUS_REFERENCE]` through `audit_plan_for_ambiguity`, and the rendering is nondeterministic run to run on one tree (``Reference `id` … [`id`, `sc`.`ns`.`t`.`id`]`` vs ``Reference `ID` … [`ID`, `sc`.`ns`.`t`.`ID`]``, 2 and 2 over 4 trials on the S5 tree). Home: follow-up (the WO forbids changing the audit here). |
| R-CS1-12 | **OPEN 2026-09-28** (final fold): twin-column creation under `caseSensitive=true` — R10 says twins are legal under `true`, but CREATE TABLE, CTAS and CREATE VIEW with `a`/`A` refuse `DataInvalid => Cannot build lower case index: a and A collide` (the fork's `Schema::build` parity check, Java `TypeUtil.indexByLowerCaseName`; the session-gated twin check correctly skips, the fork refuses past it). Only CREATE TEMPORARY VIEW succeeds. No probe measures Spark's under-`true` creation answer, so no pin lands either way; if Spark also refuses with Java's text, RePark is equal and the refusal should be pinned. Home: owner ruling (measure Spark under-`true` creation, then pin or fix). |

## S3 round (2026-09-27, lane `xs-cs1`, landed per the S3 rulings)

- S3-1: re-ran `cs_probe1/4.py repark` before editing. No `cs_df_*` key went exact
  via S2 except the exact-hit `cs_df_select_Data` (EQUAL already); the WO's
  "may already be exact" hypothesis is false for the df door (df plans do not
  lower through `plan_with_repair`), except `cs_df_selectExpr_ID`, which lowers
  through the SQL door and already refuses with an R12-only candidate-order
  DIFF. Answer shapes at baseline: `withColumn`/`drop`/`renamed` fold,
  `join`/`unionByName` answer.
- Landed in the worktree (verified, uncommitted): R7 `written_column` +
  `PyColumn::column` (net-zero in `column/mod.rs`, stays 1012);
  `bind_names(expr, schema, rule)` + `resolve_bound_expr_with` +
  rule params on drop/join/union + `resolve_written_names`;
  `frame_rule`/`bound_column`/`bound_projection`/`filter_frame_with_sql`/
  `join_on_keys`/`union_frames`/`resolve_frame_names` in `dataframe_names.rs`;
  `dataframe.rs` delegates (976, exception row retired);
  `UNRESOLVED_USING_COLUMN_FOR_JOIN` condition (43 conditions);
  Q2 `resolve_temp_view_home_ref_exact` + new `temp_view_names` module +
  one-line facade probe swaps; R8 `statistics.py` explicit columns via
  `resolve_frame_names`.
- Verified: `repark-core --lib` 832 passed; `repark-python --lib` 82 passed;
  `repark-common` 31 passed; `cross_door` 23 passed (ANSI unchanged);
  facade `test_casesens_1` + describe + filter-rewrite 121 passed, temp views
  244 passed, df-path sweep 170 passed — zero existing pins changed.
  Clippy (gate flags), fmt, `check_rust_file_size`, `check_lib_rs` (190,
  net-zero via two import compressions), `check_lib_py`, DAG (no new edge),
  panic-ban, docstring-presence, conventions, docs-links all green.
  M5 reddens `exact_rule_refuses_a_case_only_match`; M5b (frame fns ignore
  the rule) reddens `frame_functions_follow_the_rule`; M6 reddens
  `column_keeps_the_written_spelling`. All mutations reverted (verified).
- Changed-key audit S2→S3 (halt rule 2 SATISFIED): 9 keys changed, all `true`
  keys or named S3 describe legs — `cs_df_filter_ID`, `cs_df_describe_ID`,
  `cs_df_join_on_ID` (USING text byte-exact), `cs_df_drop_ID` (no-op),
  `r7_describe_ID` + `n_df_describe_two` (answer), `cs_df_unionByName`
  (legacy text byte-exact, R-CS1-9), `df_window_ID_true` (refuses),
  `cs_temp_view_upper` (Q2: refuses `table 'spark_catalog.default.TV'
  not found`, R-CS1-7 class; `cs_temp_view_exact` and the `false` leg answer).
  `cs_df_filter_ID`/`cs_df_describe_ID`/`cs_df_selectExpr_ID` are R12-only
  DIFFs (order unreproducible).
- S3 ruling Q1 (was a halt-rule-3 contradiction): six C-009 legs —
  `cs_df_select_ID`, `cs_df_select_col_ID`, `cs_df_select_data`,
  `cs_df_orderBy_ID`, `cs_df_groupBy_ID`, `cs_df_filter_str_ID` — still answer.
  The facade pre-binds them case-insensitively in `dataframe/core.py`
  (`_resolve_getitem_column_name`, `_bind_schema_column`,
  `_rebind_stable_name_column`, `_quote_filter_idents_in_fragment`) before
  Rust sees the written name; the rebound `Alias(Column{engine}, written)` is
  byte-identical to a user alias, so no Rust step can recover the written
  spelling. The WO requires these legs (C-009 + S3 pins) yet forbids touching
  `core.py` (3973, "untouched") and defers its matcher family to CASESENS-2
  (OD-1). Proof the native side is ready: user-backticked
  `filter("`ID` > 1")` (quoter passes it through) refuses with the exact Spark
  text. Ruling (2026-09-27): Q1 descopes the six legs to CASESENS-2
  (R-CS1-10, unpinned; C-009 PROVEN-partial); Q2 accepts R-CS1-9 as a dated
  text-only residue (pin asserts class + exact message). M6 bites only through
  the Rust spelling pin: the WO's facade tripwire premise does not hold in
  this tree (pre-binding names as-written with or without the fold).
- Adaptations recorded: (1) Q1's composition reaches `repark-python` as
  `repark_core::frame_names::NameRule` (a `pub use` in `case_bind.rs`;
  `repark-common` is dev-only there, so the literal path does not resolve —
  no new edge, no Cargo change). (2) `resolve_bound_expr_with` is re-exported
  through `frame_names`, not `repark_core::` root (`session.rs` sits exactly
  at 1000 and `lib.rs` is untouched). (3) `bind_projection_expr` takes the
  rule (the WO's "two frame-function calls" omits the select path, which
  cannot refuse without it). (4) The join keep-one-key projection dedupes by
  matched-key spelling instead of lowercased field names (equivalent under
  `IgnoreCase`, exact under `Exact`; removes the old `to_ascii_lowercase`
  rather than adding a comparison). (5) `filter_sql` parses with a
  normalization-off cloned state under `Exact` and routes a `FieldNotFound`
  miss through `resolve_bound_expr_with` for the Spark refusal (a folded
  parse cannot be rebound — the written spelling is already lost).
  (6) `cs_df_select_data` (lowercase `data`) belongs in the S3 facade pin
  though the WO's pin list omits it.
- Observed, out of scope: `cs_df_getitem_ID`, `cs_df_withColumn_ID`,
  `cs_df_renamed_ID`, `df_fillna_true`, `df_dropDuplicates_true`,
  `r7_selfjoin` unchanged (still diverge as before); temp-view
  REGISTRATION still folds unquoted names under `true` (unmeasured —
  `SELECT * FROM TV` over a view registered as quoted `"TV"` would refuse
  where Spark answers; no probe covers it).

## Slice 4 (2026-09-27, landed as 70a9525e without ledger notes; backfilled in the S5 round)

S4-1 new `crates/repark-core/src/column_resolution/twins.rs` (`respell_case_twins`,
R9; `ambiguity.rs` carries the twin audit) wired before the fold loop in
`plan_with_repair`. S4-2 `repark_common::names::folded_duplicate` and
`column_already_exists` (R10), called in `create_table.rs`, `ctas.rs` and
`view_ddl/execute.rs` under `false` only. Pins: `casesens_twins.rs` (5 tests),
`test_casesens_1.py::test_s4_case_twins`. Registry: ID-3 edited (twins are not
duplicates), the `E-CASE-SELECT` twin sentence retired. One existing pin
rewritten (see Tests rewritten); the S1b `twin_cte_outputs` pin now asserts
`AMBIGUOUS_REFERENCE` instead of the unique-names refusal (unit-owned pin, same
slice family). M7/M8 run in the S5 round (this ledger). `p3/tw_order_by` stays a
residue (R-CS1-11). C-011 and C-012 flipped PROVEN here.

## Slice 5 (2026-09-27, this round; the last slice)

S5-1 `partition_spec.rs` per R11 (`resolve_known_field` / `forget_field_name`
exact with the pre-commit refusal, `bound_sources` covering DROP and old
sources, `resolve_field_by_transform` exact with Java's text, `java_struct_text`
`pub(crate)`; the fork action keeps `.case_sensitive(false)`). S5-2
`sort_order.rs` `apply_write_order` takes the session rule with Java's miss text
under both rules; `alter_write_order.rs` passes the S2-Q1 composition;
`distribution/tests.rs` `declare_order` passes `IgnoreCase`. S5-3
`table_props_ddl.rs` `struct_child` / `field_child` exact. Pins:
`partition_spec/tests.rs` (2 tests), the rewritten `alter.rs` pin (OD-3),
`casesens_ddl.rs` (2 tests), `test_casesens_1.py::test_s5_iceberg_ddl_binds_exactly`
(20 DDL legs in probe order plus 12 `_meta` replays from the metadata files).
Registry: `E-CASE-PARTITION-FIELD` closed (R-1/R-2), the `V3-COV-5` stale residue
sentence trued up. Three existing pins rewritten (see Tests rewritten); the
facade sweep changes zero. Mutations M9–M12 red-then-green; M7/M8 for the S4
backfill. Probes `after-s5` change only S5 keys (plus the `tw_order_by` flake);
replay `replay-s5.json` holds every cell. C-013 … C-018 flipped PROVEN here; no
clause is OPEN, so the attestation below is filed.

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: casesens-1
  complete: true
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: All 18 clauses walked one by one against behavior — the oracle measured on live PySpark 4.1.2 + Iceberg 1.11.0, hermetic Rust pins plus the facade replay per clause, every refusal snapshotting the unchanged table or spec, mutations M1-M18 red-then-green.
      artifacts: [task/ledgers/staging/casesens-1-ledger.md, python/repark/tests/test_casesens_1.py, crates/repark-spark/src/tests/casesens_ddl.rs]
    - id: AT-2
      status: ATTACKED
      evidence: Boundaries exercised — twin spellings ID/id/Id, exact duplicates (ID-3 kept), quoted vs unquoted, dotted nested paths (s.A), transform terms (bucket(4, ID)), named fields (Kat/kat), both caseSensitive settings per seat, empty orders, unknown columns.
      artifacts: [crates/repark-spark/src/tests/casesens_ddl.rs, crates/repark-iceberg/src/write/partition_spec/tests.rs, python/repark/tests/test_casesens_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: Every refusal raises Spark's measured text (byte-exact, or head plus candidate set per R12/R13); wrong-case DML and DDL leave tables, specs, orders and identifier sets unchanged (snapshotted in the pins and the _meta replays).
      artifacts: [python/repark/tests/test_casesens_1.py, crates/repark-spark/src/tests/casesens_ddl.rs]
    - id: AT-4
      status: ATTACKED
      evidence: No shared or global state touched — the SQL true path clones the session state per statement only (M3/M4), each pin runs in its own session and warehouse, metadata assertions read committed files.
      artifacts: [crates/repark-spark/src/tests/casesens_true.rs, python/repark/tests/test_casesens_1.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, no secret, no injection or deserialization surface — name matching over closed schema and spec field lists.
    - id: AT-6
      status: ATTACKED
      evidence: Arrow value AND type pinned per leg (dtypes plus rows); display names asserted as written at every scope; divergences recorded as dated residues R-CS1-2…R-CS1-11 with homes, never absorbed.
      artifacts: [python/repark/tests/test_casesens_1.py, task/ledgers/staging/casesens-1-ledger.md]
    - id: AT-7
      status: ATTACKED
      evidence: Planning/DDL-only changes — no row path, no hot loop; the facade sweep and the four probes ran in normal time; file-size baselines ratcheted DOWN only (alter.rs 1607 -> 1606).
      artifacts: [scripts/check_rust_file_size.py, scripts/map.md]
    - id: AT-8
      status: ATTACKED
      evidence: DataFusion 54.1 normalization switch verified per statement and per mode (M3/M4); the fork action keeps its documented case_sensitive(false) with exactness owned by the pre-checks; ceilings ratcheted DOWN only with map.md lockstep in every touched directory.
      artifacts: [crates/repark-iceberg/src/write/partition_spec.rs, crates/repark-iceberg/src/write/map.md, scripts/check_rust_file_size.py]
    - id: AT-9
      status: ATTACKED
      evidence: Every changed refusal text is pinned byte-exact (or R12/R13 head) on the door that raises it; the registered class prefixes (DataInvalid, planning prefix, line/pos suffix) are asserted, not stripped silently.
      artifacts: [python/repark/tests/test_casesens_1.py, crates/repark-spark/src/tests/casesens_ddl.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Red-first held — every new pin fails with the behavior removed (M1-M18) and the pre-change answers are recorded in the after-s4 probe diffs; no dead branch ships (each new arm has a named pin).
      artifacts: [task/ledgers/staging/casesens-1-ledger.md, target/casesens-1/after-s5-diff-p2.txt]
```
