# map — repark-core/src/session/df_guards

## Purpose

DataFusion 54.1 guards that are too large to live inside
[../df_guards.rs](../df_guards.rs). That file owns the two small guards (a config default and a
wrapped optimizer rule) and declares this directory.

## Contents

- [window_rescan.rs](window_rescan.rs) — **WIN-SLIDE-1 (2026-09-04):** the `sliding_frame_rescan` analyzer rule.
  Its design note, the DataFusion contracts it reads, and the routes it does not take are in
  [../map.md](../map.md); its pins are `../tests/window_rescan.rs` and
  `python/repark/tests/test_win_slide_1.py`.
  pins: win-slide-1/C-001, C-005
- `case_bind.rs` — **U11-EDGE-1 (2026-09-26):** `bind_case_insensitive`, run first by
  `subquery.rs`'s `resolve_bound_expr` (the DataFrame door's one binding hook). An
  unqualified column the frame schema does not hold exactly binds to the single field that
  matches it case-insensitively; an exact hit, a case twin, or a qualified column stays as
  it is. DataFusion's `col()` folds `F.col("ID")` to `id`, so without it a spelled SQL
  output (`SELECT ID` → `ID`, `SELECT id AS Id` → `Id`) or a `createDataFrame` column `Id`
  could not be filtered or projected by `F.col`. Live Spark answers all of these
  (`target/probe-u11-edge-1/spark_r3.json`). Rust pins in the file's own test module.
  pins: u11-edge-1/C-015
  **Round 2 (2026-09-26, V-001..V-004):** the file is the DataFrame door's one name binder
  (`pub mod`, re-exported as `repark_core::frame_names`). Rule, under the default
  `caseSensitive=false` (the door does not read the setting, like the round-1 hook): an exact
  name wins; otherwise the single case-insensitive match binds. A qualified column binds the
  same way within the fields whose relation matches the written qualifier part by part from
  the right (`t.id` → `(t, ID)`, `x.id` stays unbound). An alias that spells the qualified
  column (`F.col("t.ID")` arrives as `t.id AS "t.ID"`) is renamed to the written segment
  (`ID`), as Spark names it. `bind_projection_expr` (the select path) keeps the written
  spelling of a bare column it rebinds (`F.col("t.id")` on `ID` → `id`). `drop_named_columns`
  drops every case-insensitive match of each name (a name with none falls back to
  DataFusion's parse, so an absent name stays a no-op); `join_on_named_keys` binds each key on
  each side, joins on the bound columns and keeps one key column per folded key (semi/anti
  keep the left columns); `union_by_folded_name` respells the right frame's fields to the
  left spelling, refuses a strict mismatch with the facade's former text (`Union can only be
  performed … mismatched columns: [...]`, now listing folded mismatches only) and unions by
  name. Spark shapes: `target/probe-u11-edge-1/vx-spark.json`, `vx2-spark.json` (the
  verifier's probes). Rust pins in the file's test module:
  `qualified_reference_binds_through_its_relation`, `qualified_alias_names_the_written_segment`,
  `projection_keeps_the_written_spelling`, `drop_removes_every_folded_match`,
  `join_binds_each_side_and_keeps_one_key`, `union_respells_the_right_and_refuses_a_mismatch`;
  the round-1 `exact_ambiguous_and_qualified_references_stay` became
  `exact_and_ambiguous_references_stay` (a qualified exact hit stays).
  pins: u11-edge-1/C-017, C-018, C-019, C-020
  **Round 4 (2026-09-26, verifier round 2 V-001/V-003):** `drop_named_columns(frame, names,
  references)` takes the string targets and the Column targets apart. A string matches a field
  by its whole text ignoring case (`drop("t.ID")` matches no field); a Column target parses with
  `Column::from_qualified_name_ignore_case` and binds through `case_hits`, the one match rule
  select and filter use (a qualified target narrows by `same_relation` from the right, so
  `F.col("t.ID")` drops `(t, ID)` and `F.col("b.id")` drops `(b, ID)`). A target matching no
  field is a no-op on both, as Spark answers (`drop("t.ID")`, `drop("u.id")`,
  `drop(F.col("u.id"))`). Spark shapes: `target/probe-u11-edge-1/vz-spark.json` (`q_drop_*`,
  `j_drop_*`). Rust pin `qualified_drop_binds_through_its_relation`. pins: u11-edge-1/C-022
  **Round 4 (2026-09-26, verifier round 2 V-002/V-008):** the binder's final rule, under
  `caseSensitive=false`: a column reference collects every field whose name matches it ignoring
  case (`case_hits`; a qualified reference first narrows to the fields whose relation matches the
  written qualifier part by part from the right). Two or more hits refuse Spark's
  `[AMBIGUOUS_REFERENCE] Reference <ref> is ambiguous, could be: [<candidates>]. SQLSTATE: 42704`
  — an exact spelling among them does not win; one hit binds (an exact one stays as written);
  none leaves the column for DataFusion's own error. `ambiguous_reference` renders the reference
  as written and each candidate as its relation parts plus the reference's spelling, backticked
  and sorted the way Spark sorts the list (measured: `[`a`.`id`, `b`.`id`]` for either frame
  order, `[`id`, `sc`.`ns`.`t_vz_1`.`id`]` with the unqualified side first); a RePark scratch
  relation (`_repark_*`, `__repark_*`) renders unqualified, as Spark has no relation there
  (`createDataFrame` twins → `[`id`, `id`]`). `refuse_ambiguous_condition` applies the rule to the
  bare identifiers of a condition join's rewritten ON text across both sides (origin-qualified
  tokens are compound and skipped), and `requalify_join_sides` re-projects a condition join's
  output under each side's own relations so later references see Spark's candidates, not the
  scratch views; it keeps the join as is when the field counts differ or the re-projection does
  not plan. The error is a DataFusion `Plan` error, so the facade raises `AnalysisException` with
  the `Error during planning: ` prefix the SQL door's L-08 refusal carries. The kept
  `exact_and_ambiguous_references_stay` now asserts the three refusals (V-008). Rust pins:
  `exact_and_ambiguous_references_stay`, `ambiguous_candidates_render_sorted_like_spark`,
  `unqualified_and_catalog_candidates_render_like_spark`,
  `requalified_join_carries_each_side_relation`. Spark shapes:
  `target/probe-u11-edge-1/vz-spark.json`, `vz2-spark.json`. pins: u11-edge-1/C-023
- `subquery.rs` — **DF-SUBQUERY-1 (2026-09-15):** the subquery machinery — outer-reference
  scope resolution (`resolve_bound_expr` / `resolve_scoped_expr` /
  `resolve_subquery_plan`, innermost-first so an unqualified `col.outer()` binds inside
  the subquery like Spark classic), the `__repark_single_row` guard UDAF that turns a
  multi-row scalar subplan into the `SCALAR_SUBQUERY_TOO_MANY_ROWS` execution error
  and its `__repark_any_row` sibling (strict=false — first-row pick for a correlated
  `LIMIT 1`, whose `Limit` is stripped before wrapping because `ScalarSubqueryToJoin`
  cannot pull one up; uncorrelated `LIMIT 1` stays untouched as already-singleton),
  and three optimizer rules: `repark_scalar_subquery_guard` (wraps non-singleton scalar
  subplans in the guard aggregate so `ScalarSubqueryToJoin`'s count-bug compensation
  reads a non-`count` aggregate and emits NULL), `repark_lateral_projection_hoist`
  (lifts `LateralJoin` right-side projections of outer refs onto the left input,
  refusing `CORRELATED_REFERENCE` under generators/non-Filter parents, and unwraps
  uncorrelated `Subquery` arms — every right-input mutation rebuilds through
  `Join::try_new` so the cached join schema stays honest; a `SubqueryAlias` on the
  right is carried through — hoisted outputs are requalified to it, inner-qualifier
  column refs are rewritten `d.x`→`t.x`, join `on`/`filter` refs to hoisted columns
  are inlined, and a still-correlated right keeps its `Subquery` marker under the
  alias), and
  `repark_projection_exists` (a projection `Exists`/`Not(Exists)` becomes a
  `count(*)`-comparison boolean that keeps the plan's field name).
  Pins: `../tests/subquery.rs` and `python/repark/tests/test_df_subquery_1.py`.
  This module is the ledger's rust-first roll-call home: all scope resolution, the
  single-row guard, the exists rewrite, the projection hoist, and the Unnest
  refusal live here in repark-core.
  pins: df-subquery-1/C-001, C-002, C-003, C-004, C-008, C-009

## Pointers

- Up: [../map.md](../map.md)
