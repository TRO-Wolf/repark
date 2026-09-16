# map — repark-core/src/session/df_guards

## Purpose

DataFusion 54.1 guards that are too large to live inside
[../df_guards.rs](../df_guards.rs). That file owns the two small guards (a config default and a
wrapped optimizer rule) and declares this directory.

## Contents

- `window_rescan.rs` — **WIN-SLIDE-1 (2026-09-04):** the `sliding_frame_rescan` analyzer rule.
  Its design note, the DataFusion contracts it reads, and the routes it does not take are in
  [../map.md](../map.md); its pins are `../tests/window_rescan.rs` and
  `python/repark/tests/test_win_slide_1.py`.
  pins: win-slide-1/C-001, C-005
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
