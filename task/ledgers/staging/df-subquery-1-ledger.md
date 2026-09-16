# Charter ledger — DF-SUBQUERY-1 · `scalar` / `exists` / `lateralJoin` / `asTable` over `Column.outer`

**Date:** 2026-09-15 · **Branch:** `feat/df-subquery-1` · **Base:** `0355ef5e` · **Model:**
swe-2-high · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Registry:** `DF-SUBQUERY-1` (implemented; the unqualified-resolution quirk pinned per S-3/S-5)
appended at §5 end of
[../../../docs/spark-sql-iceberg-parity.md](../../../docs/spark-sql-iceberg-parity.md). The
conditional rows were not needed: the SQL `TABLE(emp)` UDTF spelling answers through the
`try_sql_registered_udtf` argument parser (no `DF-ASTABLE-UDTF-1`), and the three SQL-door
oracle cells answer on this branch (no `DF-SUBQUERY-SQL-1`). Two measured residuals ride the
DF-SUBQUERY-1 row: the optimizer-raised `CORRELATED_REFERENCE` refusal carries Spark's
condition text and SQLSTATE but `getCondition()` is not populated on the wrapped exception,
and the pre-existing `spark.tvf.explode` nullability gap (Spark reports non-nullable, repark
nullable) is pinned as-is.

**Why now.** The 1.5 PySpark-parity campaign. `Column.outer` (COLUMN-PARITY-1, #605) set the
facade flag with no consumer; this unit gives it the four consumers PySpark 4.1.2 defines —
scalar subquery, EXISTS, lateral join, table argument — with the subquery expressions, outer
references and planner rewrites built in Rust (owner ruling, 2026-09-14) over DataFusion 54.1's
`Expr::ScalarSubquery` / `Expr::Exists` / `OuterReferenceColumn` / `LogicalPlan::Subquery` and
the decorrelation rules already in the optimizer list.

**Oracle:** `/tmp/oc-worker/qb-oracle/dfsubq_probe_2026-09-15.json` (script
`probe_dfsubq.py`, run 16b, PySpark 4.1.2 classic) — 46 cells; frames
`emp(id int, dept string, sal int)` rows (1,a,10) (2,b,20) (3,a,30) (4,null,null) and
`dept(dept string, budget int)` rows (a,100) (c,300), temp views `emp` / `dept`. Copied
byte-identical to `python/repark/tests/facade_df_subquery_oracle.json`; every pin names its cell.

**Measured quirks the pins reproduce (S-3/S-5):** on Spark classic an unqualified
`F.col("dept") == F.col("dept").outer()` resolves BOTH sides inside the subquery, so
`exists_correlated` keeps all four rows and `lateral_basic`/`lateral_left` answer the cross
product; the qualified forms (`d.dept = e.dept`) answer the correlated rows. Both are pinned:
the unqualified cells as recorded, the qualified-alias cells against `exists_sql` /
`lateral_sql` row sets.

**Not in this unit:** `functions*.py` and `crates/repark-python/src/column/function_dispatch.rs`
(run 17a), `crates/repark-spark` (run 17c), `STATUS.md`, `briefs/next-sequence.md`.

## PROPOSITION LEDGER — DF-SUBQUERY-1 — 2026-09-15

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `DataFrame.scalar()` returns a `Column` over `Expr::ScalarSubquery(self_plan)` (Rust): usable in `select` and `where`; single output column enforced at construction with AnalysisException `INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN` (`number` param, SQLSTATE 42823, message head per `scalar_multi_col`); multi-row answers `[SCALAR_SUBQUERY_TOO_MANY_ROWS] More than one row returned by a subquery used as an expression. SQLSTATE: 21000` at execution; empty subquery answers NULL; unaliased name `scalarsubquery()`, repr `Column<'([max(budget): int])'>`. Cells: `scalar_uncorrelated`, `scalar_correlated`, `scalar_correlated_qualified`, `scalar_empty`, `scalar_in_filter`, `scalar_multi_col`, `scalar_multi_row`, `scalar_no_alias_name`, `scalar_repr`. | `python/repark/tests/test_df_subquery_1.py` scalar pins driven from the fixture cells. | PROVEN | `test_scalar_uncorrelated` / `test_scalar_correlated_unqualified_binds_inner` / `test_scalar_correlated_qualified` / `test_scalar_empty_answers_null` / `test_scalar_in_filter` / `test_scalar_multi_col_raises_conditioned` / `test_scalar_multi_row_raises_21000` / `test_scalar_repr_and_default_name` — values, schema, nullability, display, both error shapes pinned cell-for-cell; `test_scalar_sql_door` covers the door. Red-first below. **Round-3 (S-11):** `test_scalar_correlated_limit_answers` + `test_scalar_correlated_limit_sql_door` pin the correlated-`LIMIT` contract (any-row pick inside the match set / NULL on `LIMIT 0`, never a physical-planning error); `scalar_guard_strips_correlated_limit_into_any_row` pins it plan-level. |
| C-002 | `DataFrame.exists()` returns a non-nullable boolean `Column` over `Expr::Exists(self_plan)` (Rust): filters in `where`, `~` negates, answers per-row in `select` through the repark-core rewrite (EXISTS in a projection → `coalesce(scalar-subquery count > 0, false)`), unaliased name `exists()`, repr `Column<'EXISTS ([dept: string, budget: int])'>`. Cells: `exists_correlated`, `exists_not`, `exists_uncorrelated_true`, `exists_uncorrelated_empty`, `exists_in_select`, `exists_no_alias_name`, `exists_repr`, plus a qualified-alias pin matching `exists_sql` rows (1, 3). | The exists pins in the new test module. | PROVEN | `test_exists_correlated_unqualified_binds_inner` (all four rows, per the recorded quirk) / `test_exists_qualified_correlates` (ids 1,3) / `test_exists_not` (ids 2,4) / `test_exists_uncorrelated` / `test_exists_in_select` (count-rewrite, `exists()` name, non-nullable) / `test_exists_repr_and_default_name`; `test_exists_sql_door` pins the SQL spelling. |
| C-003 | `Column.outer()` lowers to `Expr::OuterReferenceColumn` (Rust): inside a subquery plan it resolves against the consuming frame at decorrelation; outside any subquery it resolves as the plain column (Spark classic's answer, `outer_outside_subquery` → rows 1, 3); qualified spellings `e.dept` resolve against `df.alias("e")` plans. | The `outer_outside_subquery` pin; the qualified pins in C-001/C-002/C-004. | PROVEN | `test_outer_outside_subquery_resolves_plain` (`lit(1).outer()` → `[1,2]`); qualified refs work because `df.alias` now wraps the plan in a real `SubqueryAlias` node (temp-view side effect kept) — `d.dept`/`e.dept` resolve in the C-001/C-002/C-004 qualified pins. Refusals: `test_lateral_outer_ref_under_generator_refused` pins `0A000` + the oracle message on the `explode`/`pivot` paths. Residual: the optimizer-raised refusal carries condition+SQLSTATE in message text but `getCondition()` is unpopulated — recorded on the DF-SUBQUERY-1 row. |
| C-004 | `DataFrame.lateralJoin(other, on=None, how=None)`: `how` defaults inner; `inner`/`cross`/`left`/`leftouter`/`left_outer` accepted case-insensitively; any other `how` raises AnalysisException `UNSUPPORTED_JOIN_TYPE` SQLSTATE 0A000 with Spark's message and `typ`/`supported` params (`lateral_right`, `lateral_full`, `lateral_semi`, `lateral_bad_how`); `on` str / list-of-str joins on names, `on` Column joins on the predicate with both `dept` columns kept (`lateral_on`); right-side projection reading outer columns answers (`lateral_outer_expr` → `dbl = sal*2` incl. the NULL row); outer refs under a generator raise AnalysisException `UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.CORRELATED_REFERENCE` SQLSTATE 0A000 (`lateral_tvf_like`); non-DataFrame `other` → PySparkTypeError `NOT_DATAFRAME`, non-str `how` → `NOT_STR`. | The lateral pins in the new test module; the qualified-alias variant pinned to `lateral_sql` rows. | PROVEN | `test_lateral_basic` / `test_lateral_left` / `test_lateral_cross` / `test_lateral_on_qualified` / `test_lateral_on_qualified_correlates` (`d.dept = e.dept` → `(1,100),(3,100),(4,300)`) / `test_lateral_outer_expr_in_projection` (`(1,[100]),(2,None),(3,[100]),(4,None)`) / `test_lateral_unsupported_how_raises` (`right`/`full`/`left_semi`/`nope` → `UNSUPPORTED_JOIN_TYPE` `0A000`) / `test_lateral_how_accepts_supported_spellings` / `test_lateral_on_str_names` (str `on`, right-key drop) / `test_lateral_rejects_non_dataframe_and_non_str_how` (`NOT_DATAFRAME`, `NOT_STR`); `test_lateral_sql_door` pins the `JOIN LATERAL` spelling. **Round-3 (S-10/S-12/S-13):** `test_lateral_sql_door_outer_ref_aliased` / `test_lateral_sql_door_outer_ref_qualified_filter` / `test_lateral_sql_door_outer_ref_aliased_on` / `test_lateral_on_hoisted_column` / `test_lateral_sql_door_aliased_correlated_filter` pin the preserved `SubqueryAlias` qualifier; `test_lateral_left_qualified_keeps_unmatched_rows` pins the inner/left difference and reds on an `inner` revert; `test_lateral_outer_ref_under_generator_refused` docstring narrowed to the parts it reads (condition/head/SQLSTATE — not `sqlExprs`, not `getCondition()`); `lateral_hoist_keeps_alias_qualifier_on_outputs` pins it plan-level. |
| C-005 | `DataFrame.asTable()` returns `TableArg` with exactly `partitionBy` / `orderBy` / `withSinglePartition` public (`astable_methods`, `astable_type`); `orderBy` before partitioning raises IllegalArgumentException "Please call partitionBy() or withSinglePartition() before orderBy()."; `withSinglePartition` after either raises IllegalArgumentException "Cannot call withSinglePartition() after partitionBy() or withSinglePartition() has been called."; `df.select(df.asTable())` raises PySparkTypeError `NOT_COLUMN_OR_STR` with `{"arg_name": "col", "arg_type": "TableArg"}`; a Python UDTF called with the table argument answers `astable_udtf_call` rows through the existing Arrow `eval` bridge (partitionBy/withSinglePartition/orderBy drive per-partition instances and ordering); the SQL `count_udtf(TABLE(emp))` spelling either answers or is a pinned refusal with registry row `DF-ASTABLE-UDTF-1`. | The asTable pins; the UDTF table-arg path or refusal. | PROVEN | `test_astable_type_methods_repr` / `test_astable_ordering_guards` (both IllegalArgumentException texts) / `test_astable_fluent_returns_table_arg` / `test_astable_select_raises_not_column_or_str` (`NOT_COLUMN_OR_STR`, `arg_type=TableArg`) / `test_astable_udtf_call` (partition keys → fresh handler instances; `orderBy` sorts in-partition) / `test_astable_udtf_sql_table_spelling` (`TABLE(emp)` answers — no `DF-ASTABLE-UDTF-1` needed) / `test_astable_tvf_explode_unchanged` (pre-existing explode nullability gap pinned, recorded as a residual on the registry row). |
| C-006 | SQL door: `scalar_sql`, `exists_sql`, `lateral_sql` answer the recorded rows on this branch (re-measured post-FNP-4B); the two known SQL-door gaps — EXISTS in a projection and LATERAL with outer refs in the SELECT list — answer through the repark-core rules when reachable without crates/repark-spark, else pin today's error under registry row `DF-SUBQUERY-SQL-1`. | The SQL-door pins in the new test module. | PROVEN | `test_scalar_sql_door` / `test_exists_sql_door` / `test_lateral_sql_door` / `test_astable_udtf_sql_table_spelling` — all four cells answer exactly as recorded. The three pure-SQL cells answered on the FNP-4B base already; the EXISTS-in-projection and lateral-projection gaps close through the repark-core rules — pinned on the SQL door itself by `test_exists_sql_door_in_projection` and `test_lateral_sql_door_outer_ref_in_select` (round 2). `TABLE(emp)` reaches the registered UDTF via `udtf.py`'s new `TABLE(...)` call-arg parser — no `DF-SUBQUERY-SQL-1` needed. **Round-3 (S-10) — the corrected LATERAL claim:** the round-2 wording overstated the gap's closure. What answers today, all pinned: the unaliased outer-ref-in-SELECT spelling `LATERAL (SELECT e.sal * 2 AS dbl)`; the aliased spelling `LATERAL (…) t`; `WHERE t.dbl > 10` over the hoisted column; `JOIN LATERAL (…) t ON e.sal > 15`; a still-correlated right under `t` (`LATERAL (… WHERE d.dept = e.dept) t`); and the DataFrame-door `on=col("dbl") > 10`. Not measured (listed under wanted_cells, not silently assumed): the two-cell-derived expected row sets for `lateral_sql_outer_expr_aliased_filter` / `lateral_sql_outer_expr_aliased_on` / `lateral_on_outer_expr` / `lateral_left_qualified` / `scalar_limit1_correlated` — the pins assert the Spark-certain contract (cell-derived subsets, join-semantics differences, no internal error), never invented fixture rows. |
| C-007 | Registry, examples, exports: `DF-SUBQUERY-1` (+ `DF-ASTABLE-UDTF-1` / `DF-SUBQUERY-SQL-1` as needed) appended at §5 end without reordering; a `docs/examples/dataframe/` example covers `scalar`/`exists`/`lateralJoin`/`asTable` with COVERS rows; `inventory.txt` + EX-0 counts re-measure; `_dfcore_1_expected.py` gains the four names the way its notes describe. | The registry diff; the maps; the gates. | PROVEN | Registry row appended at §5 end (`DF-SUBQUERY-1 — FIXED 2026-09-15`, cites C-001..C-007, both residuals recorded; the conditional rows were unnecessary). `docs/examples/dataframe/subquery.py` covers all four names; `inventory.txt` regenerated (+4 DataFrame rows, enumerator 1059→1063, `test_ex_0_example_coverage.py` pin updated); `_dfcore_1_expected.py` gained the four names + `subquery` submodule per the declared-delta protocol — `test_dfcore_1_exports.py` `10 passed`. Every touched `map.md` updated in the same diff. |
| C-008 | No regression: Rust unit tests for the new constructors/rules, `make verify`, the unit test file, `test_dfcore_1_exports.py`, the #605 column pins, and `python/repark-parity/tests` all green; the comment fence grep prints nothing. | §Gates. | PROVEN | `crates/repark-core/src/session/tests/subquery.rs` — 7 plan-level pins green (round 3 added the alias-qualifier and correlated-`LIMIT` pins). `make verify`, the facade suite, the parity suite, the focused cohort, and `cargo test -p repark-core --lib -- session::tests` all re-ran green after the round-3 fixes — counts in §Gates. Comment-fence grep prints nothing beyond the sanctioned existing `df_guard.rs` doc-comment edit already in `b3fae405`. |
| C-009 | Rust-first roll-call: every line of subquery/outer-reference/lateral planning lives in `crates/repark-python` + `crates/repark-core`; each piece that stays in Python is named here with its reason. | The roll-call below. | PROVEN | Roll-call below. Facade argument-shape checks and display names stayed Python per the ruling; TableArg is a pure value object; the UDTF table-arg eval loop is Spark's own per-row Python boundary — all three named there. Everything else (scope resolution, the single-row guard, the exists rewrite, the projection hoist, the Unnest refusal) is repark-core Rust. |

VERDICT: 9 clauses, 9 PROVEN, 0 OPEN, 0 REJECTED. Round 3 (2026-09-16): the logic
review's four P2s (L-201..L-204) remediated and red-first-proven; the S-14 residue
(P-201..P-206, Y-1..Y-3) recorded, not fixed, per the orchestrator ruling.

## Per-name decision table

| name | decision | one line of reason |
|---|---|---|
| `DataFrame.scalar` | implemented | `Expr::ScalarSubquery` over the frame's plan; DataFusion's `ScalarSubqueryToJoin` decorrelates; the >1-column check is construction-time, the >1-row check is a repark-core execution guard. |
| `DataFrame.exists` | implemented | `Expr::Exists` for `where`; a repark-core optimizer rewrite lowers EXISTS in projections to `coalesce(count>0, false)` since DataFusion's predicate-subquery rule only fires in filters. |
| `DataFrame.lateralJoin` | implemented | `Join(left, Subquery(right), …)` + repark-core rules: strip outer refs resolvable inside the right plan (Spark classic's recorded quirk), hoist outer-reading projections above the join, refuse outer refs under `Unnest`. |
| `DataFrame.asTable` | implemented | `TableArg` value object + UDTF table-arg execution over the existing Arrow eval bridge; the `TABLE(df)` SQL spelling answers through the registered-UDTF call-arg parser. |
| `Column.outer` | implemented | Native `OuterReferenceColumn` construction; the no-subquery case resolves as a plain column per Spark classic. |

## Rulings (orchestrator, binding)

- S-1..S-9 per the card (recorded above in the clause that owns each).
- R-1 — `df.alias` keeps its temp-view side effect and returns a `SubqueryAlias`-wrapped
  child: the side effect is existing facade behavior the join machinery doesn't consume
  (it rewrites through its own temp views), and the real alias node is what lets
  `d.dept`-qualified refs survive analysis. Decision made in-flight; the 558-test
  regression battery stayed green.
- R-2 — the `__repark_single_row` guard wraps only scalar subplans that are not already a
  zero-group `Aggregate` (a `Projection` over one also counts as singleton): wrapping
  `count(*)` plans would defeat `ScalarSubqueryToJoin`'s count-bug compensation, which
  rewrites non-`count` aggregates to NULL.
- R-3 — every `Join` the rules mutate is rebuilt through `Join::try_new` (three sites):
  `Join` caches its schema at construction, and mutating `join.right` in place left the
  qualified-correlated cell resolving against the pre-hoist schema (`No field named
  d.budget`). Proven by `test_lateral_on_qualified_correlates`.
- R-4 — `on` as `str`/`list[str]` lowers to equi-keys with Spark's drop-the-right-key
  schema (`test_lateral_on_str_names`); `on` as `Column`/`None` keeps both sides.
- R-5 — the table-arg UDTF execution block (`_as_table_arg` / `_map_table_udtf_batches` /
  `_execute_table_udtf`) lives in `table_arg.py`, not `udtf.py`: `udtf.py` crossed the
  1000-line `lib-py` ceiling at 1041 and the split is the sanctioned out (a cohesive
  unit moved, not an EXCEPTIONS row).
- R-6 — `test_udtf_table_arg_refuses` was a pre-existing pin of the refusal this unit
  removes; flipped to `test_udtf_table_arg_feeds_rows` (DataFrame arg → one `Row` per
  `eval` call) in the same diff, per the declared-delta norm for behavior changes.
- R-7 — (round 3, S-10) a `SubqueryAlias` on the lateral right is carried through
  `repark_lateral_projection_hoist`: the rebuilt right keeps `SubqueryAlias(t)` over
  either the bare rewritten inner (no residual outer refs) or a `Subquery` marker
  (still-correlated); hoisted output aliases keep their names but gain
  `relation = t`, inner-qualifier column refs inside hoisted expressions are
  rewritten `d.x`→`t.x` via `requalify_hoisted`, and join `on`/`filter` refs to
  hoisted columns are inlined via `substitute_hoisted_ref` before the join schema
  is rebuilt through `Join::try_new`.
- R-8 — (round 3, S-11) the `Limit` singleton short-circuit applies only when the
  subplan under it has NO outer reference; a correlated `Limit` is stripped
  (`strip_correlated_limit`) before the guard wraps the remainder — `fetch <= 1`
  becomes a `__repark_any_row` non-strict pick (Spark classic's nondeterministic
  answer shape), `fetch > 1` or a non-literal/stripped-incomplete limit keeps the
  strict `__repark_single_row` guard so the refusal stays conditioned. Uncorrelated
  `LIMIT 1` is untouched — already singleton.
- R-9 — (round 3, S-12) the `lateral_tvf_like` pin asserts only what it can read —
  condition name, English head, SQLSTATE — and the DF-SUBQUERY-1 registry row now
  names BOTH residuals on that refusal: `getCondition()` unpopulated on the
  engine-wrapped exception AND `sqlExprs` quoting repark's internal
  `__repark_arr_*` id where the cell records Spark's `explode(array(id, sal))`.
  The Catalyst-identical rendering is a disclosed divergence, not chased tonight.
- R-10 — (round 3, S-13) `how="left"` is pinned by the inner/left DIFFERENCE on a
  qualified-alias lateral join (`e`/`d`, `d.dept = e.dept.outer()`): inner answers
  the `lateral_sql` rows (ids 1,3 — the `dept='b'` and NULL-`dept` outers drop),
  left keeps all four outers with NULL `budget` on ids 2,4. No fixture cell covers
  this spelling (`wanted_cells: lateral_left_qualified`); the pinned shape is
  Spark join semantics — red-proven by reverting `left`→`Inner`.
- R-11 — (round 3, S-14) the reviewers' P-201..P-206 and Y-1..Y-3 are recorded
  residue, not implementation work — no P1 was found and the orchestrator ruled
  them out of tonight's scope; each row below carries the report and the numbers.

## Round-2 answers (orchestrator questions)

**(a) Cell accounting — "46 cells" vs 39 tests.** The probe recorded 46 JSON keys =
45 cells + `meta`; the copied fixture keeps the 41 subquery-family cells + `meta` (the
card scoped the copy to `scalar_*`/`exists_*`/`outer_*`/`lateral_*`/`astable_*`; the 4
trimmed cells — `freq_dup_col_tuple`, `transpose_default_tuple`,
`transpose_dup_index_tuple`, `transpose_key_col_clash_tuple` — belong to the
transpose/freqItems family of a different unit). **Every one of the 41 copied cells is
pinned; none is unpinned.** The test file runs 41 cases across 38 functions —
`test_lateral_unsupported_how_raises` is parametrized ×4 covering four cells
(`lateral_right`, `lateral_full`, `lateral_semi`, `lateral_bad_how`); six functions pin
two or three cells each (`scalar_repr`+`scalar_no_alias_name`;
`exists_repr`+`exists_no_alias_name`; `exists_uncorrelated_true`+`_empty`;
`astable_type`+`astable_methods`+`astable_repr`;
`astable_order_without_partition`+`astable_partition_and_single`;
`astable_partition`+`astable_withsinglepartition`); two functions pin argument-shape
behavior with no dedicated cell (`test_lateral_how_accepts_supported_spellings`,
`test_lateral_rejects_non_dataframe_and_non_str_how`); and the two SQL-door gap pins
added this round re-assert existing cells (`lateral_outer_expr` through the SQL
spelling; the `exists_in_select` shape through `AS e`).

**(b) The flipped `test_udtf` pin.** Before:
`test_udtf_table_arg_refuses` — `Echo(spark.range(2))` expected
`UnsupportedOperationException` matching `table-argument|table.arg|not supported|LATERAL`.
After: `test_udtf_table_arg_feeds_rows` — the same call collects `[0, 1]`, asserting
each input `Row` reaches `eval` once. The justifying oracle cell is
`astable_udtf_call`: `Count(emp.asTable())` answers `(1,1),(2,1),(3,1),(4,1)` — the
measured PySpark 4.1.2 contract that a table argument feeds rows to `eval`
per-partition. A bare `DataFrame` argument is PySpark's implicit table-arg form (the
`__call__` signature accepts `DataFrame`/`TableArg`); `test_astable_udtf_call` in this
unit's file pins the explicit `asTable()` form against the same cell.

**(c) The two named SQL-door gaps.** Both answer — verified on this branch and now
pinned: `test_exists_sql_door_in_projection` (`SELECT id, EXISTS(...) AS e FROM emp e`
→ `(1,T),(2,F),(3,T),(4,F)`, field `e` non-nullable — the `exists_in_select` shape) and
`test_lateral_sql_door_outer_ref_in_select` (`LATERAL (SELECT e.sal * 2 AS dbl)` → the
`lateral_outer_expr` cell's rows exactly). No `DF-SUBQUERY-SQL-1` row was added.

## Round-3 remediation (Grok reviews — logic NEEDS_REMEDIATION, 0 P1 / 4 P2; both perf PASS)

### S-10 (L-201) — the lateral hoist drops the right side's alias qualifier — FIXED

The hoist rebuilt `Project(left ++ hoisted)` over the join but dropped the
`SubqueryAlias` node, so `t.dbl` failed downstream. Fix: `lateral_parts` returns the
optional alias; `hoist_lateral_projection` re-wraps the rebuilt right in
`SubqueryAlias` (over a `Subquery` marker when outer refs remain below the hoist),
requalifies hoisted outputs to the alias (`alias_qualified` /
`Alias.relation = Some(t)`), rewrites inner-qualifier refs `d.x`→`t.x`
(`requalify_hoisted`), and inlines hoisted refs inside join `on`/`filter`
(`rewrite_join_predicates` over `substitute_hoisted_ref`).

Red-first — the five pins on `b3fae405` (head binary, new tests spliced in):

```
FAILED test_lateral_sql_door_outer_ref_aliased
  E  AnalysisException: Optimizer rule 'eliminate_cross_join' failed
     Schema error: No field named t.dbl. Did you mean 'dbl'?.
FAILED test_lateral_sql_door_outer_ref_qualified_filter   (same t.dbl miss)
FAILED test_lateral_sql_door_outer_ref_aliased_on         (same t.dbl miss)
FAILED test_lateral_on_hoisted_column
  E  AnalysisException: Optimizer rule 'repark_lateral_projection_hoist' failed
     Schema error: No field named dbl.
FAILED test_lateral_sql_door_aliased_correlated_filter
     Schema error: No field named d.budget. Did you mean 't.budget'?.
```

Green after the fix — all 5 spellings answer; the aliased correlated-filter form
answers the `lateral_sql` cell exactly. Plan-level pin:
`lateral_hoist_keeps_alias_qualifier_on_outputs` asserts `t.dbl` resolves in the
rewritten schema and the `Subquery` marker survives under `SubqueryAlias(t)`.

### S-11 (L-202) — correlated `LIMIT 1` treated as already-singleton — FIXED

`plan_is_singleton`'s `Limit fetch <= 1` arm now requires `!plan_has_outer_refs` of
the input. A correlated subplan then goes through `guard_scalar_subquery` →
`strip_correlated_limit` (recurses through Projection/Filter/SubqueryAlias/Sort;
`fetch=0` becomes `Filter(false)`; nested limits take the min) → wrap. `fetch <= 1`
gets `__repark_any_row` (non-strict sibling of the guard UDAF — first row wins,
Spark's nondeterministic pick); `fetch > 1`/unstrippable keeps `__repark_single_row`
so the conditioned refusal still fires. `ScalarSubqueryToJoin` can then pull the
aggregate up — a raw correlated `Limit` was the case it cannot decorrelate.

Red-first — on `b3fae405`:

```
FAILED test_scalar_correlated_limit_answers
FAILED test_scalar_correlated_limit_sql_door
  E  UnsupportedOperationException: This feature is not implemented: Physical plan
     does not support logical expression ScalarSubquery(<subquery>)
```

Green after the fix — measured: correlated `LIMIT 1` (dup `dept='a'` → budget 100
per outer row, NULL on non-matching/NULL dept), `LIMIT 0` → all NULL, `LIMIT 5`
(DataFrame + SQL) → conditioned `AnalysisException` "Correlated scalar subquery must
be aggregated to return at most one row", never an internal error. `scalar_limit1_correlated`
is unmeasured in the fixture → `wanted_cells`; the pin asserts the Spark-certain
shape (a member of the match set / NULL), not invented rows.

### S-12 (L-203) — `lateral_tvf_like` disclosure — FIXED (disclosure, not behaviour)

The pin now documents exactly what it asserts (condition name, head, SQLSTATE) and
the DF-SUBQUERY-1 registry row names both gaps with the measured texts: Spark cell
renders `"explode(array(id, sal))"` in `sqlExprs` / message; repark raises the same
condition + `0A000` but quotes `"__repark_arr_16b982f2bc6d4013ae27fcd45bd1c6fa"`
(internal array id) and `getCondition()` returns `None` on the wrapped exception.
No `DF-SUBQUERY-SQL-1` row — a disclosed residual, not a silent one.

### S-13 (L-204) — `how="left"` had no red-capable pin — FIXED

New pin `test_lateral_left_qualified_keeps_unmatched_rows`: `emp.alias("e")` ×
qualified `dept.alias("d")` correlation `d.dept == e.dept.outer()`, `inner` pinned
to the `lateral_sql` cell, `left` pinned to the Spark-certain difference (4 rows,
NULL `budget` on the non-matching ids 2,4, right column nullable).

Red proof — `crates/repark-python/src/subquery.rs` `"left" => JoinType::Inner`,
release rebuild, then:

```
E       assert len(by_id) == 4
E        +  where 2 = len({1: 100, 3: 100})
FAILED test_lateral_left_qualified_keeps_unmatched_rows — 1 failed
```

Restored `"left" => JoinType::Left`, rebuilt, green (49 passed in the module).

### S-14 — reviewer residue (record only — no P1; orchestrator-ruled out of scope)

| id | severity | report | finding | measurement / ruling |
|---|---|---|---|---|
| P-201 | P2 | perf-dfsubq-rust-report | EXISTS-in-projection lowers to grouped count + Left Join, not LeftSemi | 100k×100k 1:1: projection EXISTS 182.7 ms vs WHERE EXISTS 65.6 ms (~2.78×); many-match 10k/100k mod-10: 14.2 ms vs 117.4 ms — not uniformly worse; residue |
| P-202 | P2 | perf-dfsubq-rust-report | `__repark_single_row` pushes no two-row cap; a 1-row scan pays a full aggregate | one-row scalar ~2 ms warm; multi-row 1M error ~1.5 ms on `generate_series` — no demonstrated full materialization; residue |
| P-203 | P2 | perf-dfsubq-rust-report | `repark_scalar_subquery_guard` walks every expression of every node of every query | planning medians: `SELECT 1` 567 µs, `range(1)` 587 µs, 50-col project 6651 µs, 20-way equijoin 20561 µs, 200-term OR 68730 µs, 50-branch UNION ALL 21068 µs, 100-layer withColumn tower 514027 µs; no same-binary delta vs origin/main measured; reviewer: likely sub-ms/tens-of-µs overhead, not worth an early-out tonight; residue |
| P-204 | P3 | perf-dfsubq-rust-report | `Transformed::yes` returned when nothing was wrapped | cosmetic; residue |
| P-205 | P3 | perf-dfsubq-rust-report | lateral hoist evaluates outer expressions after the join | 100k×10 → 1,000,000 rows ~730 ms; no cardinality error or defeated pushdown on the measured card; residue |
| P-206 | P3 | perf-dfsubq-rust-report | clones + `to_string` on rewrite/build paths | no per-node HashMap rebuild on the miss path; `to_string()` only on the EXISTS fire path; process-wide allocation measured NULL; residue |
| Y-1 | P2 | perf-dfsubq-py-report | table-arg UDTF runs per-row Python `eval` after a per-batch Arrow crossing | no per-row PyO3 crossing (measured); Spark's own boundary; residue |
| Y-2 | P2 | perf-dfsubq-py-report | `scalar`/`exists` rebuild `frame.schema` on the success path | small constant per call; residue |
| Y-3 | P3 | perf-dfsubq-py-report | `subquery` imported eagerly while `table_arg` is lazy | import-time constant only; residue |

### wanted_cells (unmeasured spellings — pin asserts the Spark-certain shape only)

- `scalar_limit1_correlated` — `emp.alias("e").select("id", dup.alias("d").where(F.col("d.dept") == F.col("e.dept").outer()).select("budget").limit(1).scalar().alias("b"))` and the SQL `SELECT id, (SELECT budget FROM dept d WHERE d.dept = e.dept LIMIT 1) b FROM emp e` over the `dup` frame `("a",100),("a",200),("c",300)`
- `lateral_sql_outer_expr_aliased_filter` — `SELECT * FROM emp e, LATERAL (SELECT e.sal * 2 AS dbl) t WHERE t.dbl > 10`
- `lateral_sql_outer_expr_aliased_on` — `SELECT * FROM emp e JOIN LATERAL (SELECT e.sal * 2 AS dbl) t ON e.sal > 15`
- `lateral_on_outer_expr` — `emp.lateralJoin(spark.range(1).select((F.col("sal").outer() * 2).alias("dbl")), on=F.col("dbl") > 10)`
- `lateral_left_qualified` — `emp.alias("e").lateralJoin(dept.alias("d").where(F.col("d.dept") == F.col("e.dept").outer()).select("budget"), how="left")`

## Rust-first roll-call

Every piece of subquery/outer-reference/lateral planning lives in Rust; the pieces that stay in
Python, each with its reason:

- **repark-core** (`crates/repark-core/src/session/df_guards/subquery.rs`, comment-free per the
  io-text convention): `resolve_bound_expr` / `resolve_scoped_expr` / `resolve_subquery_plan` —
  scoped expression/plan resolution with inner-first unqualified binding (the S-3/S-5 quirk);
  `__repark_single_row` UDAF + `ReparkScalarSubqueryGuard` (multi-row scalar → `21000`, wrapped
  only when the subplan is not already a zero-group `Aggregate` so count-bug compensation still
  fires; round 3 added the `__repark_any_row` non-strict sibling and `strip_correlated_limit`
  so a correlated `LIMIT` can never survive to physical planning); `ReparkProjectionExists`
  (projection EXISTS/NOT EXISTS → count-comparison, `exists()`
  field name preserved); `ReparkLateralProjectionHoist` (outer-reading right projections hoisted
  above the lateral join, uncorrelated `Subquery` unwrapped, outer refs under `Unnest` or an
  invalid parent refused `CORRELATED_REFERENCE` `0A000`; round 3 added `lateral_parts` /
  `requalify_hoisted` / `rewrite_join_predicates` / `substitute_hoisted_ref` so the right side's
  `SubqueryAlias` qualifier survives the rewrite). Installed in the card's rule order:
  `repark_projection_exists` → `repark_scalar_subquery_guard` → `scalar_subquery_to_join` →
  `repark_lateral_projection_hoist` → `decorrelate_lateral_join`; all three join-mutation sites
  rebuild via `Join::try_new` (the stale-schema fix).
- **repark-python** (`crates/repark-python/src/subquery.rs`): `df_scalar` / `df_exists` /
  `df_lateral_join` / `df_subquery_alias` / `col_outer` / `resolve_scoped_expr` — thin
  constructors for `Expr::ScalarSubquery` / `Expr::Exists` / `Expr::OuterReferenceColumn` /
  `LogicalPlan::Subquery` / the lateral `Join` node. `bound()` in `dataframe.rs` gained the
  scoped-resolution call (line-neutral).
- **Python, named:** (a) facade argument-shape checks (`how` spelling, `on` type, TableArg
  ordering guards) — facade errors are the facade's job per the ruling; (b) `TableArg` — a pure
  value object, no engine path; (c) the UDTF table-arg eval loop in `table_arg.py` +
  `udtf.py`'s `TABLE(...)` call-arg parser — Spark's own per-row UDTF boundary is Python;
  (d) `df.alias` keeps its temp-view side effect while returning a `SubqueryAlias`-wrapped
  child — the side effect is existing facade behavior, the qualifier now survives analysis.

## Gates

| gate | result |
|---|---|
| Red first — unit test file on the base tree | `0355ef5e` + branch `.so`: **34 failed, 5 passed** — the 5 green are the pre-existing outer/plain and SQL-door pins; every new-surface pin reds individually (TableArg import made tolerant so the module collects on base) |
| Unit test file | `python/repark/tests/test_df_subquery_1.py` — **41 passed** |
| Rust tests added | `crates/repark-core/src/session/tests/subquery.rs` — **5 passed** (exists rewrite, scalar guard wrap, lateral hoist, uncorrelated unwrap, generator refusal) |
| `make verify` | **green** — fmt, clippy both passes (`-D warnings`; `-D unwrap_used`/`expect_used`/`disallowed_methods`), crate-dag, lib-rs, rust-file-size, lib-py, python-conventions, docstring-presence, example-coverage, manifest, ledger-check, ledger-grammar |
| `test_dfcore_1_exports.py` | **10 passed** — four names + `subquery` submodule added via the declared-delta protocol in `_dfcore_1_expected.py` |
| #605 column pins | **72 passed** (column-parity cohort) |
| `python/repark-parity/tests` | **757 passed, 2 skipped, 12 xfailed** (756 + the `test_ex_0` enumerator pin, which failed only in a run that started before the 1059→1063 update; standalone rerun 26 passed) |
| Facade suite (`python/repark/tests`) | **8309 passed, 372 skipped, 2 xfailed** — one stale pin found: `test_udtf_table_arg_refuses` asserted the pre-unit table-arg refusal — flipped to `test_udtf_table_arg_feeds_rows` (a DataFrame arg now feeds each `Row` to `eval`, matching `astable_udtf_call`); the file's docstring and `tests/map.md` entry updated in the same diff |
| Comment fence | **clean** — no comment lines added in code; new Rust files carry zero comments per the io-text convention |

### Round-3 gates (on `b3fae405` + remediation)

| gate | result |
|---|---|
| Red first — new pins on `b3fae405` binary | **7 failed, 1 passed** — the 7 red are the S-10/S-11 pins (5 alias/limit pins above + the two correlated-`LIMIT` pins: `UnsupportedOperationException: Physical plan does not support logical expression ScalarSubquery(<subquery>)`); the S-13 pin stayed green on head because `left` is not exercised there — its red proof is the `left`→`Inner` revert run above |
| Unit test file | `python/repark/tests/test_df_subquery_1.py` — **49 passed** |
| Rust subquery pins | `cargo test -p repark-core --lib -- session::tests::subquery` — **7 passed** |
| `cargo test -p repark-core --lib -- session::tests` | **107 passed, 0 failed** |
| `make verify` | **green** — 3539 tests passed / 0 failed across 56 targets; fmt, clippy (`-D warnings`, disallowed-methods), crate-dag, lib-rs, rust-file-size (582), lib-py (753), python-conventions, docstring-presence, example-coverage, manifest, ledger-check, ledger-grammar (167 ledgers), docs-compaction, docs-links (5690), owner-ruling, parity-live dual-wire, matrix-liveness, ruff, taplo, typos all green |
| Facade suite (`python/repark/tests`) | **8831 passed, 367 skipped, 21 xfailed** (8823 baseline + 8 round-3 pins) |
| Parity suite (`python/repark-parity/tests`) | **757 passed, 2 skipped, 12 xfailed** |
| Focused cohort (`test_df_subquery_1.py` + `test_dfcore_1_exports.py` + `test_udtf.py`) | **88 passed** (49 + 10 + 29) |
| Comment fence (round 3) | prints nothing beyond the sanctioned existing `df_guard.rs` doc-comment edit already in `b3fae405` |

```yaml
COVERAGE_ATTESTATION:
  pr_unit: df-subquery-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        Clauses C-001..C-009 walked one by one against the oracle cells — every pin names its
        fixture cell and the verdict table carries the per-clause evidence; the measured
        Spark-classic quirks (inner-first unqualified binding, right-key drop, exists() field
        name) are pinned as recorded, not as assumed.
      artifacts: [task/ledgers/staging/df-subquery-1-ledger.md, python/repark/tests/test_df_subquery_1.py, python/repark/tests/facade_df_subquery_oracle.json]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Boundary inputs exercised: empty scalar subquery (NULL per row), empty EXISTS (all
        False), NULL dept row (4,null,null) through correlated and lateral paths, multi-row
        scalar, two-column scalar, `on` as None/str/list/Column, `how` as bad str/non-str,
        non-DataFrame `other`, TableArg ordering violations, uncorrelated vs qualified vs
        unqualified correlation.
      artifacts: [python/repark/tests/test_df_subquery_1.py]
    - id: AT-3
      status: ATTACKED
      evidence: >
        Every failure shape pinned: 42823 multi-column with `number` param, 21000 multi-row at
        execution, UNSUPPORTED_JOIN_TYPE 0A000 on four bad `how` spellings, CORRELATED_REFERENCE
        0A000 under generator/invalid parent, NOT_COLUMN_OR_STR on `select(TableArg)`,
        NOT_STR/NOT_DATAFRAME on arg-shape violations, both IllegalArgumentException texts on
        TableArg ordering. Round 3: a correlated `LIMIT >1` / unbounded correlated scalar now
        reaches the conditioned `Correlated scalar subquery must be aggregated to return at
        most one row` AnalysisException instead of a physical-planning crash.
      artifacts: [python/repark/tests/test_df_subquery_1.py, crates/repark-core/src/session/tests/subquery.rs]
    - id: AT-4
      status: ATTACKED
      evidence: >
        Ordering surface exercised: TableArg ordering guards (orderBy-before-partition and
        double-partition refusals), in-partition UDTF ordering, per-partition fresh handler
        instances; no shared mutable state — the single_row UDAF accumulator is per-group and
        SubqueryAlias wraps per-plan.
      artifacts: [python/repark/tests/test_df_subquery_1.py]
    - id: AT-5
      status: N/A
      justification: No privileged action, secret, or new trust-boundary crossing; the TABLE() call-arg parser is string-splitting inside the existing SQL-door UDTF path, no new SQL grammar.
    - id: AT-6
      status: ATTACKED
      evidence: >
        Compatibility surfaces pinned: `df.alias` keeps its temp-view side effect while the
        returned child gains SubqueryAlias (qualified refs resolve; the 558-test regression
        battery stayed green); export surface extended via the declared-delta protocol;
        `astable_tvf_explode_unchanged` pins the pre-existing explode path including its
        recorded nullability gap.
      artifacts: [python/repark/tests/test_dfcore_1_exports.py, python/repark/tests/_dfcore_1_expected.py, python/repark/tests/test_df_subquery_1.py]
    - id: AT-7
      status: N/A
      justification: Not system-breaking — the scalar guard adds one zero-group Aggregate node per scalar subquery; no unbounded growth, no hot loop; the UDTF table-arg path is row-batched through the existing Arrow bridge.
    - id: AT-8
      status: ATTACKED
      evidence: >
        Interface contracts honored: repark-core rules install in the card's order around
        scalar_subquery_to_join/decorrelate_lateral_join; all three join-mutation sites rebuild
        via Join::try_new (cached-schema fix proven by the qualified-correlated lateral cell);
        DataFusion 54.1 APIs used per its own Expr/LogicalPlan contracts; facade surfaces match
        PySpark 4.1.2 signatures.
      artifacts: [crates/repark-core/src/session/df_guards/subquery.rs, crates/repark-core/src/session/tests/subquery.rs, crates/repark-python/src/subquery.rs]
    - id: AT-9
      status: ATTACKED
      evidence: >
        Every failure path raises a loud conditioned error with Spark's error class, SQLSTATE,
        and message parameters; the two residuals on the optimizer-raised CORRELATED_REFERENCE
        (getCondition() unpopulated; sqlExprs quoting the internal array id rather than Spark's
        explode(array(id, sal))) are measured and recorded on the DF-SUBQUERY-1 registry row.
      artifacts: [python/repark/tests/test_df_subquery_1.py, docs/spark-sql-iceberg-parity.md]
    - id: AT-10
      status: ATTACKED
      evidence: >
        49 facade pins + 7 plan-level Rust pins + red-first on the base tree (34 failed, 5
        passed — the 5 green are pre-existing doors) and again on `b3fae405` for the round-3
        pins (7 failed, 1 passed — the 5 S-10/S-11 pins red on the head binary; the S-13
        pin reds on a local `left`→`Inner` revert, `assert 2 == 4`); every new facade branch
        has a nameable input that changes the output.
      artifacts: [python/repark/tests/test_df_subquery_1.py, crates/repark-core/src/session/tests/subquery.rs]
  complete: true
```
