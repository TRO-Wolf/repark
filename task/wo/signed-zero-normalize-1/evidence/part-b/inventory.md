# SIGNED-ZERO-NORMALIZE-1 part B: insertion inventory (keys)

Model `claude-opus-5-5`, 2026-11-12, on main `ae247767` (part A merged as `04d84ab9`). DataFusion is the pinned
54.1.0 from the cargo registry; arrow 58.4.1; the fork is the pinned `66901f02`. Cells are cited by the step-0 ids in
[step0/table.md](step0/table.md). "READ" means the code was opened at the cited line; "MEASURED" means a step-0 cell
holds it; "INFERRED" means neither, and the builder confirms it in item 1.

**The guarded property.** Every float key position (grouping, distinct, join, set operation, window partition and
order, sort, repartition, and the non-literal float comparisons) treats `-0.0` and `0.0` as one value and every NaN as
one value, greatest, as Spark does. Stored values are never normalised, except where Spark itself outputs the
normalised key (group and distinct outputs, set-operation outputs).

**Why a seat is needed (MEASURED and READ).** DataFusion hashes floats by their raw bits
(`datafusion-common-54.1.0/src/hash_utils.rs:187-192`, `hash_float_value!`), and the arrow comparison kernels and
`ScalarValue` order floats by totalOrder. So a hash join, a hash aggregate, a sort and a window partition all split
`-0.0` from `0.0` and a sign-set NaN from the canonical one. Step 0: 438 attributable WRONG-READ and 74 WRONG-WRITE
cells across every site and door.

## 1. Where key evidence is created, moved or destroyed

One row per place a key position or a float pairing comes into existence or changes, answered for seat 2 (the late
optimizer rule that wraps float keys in `__repark_normalize_zero__`).

| # | Where | Creates or moves | Seen at seat 2? | Because / evidence |
|---|---|---|---|---|
| 1 | SQL planner (`datafusion-sql`): `GROUP BY`, `DISTINCT`, `JOIN … ON / USING / NATURAL`, `UNION / INTERSECT / EXCEPT [ALL]`, `OVER (PARTITION BY … ORDER BY …)`, `ORDER BY` | `Aggregate.group_expr`, `Distinct`, `Join.on` (USING and NATURAL become `on` pairs), `Union`, the semi and anti joins of `INTERSECT`/`EXCEPT` (`LogicalPlanBuilder::intersect`/`except`), `Window`, `Sort` | **yes** | Seat 2 runs last in every optimizer pass, after all of these exist. GB-*, DI-*, JN-*, SO-*, WI-*, OB-* |
| 2 | Session analyzer list: `type_coercion`, `struct_setop_names` (unit C, after coercion), `struct_operands_by_position` (#1292/#1295, before coercion), the Spark-door join-condition rule, `SparkIntegralLiteral`, seat 1 (`signed_zero_literals`, last core rule) | coercion casts on key pairs; struct relabelling; seat 1 rewrites only literal-zero comparisons | **yes**, later | None of them builds a float key; seat 1 leaves a bare float key alone ("a bare float key stays part B's", `signed_zero/map.md`). Seat 2 sees their output |
| 3 | The `F.expr` analyzer (`build_expr_context`, `expr_build.rs:276-305`) | analyses a column-free fragment eagerly; returns a column fragment unanalysed | **yes, on the consumer session** | The fragment is embedded in the consumer frame's plan, which runs the full optimizer. MEASURED: every `f-expr` key cell (GB-1, DI-1, JN-*, WI-1) |
| 4 | DataFrame binders (`repark-python`): `join_on_names` `dataframe/mod.rs:571`, `aggregate` `:629`, `union` `:658`, `distinct` `:698`, `distinct_on` `:714`; `join_on_keys` `dataframe_names.rs:144`; RePark's `join_exact_sides` `df_guards/join_exact.rs:23`; the facade's set operations as SQL text (`core.py:2628`), `drop_duplicates` `:2680`, `group_by` `:2348`, `join` `:2151`; pivot discovery `joins_columns.py:506` | `LogicalPlanBuilder` joins, aggregates, unions, distincts; the facade's SQL text set operations | **yes** | Each builds an ordinary logical plan that the session's optimizer runs. MEASURED: the `typed` cells. The pivot's explicit-value arm is literal (`CASE WHEN k = v`), part A's seat 1 |
| 5 | Optimizer rules that create or move keys (DataFusion 54.1.0 `optimizer.rs:281-310`): `rewrite_set_comparison`, `replace_distinct_aggregate` (Distinct → Aggregate), `decorrelate_predicate_subquery` (`IN`/`EXISTS` → semi/anti joins, `NOT IN` → null-aware anti join), `scalar_subquery_to_join`, `decorrelate_lateral_join`, `extract_equijoin_predicate` (filters → `on`), `eliminate_cross_join` (comma joins → `on`), `filter_null_join_keys`, `push_down_filter` (moves join predicates), `single_distinct_to_groupby` (distinct aggregate → group key), `eliminate_group_by_constant`, `common_sub_expression_eliminate` (`__common_expr_N`), `optimize_projections`; RePark's `ReparkScalarSubqueryGuard`, `ReparkProjectionExists`, `ReparkLateralProjectionHoist` (`df_guards.rs:122-130`) | the key lists seat 2 must wrap | **yes**: seat 2 is appended after the whole list, so in each pass it sees every key these rules made | The sketch's reason seat 2 cannot be an analyzer rule. MEASURED: SQ-in, SQ-exists, SQ-scalar, JN-comma, DI-3, DI-4, SO-intersect, SO-except |
| 6 | The `PREPARE` arm's bare optimizer (`datafusion-54.1.0/src/execution/context/mod.rs:734-760`) and `EXECUTE` | the stored plan is optimised at `PREPARE`, then analysed and optimised again at `EXECUTE` | **yes, twice** | This is the review-cap memo's seat-2 idempotence row: the key must be wrapped once after both runs. MEASURED: PR-1, PR-2 (native) |
| 7 | Stored plans re-entering: views (`create_view` stores the coerced body), temporary views (`temp_views.rs:166` replans through the session), `cache()`, DataFrame temp views | the body re-enters the optimizer | **yes** | Re-planned through the session optimizer; idempotence (§3) keeps one wrap. MEASURED: TV-1 (Spark door; native has no temporary views) |
| 8 | Physical planning: `EnforceDistribution` (hash repartition on join and aggregate keys), `JoinSelection`, sort-merge join, `RepartitionExec` hash, `AggregateExec` group values (`aggregates/group_values/single_group_by/primitive.rs` hashes by bits), `SortExec` / TopK comparators, `BoundedWindowAggExec` partition ranges; RePark's `ParallelWindowRule` (`parallel_window/rule.rs:56-58` only fires with no `partition_by`), `ParallelProjectionRule`, `NljBuildSideReset` | physical key expressions | **yes, by derivation** | Physical keys are planned from the logical key expressions, so a wrapped logical key is the physical key (INFERRED for every physical rule; MEASURED for hash join, hash aggregate, sort, window, sort-merge: JN-sortmerge). Spill paths: NOT MEASURED (order pin D16) |
| 9 | Hash-join and TopK dynamic filters (`enable_join_dynamic_filter_pushdown`, default true, `datafusion-common-54.1.0/src/config.rs:1077`) | min/max and membership filters derived from build-side keys, pushed to the probe scan | **yes, derived from the wrapped key** | Correct after the wrap, but a filter over `normalize(d)` cannot prune Parquet statistics: a cost, not a correctness, item (order, cost gates) |
| 10 | MERGE match discovery (`repark-iceberg/src/write/merge/merge_sql.rs:132-145`: `… FROM source JOIN target ON {on} GROUP BY _file, _pos`, run by `ctx.sql` on the session, `merge/mod.rs:516`) and the cardinality check (`merge/copy_on_write.rs:230-235`: `match_count > 1`) | the MERGE `ON` join and the per-target-row match count | **yes** | It is SQL text planned on the session context, so seat 2 wraps its join keys. MEASURED: WR-merge-on, WR-merge-notmatched, WR-merge-card (m1: RePark accepts where Spark raises `MERGE_CARDINALITY_VIOLATION`) on three kinds |
| 11 | Row-level DELETE / UPDATE / INSERT with a subquery on a float key | semi, anti and null-aware anti joins inside DML | **yes** | MEASURED: WR-delete-in, WR-delete-exists, WR-update-in, WR-insert-notin on three kinds. The native door refuses a subquery in DML on every type |
| 12 | CTAS / INSERT from a `GROUP BY` or `DISTINCT` | stores the group output value | **yes** (the group output is the wrapped key) | Spark stores `0.0` (WR-ctas-group, WR-ctas-distinct). Plain projections are never wrapped, so `-0.0` in a plain `INSERT … SELECT` stays bit for bit (part A's D11; UNION ALL control SO-unionall) |
| 13 | Iceberg identity partitioning on a float column: `PartitionValueCalculator` (fork) and RePark's router (`write/distribution/router.rs:33-36`, `create_hashes` over partition values) | partition values from stored values; slot routing by bits | **no, and must not be** | Spark writes separate `-0.0` and `0.0` partitions; RePark does too (`partition_check.py`; PT-1 EQUAL). Routing by bits only places rows. Out of seat 2 |
| 14 | Iceberg `.partitions` / `.files` metadata tables | aggregate partition values | **no** (metadata-table code, not a plan key) | MEASURED PT-3: RePark's `.partitions` merges `-0.0` with `0.0` and NaN with NULL (3 rows; Spark 5); `.files` shows the NaN partition as NULL. Carded `ICEBERG-PARTITIONS-TABLE-FLOAT-1`; the manifest value is NOT MEASURED |
| 15 | Write-sort keys (`write/distribution.rs:275`, `CanonicalFloatExpr`, `distribution/canonical_float.rs:17-64`) | the table's sort order as a physical sort key | **no, not needed** | It canonicalises NaN only and orders rows inside files; no answer depends on the order of `-0.0` against `0.0` there. Its NaN map is the precedent the UDF's kernel can share |
| 16 | Metadata-only DELETE and conflict filters (`meta_delete.rs`, `conflict_filter.rs`) | literal predicates only | n/a | Part A's seat (named float-zero guard in `literal_datum`) |
| 17 | MERGE target pruning (`merge/target_scan.rs:31`, `scan_prune.rs:248, 342`) | residual join-key pruning | n/a | Integer keys only (READ); a float key never prunes |
| 18 | Fork scan pushdown (`expr_to_predicate.rs`) | filters into Iceberg | n/a | Seat 2 never touches `TableScan.filters` (sketch §3), and `__repark_normalize_zero__` is not convertible, so nothing new reaches an Iceberg evaluator |
| 19 | Distributed executors (`repark-distributed/src/codec.rs:64, 157`; `cluster.rs:479`) | plans decoded on executors | **no today** | UDFs resolve by name; executors must register the UDF and rules when the distributed door lands. Carded `DIST-SIGNED-ZERO-1` (sketch §4) |
| 20 | Other core contexts: `transpose.rs:269, 292` (built from the session state), `microbatch/run.rs:407` (a snapshot of the session state) | ordinary plans | **yes** | Both reuse the session state, so its optimizer rules |

## 2. Consumers of the pairing (Q4): float equality or ordering the key wrap cannot reach

Each entry is a part B seat, a part A seat, declared, or carded with its id. Read from the pinned 54.1.0 source and
RePark main.

| Consumer | Where | Disposition |
|---|---|---|
| `Join.on` pairs of every kind, including decorrelated `IN` / `EXISTS` / scalar subqueries, USING, NATURAL, comma joins | logical `Join` | **part B seat** (JN-*, SQ-*) |
| `Join { null_aware: true }` (`NOT IN` subquery) | `decorrelate_predicate_subquery` | **owner question Q1.** Spark's default null-aware broadcast anti join keeps `-0.0` distinct, unlike every other Spark plan for the same query (`raw/spark-naaj-check.jsonl`). Default: wrap it (IEEE), and declare Spark's default-plan answer an oracle defect |
| `Join.filter` and every non-literal float comparison (`= <> < <= > >= <=> IS DISTINCT FROM`, `BETWEEN`, `IN` list items, simple `CASE` `WHEN`, aggregate and window `FILTER`) | expressions in any node | **part B seat** (CM-1…CM-12, JN-nonequi). A literal-zero comparison is part A's seat 1 |
| Aggregate group keys, `GroupingSet` members, distinct-aggregate arguments | `Aggregate` | **part B seat** (GB-*, DI-*) |
| `Distinct::All` / `Distinct::On` | `Distinct` (an `Aggregate` by the time seat 2 runs) | **part B seat** (DI-1, DI-2; `DISTINCT ON` has no oracle) |
| Set operations, `ALL` and distinct | `Union` + `Aggregate`, semi / anti joins | **part B seat** for the float pairing (SO-*). The multiset counts of `INTERSECT ALL` / `EXCEPT ALL` are wrong for every type (decimal twin WRONG-READ): carded `SQL-SETOP-ALL-MULTISET-1`, owner question Q2 |
| `Window` partition and order keys | `Window` | **part B seat** (WI-*) |
| `Sort` keys, TopK | `Sort` | **part B seat** (OB-1…OB-3). Ties with no tie-breaker are declared ORDER-DIFFER (OB-4) |
| `Repartition` hash keys | `Repartition` | **part B seat** (GB-10 typed and native) |
| `RecursiveQuery { is_distinct }` | `RecursiveQuery` | **part B seat** by the sketch's node list; no Spark oracle (Spark refuses `UNION` there); plan-shape pin |
| Physical hashing and comparison: `HashJoinExec`, `AggregateExec` group values, `RepartitionExec`, `SortMergeJoinExec`, `SortExec`/TopK, window partitioning | `physical-plan` | **part B, by derivation** from the wrapped logical keys |
| Hash-join / TopK dynamic filters | `physical-plan` | **part B, by derivation**; a cost item |
| `min` / `max` (`AggregateStatistics` may answer from file statistics) | `functions-aggregate`, physical optimizer | **declared** SIGN-ONLY (MM-1) |
| `nullif`, `greatest` / `least`, `approx_distinct`, `mode`, `median` ties, the legacy division guard `nullif(divisor, 0.0)` | `datafusion-functions*`, `repark-functions/src/analyzer.rs` | **carded** `FUNC-FLOAT-EQ-1` (ruling G) |
| Array and map functions (`array_contains`, `array_position`, `array_distinct`, `array_union` / `intersect` / `except`, map subscript), `collect_set` | `datafusion-functions-nested` | **carded** `COLLECTION-FLOAT-EQ-1`; seat 2 must not wrap function arguments (AR01, AR02 stay EQUAL) |
| Floats nested in struct, array or map keys | nested `eq` / row format | **part 2** (the nested-float order, ruling 2026-11-11 Q3); `NESTED-FLOAT-KEY-1` |
| Iceberg partition metadata tables | `.partitions`, `.files` | **carded** `ICEBERG-PARTITIONS-TABLE-FLOAT-1` (PT-3) |
| Facade `dropDuplicates`, `is_duplicated` (`repark-python/src/is_duplicated.rs:146`), `freqItems` (`freq_items.rs:153`) | facade helpers | `dropDuplicates` is a plan aggregate, **part B seat** (DI-6). `is_duplicated` and `freqItems` already normalise (precedents for the shared helper) |
| Pivot discovery | facade `pivot` | **part B seat** via the discovery aggregate; GB-11 is confounded by `PIVOT-EMPTY-CELL-COUNT-1` |

## 3. Seat 2: where it sits and how its idempotence is proven

**Placement.**
- **Rule:** `signed_zero_keys`, an `OptimizerRule` in `crates/repark-core/src/session/df_guards/signed_zero/keys.rs`.
- **List position:** appended after the loop in `unnest_safe_optimizer_rules` (`df_guards.rs:117-142`), so it runs last
  in every pass, after `optimize_projections`.
- **UDF:** `__repark_normalize_zero__`, in `signed_zero/udf.rs`. It is registered in
  `context_with_df_54_1_rule_guards` beside `register_repark_isnan` (`df_guards.rs:100`).
- **Switch:** the rule reads the per-door `keys` switch (design point 3 of the order). With the switch off it is
  installed but returns `Transformed::no` at the root, so the rule list (and its pin) is the same on both doors.

**What it wraps.** Every `Float32`/`Float64` key expression in the positions of §2 marked "part B seat". It never wraps
a literal: a literal in a wrapped position is canonicalised in place. It never wraps function arguments, plain
projection values or `TableScan.filters`.

**Idempotence: the three mechanisms (sketch §1).**
1. Skip an expression that is already a `__repark_normalize_zero__` call.
2. Skip a column whose defining expression in the immediate input is such a call: a `Projection` expression, an
   `Aggregate` group expression or a `Window` expression, through `Alias` and `SubqueryAlias`. This covers
   `common_sub_expression_eliminate`'s `__common_expr_N` columns in later passes, and group outputs read by the
   parent.
3. The UDF's `simplify` collapses `normalize(normalize(x))`.

Correctness never depends on them, because the function is idempotent; the pins hold the single-wrap property.

**Proof obligations (the pins), one per re-entry path of §1:**

| Re-entry | Pin |
|---|---|
| Several passes in one optimizer run (`max_passes`) | Optimize a plan with every §2 node kind twice; the plan is identical, and each float key carries exactly one wrapper (EXPLAIN count per key) |
| `PREPARE` then `EXECUTE` (review-cap memo §7) | Native: `PREPARE p(DOUBLE) AS SELECT d, count(*) … GROUP BY d` and the PR-2 join; `EXPLAIN EXECUTE p(-1.0)` shows one wrapper per key; the answers are PR-1/PR-2's truth |
| Views, temporary views, `cache()` | TV-1 plus an `EXPLAIN` over a view whose body groups on a float key: one wrapper |
| `F.expr` fragments analysed in `EXPR_CONTEXT` and re-planned | the `f-expr` key cells' `EXPLAIN`: one wrapper |
| CSE-shared keys (`PARTITION BY d ORDER BY d`, a key used in two places) | WI-6-style query: one `normalize(d)` shared, not stacked |
| Group output read by a parent (`SELECT x.d FROM (… GROUP BY d) x JOIN …`) | the parent's key is not re-wrapped (mechanism 2), checked by EXPLAIN |

**Order against today's rules.**
- **Seat 1:** an analyzer rule, and seat 1b runs right after `simplify_expressions`. Both run before seat 2 in every
  pass, and they rewrite only literal-zero comparisons. Seat 2 skips a comparison with a literal operand, so the two
  never touch the same node.
- **Unit C's `struct_setop_names` and #1292/#1295's struct rules:** analyzer rules on struct types only. Seat 2
  touches only `Float32`/`Float64` keys, so floats inside structs are left alone (part 2).
- **`LISTVIEW-INNER-JOIN-KEY-1` (#1304, merged on main):** it changed the key-name join path. Seat 2 sees its
  output join keys like any other. Item 1 re-checks the `join_on_names` cells (JN-using typed) on the branch.
