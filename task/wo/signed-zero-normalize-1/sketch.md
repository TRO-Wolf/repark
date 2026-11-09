# SIGNED-ZERO-NORMALIZE-1 — design sketch (off-lane, design only)

Sketcher: Claude Opus 5.5 (`claude-opus-5-5`), 2026-11-08. Input to owner ruling G and to the build brief. Nothing
here is built, committed or measured by the sketcher.

Sources read: the Fable consult memo (§Q3 and the reconciliation), the Hermes audit (findings 1–2 that change a
recommendation, 1–3 that do not), `signed-zero-table.md` and its probe on
`origin/docs/followup-dml-conjunct-and-signed-zero`, the envelope `findings.md` p15 rows, the extension table
`signed-zero-extension-table.md` at commit `94e1843e` (E1–E9, R1–R5), and code at `origin/main` (`bd9c5437`). The
fork is the pinned `66901f02` that `Cargo.lock` names (`Cargo.lock:3301-3303` on main, and the same at `fdc61f26`,
the wheel the extension measured), DataFusion 54.1.0, arrow 58.4.1, parquet 58.4.1. Every `file:line` below was
opened.

## 0. What changed against the memo (read first)

1. **The pinned fork already keeps every float-zero predicate out of Iceberg pushdown.**
   `convert_filters_to_predicate` filters each converted conjunct through `predicate_binds_soundly`
   (`crates/integrations/datafusion/src/physical_plan/expr_to_predicate.rs:52-60, 62-83`). Inside it,
   `literal_binds_soundly` returns `false` for a `Float`/`Double` literal equal to zero on a float column
   (`:116-118`). Both `±0.0` hit that guard, which is applied to `Binary` and `Set` predicates and recurses through
   `And`/`Or`/`Not`. So on the DataFusion scan path, none of `d = 0.0`, `d IN (0.0, …)`, `d >= -0.0` or any
   rewritten form reaches `InclusiveMetricsEvaluator`, the page index, or `Reference::is_in`.
   - The memo's pruning analysis (the "`d = -0.0` branch keeps the file" argument) and the audit's `IN` hash-set
     consequence do not apply to that path at this revision.
   - The RePark misses the extension measured on two-file and paged tables (E1, E3, R1) are row-level totalOrder
     misses, not pruning. RePark's comparison misses `-0.0` whether or not the file survives.
2. **Two RePark SQL-text translators build Iceberg predicates outside the logical plan, so neither seat can reach
   them.**
   - `meta_delete.rs` (the metadata-only DELETE) and `conflict_filter.rs` (commit conflict filters) parse the SQL
     text with sqlparser. They emit `equal_to(Datum::double(0.0))` and `is_in(...)` through the shared
     `literal_datum` (`crates/repark-iceberg/src/write/conflict_filter.rs:284-303, 317-330`; `meta_delete.rs:15,
     242-248, 275-286`).
   - The Spark door tries the metadata delete **before** the row-level path (`crates/repark-spark/src/router/delete_update.rs:36-39`).
     The native door does too (`crates/repark-sql/src/router.rs:243`).
   - The fork's `can_delete_using_metadata` drops files that the totalOrder inclusive evaluator prunes and deletes
     files the strict evaluator proves all-match (`crates/iceberg/src/table.rs:338-397`). On the E1 two-file
     table (one file `min = max = -0.0`, one `min = max = 0.0`), `DELETE … WHERE d = 0.0` therefore commits as a
     metadata delete of the `0.0` file alone. The `-0.0` row survives even after both seats land.
   - This is the node the audit missed that matters most. The guard belongs in this unit (§5).
3. **Writer normalisation is refuted by measurement.** pyarrow reads `min = max = -0.0` on a RePark-written
   `-0.0`-only file (extension preamble), although parquet 58.4.1's encoder calls `replace_zero`
   (`parquet-58.4.1/src/column/writer/encoder.rs:363-364`). The design does not rely on any writer behaviour.
4. **The truth column is Spark SQL semantics, not Spark-on-Iceberg.** Spark 4.1.2 with Iceberg 1.11.0 drops the
   `-0.0`-only file and page for `d = 0.0`, `IN (0.0)`, `IN (0.0, 10.0, 20.0, 30.0)` and `d >= 0.0`. Spark's direct
   Parquet read returns both zeros (extension E1, E3, E4, R1). Those Spark-on-Iceberg answers are labelled
   `PRUNED-WRONG` (an oracle defect) and declared. RePark follows the single-file and direct-Parquet answers, on the
   AUD-024 precedent.
5. **The literal rewrite is changed from the memo's OR form to a bound form that is a fixpoint** (§2). The rewrite
   then runs safely at two seats (analyzer, and right after `simplify_expressions`) and in every optimizer pass. No
   NaN result changes, and any totalOrder-based optimizer or evaluator reasoning stays exact.

---

## 1. Seats and installation

Three rule objects and one UDF, all in `repark-core`. The new submodule is `crates/repark-core/src/session/df_guards/signed_zero.rs`
with `literals.rs`, `keys.rs` and `udf.rs`, beside the existing `df_guards/` submodules (`df_guards.rs:18-30`).
`repark-core` does not depend on `repark-functions` (`crates/repark-core/Cargo.toml:21-28`), so nothing here can
live in the Spark-only function crate. A core-registered UDF has a precedent: `register_repark_isnan`
(`crates/repark-core/src/isnan.rs:15-17`, called at `df_guards.rs:95`).

| piece | kind | installed at | runs |
|---|---|---|---|
| `__repark_normalize_zero__` | `ScalarUDF` | `context_with_df_54_1_rule_guards`, beside `register_repark_isnan` (`df_guards.rs:92-96`) | n/a |
| seat 1 `signed_zero_literals` | `AnalyzerRule` | `df_guards::install_signed_zero_literals(&context)`, called once from `ReparkSessionBuilder::build` right after `ext.register` and `dialect.on_session_built` (`crates/repark-core/src/session.rs:319-327`) | last of the core-built analyzer rules |
| seat 1b `signed_zero_literals` | `OptimizerRule` (same rewrite) | `unnest_safe_optimizer_rules`: a new match arm pushes it right after DataFusion's `simplify_expressions` (`df_guards.rs:106-127`) | every pass, before `replace_distinct_aggregate` and `push_down_filter` |
| seat 2 `signed_zero_keys` | `OptimizerRule` | `unnest_safe_optimizer_rules`, appended after the loop (`df_guards.rs:106-127`) | every pass, last |

**Seat 1 order on a Spark-door session after the unit.**

1. DataFusion's `resolve_grouping_function` and `type_coercion` (`datafusion-optimizer-54.1.0/src/analyzer/mod.rs:87-93`),
   with the extension's pre-coercion inserts: higher-order preparation, `SparkIntegralLiteral`, the join-condition
   rule and map subscript (`crates/repark-spark/src/extension.rs:135-144`;
   `spark_literal_typing.rs:42-52`).
2. `sliding_frame_rescan` (`df_guards/window_rescan.rs:22-27`).
3. The register-time Spark rules (`extension.rs:149-164`), including `FoldSparkNumericCasts` (`:159`).
4. **`signed_zero_literals`.**
5. The Python `install_shared_analyzer_rules` (integer overflow and bool→decimal) (`crates/repark-python/src/session.rs:26`;
   `crates/repark-functions/src/bool_decimal.rs:28-31`). Neither builds a float comparison.

Seat 1 runs after type coercion, because it needs operand types. It also runs after Spark literal widening and
folding: `CAST(Decimal128(0,1,1) AS Float64)` has already become `Float64(0.0)` (`spark_typed.rs:43-83`). It runs
before any optimizer rule, so the rewritten predicate is what filter pushdown sees.

The alternative is the tail of the base list `analyzer_rules_with_df_54_1_rule_guards` (`df_guards.rs:100-103`).
It works but runs before `FoldSparkNumericCasts`. It is kept as the fallback if the post-register call site is
rejected. The recogniser below accepts cast-of-literal shapes either way.

**Seat 1b.** The same rewrite runs after `simplify_expressions` (`datafusion-optimizer-54.1.0/src/optimizer.rs:285`),
which is before `push_down_filter` (`:301`). It exists for zero operands that become literals only in the
optimizer:
- constant folding (`d = 1 - 1.0`);
- the native `CAST('-0.0' AS DOUBLE)`, where the native door has no `FoldSparkNumericCasts`;
- placeholders bound after analysis, which a server door with prepared statements will produce.

Because the rewrite is a fixpoint (§2), running it in every pass is safe.

**Seat 2.** Appended after DataFusion's list, so in each pass it runs after:
- `replace_distinct_aggregate` (`optimizer.rs:286`; the rule at `replace_distinct_aggregate.rs:83-195`);
- `decorrelate_predicate_subquery` (`:288`) and `scalar_subquery_to_join` (`:289`);
- `extract_equijoin_predicate` (`:291`) and `eliminate_cross_join` (`:294`);
- `push_down_filter` (`:301`) and `single_distinct_to_groupby` (`:302`).

By then every key list it must wrap exists as a plan field. The optimizer runs up to `max_passes` passes
(`optimizer.rs:388`), so seat 2 must not stack wraps. Idempotence:
- (i) skip an expression that is already a `__repark_normalize_zero__` call;
- (ii) skip a float column whose defining expression in the immediate input (a `Projection` expression, an
  `Aggregate` group expression, or a `Window` expression, through `Alias`/`SubqueryAlias`) is such a call; this
  covers `common_sub_expression_eliminate`'s `__common_expr_N` columns in later passes;
- (iii) the UDF's `simplify` collapses a directly nested call (§4).

Correctness never depends on (i)–(iii), because the function is idempotent. The plan-shape pin (§10) holds the
single-wrap property.

**Every route gets both seats.**

| route | session that analyses and optimises the statement | seat 1 / 1b / 2 |
|---|---|---|
| `spark.sql` | `PyReparkSession::new` → `ReparkSessionBuilder::build` with `SparkDialect` + `SparkExtension` (`crates/repark-python/src/session.rs:112-131`) | yes / yes / yes |
| `filter(str)` | parsed on the frame's own session state (`crates/repark-python/src/column/expr_build.rs:46-72`); the filter plan runs on the same core session | yes / yes / yes |
| `F.expr` | parsed in `EXPR_CONTEXT`, a standalone `SessionContext` that is not a core session (`expr_build.rs:263-298`). A column-referencing fragment comes back unanalysed (`parse_unresolved_expr`, `expr_build.rs:225-251`). A column-free one is analysed eagerly there. Either way the `Expr` is embedded in the consumer frame's plan, and the consumer core session runs the full analyzer and optimizer at execution. | yes / yes / yes, on the consumer session. `EXPR_CONTEXT` itself gets nothing and needs nothing (the rewrite is idempotent, so a later consumer pass is exact) |
| typed Column API | `PyColumn` expressions embedded in the consumer frame | yes / yes / yes |
| native `repark.sql()` | `PyReparkSession::native` → the same builder with `NativeDoorTypes` (`session.rs:148-166`; `session_runtime.rs:404-427`) | yes / yes / yes |
| future server door | any session built by `ReparkSessionBuilder::build` | inherits all three |

A per-door policy hook (§8) lets a door switch seat 1/1b or seat 2 off without moving code.

---

## 2. Seat 1 rule table

**Zero operand.** An expression that is one of:
- `Literal(Float32|Float64 ±0.0)`;
- `Cast`/`TryCast` of a numeric, decimal or string `Literal` to `Float32`/`Float64`, whose `ScalarValue::cast_to`
  value is `±0.0` (the same evaluation `FoldSparkNumericCasts` uses, `spark_typed.rs:76-82`);
- `Negative` of either.

The other side `e` is **any** float-typed expression, not only a column. That covers the extension's `d * 0 = 0`
(E7: RePark 3 rows against truth 4 on all five routes). `e` may itself be a zero operand (literal against literal).

**Canonicalisation.**
1. A zero operand on the left becomes `e swapped_op z`, with `<`↔`>`, `<=`↔`>=`, and `=`, `<>`, `<=>`, `IS DISTINCT FROM`
   symmetric.
2. A `BETWEEN` with a zero bound is expanded first: `e BETWEEN lo AND hi` → `e >= lo AND e <= hi`; `NOT BETWEEN` →
   `e < lo OR e > hi`. Seat 1 must do this itself, because DataFusion expands `BETWEEN` only in the optimizer
   (`expr_simplifier.rs:1645-1654`).

**Rewrite.** Let `T` be totalOrder as the arrow kernels and the fork use it (`arrow-ord-58.4.1/src/cmp.rs:70-78`;
fork `datum.rs:219-227`): `-NaN < -Inf < negatives < -0.0 < +0.0 < positives < +Inf < +NaN`. In the table, `z` is
either zero.

| form | rewrite | proof under `T` (for every non-NaN `e`, with NULL → NULL kept) |
|---|---|---|
| `e < z` | `e < -0.0` | IEEE `e < 0` ⇔ `e` is a nonzero negative ⇔ `e <_T -0.0` |
| `e <= z` | `e <= +0.0` | IEEE `e <= 0` ⇔ `e` is negative or a zero ⇔ `e <=_T +0.0` |
| `e > z` | `e > +0.0` | IEEE `e > 0` ⇔ `e` is a nonzero positive ⇔ `e >_T +0.0` |
| `e >= z` | `e >= -0.0` | IEEE `e >= 0` ⇔ `e` is a zero or positive ⇔ `e >=_T -0.0` |
| `e = z` | `e >= -0.0 AND e <= +0.0` | `[-0.0, +0.0]_T = {-0.0, +0.0}`; NULL gives `NULL AND NULL` = NULL |
| `e <> z` | `e < -0.0 OR e > +0.0` | the complement of the row above; NULL gives `NULL OR NULL` = NULL |
| `e IN (…, z, …)` | `e IN (rest) OR (e >= -0.0 AND e <= +0.0)`; `e IN (z)` → the bound pair | IN is a disjunction of `=`; dropping the zero items from the list and adding the exact `=` row is equivalent, including a NULL item (`NULL OR …` keeps the 3VL value) |
| `e NOT IN (…, z, …)` | `e NOT IN (rest) AND (e < -0.0 OR e > +0.0)` | the dual of the row above |
| `e <=> z` | `e IS NOT NULL AND e >= -0.0 AND e <= +0.0` | NULL gives `false AND …` = false; non-NULL gives the `=` row, never NULL |
| `e IS DISTINCT FROM z` | `e IS NULL OR e < -0.0 OR e > +0.0` | NULL gives true; non-NULL gives the `<>` row, never NULL |
| `NOT (any row above)` | no special case; the inner rewrite applies | each rewrite equals the original IEEE comparison as a 3-valued function of `e`, so any context around it (NOT, CASE, IS TRUE, projection) keeps its value |

**Fixpoint.** Every output literal is already the canonical bound for its operator. Re-applying the table to
`e >= -0.0`, `e <= +0.0`, `e < -0.0` or `e > +0.0` is the identity. The memo's `d = 0.0 OR d = -0.0` form would
double at each pass; this form does not.

Where `e` is not a column it appears twice. `common_sub_expression_eliminate` (`optimizer.rs:306`) shares it.

**NaN results are unchanged.** NaN sits outside `[-0.0, +0.0]_T` on both signs, and each rewrite moves only the
boundary between `-0.0` and `+0.0`. So every NaN input gives the same result before and after:
- `+NaN`: false for `<` and `<=`, true for `>`, `>=`, `<>` and `IS DISTINCT FROM`, false for `=` and `<=>`;
- `-NaN`: true for `<` and `<=`, false for `>` and `>=`, true for `<>`.

`-NaN`'s placement below `-Inf` is a pre-existing divergence that seat 1 does not touch (see §6).

**Optimizer interactions.**
- `ScalarValue` equality is bitwise and its order is totalOrder (`datafusion-common-54.1.0/src/scalar/mod.rs:453,
  488-489, 588, 644-645`).
- `simplify_predicates` keeps the most restrictive bound by `try_cmp` and folds unequal equalities to `false`
  (`simplify_expressions/simplify_predicates.rs:135-145, 185-231`, called at `push_down_filter.rs:790`).
- Seat 1 removes every zero equality and makes every zero bound IEEE-exact under totalOrder, so those merges become
  sound. Today `d = 0.0 AND d = -0.0` folds to `false` (a WRONG-READ on native, `F.expr` and typed; IEEE says both
  zero rows). After seat 1 it keeps both rows.
- The OR→`IN` folding (`expr_simplifier.rs:1785-1809`, guarded by `are_inlist_and_eq`, `:2207-2219`) and
  `ShortenInListSimplifier` (`inlist_simplifier.rs:38-56`) never see a zero item after seat 1.

**Pushdown at the pinned fork.**

| form after seat 1 | pushed into the fork's scan? | why (`expr_to_predicate.rs`) |
|---|---|---|
| any form carrying a float zero literal (all rows above) | no, the whole top-level conjunct is dropped | `literal_binds_soundly` zero arms `:116-118`, applied through `predicate_binds_soundly` `:62-83` |
| `<=>` / `IS DISTINCT FROM` with a non-NaN literal | no | `to_iceberg_operation` has no such arm (`:444-458`); only `<=> NaN` becomes `is_nan` (`:507-526`). The memo's `:220` belongs to another revision |
| `NOT` over a float predicate | no | `not_operand_is_sound` (`:430-442`) |
| float predicates with a nonzero literal | yes, unchanged by the unit | |

Nothing that used to push stops pushing: every zero-literal predicate was already refused. Pushdown stays
`Inexact` (fork `crates/integrations/datafusion/src/table/mod.rs:236-242`).

**How the bound form keeps the zero-only files** (point 1 of the coordinator's note). The memo's "`d = -0.0`
branch" is the `e >= -0.0` lower bound here. Take file A (`min = max = -0.0`) and file B (`min = max = +0.0`) under
the fork's totalOrder evaluators:

| predicate | file A | file B |
|---|---|---|
| `d >= -0.0 AND d <= +0.0` | kept: `upper -0.0 >= -0.0`, `lower -0.0 <= +0.0` | kept |
| `d < -0.0 OR d > +0.0` | pruned (correct: every row is a zero) | pruned (correct) |

The same holds for DataFusion's row-group, page-index and bloom pruning: any `[min, max]` meeting `[-0.0, +0.0]` is
kept, whatever the writer did with zero statistics.

So the fork's bounds path needs no fix for this unit. Two reasons: the guard keeps zero literals away from it, and
the bound form would be exact even if the guard were relaxed. Raw zero datums from other callers are fixed on the
RePark side (§5). A fork-side change matters only if someone later relaxes `literal_binds_soundly`; it is carded
(§10).

**Spark literal typing per route** (point 2 of the coordinator's note). The rewrite depends only on the operator,
never on the literal's sign. So every route converges, including those where Spark drops the sign.

| route | parser setting | `0.0` / `-0.0` | `CAST('-0.0' AS DOUBLE)` | integer `0` | measured today |
|---|---|---|---|---|---|
| `spark.sql`, `filter(str)` | `parse_float_as_decimal = true` (`crates/repark-spark/src/extension.rs:17-21`) | both `Decimal128(Some(0),1,1)`: the sign of `-0.0` is lost (`datafusion-sql-54.1.0/src/expr/value.rs:93-94, 361-369`; `unary_op.rs:63-70`). `SparkIntegralLiteral` widens to `CAST(… AS Float64)` for `= <> < <= > >=`, `IN` and `BETWEEN` (`spark_literal_typing.rs:275-385`), and `FoldSparkNumericCasts` folds it to `Float64(+0.0)` | `Float64(-0.0)` after the fold (sign kept) | `Int64(0)` narrowed, then coerced cast, then folded | `d > -0.0` is right only because the sign is dropped (E7); `d <= CAST('-0.0' AS DOUBLE)` is wrong (R2); `<=>` refuses (E6) |
| `F.expr` | `EXPR_CONTEXT` default parser (`expr_build.rs:275-298`) | `Float64(0.0)` / `Float64(-0.0)` (`value.rs:84-101`) | `Cast(Utf8)`; folded only in the optimizer, so seat 1b catches it | `Int64(0)`, then coerced cast | `d > -0.0` adds row 6 (E7) |
| typed Column API | `PyColumn.literal(float)` | `Float64(±0.0)` | n/a | n/a | same as `F.expr` |
| native | DataFusion default parser | `Float64(±0.0)` | `Cast(Utf8)`; seat 1b catches it | coerced cast | `d = -0.0` WRONG-READ and WRONG-WRITE (E1) |

**The `<=>` refusal.** "Cannot cast to Decimal128(30, 15). Overflowing on NaN" (E6, R3) is the decimal path.
`is_float_comparison` lacks `IsDistinctFrom`/`IsNotDistinctFrom` (`spark_literal_typing.rs:275-285`), so the decimal
literal is not widened. Coercion then casts the double column to decimal, and the NaN row overflows.

The u-variant, which has no NaN or Infinity rows, returns both rows today only because both zeros cast to decimal 0
(Table B `c3|u` EQUAL). Seat 1 cannot see a float comparison while the column is cast to decimal. So the fix is a
prerequisite and goes **in this unit**: add the two operators to that match, owned by `repark-spark`'s
`SparkIntegralLiteral` pre-coercion rule. It also changes `d <=> 1.5` on the Spark door from a decimal to a double
comparison, which is Spark's typing for a double column against a decimal literal.

---

## 3. Seat 2 node list

**Wrapping rules.**
- Wrap a `Float32`/`Float64` expression `e` as `__repark_normalize_zero__(e)`.
- Never wrap a literal. Where a literal sits in a wrapped position (simple `CASE` `WHEN` values), rewrite its value
  instead: `-0.0` → `0.0`, any NaN → the canonical NaN.
- **Name preservation.** Use `NamePreserver` (`datafusion-expr-54.1.0/src/expr_rewriter/mod.rs:319-332, 361-373`,
  which restores with `alias_qualified`, so qualifiers survive). Alias only where the expression feeds the node's
  output schema: `Projection`, `Aggregate` group and aggregate expressions, and `Window` expressions. Never alias
  key-only positions (`Sort`, `Repartition`, `Join`, `Filter`); `NamePreserver` already excludes `Filter`, `Join`,
  `TableScan` and `Limit` (`:323-330`). The pattern is the one `spark_literal_typing.rs:140, 142, 152` and
  `window_rescan.rs:51` use.
- **Why seat 2 cannot run in the analyzer.** The common reason is that the key positions are created or moved by
  optimizer rules (§1). The table gives the specific reason per node.

| node / expression | exact field wrapped | name kept by | why not earlier |
|---|---|---|---|
| `Aggregate::group_expr` (`logical_plan/plan.rs:3667-3671`) | each float group expression; inside a `GroupingSet` (ROLLUP/CUBE/GROUPING SETS), each inner expression rather than the set | qualified alias per expression; never alias the `GroupingSet` itself | `replace_distinct_aggregate` and `single_distinct_to_groupby` create group keys in the optimizer |
| distinct aggregates (`expr.rs:1100-1109`) | `args` when `distinct == true`, and `order_by` sort expressions | alias the whole aggregate expression back to its original name (otherwise `count(DISTINCT d)` is renamed and parents break) | `single_distinct_to_groupby` turns single-distinct aggregates into group keys first; multi-distinct ones keep `distinct` args. `Distinct::On` becomes `first_value … ORDER BY` only in the optimizer |
| `Distinct::All` (UNION DISTINCT, `SELECT DISTINCT`, the left dedup of INTERSECT/EXCEPT) (`plan.rs:3520`; `builder.rs:1414-1435`) | at seat 2 it is already an `Aggregate` over every column (`replace_distinct_aggregate.rs:88-123`), so it is covered by the group-key row, output `0.0` like Spark (Table B `dst`, `isc`). If one survives (a skipped rule), insert a `Projection` on its input: `__repark_normalize_zero__(c) AS c` (qualified) per float column | the projection's qualified aliases | at the analyzer it has no key list (audit f) |
| `Distinct::On` (`plan.rs:3539`) | becomes an `Aggregate` on `on_expr` + `first_value(… ORDER BY sort_expr)` + `Sort` + `Projection` (`replace_distinct_aggregate.rs:124-192`); covered by the group-key, aggregate `order_by` and `Sort` rows | as those rows | the same rule |
| `Join::on` (`plan.rs:3998-4014`) | both sides of every float pair. Covers INTERSECT/EXCEPT (`LeftSemi`/`LeftAnti`), INTERSECT ALL/EXCEPT ALL (no left dedup, `builder.rs:1414-1423`), decorrelated `IN`/`EXISTS`/`= ANY`, `USING` and natural joins | none needed: `on` keys are not output columns; `null_equality` unchanged | decorrelation (`optimizer.rs:288-289`), `extract_equijoin_predicate` (`:291`) and `eliminate_cross_join` (`:294`) create or move keys |
| `Join::filter` | the non-literal comparisons inside it (the BinaryExpr row) | n/a | as above |
| `Window` (`plan.rs:2715`; `expr.rs:1232-1247`) | `partition_by`, `order_by`, and **`args` when `distinct`** (audit missed) | alias each window expression back | not forced by ordering; kept in seat 2 so one idempotence rule covers every key, and `common_sub_expression_eliminate` (`optimizer.rs:306`) can share `normalize(d)` between partition and order |
| `Repartition` `Hash(exprs, n)` / `DistributeBy(exprs)` (`plan.rs:2999, 4235-4243`) | each float expression | none (keys only) | none; seat 2 for uniformity. Spark parity is not observable (partition ids differ by hash function anyway). Wrapping keeps "equal keys co-locate" and lets distribution enforcement reuse a user repartition for a wrapped aggregate |
| `InSubquery` / `SetComparison` (`expr.rs:1311-1370`) | by seat 2 these are joins (`rewrite_set_comparison` `optimizer.rs:282`, `decorrelate_predicate_subquery` `:288`), so covered by `Join::on`. If one survives as an expression, wrap the outer `expr` and put `normalize(c) AS c` on the subquery's single output column | alias on the subquery projection | the analyzer sees an expression with no join keys yet |
| `Sort` (`plan.rs:3987`) | each float `SortExpr.expr`; TopK uses the same keys | none (keys only); output values untouched | not forced, since no optimizer rule creates sort keys; wrapping late leaves the analyzer-time sort keys that RePark's sort-name guards (`df_guards.rs:27`) inspect unchanged |
| non-literal comparisons in any node expression (Filter, Projection, Join filter, Aggregate and Window FILTER, Sort, CASE conditions): `BinaryExpr` with `= <> < <= > >= <=> IS DISTINCT FROM`, both sides float, neither a literal | both operands | `NamePreserver` per node | `scalar_subquery_to_join` creates `t.d = __scalar_sq.max` and `push_down_filter` moves predicates; the late seat sees final positions after seat 1/1b have settled the literal cases |
| `Between` with a non-literal bound (`expr.rs:901`) | `expr` and the non-literal bound(s); nonzero literal bounds stay (`T` = IEEE for them); zero bounds were expanded by seat 1 | as above | as above |
| simple `CASE e WHEN …` (`expr.rs:845`) | `Case::expr` and every `WHEN` operand (literal values canonicalised in place, not wrapped) | as above | the base comparison is internal to `CaseExpr`, so no BinaryExpr exists to rewrite |
| non-literal `InList` (`expr.rs:1341`) | `expr` and the non-literal items | as above | as above |
| `RecursiveQuery { is_distinct: true }` (`plan.rs:2266-2276`) | a `normalize(c) AS c` projection over both `static_term` and `recursive_term` outputs for float columns (`replace_distinct_aggregate` does not touch this node) | qualified aliases | none; seat 2 for uniformity |

**`TableScan::filters` are deliberately not rewritten by seat 2.** `map_expressions` would reach them
(`logical_plan/tree_node.rs:615-630`). But a provider has already decided `Exact`/`Inexact` on the unwrapped form in
`push_down_filter`, and the two `Exact` providers evaluate their own copy:
- Postgres pushdown renders SQL, and Postgres float equality is IEEE (`crates/repark-connect/src/pushdown.rs:149-160`);
- `ReplanningTempView` replans through the session (`crates/repark-spark/src/view_ddl/temp_view.rs:82-87`;
  `crates/repark-core/src/session/temp_views.rs:166` plans an optimised plan).

Inexact providers keep the `Filter` above the scan, and seat 2 wraps that copy. Zero-literal forms reach the scan
already rewritten, because seat 1b runs before `push_down_filter`.

**Found beyond the audit's list.**
1. **The SQL-text Iceberg predicate builders** (`meta_delete.rs`, `conflict_filter.rs`). They run before or beside
   the plan and are fixed in §5.
2. `WindowFunctionParams::distinct` args (`expr.rs:1245-1246`).
3. `GroupingSet` inner expressions, which need per-expression aliases.
4. Aggregate and window `FILTER` clauses (covered by the BinaryExpr row).
5. **One facade-synthesised float zero comparison.** `reciprocal_trig_or_inf` builds
   `CASE WHEN divisor = 0.0 THEN +Inf ELSE 1/divisor` (`crates/repark-python/src/column/expr_build.rs:24-34`), used
   by `F.sec` and `F.csc` (`function_dispatch.rs:208, 212`). After seat 1, `csc(-0.0)` returns `+Inf`, where Spark's
   `1/sin(-0.0)` is `-Inf`. The THEN arm must carry the divisor's sign (an IEEE reciprocal avoids comparisons
   altogether), with a cell. A census of engine code finds no other float zero comparison; the other `lit(0…)`
   comparisons are integer (`dynamic_flatten.rs:356`, `df_guards/subquery.rs:387`, `generator.rs:760`).
6. Hash-join dynamic filters are derived from the wrapped keys, so they stay correct but stop pruning the probe scan
   on float keys (§7).
7. Checked by reading, no gap:
   - `Unnest`: no key semantics.
   - `Limit` with ties: DataFusion has none.
   - `scan_batches.rs` has no float arm (`crates/repark-iceberg/src/catalog/scan_batches.rs:99-109`).
   - `scan_prune.rs` is integer-key only (`range_predicate_i64`/`_i32`, `IntKeyWidth`).
8. Expected but not opened, so held by cells instead: physical distribution, sorting and join selection rules,
   TopK, sort-merge join and the spill paths are expected to evaluate the operators' own (wrapped) key expressions,
   and RePark's `ParallelWindowRule` (`df_guards.rs:89`) to partition by the window's (wrapped) partition
   expressions. D9 (sort-merge, TopK, window) and D16 (spill) pin each one.

---

## 4. The UDF

- **Name.** `__repark_normalize_zero__`, following the internal spelling of `__repark_bool_to_decimal__`
  (`bool_decimal.rs:22`) and `__repark_spark_as__` (`spark_typed.rs:18`). The memo's `repark_normalize_zero` is the
  same function.
- **Semantics.** One argument, `Float32` or `Float64`; the return type, field and nullability equal the input's.
  - NULL → NULL; the null buffer passes through.
  - Any NaN (any sign or payload) → the canonical quiet NaN (`f64::NAN` / `f32::NAN`, positive).
  - `-0.0` → `+0.0`.
  - Everything else is identity, bit for bit.
  - Two RePark precedents already implement exactly this: `normalize_f32`/`normalize_f64` (`value + 0.0`, NaN →
    `NAN`) at `crates/repark-python/src/is_duplicated.rs:138-152`, and `canonical_float_bits` at
    `crates/repark-core/src/freq_items.rs:153-160`. The build should share one helper, homed in core or common.
  - Under totalOrder the canonical NaN is the greatest value and equal to itself. That is Spark's NaN rule, so wrapped
    keys order and group exactly as Spark's.
- **Kernel.** One pass. Fast path: a vectorisable scan for `-0.0` bits or non-canonical NaN bits; if none is found,
  return the input `ArrayRef` unchanged with no allocation. Otherwise one allocation and a branch-free map.
  `ColumnarValue::Scalar` is mapped directly.
- **Volatility `Immutable`.** The optimizer may move it through projections, push it with filters, share it through
  common-subexpression elimination, and fold it on literals: the const evaluator accepts `Immutable`
  (`expr_simplifier.rs:614-620`), so `normalize(-0.0)` becomes the literal `0.0` in the next simplify. `Stable`
  would also fold; `Volatile` would block all of this.
- **`simplify` override** (the default returns the original, `datafusion-expr-54.1.0/src/udf.rs:735-741`).
  `normalize(normalize(x))` → `normalize(x)`; a non-float argument (never produced) returns the argument.
- **`output_ordering` stays the default, unordered** (`udf.rs:938+`). The map is monotone for every value except a
  sign-set NaN, which moves from first to last, so it may not claim order preservation. The cost is that a sort or
  streaming aggregate over a pre-sorted float column is not elided (§7).
- **No cost model exists for scalar UDFs in DataFusion 54.** The optimizer treats it as any expression, and the only
  plan-time removal is constant folding on literal arguments.
- **Serialisation by name** (§1.5 keep-distributed-open). The UDF is registered on every core session, and Ballista
  plan decoding resolves UDFs by name through the codec (`crates/repark-distributed/src/codec.rs:97-98`). Executors
  must register the same UDF and the three rules when the distributed door lands (carded).

---

## 5. The fork `IN` gap and the text translators

**The gap is closed on the DataFusion scan path at the pinned fork.** `Reference::is_in` does collect literals into
an `FnvHashSet` (fork `crates/iceberg/src/expr/term.rs:273-279`). `OrderedFloat`'s `PartialEq` is IEEE for zeros
(`ordered-float-4.6.0/src/lib.rs:317-326`), so the two zeros collapse, and `InclusiveMetricsEvaluator::r#in` then
compares with totalOrder (`inclusive_metrics_evaluator.rs:396-438`, `ge` at `:419`, `le` at `:431`). But a float
`IN` list holding a zero never reaches `is_in` from the scan: the `Set` arm of `literal_binds_soundly` drops it
(`expr_to_predicate.rs:75-81, 116-118`). After seat 1, no RePark-built `IN` list holds a zero anyway (§2).

**The gap is open on two RePark paths, which share `literal_datum` and bypass the plan:**
- `meta_delete::in_list_predicate` → `is_in` (`crates/repark-iceberg/src/write/meta_delete.rs:275-286`), and
  `comparison_predicate` → `equal_to` (`:225-248`). These are fed to `can_delete_using_metadata` (`:115-121`), which
  runs the inclusive and strict totalOrder evaluators (fork `table.rs:354, 393-394`).
- `conflict_filter::in_list` → `is_in` (`conflict_filter.rs:210-228`) and `comparison` for `=`/`<>`
  (`:160-183, 189-208`), used for identity DML and MERGE conflict filters (`predicate_dml.rs:223, 302`;
  `merge/mod.rs:480`).

**Choice: the RePark side, in this unit.** `literal_datum` (`conflict_filter.rs:284-303`) returns `None` for a
`Float`/`Double` field when the parsed value is a zero of either sign. That mirrors the fork's own guard
(`expr_to_predicate.rs:116-118`). The effects:
- **Metadata delete.** `exact_predicate` fails, so `plan_metadata_delete` returns `None` and the statement falls
  through to the row-level path (`delete_update.rs:36-39`), which carries seat 1. The only loss is the
  metadata-only fast path for float-zero DELETE statements.
- **Conflict filter.** `widest` drops the conjunct or widens to `AlwaysTrue` (`conflict_filter.rs:67-72, 82-101`),
  which is conservative: more conflicts, never fewer.
- **Owning layer:** `repark-iceberg` write path.

Not chosen: emitting the bound form from the text translators instead of refusing. It is exact under the fork's
evaluators — a strict `d >= -0.0 AND d <= +0.0` deletes file A whole — and keeps the fast path, but it duplicates
the seat-1 table in a second parser. Carded as a performance follow-up.

**Fork side.** Checking both zeros in `r#in` (and treating a zero datum as `[-0.0, +0.0]` in `eq`, `not_eq` and the
strict evaluator) is not needed for RePark's correctness at `66901f02`. It would matter only for other fork callers,
or if the zero guard is relaxed to regain pruning. Card it as an optional fork PR, `FORK-FLOAT-ZERO-BOUNDS-1`. Do not
relax `literal_binds_soundly` in this unit.

**Tests.**
- **Rust, `repark-iceberg`, mutation-proof.** A table with two hand-built data files with bounds `[-0.0,-0.0]` and
  `[+0.0,+0.0]` (the shape E1 measured on RePark-written and Spark-written files). For `DELETE … WHERE d = 0.0`,
  `d = -0.0`, `d IN (0.0, 1.0)` and `0.0 = d`:
  - assert `delete_predicate` is `None`;
  - assert the statement deletes both rows;
  - removing the guard must turn the test red: the metadata path deletes one file.
- **`conflict_filter`.** `from_selection` on the same forms yields no predicate carrying a float zero.
- **Cells:** §9 D1.

---

## 6. Interactions

- **NaN (AUD-024 unchanged).**
  - Seat 1/1b: proved in §2. No NaN comparison result changes.
  - Seat 2: results change only for non-canonical NaNs (sign bit set or nonzero payload) in wrapped key positions,
    and only toward Spark (all NaN one value, greatest). Canonical NaN keys, which `CAST('NaN' AS DOUBLE)` produces
    (the seed's id 3, extension R4 "NaN is one value"), are unchanged.
  - Neither seat sends anything new to an Iceberg evaluator: zero literals stay refused (`expr_to_predicate.rs:116-118`),
    and the UDF is not convertible (only `isnan` is, `:463-481`). So AUD-024's evaluator cells cannot move.
  - The pre-existing `-NaN` ordering divergence outside key positions (a raw `d < 1.0` is true for `-NaN`) is left
    alone and carded with measurement (§9 D10).
- **MERGE cardinality.** After seat 2, `MERGE … ON t.d = u.d` with both zeros on each side matches each target zero
  row twice. The existing check raises `MERGE_CARDINALITY_VIOLATION` (`crates/repark-iceberg/src/write/merge/copy_on_write.rs:230-235`).
  That turns Table B `m1` from ACCEPTED-WHERE-SPARK-REFUSED into EQUAL on v2 CoW, v2 MoR and v3. This is a
  user-visible change: a statement that used to succeed now refuses, as Spark does. The build must confirm the MoR
  and v3 paths reach the same check (Table B measured all three accepting). A single-match MERGE (`t = {-0.0}`,
  `u = {0.0}`) must now update, where it used to fall to NOT MATCHED (§9 D7).
- **`min`/`max` sign and zero tie order: declared, not fixed.** Proposed registry text:
  - *SIGN-ONLY (signed zero, min/max):* "When the minimum or maximum of a FLOAT or DOUBLE column is a zero, RePark
    may return the other sign than Spark. Spark returns the first zero it meets; RePark orders `-0.0` below `0.0`.
    The two values are equal under SQL comparison." Evidence: Table B `mxmn` (t, tf, u, uf).
  - *ORDER-DIFFER (signed zero, ties):* "ORDER BY on a FLOAT or DOUBLE key treats `-0.0` and `0.0` as equal. Like any
    tie, their relative order is unspecified; add a tie-breaking key for a deterministic order." Evidence: Table B
    `ord` rows. With a tie-breaker the order is now deterministic and equal to Spark (§9 D9 `ORDER BY d, id`, which
    is WRONG today).
- **Iceberg write path: no normalisation of stored values.**
  - Seat 1 rewrites only boolean predicates.
  - Seat 2 rewrites only key positions. The exceptions are dedup and group outputs (GROUP BY, DISTINCT, UNION,
    INTERSECT, recursive-distinct), where Spark outputs the normalised key too (Table B `grp`, `dst`, `isc`).
  - Plain `INSERT … SELECT`, CTAS, `UPDATE SET` and `MERGE SET` values are projections, never wrapped, and the store
    paths (`InsertStoreAssignment`, `store_cast`, `StoreOverflowCast`; `extension.rs:154-162`) are untouched. So
    `-0.0` is stored bit for bit. Pinned by §9 D11.
  - Identity partition values are computed from stored values. A `PARTITIONED BY (d)` table keeps `d=-0.0` and
    `d=0.0` partitions (measured in §9 D12).
- **Spill.** The wrapped key is an expression evaluated before the operator hashes, so a spill is expected to write
  normalised key arrays. I did not open the aggregate, sort or hash-join spill code and make no claim beyond that
  expectation; the E8 shape with a recorded spill metric must show it (§9 D16). E8 confirms the class at scale (502 groups against 501;
  1,000,000 pairs against 2,000,000; self-join 1e9 against 2e9) but captured no spill counter.
- **Physical shapes that bypass logical keys.** None found by reading; the unopened physical paths are held by cells (§3, items 7–8). A physical-only behaviour worth stating:
  `AggregateStatistics` may answer `min`/`max` from file statistics. E1 shows those statistics keep the stored sign
  (`min = max = -0.0`), so the answer stays inside the declared SIGN-ONLY divergence.
- **`<=>` decimal-cast refusal (memo Q3 item 5).** Owning layer: `repark-spark` `SparkIntegralLiteral`
  (`spark_literal_typing.rs:275-285`). **Fixed in this unit** (§2); it is a prerequisite for seat 1 on `spark.sql`
  and `filter(str)`.
- **Step 3.2 struct comparisons.** Seats 1 and 2 touch only `Float32`/`Float64` operands, so they neither trigger nor
  mask Step 3.2's struct trigger. Extension R5 (`named_struct('a', i, 'b', 2) = named_struct('b', 2, 'a', 1)` and the
  partial-overlap form insert 2 rows on all four Spark routes; Spark inserts none, a WRONG-WRITE) confirms the
  audit's three-part trigger and stays in Step 3.2. Floats nested in struct or array keys are carded
  (`NESTED-FLOAT-KEY-1`). If Step 3.2's shared helper walks struct comparisons, it must not normalise floats.

---

## 7. Performance

- **Literal rewrite: free.** The only plan change is a second bound on `=`/`<>` forms. No pushdown is lost, because
  zero literals were never pushed at this fork (§2).
- **Key wrap.** One kernel pass per float key per wrapped site per batch.
  - When a batch holds no `-0.0` and no non-canonical NaN, the fast path returns the input array with no allocation.
    The cost is one vectorised scan of 8 bytes per row.
  - Otherwise there is one allocation and one pass.
  - Indirect costs: hash-join dynamic filters on wrapped keys stop pruning the probe scan, and pre-sorted float keys
    lose sort and streaming elision (§4).
- **Honest estimate on E5's shape.** E5's baseline: 10M rows, a quarter `-0.0`, GROUP BY 34.5 ms, join 20.6 ms,
  measured at load average 12, so not idle. Every batch takes the slow path. One extra pass over 80 MB is a material
  fraction of a 34.5 ms query, so the ratio may approach the gate. Data without `-0.0` takes the fast path.
- **Measurement (§9 D15).** An idle box, the pre-unit wheel against the post-unit wheel, five runs each, median,
  like-for-like:
  - the C-012 set under the PERF-1 ≤1.20 gate;
  - one float-key JOIN `count(*)` cell and one float-key GROUP BY cell on E5's table;
  - the same two cells on a 10M table with no `-0.0` (the fast path);
  - the E5 join count recorded (it was not).
  - The two float-key cells gate at the same ≤1.20 ratio. The fast-path cells are reported, expected near 1.0.
- **Fallback if a gate fails.** Per the memo's "What would change", the literal rewrite (seats 1/1b) stays
  unconditional. The key wrap becomes conditional on a session flag through the per-door policy hook (§8), proposed
  name `repark.sql.signedZero.normalizeKeys`: Spark door default on, native door default off (totalOrder keys,
  declared). The owner decides the Spark-door default (§12). Default stays on: the class includes the MERGE
  cardinality miss and the single-match MERGE write.

---

## 8. Doors and the Trino question

- **Default: both doors, all three rule objects** (the route table in §1). E9 did not run (no Trino image under
  rootless Docker), so the native oracle is still ORACLE-MISSING. The native answers were recorded for later
  comparison.
- **Mechanism.** `SessionExtension` gains a policy hook, consumed by `context_with_df_54_1_rule_guards` and the seat-1
  installer, with two independent booleans: `literals` (seats 1/1b) and `keys` (seat 2), both defaulting to on.
  `NativeDoorTypes` (`session_runtime.rs:404-427`) is where the native door would override it.
- **If Trino keeps the zeros distinct in `=` or the comparisons:** native `literals = false` (totalOrder comparisons,
  declared).
- **If Trino keeps them distinct only in GROUP BY, DISTINCT, JOIN or INTERSECT:** native `keys = false` and
  `literals` stays on.
- **If both:** both off on native. The Spark door is unaffected.
- This meets the memo's "moves to the Spark door's extension" without moving code, and a partial Trino result maps
  to a single boolean. The text-translator guard (§5) stays on for both doors in every case. It refuses a fast path
  and never changes a result, so it is correct under either semantics.

---

## 9. Domain table for the build

**Truth column, for every cell:** Spark SQL semantics. For Iceberg reads of zero-only files and pages it is Spark's
single-file or direct-Parquet answer (E4), not Spark-on-Iceberg; a mismatching Spark-on-Iceberg answer is labelled
`PRUNED-WRONG` and declared. "Five routes" means `spark.sql`, `filter(str)`, `F.expr`, typed and native. "Three kinds"
means v2 CoW, v2 MoR and v3.

**Axes.**
- operator (`= <> < <= > >= <=> IS DISTINCT FROM IN NOT IN BETWEEN NOT BETWEEN NOT(…)`);
- literal sign and side;
- literal shape per route (§2);
- left operand: a column, an expression, or `CAST(f AS DOUBLE)`;
- column type: DOUBLE or FLOAT;
- NULL, NaN and ±Inf rows;
- route; statement (SELECT, DELETE, UPDATE, INSERT…SELECT, CTAS, MERGE condition); kind;
- file layout: single file, RePark-written zero-only files, Spark-written zero-only files;
- key site (§3); spill; timing.

**Already measured** (Table B and the extension, not repeated): c1–c7, grp/dst/ord/cst/mxmn/win/jn/isc/exc,
m1–m3, p15, E1, E3, E5–E8, R1–R4.

**Further cells the build needs:**

| id | statement or shape | doors | kind | oracle |
|---|---|---|---|---|
| D1 | metadata-delete path: the E1 two-file table, `DELETE … WHERE d = 0.0`, `d = -0.0`, `d IN (0.0, 1.0)`, `0.0 = d`; plus the §5 hand-built-bounds Rust test | `spark.sql`, native | three kinds; Rust test n/a | truth (both rows deleted); Spark-on-Iceberg recorded as PRUNED-WRONG |
| D2 | computed and folded zero operands: `d = 1 - 1.0`, `d >= CAST(0 AS DOUBLE) * 1`, `d < abs(-0.0)`, `CAST(f AS DOUBLE) = 0.0`, `d * 0 <> 0` | five routes | v2 CoW | Spark |
| D3 | literal against literal: `SELECT CAST('-0.0' AS DOUBLE) = 0.0`, `… < 0.0`, `… <=> 0.0` | `spark.sql`, native | n/a | Spark |
| D4 | the full seat-1 matrix not yet covered: `d NOT IN (0.0, 1.0)`, `d NOT IN (0.0, 10.0, 20.0, 30.0)`, `d IN (-0.0, 10.0, 20.0, 30.0)`, `d BETWEEN -1.0 AND 0.0`, `d NOT BETWEEN 0.0 AND 1.0`, `d BETWEEN CAST('-0.0' AS DOUBLE) AND 0.0`, `NOT (d < 0.0)`, `NOT (d >= CAST('-0.0' AS DOUBLE))`, `d = 0.0 AND d = -0.0`, `d IS DISTINCT FROM 0.0` (native) / `NOT (d <=> 0.0)`; on typed: `F.col('d') == 0.0`, `!= -0.0`, `< -0.0`, `<= -0.0`, `> -0.0`, `>= 0.0`, `eqNullSafe(0.0)`, `~eqNullSafe(0.0)`, `isin(0.0, 10.0, 20.0, 30.0)`, `~isin(0.0, 1.0)`, `between(-1.0, 0.0)`, `F.lit(0.0) <= F.col('d')` | five routes (typed rows on typed) | v2 CoW | Spark |
| D5 | the D2 and D4 rows on a FLOAT column (`tf`) | five routes | v2 CoW | Spark |
| D6 | names and schema kept: `SELECT d = 0.0 FROM t` unaliased, `SELECT d <=> 0.0`, `df.select(F.col('d') == 0.0).columns`, `SELECT d, count(*) … GROUP BY d`, `SELECT count(DISTINCT d)`, `SELECT x.d FROM (SELECT d FROM t GROUP BY d) x`, `SELECT t.d FROM t GROUP BY t.d` | `spark.sql`, typed, native | v2 CoW | Spark (names and values) |
| D7 | DML twins of every new read row: DELETE, UPDATE, INSERT…SELECT, CTAS. MERGE conditions: `WHEN MATCHED AND t.d = 0.0 THEN UPDATE`, `WHEN NOT MATCHED AND u.d = 0.0 THEN INSERT`. Single-match `MERGE ON t.d = u.d` with `t = {-0.0}`, `u = {0.0}`, WHEN MATCHED UPDATE and WHEN NOT MATCHED INSERT | `spark.sql`; native for DELETE/UPDATE | three kinds | Spark (truth on zero-only files) |
| D8 | non-literal comparisons, columns `d`, `e` with rows `(-0.0, 0.0)`, `(0.0, -0.0)`, `(NaN, NaN)`, `(NULL, 0.0)`: `d = e`, `d <> e`, `d < e`, `d <= e`, `d <=> e`, `d BETWEEN e AND 1.0`, `d IN (e, 1.0)`, `CASE d WHEN 0.0 THEN 'z' ELSE 'n' END`, `CASE d WHEN e THEN 'z' END`, `JOIN ON t.d < u.d` | `spark.sql`, native | v2 CoW | Spark |
| D9 | key sites beyond R4 and E8: `UNION` (distinct) of `t` and `u`; `INTERSECT ALL`; `EXCEPT ALL`; LEFT, RIGHT and FULL OUTER JOIN on `d`; `LEFT SEMI JOIN`; `LEFT ANTI JOIN`; `JOIN USING (d)`; `WHERE d IN (SELECT d FROM u1)` with `u1 = {0.0}` only (R4's cell does not discriminate because `u` holds both signs); `WHERE EXISTS (SELECT 1 FROM u1 WHERE u1.d = t.d)`; `WHERE d = (SELECT max(d) FROM u2)` with `u2 = {-0.0}`; `count(DISTINCT d), count(DISTINCT e)` (the multi-distinct path); `sum(DISTINCT d)`; `GROUP BY ROLLUP(d)`; `GROUP BY CUBE(d, id % 2)`; `DENSE_RANK() OVER (ORDER BY d)`; `ROW_NUMBER() OVER (ORDER BY d, id)`; `count(*) OVER (ORDER BY d RANGE BETWEEN 1 PRECEDING AND CURRENT ROW)`; `ORDER BY d, id` on `u` (WRONG today); `ORDER BY d, id LIMIT 1`; `DISTRIBUTE BY d` then `GROUP BY d`; recursive CTE with `UNION` (if Spark 4.1 refuses it, native only with Trino); one float-key join with `datafusion.optimizer.prefer_hash_join = false` (the sort-merge path); one temp-view read with a pushed float predicate | `spark.sql`, native | v2 CoW | Spark (Trino for native-only) |
| D10 | NaN payloads: a column holding `CAST('NaN' AS DOUBLE)`, `sqrt(-1.0)` and `ln(-1.0)` rows: GROUP BY, DISTINCT, JOIN, ORDER BY, `= CAST('NaN' AS DOUBLE)`, `< 1.0` | `spark.sql`, native | v2 CoW | Spark (shows whether non-canonical NaN arises; pins the declared seat-2 delta) |
| D11 | stored values kept: `INSERT INTO t2 SELECT d FROM t`, CTAS, `UPDATE t SET d = -0.0 WHERE id = 6`, `MERGE … UPDATE SET d = u.d`; then `SELECT id, CAST(d AS STRING)` | `spark.sql` | three kinds | Spark (`-0.0` survives) |
| D12 | identity partition on `d` with `-0.0` and `0.0` rows: `SELECT … WHERE d = 0.0`, `DELETE … WHERE d = 0.0` (metadata `selects_partitions`), `GROUP BY d` | `spark.sql` | v2 CoW, v2 MoR | truth; Spark-on-Iceberg recorded |
| D13 | reciprocal trig: `F.csc(F.lit(-0.0))`, `F.csc(F.col('d'))` on id 4 | typed | n/a | Spark (`-Infinity`) |
| D14 | plan shape, no oracle: EXPLAIN per node kind shows one `__repark_normalize_zero__` per key and zero comparisons in bound form; analyse and optimise twice gives the same plan | native, `spark.sql` | n/a | pin |
| D15 | timing (§7) | `spark.sql` | n/a | pre-unit wheel |
| D16 | spill with evidence: the E8 shape with `memory_limit_gb` low on both doors **including the facade** (E8's facade ran at default memory), `EXPLAIN ANALYZE` spill metrics recorded as nonzero | `spark.sql`, native | n/a | Spark |
| D17 | carding only, out of clause: `collect_set(d)`, `approx_count_distinct(d)`, `mode(d)`, `array_distinct(array(d, -d))`, `array_contains(array(CAST('-0.0' AS DOUBLE)), 0.0)`, `array_union`, `map(CAST('-0.0' AS DOUBLE), 'a')[0.0]`, `GROUP BY named_struct('x', d)`, `GROUP BY array(d)`, `nullif(d, 0.0)`, `greatest(CAST('-0.0' AS DOUBLE), 0.0)` | `spark.sql` | v2 CoW | Spark |

**Missing evidence to close** (from the extension):
- **E2 (Spark-written table read by RePark).** The memory catalog cannot see a Hadoop-catalog table. It needs a
  catalog both engines open (a shared REST or SQL catalog, or a RePark Hadoop-catalog reader). E1 shows RePark-written
  files carry the same `min = max = -0.0` statistics, and D1's hand-built test covers the metadata path, so E2 is
  confirmation, not a blocker.
- **E4 RePark side.** Native `read_parquet` is unresolved (`UNRESOLVABLE_TABLE_VALUED_FUNCTION`). Use the facade's
  `spark.read.parquet(path).filter("d = 0.0")`, or native `CREATE EXTERNAL TABLE … STORED AS PARQUET` if the
  local-fs guard allows it. Add a bloom-filtered file.
- **E8 spill metric.** None was read (D16).
- **E5 join count.** Not recorded (D15).
- **E9 Trino.** The image must be pulled (P5).

---

## 10. Pins and acceptance

**Pins.** Each is red on base `main` and green after, in debug and release (`cargo test` and `cargo test --release`
for the Rust pins; the facade suite on the release wheel).
1. **P1, seat-1 table (Rust, `repark-core`).** A core session over a MemTable seed (eight rows: ±0, NULL, NaN, ±Inf,
   a positive and a negative value). The table rows are operator × literal sign × side × NOT-wrap × type × literal
   shape (`Float64`, `CAST(Decimal128)`, `CAST(Utf8)`, `Int`, folded). Expected id sets come from the truth. One
   case asserts the fixpoint: analyse and optimise twice, identical plan.
2. **P2, per door (facade).** The c1–c7, E6, E7, R2, R3, D2, D3, D4 and D5 rows on all five routes, against the
   recorded Spark oracle JSONL (`raw/` on the follow-up branch), with the truth override for zero-only-file cells.
3. **P3, DML.** The p15 rows, E1 DML and D7 twins on three kinds, compared by post-state equality with the truth.
4. **P4, text translators (Rust, `repark-iceberg`).** The §5 metadata-delete and conflict-filter tests.
5. **P5, key sites.** Table-driven over §3's node list: R4, E8 (scaled down), D8, D9 and m1 (now
   `MERGE_CARDINALITY_VIOLATION` on three kinds).
6. **P6, plan shape and names.** D14 wrap counts. D6 schema equality, qualifiers included.
7. **P7, NaN and stored bits.** D10 unchanged except the declared delta; D11 `-0.0` preserved.
8. **P8, performance.** D15 recorded against the gate.
9. **Mutation record in the ledger.** Disabling seat 1 turns the literal rows red. Disabling 1b turns D2 red.
   Disabling seat 2 turns the key rows red. Removing the `literal_datum` guard turns P4 and D1 red. Removing the
   `<=>` arms brings back the E6 refusal.

**Verifier standing claims.**
1. Every domain cell is EQUAL to the truth column or carries a declared label: SIGN-ONLY (min/max), ORDER-DIFFER
   (zero ties), or PRUNED-WRONG (Spark-on-Iceberg oracle defect).
2. **The same expression in DML.** For every read predicate row, the DELETE, UPDATE, INSERT…SELECT, CTAS and
   MERGE-condition twins touch exactly the read's rows. This holds on three kinds and on every door that accepts
   DML, through both the row-level path and the metadata-delete door (`delete_update.rs:36`; `router.rs:243`).
3. **Pushdown exactness.**
   - No predicate with a float zero literal reaches an Iceberg evaluator from any RePark path: the fork guard covers
     the DataFusion scan, and the `literal_datum` guard covers `meta_delete` and `conflict_filter`.
   - Every rewritten form keeps every file, row group and page whose bounds meet `[-0.0, +0.0]` under totalOrder
     (§2), so a later guard relaxation stays exact.
   - Scan pushdown stays `Inexact`.
   - Seat 2 does not touch `TableScan::filters` (§3).
4. **Fixpoint.** Seats 1/1b are idempotent, and seat 2 wraps each key once on the pinned shapes.
5. **Names and schemas** are unchanged for every cell.
6. **Stored values** are never normalised.
7. **NaN.** Seats 1/1b change no NaN result; seat 2 changes only non-canonical NaN keys, toward Spark.
8. **Doors.** Both doors carry all three rule objects (§1), and a builder-built server door inherits them.
9. **Performance** is within the gate, or the fallback is ruled.

**Outside the clause, carded.**
- min/max sign and zero tie order: declared (§6).
- Native `CAST(Infinity AS VARCHAR)`: memo Q6.
- `COLLECTION-FLOAT-EQ-1`: array and map functions and `collect_set`.
- `FUNC-FLOAT-EQ-1`: `nullif`, `greatest`/`least`, `approx_count_distinct`, `mode`, measured by D17. `nullif(-0.0, 0.0)`
  is a likely WRONG-READ, and its owner may pull it in.
- `NESTED-FLOAT-KEY-1`: floats inside struct and array keys.
- `NAN-PAYLOAD-1`: raw `-NaN` ordering outside key positions.
- `FORK-FLOAT-ZERO-BOUNDS-1`: an optional fork PR.
- `TEXT-PREDICATE-BOUND-FORM-1`: keep the metadata fast path for float-zero DELETE statements.
- `DIST-SIGNED-ZERO-1`: register the UDF and rules on distributed executors.
- Float16 and dictionary-encoded float keys.

---

## 11. Unit size and split

One Opus 5.5 lane (the one-lane rule of 11-02) carries two stacked PRs in order. The fork part is not built.

| part | content | size | tier | verifiers (R7) |
|---|---|---|---|---|
| **A, SIGNED-ZERO-LITERALS** | seat 1 + 1b; the `<=>` widening arms; the `literal_datum` guard; the reciprocal-trig repair; pins P1–P4 and P6 (names); D1–D7, D11, D13 | about 500–700 lines including tests | Opus 5.5 high: three crates, analyzer and optimizer ordering, a fixpoint proof | Opus 5.5 high verifier and Muse MAX second verifier, in parallel (design-heavy) |
| **B, SIGNED-ZERO-KEYS** | the UDF + seat 2 over §3's node list; the policy hook; pins P5–P8; D8–D10, D12, D14–D16; the declared-divergence registry rows | about 800–1,100 lines including tests | Opus 5.5 high: the node list, aliases, grouping sets, idempotence across passes | the same pair, in parallel; perf cells re-run by the orchestrator before merge |
| fork `IN` / bounds | not needed at `66901f02` (§5) | n/a | card only | n/a |

- **Why A first.** A closes the whole literal class, including the 17 WRONG-WRITE p15 cells, the E1 native and
  `<>` writes, and the metadata-delete path. Those are the label's wrong-write blocker, and A carries no perf risk.
- **B** closes the key class (grp, dst, win, jn, isc, R4, E8, m1) behind the perf gate. Its fallback (§7) can ship
  without re-opening A.
- A clerk (Haiku or Muse) runs the D-cell measurements in parallel with A's build.

---

## 12. Open questions for the owner (each with a default)

1. **Literal rewrite form:** the bound form (a fixpoint, run at seats 1 and 1b) instead of the memo's OR form.
   *Default: bound form.*
2. **Seat 1 call site:** after `ext.register`/`dialect.on_session_built` in `ReparkSessionBuilder::build`, with the
   function homed in `df_guards.rs`, rather than the tail of the base analyzer list. *Default: post-register.*
3. **Seat 1b** (the same rewrite after `simplify_expressions`) is in the clause. *Default: yes.*
4. **The text-translator guard** (`literal_datum` refuses float zeros, so metadata delete falls back) is in this unit.
   *Default: yes.* The bound-form translation is carded.
5. **The fork `IN`/bounds change** is carded, not built. *Default: card `FORK-FLOAT-ZERO-BOUNDS-1`; never relax
   `literal_binds_soundly` here.*
6. **The truth column** is Spark SQL semantics; Spark-on-Iceberg zero-only-file answers are labelled PRUNED-WRONG and
   declared (the AUD-024 precedent). *Default: yes.*
7. **UDF NaN canonicalisation**, with the declared delta for non-canonical NaN keys. *Default: canonical positive
   NaN.*
8. **The split:** A (literals) then B (keys), two stacked PRs in one Opus 5.5 lane, verified in parallel by Opus 5.5
   high and Muse MAX. *Default: yes.*
9. **The perf gate** for the two float-key cells: ≤1.20 like-for-like median against the pre-unit wheel, the same as
   PERF-1. If it fails: the key-wrap flag, Spark door on, native off. *Default: keep it on for the Spark door and
   declare the cost.*
10. **Trino mapping:** two independent per-door booleans; both on for both doors until E9 runs. *Default: yes.*
11. **The `<=>` widening fix** (two arms in `is_float_comparison`) rides in part A. *Default: yes.*
12. **`nullif` and the other function-internal float comparisons** are carded (`FUNC-FLOAT-EQ-1`), unless D17 shows
    a WRONG-READ the owner wants in A. *Default: card.*
