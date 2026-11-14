# SIGNED-ZERO-CONST-COMPARE-1 — step 0 inventory

Base: main `d843d37e`. DataFusion 54.1.0, arrow 58.4.1, the fork at `b6e578dc` (the rev `Cargo.lock` names for
`iceberg-datafusion`). Every `file:line` below was opened. "Seen" means: does a comparison of two float constants
(`0.0` against `-0.0`, or two NaNs) reach this seat with both operands as values, so that the seat decides the
answer?

## How the wrong answer is made (traced on the Spark door, `EXPLAIN VERBOSE`)

`SELECT CAST('0.0' AS DOUBLE) = CAST('-0.0' AS DOUBLE)`:

1. **Analyzer, `spark_float_stringify`** turns each string-to-float cast into a call:
   `__repark_parse_java_double__(__repark_decimal_cast_nullable__(Utf8("0.0")))`. The literal fold in
   `rewrite_float_cast` does not fire because the cast's input is the nullable marker, not a literal.
2. **Analyzer seat 1, `signed_zero_literals`**, sees neither side as a zero operand. Its recogniser accepts only a
   float literal, `Negative` of one, or a `Cast`/`TryCast` of a literal. A function call is none of these, so the
   plan is unchanged.
3. **Optimizer, `simplify_expressions`**: the `ConstEvaluator` folds both calls (each is `Immutable`), then folds
   the comparison itself. It evaluates the physical `BinaryExpr` over a one-row batch, and Arrow compares floats
   by totalOrder: `0.0 = -0.0` is `false`, `-0.0 < 0.0` is `true`. The result is `Boolean(false)`.
4. **Optimizer seat 1b**, which runs right after `simplify_expressions`, then sees only `Boolean(false)`.

On the native door step 1 does not exist, so the analyzer seat sees `CAST(Utf8("-0.0") AS Float64)` as a zero and
rewrites the comparison to the bound form; that cell is right. Any pair where **neither** side is a recognised zero
operand (for example `CAST(0 AS DOUBLE) * -1` against `-CAST(0 AS DOUBLE) * -1`) reaches step 3 on every door.

## Inventory

### A. DataFusion's simplifier and `ConstEvaluator`

| # | seat | file:line (DataFusion 54.1.0 unless named) | seen? | because |
|---|---|---|---|---|
| A1 | `ConstEvaluator` traversal | `datafusion-optimizer/src/simplify_expressions/expr_simplifier.rs:515-580` | **yes — the seat** | `f_up` folds every evaluable node bottom-up; the comparison node is folded once both children are literals |
| A2 | `ConstEvaluator::can_evaluate` | `expr_simplifier.rs:625-702` | yes | `BinaryExpr`, `InList`, `Between`, `Case`, `Negative`, `Cast` and `Immutable` scalar functions are all evaluable, so every constant form in the table is folded |
| A3 | `ConstEvaluator::evaluate_to_scalar` | `expr_simplifier.rs:705-760` | yes | builds the physical expression and evaluates it on `DUMMY_BATCH`; the answer comes from Arrow (F1–F3) |
| A4 | the simplify loop | `expr_simplifier.rs:208-243` | yes | `const_evaluator`, then `simplifier`, then guarantees, repeated; constant folding always runs first in a cycle |
| A5 | where the loop runs | `datafusion-optimizer/src/optimizer.rs:285` (`SimplifyExpressions` is the fourth rule of each pass, after `RewriteSetComparison`, `OptimizeUnions` and `UnionsToFilter`) | yes | also run by RePark's seat 1b after its rewrite (`crates/repark-core/src/session/df_guards/signed_zero/literals.rs:46-52`) |

### B. The expression simplifier's comparison rules

| # | seat | file:line | seen? | because |
|---|---|---|---|---|
| B1 | `A = A → A IS NOT NULL OR NULL` | `expr_simplifier.rs:851-866` | no | needs structurally equal operands; `0.0` and `-0.0` are different literals (`ScalarValue` equality is bitwise, D1), and a constant `A = A` folds in A1 first |
| B2 | `A >= c AND c <= A → A = c` | `expr_simplifier.rs:1058-1080`, `utils.rs:230` | no for constants | constant conjuncts are folded by A1 before this runs; on columns, seat 1's bound pair `>= -0.0 AND <= 0.0` has unequal bounds under D1, so it is not collapsed back (pinned by part A) |
| B3 | `A = L1 AND A != L2 → A = L1` | `expr_simplifier.rs:1081-1097`, `utils.rs:296` | no for constants | as B2; needs a non-literal `A` |
| B4 | `BETWEEN` expansion | `expr_simplifier.rs:1641-1654` | no for constants | a constant `BETWEEN` is folded whole by A1 (it is evaluable, A2) |
| B5 | `IN` rules, OR→`IN` merge, `are_inlist_and_eq` | `expr_simplifier.rs:1748-2010, 2192-2219` | no for constants | a constant `IN` list is folded whole by A1 through the physical `InList` (F4) |
| B6 | `ShortenInListSimplifier` | `simplify_expressions/inlist_simplifier.rs:35-56` | no | runs after the loop on surviving lists only |
| B7 | `unwrap_cast` in comparison | `simplify_expressions/unwrap_cast.rs:64-140`; `datafusion-expr-common/src/casts.rs:35, 122` | no | needs a column under the cast; a constant cast is folded by A1 |
| B8 | `simplify_predicates` (from `push_down_filter`) | `simplify_expressions/simplify_predicates.rs:103-145, 185-231` | no | groups `column op literal` predicates by column (`extract_column_from_expr`, `:243`); constant predicates never reach it |

### C. RePark's own literal and cast rules

| # | seat | file:line (RePark `d843d37e`) | seen? | because |
|---|---|---|---|---|
| C1 | seat 1 recogniser `zero_type` | `crates/repark-core/src/session/df_guards/signed_zero/literals.rs:401-423` | **yes — misses** | accepts `Literal`, `Negative`, and `Cast`/`TryCast` of a literal; a function call (C3), arithmetic, a nested cast, or a decimal cast that is not yet folded are not zero operands |
| C2 | seat 1 / 1b installation | `literals.rs:22-53`; `crates/repark-core/src/session/df_guards.rs:198-205, 228-233` | yes | the analyzer seat runs before any fold; seat 1b runs only **after** `simplify_expressions`, by which time A1 has folded the constant comparison |
| C3 | `spark_float_stringify` cast rewrite | `crates/repark-functions/src/java_double.rs:114-124, 295-327`; `java_double/parse_float.rs:166-185` | yes — makes the shape | every Spark-door and DataFrame string-to-float cast whose input is not a bare literal becomes `__repark_parse_java_double__`/`__repark_parse_java_float__`; installed on the Spark extension (`crates/repark-functions/src/registration.rs:37`) |
| C4 | `FoldSparkNumericCasts` | `crates/repark-spark/src/spark_typed.rs:22-83` | no | folds a numeric or decimal literal cast to a float literal (so `CAST(0 AS DOUBLE)` becomes `Float64(0)`, which C1 then sees); it compares nothing |
| C5 | `SparkIntegralLiteral` (seat E's gate) | `crates/repark-spark/src/spark_literal_typing.rs:24-80` | no | types integral literals; `plan_may_narrow` skips plans without integral or decimal literals; it compares nothing |
| C6 | seat 2 `signed_zero_keys` | `crates/repark-core/src/session/df_guards/signed_zero/keys.rs` | no | wraps key positions (join, group, distinct, set operations); a constant comparison is not a key |
| C7 | store-range fold | `crates/repark-iceberg/src/write/store_fold.rs:43-60, 132-140` | no | folds a store input to check its range before a write; it never decides a comparison result |
| C8 | connect pushdown simplification check | `crates/repark-connect/src/pushdown.rs:170-182` | no | runs `ExprSimplifier` only to test whether a filter is already simple; the result is discarded |

### D. `ScalarValue` equality and order

| # | seat | file:line | seen? | because |
|---|---|---|---|---|
| D1 | `PartialEq` for floats | `datafusion-common/src/scalar/mod.rs:453, 478-491` | no — not on the path | bitwise (`to_bits`); it decides plan-rule matches (B1–B3), never the value of a folded comparison, which comes from Arrow (F2) |
| D2 | `PartialOrd` / `try_cmp` for floats | `scalar/mod.rs:588, 634-647, 4597` | no for constants | totalOrder; used by B8 and interval code over column predicates |
| D3 | `Hash` | `scalar/mod.rs:933` | no | used for de-duplication of plan expressions only |

### E. Interval and range analysis

| # | seat | file:line | seen? | because |
|---|---|---|---|---|
| E1 | `Interval::equal`, `gt`, `gt_eq`, `contains_value` | `datafusion-expr-common/src/interval_arithmetic.rs:465, 499, 555, 712` | no | used for selectivity and join-filter bounds over column intervals; a constant predicate is already a boolean literal when the physical plan is built |
| E2 | physical `PhysicalExprSimplifier` constant fold | `datafusion-physical-expr/src/simplifier/const_evaluator.rs:46-90`; callers `datafusion-datasource-parquet/src/opener/mod.rs:848`, `row_group_filter.rs:369`, `datafusion-pruning/src/pruning_predicate.rs:475, 493` | no for the table's cells | the logical fold (A1) runs first; it becomes reachable only where a column is replaced by a value at scan time (a partition value), which is a lead and not a cell here |

### F. Arrow kernels (where the folded value comes from)

| # | seat | file:line | seen? | because |
|---|---|---|---|---|
| F1 | physical comparison | `datafusion-physical-expr-common/src/datum.rs:59` (`apply_cmp`) | yes | A3 evaluates the comparison through it |
| F2 | Arrow `cmp` | `arrow-ord-58.4.1/src/cmp.rs:220, 402-419` | yes | `Op::Equal`/`NotDistinct` use `T::is_eq` |
| F3 | float `is_eq` / `compare` | `arrow-array-58.4.1/src/arithmetic.rs:39, 393-398` | yes | totalOrder: `-0.0 < 0.0`, and two NaNs are equal only with equal bits |
| F4 | physical `IN` static filter | `datafusion-physical-expr/src/expressions/in_list/strategy.rs:51`; `primitive_filter.rs:29-60` | yes for `IN` | `OrderedFloat64` hashes and compares by bits, so `-0.0 IN (0.0)` folds to `false` |

### G. The fork's predicate conversion and RePark's SQL-text translators

| # | seat | file:line | seen? | because |
|---|---|---|---|---|
| G1 | `to_iceberg_binary_predicate` | fork `crates/integrations/datafusion/src/physical_plan/expr_to_predicate.rs:549-564` | no | a literal against a literal is `NotTransformed` (`:561`); a folded `WHERE false` becomes an empty relation before any scan |
| G2 | `predicate_binds_soundly` zero guard | fork `expr_to_predicate.rs:62-135` | no | applies to a column against a zero literal only |
| G3 | metadata-only DELETE | `crates/repark-iceberg/src/write/meta_delete.rs:171-240` | no | `comparison_predicate` needs a column on one side, so a constant conjunct gives `None` and the row-level path runs |
| G4 | commit conflict filter | `crates/repark-iceberg/src/write/conflict_filter.rs:104-177` | no | the same column requirement; an untranslated conjunct widens the filter, it never narrows the answer |

## How it was searched

- `EXPLAIN VERBOSE` on both doors for the headline cell (steps 1–4 above), on a package of `6f9f2aaa` and again
  on the step 0 build of `d843d37e`.
- DataFusion: `grep` for `ConstEvaluator`, `can_evaluate`, `evaluate_to_scalar`, `simplify_with_cycle_count`,
  `// Rules for`, `try_cmp`, `partial_cmp`, `PhysicalExprSimplifier`, `OrderedFloat` across
  `datafusion-{optimizer,common,expr-common,physical-expr,physical-expr-common,pruning,datasource-parquet}-54.1.0`.
- Arrow: `is_eq`, `total_cmp` in `arrow-array-58.4.1` and `compare_op` in `arrow-ord-58.4.1`.
- RePark: `grep -rn` over `crates/` for `create_physical_expr(`, `ExprSimplifier::new`, `.simplify(`,
  `SimplifyExpressions::default`, `parse_java_double`, `DECIMAL_CAST_NULLABLE`, and the seat-installation names
  (`install_literal_seat`, `literal_optimizer_seat`, `signed_zero_policy`).
- The fork: `to_iceberg_predicate`, `to_iceberg_binary_predicate`, `literal_binds_soundly` in the checkout of
  `b6e578dc`.

## The proposed seat

One position, two files, both in `repark-core`:

- **Where:** a new instance of the literal rewrite placed **before** `simplify_expressions` in
  `unnest_safe_optimizer_rules` (`df_guards.rs:228-233`), beside the existing seat 1b after it.
- **What it adds:** C1's recogniser also accepts a **column-free, non-volatile float operand** of a comparison
  (`=`, `<>`, `<=>`, `IS DISTINCT FROM`, `<`, `<=`, `>`, `>=`, `IN`, `NOT IN`, `BETWEEN`, simple `CASE`). The
  operand is folded alone with the session's simplifier; if the result is a float zero literal, it is substituted
  and the existing bound-form rewrite applies.
- **Why there:** it is the last point before A1 decides the answer. No DataFusion code changes, and every door
  reaches it, because every plan passes the optimizer: Spark-door SQL, the DataFrame API, `F.expr`, the native door,
  and the DML re-plans (`ctx.sql` on the session context).
- **NaN:** unchanged. A folded NaN operand is not a zero, so no rewrite fires, and NaN-against-NaN cells stay with
  NAN-COMPARE-LITERAL-1 (`greatest`/`least` with NAN-GREATEST-ARRAY-1).
- **Out of reach, recorded:** `greatest`, `least`, `coalesce` return a value, not a comparison result. `nullif`
  and `array_contains` compare inside a function, so they are I5 of NAN-COMPARE-SPARK-1's inventory.

**The alternative** (for the critic to weigh): wrap each column-free float comparison operand in
`__repark_normalize_zero__` at the same position, and let A1 fold it. This also canonicalises NaN in constant
comparisons, which is NAN-COMPARE-LITERAL-1's scope. It is therefore not the proposal.

## After the inventory critic (2026-11-14, `VERDICT: GAPS`)

The critic agreed with the seat. The orchestrator ruled within the unit: take the recogniser extension, reject the
`__repark_normalize_zero__` wrap (it would move NaN cells that NAN-COMPARE-LITERAL-1 owns), keep NaN unchanged and
keep the volatility guard. The critic's items and what the build does with them:

| item | finding | in the build |
|---|---|---|
| M1 | `MERGE … WHEN MATCHED AND p` re-plans the clause predicate as SQL text (`crates/repark-iceberg/src/write/merge/merge_sql.rs:44-150`) | measured (`merge_clause` in the DML part) and pinned; no new seat |
| M2 | a `TEMP VIEW` and a `PREPARE` body with a constant comparison are wrong on the Spark door | pinned; both re-optimize on use |
| M3 | `d BETWEEN <constant zero> AND <constant zero>` is right on main | pinned as a must-not-move cell |
| M4 | `IS DISTINCT FROM` and `IS NOT DISTINCT FROM` | named; the seat's operator set includes both (`literals.rs:68-80`) |
| M5 | searched `CASE` and `IF` are reached through their inner comparison | named and pinned |
| M6 | a scalar-subquery constant comparison | the optimizer applies a rule with an apply order through `rewrite_with_subqueries` (`datafusion-optimizer-54.1.0/src/optimizer.rs:403`), so the seat reaches subquery plans; pinned |
| M7 | no door binds a float zero to a placeholder after analysis (`EXECUTE … USING` is unsupported) | recorded; the earlier open item 4 is withdrawn |
| M8 | `nullif` and the `greatest`/`least` ties are FUNC-FLOAT-EQ-1 cells; `array_contains` belongs to the collection card; `greatest`/`least` keep Spark's first argument deterministically, so they are not a SIGN-ONLY declaration | recorded on those cards |
| W1 | the native door's wrong pairs are every pair with no recognised operand, not only those that meet `arith` | `step0-ready.md` corrected |
| W2 | A5's position | corrected above |
| §4 | native DML was a probe artifact (uncollected `INSERT`) | re-measured; the table is republished |
