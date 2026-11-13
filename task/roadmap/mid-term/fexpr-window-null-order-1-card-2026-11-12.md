# Card FEXPR-WINDOW-NULL-ORDER-1 — an `F.expr` fragment's `ORDER BY` placed NULLs backwards

**Date:** 2026-11-12 · **Severity:** S2 (wrong rows, no error; the dedup shape feeds writes) ·
**Built by:** Claude Opus 5.5 (`claude-opus-5-5`), the second builder · **Source:** owner ruling of
the same day, verbatim "Go with the defaults" (signed-zero part B, Q6): fix it before the scoped label
ICE-V2-FLAT-AWS-LABEL-1, as its own small PR in v1.5.7; the native door keeps its own dialect.
**Status:** **FIXED by this unit** (branch `fix/fexpr-window-null-order-1`).

## Why

Spark places NULLs **first** for `ASC` and **last** for `DESC` unless the text says `NULLS FIRST` or
`NULLS LAST`. On main, an `ORDER BY` inside an `F.expr` fragment placed them the other way in both
directions. `F.expr('row_number() OVER (ORDER BY m DESC)')` gave row 1 to the NULL row, and the
common dedup `withColumn('r', F.expr('row_number() OVER (ORDER BY ts DESC)')).filter('r = 1')` kept
the NULL-timestamp row where Spark keeps the latest. `spark.sql`, `selectExpr`, the sort methods and
the typed `Window` were right.

## Cause and seat

`F.expr` text is planned by `build_expr_context` (`crates/repark-python/src/column/expr_build.rs`)
through DataFusion's `SessionState::create_logical_expr`. Every `ORDER BY` an expression can carry
reaches DataFusion's `order_by_to_sort_expr`, which fills an unstated NULL placement from
`sql_parser.default_null_ordering`. That option defaults to `nulls_max` (PostgreSQL's rule, NULLs
largest). The Spark door never reads it: its AST pass (`apply_default` in
`crates/repark-spark/src/spark_ast.rs`) writes `NULLS FIRST` for `ASC` and `NULLS LAST` for `DESC`
into window specs and query `ORDER BY` before planning. The fragment path has no such pass.

The fix sets `default_null_ordering = "nulls_min"` (NULLs smallest, Spark's rule) in
`build_expr_context`. It is one seat, and it covers every wrong row: window `ORDER BY`, and the query
`ORDER BY` of a scalar subquery inside a fragment. An explicit `NULLS FIRST` / `NULLS LAST` still wins,
because DataFusion reads the option only when the text states no placement. Nothing else reads the
fragment context, so the Spark door, `selectExpr` and the native door do not move.

## Step 0 (main `a3f8c7d9`, release build, against PySpark 4.1.2)

Evidence: [task/wo/fexpr-window-null-order-1/](../../wo/fexpr-window-null-order-1/map.md). Rows
`(1, 10, 'b', 1.5, 1)`, `(2, NULL, 'e', 2.5, 1)`, `(3, 30, NULL, 3.5, 2)`, `(4, 20, 'a', NULL, 2)`,
`(5, 40, 'd', 0.5, 1)` as `(id, ki INT, ks STRING, kd DOUBLE, g)`; one NULL per key column.

| Inventory row (what a fragment can carry) | Cells | Main | This unit |
|---|---|---|---|
| Window `ORDER BY`: `row_number`, `rank`, `dense_rank`, `first_value`, `last_value`, `lag`, `lead`, running `sum`, `collect_list`; seven directions on INT, STRING and DOUBLE keys, five multi-key orders, `PARTITION BY` | `F.expr` in `select` 288, in `withColumn` 288 | 169 + 169 WRONG-READ (every unstated placement); 119 + 119 EQUAL (explicit placement) | all EQUAL |
| The dedup shape, plain and partitioned | 42 | 18 WRONG-READ | all EQUAL |
| Scalar subquery `(SELECT … ORDER BY a [DESC] LIMIT 1)` | 2 | 2 WRONG-READ | EQUAL |
| `WITHIN GROUP (ORDER BY …)` | 11 forms × 2 fragment doors | `percentile_cont` EQUAL (NULL keys are dropped, so placement cannot show); `percentile_disc`, `mode`, `listagg`, `string_agg` refuse on every RePark door | unchanged |
| Aggregate `ORDER BY` (`array_agg(id ORDER BY ki)`, `string_agg(…, ',' ORDER BY ks)`, `first_value(id ORDER BY ki DESC)`) | 4 forms | Spark refuses the syntax; RePark accepts on every door | see finding F1 |
| `array_sort` with a comparator | 1 | the lambda refuses on `F.expr` and `spark.sql` (`Unsupported ast node in sqltorel: Lambda`); no `ORDER BY` | unchanged |

Controls, all unchanged: `spark.sql` windows (295 cells EQUAL), `selectExpr` (290 EQUAL),
`orderBy(F.expr(k))`, `sort`, `orderBy(F.expr(k).<method>())`, `sort(col.<method>())`, the typed
`Window.orderBy(col.<method>())` (18 each), and the native `repark.sql()` door (262 answered cells,
its own `nulls_max` dialect).

## Pins

- `crates/repark-python/src/column/expr_tests.rs::fragment_order_by_takes_spark_null_placement_unless_explicit`:
  the sort options of window and subquery `ORDER BY` at the seat. Red on main (`(true, false)` for a
  bare `ORDER BY m`), green here.
- `python/repark/tests/functions/test_fexpr_window_null_order_1.py` over
  `fexpr_window_null_order_1_spark_oracle.json`: the 358 wrong cells at Spark's rows, the 265
  explicit-placement fragment cells, and every control (the native cells at main's own answer). On
  main's build the two fixed groups fail, 358 of 358 cells, and the 13 other groups pass; here all 15
  pass.

## Findings (not ruled, not in this unit)

- **F1. Aggregate `ORDER BY` is accepted where Spark refuses it, and the doors now differ on it.**
  Spark 4.1.2 refuses `array_agg(id ORDER BY ki)` with `PARSE_SYNTAX_ERROR`. RePark accepts it on
  every door. After this unit the `F.expr` door orders its NULLs Spark's way (NULL first for `ASC`),
  while `spark.sql`, `selectExpr` and the native door keep `nulls_max`, because the Spark door's AST
  pass does not visit function-argument `ORDER BY`. Six fragment cells moved: `array_agg(id ORDER BY
  ki)` over all rows answers `[2, 1, 4, 3, 5]` through `F.expr` and `[1, 4, 3, 5, 2]` through
  `selectExpr`; `first_value(id ORDER BY ki DESC)` answers 5 and 2. No Spark answer exists for this
  syntax, so no cell pins either side. A refusal of the syntax on every door would settle it.
- **F2. `WITHIN GROUP` coverage:** `percentile_disc`, `mode`, `listagg` and `string_agg` with
  `WITHIN GROUP` refuse on every RePark door where Spark answers. When they are added, the Spark
  door's AST pass must also fill their `ORDER BY` placement; the fragment door already does.
- **F3. `F.expr('k DESC')` refuses with a parse error,** where Spark parses `k DESC` as `k` aliased
  `DESC` and sorts ascending. It is a refusal, not a wrong read.
