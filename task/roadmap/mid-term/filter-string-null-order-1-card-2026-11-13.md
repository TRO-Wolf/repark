# Card FILTER-STRING-NULL-ORDER-1 — an `ORDER BY` inside a string `filter` / `where` placed NULLs backwards

**Date:** 2026-11-13 · **Severity:** S2 (wrong rows, no error; a filter can feed a write) ·
**Built by:** Claude Opus 5.5 (`claude-opus-5-5`), the second builder · **Source:** finding C1 of
both verifiers of PR #1316 (FEXPR-WINDOW-NULL-ORDER-1); owner ruling of 2026-11-13, verbatim "Merge
anything you need. Then proceed with defaults.": card it at S2 and fix it before the scoped label
ICE-V2-FLAT-AWS-LABEL-1, as a small unit in the second-builder queue after Q10; the native door keeps
its own dialect (ruling Q6). **Status:** **FIXED by this unit** (branch
`fix/filter-string-null-order-1`, built on #1316, which has since merged).

## Why

Spark places NULLs **first** for `ASC` and **last** for `DESC` unless the text says `NULLS FIRST` or
`NULLS LAST`. A string predicate such as
`w.filter("ki = (SELECT a FROM VALUES (NULL), (10), (20) AS q(a) ORDER BY a DESC LIMIT 1)")` returned
`[]` where Spark returns the row with `ki = 20`, while `filter(F.expr(<same text>))` was right on
#1316. A string predicate was planned on the frame's own session state, which keeps DataFusion's
`nulls_max` default, and no Spark AST pass runs on the text.

## Inventory (insertion-inventory rule)

Every DataFrame or Column method that takes SQL text, and the seat that plans it:

| Surface | Plans on | Cite |
|---|---|---|
| `DataFrame.filter(str)`, `where(str)` (`where = filter`) | the frame's session state: `PyDataFrame.filter_sql` → `filter_frame_with_sql` → `parse_canonical_predicate` (case-insensitive) or `parse_canonical_predicate_exact` (`spark.sql.caseSensitive=true`) | `python/repark/src/repark/spark/dataframe/core.py:1154-1156`, `:1165`; `crates/repark-python/src/dataframe/mod.rs:490-500`; `crates/repark-python/src/dataframe_names.rs:115-122`; `crates/repark-python/src/column/expr_build.rs` |
| `DataFrame.filter(Column)` whose `join_sql_part()` carries an attribute token (a frame with display names, such as a join) | re-rendered as SQL text and sent to the same `filter_sql` | `core.py:1146-1150` |
| `F.expr(str)` anywhere (`select`, `withColumn`, `filter`, `agg`, `groupBy().agg`, join conditions) | the fragment context `build_expr_context`, `nulls_min` since #1316 | `crates/repark-python/src/column/expr_build.rs` (`sql_context`, `build_expr_context`) |
| `DataFrame.selectExpr(str)` | the Spark door: a scratch temp view and `session.sql` | `python/repark/src/repark/spark/filter_quote.py:582-597` |
| `rollup` / `cube` / grouping sets with `F.expr`, join conditions rendered by `join_sql_part()`, `MergeIntoWriter` string conditions | the Spark door (`session.sql`), whose AST pass `apply_spark_order_by_defaults` fills the placement | `python/repark/src/repark/spark/dataframe/joins_columns.py:473-503`; `python/repark/src/repark/spark/merge.py:43-96`, `:162`; `crates/repark-spark/src/spark_ast.rs` |
| `when`/`otherwise`, `agg(dict)`, `GroupedData` string forms, `sort("k")`, `join(on=str)` | no SQL text: a column or function name | |
| `PolarsFrame.filter(str)` | delegates to the frame's `filter` (`python/repark/src/repark/spark/polars.py:155-158`) | the first row |

Only the first two rows planned SQL text on the frame's state; `parse_canonical_predicate*` are the
only frame-state SQL parses in `repark-python`, `repark-core` and `repark-spark`.

## Step 0 (base `35c5de77`, release build, against PySpark 4.1.2)

Evidence: [task/wo/filter-string-null-order-1/](../../wo/filter-string-null-order-1/map.md). Rows
`(1, 10, 'b', 1.5)`, `(2, NULL, 'e', 2.5)`, `(3, 30, NULL, 3.5)`, `(4, 20, 'a', NULL)`,
`(5, 40, 'd', 0.5)` as `(id, ki INT, ks STRING, kd DOUBLE)`. 28 predicates on INT, STRING and DOUBLE
keys: a scalar subquery `ORDER BY … LIMIT` with `ASC`, bare, `DESC`, the four explicit placements,
multi-key, `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS`, a window inside a subquery, a derived-table count.

| Door | Cells | Base | This unit |
|---|---|---|---|
| `filter(str)` on `spark.table`, `where(str)`, `filter(str)` on a `spark.sql` frame, a `createDataFrame` frame, a derived frame, a case-sensitive session | 28 each | 20 WRONG-READ (every unstated placement), 8 EQUAL (explicit placement) | all EQUAL |
| a joined frame: `filter(w[k] == F.expr(<subquery>))` (the attribute-token path) and `filter(str)` | 3 each | 3 WRONG-READ | all EQUAL |
| `filter(F.expr)`, `spark.sql` `WHERE` | 28 each | EQUAL | unchanged |
| `selectExpr("(<predicate>) AS r")` | 28 | 23 EQUAL; the 5 `IN` subqueries refuse in a projection, as before | unchanged |
| native: `repark.sql(…).filter(str)` and `repark.sql(… WHERE …)` | 28 each | its own `nulls_max` dialect | unchanged |
| controls: four sort methods, two typed `Window` cells, `spark.sql` window and `ORDER BY`, `F.expr` window, `selectExpr` window, native window and `ORDER BY` | 12 | EQUAL (native at its own answer) | unchanged |

## Cause and seat

`parse_canonical_predicate` called `DataFrame::parse_sql_expr`, and `parse_canonical_predicate_exact`
called `SessionState::create_logical_expr`, both on the frame's state. DataFusion's
`order_by_to_sort_expr` fills an unstated placement from `sql_parser.default_null_ordering`, which
that state leaves at `nulls_max`. Both now plan on `predicate_state(frame)`
(`crates/repark-python/src/column/expr_build.rs`): a clone of the frame's state that sets
`default_null_ordering = "nulls_min"` when the frame's session config carries `SparkDoorSession`, a
marker `SparkExtension` installs in `install_spark_session_conf`
(`crates/repark-spark/src/extension.rs`, read by `repark_spark::is_spark_door_session`). The native
session (`PyReparkSession::native`) never installs `SparkExtension`, so its frames keep `nulls_max`.
One seat covers both rows of the inventory, because the attribute-token path ends in the same
`filter_sql`. An explicit `NULLS FIRST` / `NULLS LAST` still wins: DataFusion reads the option only
when the text states no placement. The marker changes nothing else: only `predicate_state` reads it.

## Pins

- `crates/repark-python/src/column/expr_tests.rs::a_string_predicate_takes_spark_null_placement_only_on_the_spark_door`:
  the sort options of scalar, `IN`, `EXISTS`, multi-key, window and explicit `ORDER BY` inside a
  predicate, through both parse functions, on a real Spark-door frame and a real native frame. Red
  with the seat removed (`[(false, true)]` for `ORDER BY a DESC` on the Spark door), green here.
- `python/repark/tests/functions/test_filter_string_null_order_1.py` over
  `filter_string_null_order_1_spark_oracle.json`: the 126 wrong cells at Spark's rows, the 137
  cells that already matched Spark (explicit placement and every control), and 58 native cells at
  the base's own answer. On the base build the 8 fixed groups fail and the 14 others pass; here all
  22 tests pass.

## Findings (not ruled, not in this unit)

- **F1. #1316's F1 reaches the string filter too.** An aggregate `ORDER BY` inside a subquery of a
  string predicate (`array_agg(a ORDER BY a)`), which Spark refuses to parse, now orders NULLs like
  `F.expr`, while `spark.sql` and `selectExpr` keep `nulls_max`. A refusal of the syntax on every
  door would settle it, as #1316's card says.
- **F2. A join with a projection of the same view refuses.** `w.join(spark.sql("SELECT id AS rid,
  ki AS rki FROM w"), w["id"] == right["rid"])` raises `UNRESOLVED_COLUMN` for `id` on the base and
  here, where Spark joins. The probe uses an independent `VALUES` source for the right side.
- **F3. An `IN` subquery in a projection refuses** (`selectExpr("(ki IN (SELECT …)) AS r")`), as
  #1316's verifiers recorded (`q05`); the filter forms answer.
