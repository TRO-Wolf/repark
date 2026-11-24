# Card TRY-CAST-NONNULL-FIELD-1: a function over `TRY_CAST` of a NOT NULL column crashes on its NULLs

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from an outside nullability sweep, checked
here against Spark. **Severity:** S2 (owner, 2026-11-24): loud. Every cell crashes; none answers wrongly.
**Status:** fixed on `fix/try-cast-nonnull-field-1` (2026-11-24); evidence in
[task/wo/try-cast-nonnull-field-1/evidence/](../../../wo/try-cast-nonnull-field-1/evidence/).

## Measured

- **Sweep (542 cells, live Spark 4.1.2, both ANSI modes):**
  - On main `e80e6bc8`, 87 cells per ANSI mode answered on Spark and crashed on RePark with
    `Arrow error: … Column '…' is declared as non-nullable but contains null values`.
  - On main `514281bd4` (after `#1417` and `#1423`), 72 non-nullable crashes remained per ANSI mode: 63 in-scope
    `TryCastExpr::return_field` crashes (all 63 with logical `df.schema` already matching Spark) plus 9 separate UDF
    logical-nullability defects (`array_remove` × 7, `make_dt_interval` × 2).
  - On head, **63/63 in-scope sweep cells per ANSI mode** match live Spark 4.1.2 on rows, `df.schema` types,
    `df.dtypes`, `df.schema` nullability, and Arrow nullability; 0 regressions and 0 unintended moves across all 542
    cells.
- **Extra doors, sources, writes, pushdown, and native door (109 Spark cells + 8 Iceberg writes + 12 pushdown cells
  per ANSI mode; 16 native cells):**
  - **29/29** crashing Spark read cells per ANSI mode fixed (`spark.sql`, `selectExpr`, `F.expr`,
    `F.col(...).try_cast(...)`, `CastShield` narrowing `TRY_CAST` comparisons, HOF `transform` Rust `ListArray`
    panic, `ProjectionPushdown` merged projections, cross-ANSI temp-view predicate, `EXPLAIN ANALYZE`, Iceberg NOT NULL,
    Parquet/in-memory `coalesce` NOT NULL).
  - **3/3** crashing Iceberg writes per ANSI mode fixed (`INSERT INTO` nullable column, `CTAS` of `abs(TRY_CAST(...))`
    and `TRY_CAST(...) + 1`); `INSERT INTO` a NOT NULL target still refuses where Spark refuses
    (`[NOT_NULL_ASSERT_VIOLATION]`); `UPDATE` and `MERGE` stay equal to Spark.
  - **7/7** crashing native `repark.sql()` cells fixed.
  - **Pruning and `EXPLAIN`:** byte-identical normalized plans on every query without `TRY_CAST` and on every bare
    `TRY_CAST` `WHERE` and dynamic join filter; Iceberg (`N=1`) and Parquet (`3 total -> 1 matched`) prune the same
    files as on main.

Re-measured on main `514281bd4` and head, over `(SELECT 'x' AS s UNION ALL SELECT '7')`:

| Statement | Spark 4.1.2 | Main `514281bd4` | Head |
|---|---|---|---|
| `SELECT abs(TRY_CAST(s AS INT))` | NULL, 7 | **crash** | NULL, 7 |
| `SELECT TRY_CAST(s AS INT) + 1` | NULL, 8 | **crash** | NULL, 8 |
| `SELECT -TRY_CAST(s AS INT)` | NULL, -7 | NULL, -7 | NULL, -7 |

## Cause

- In `datafusion-physical-expr` 54.1.0, `TryCastExpr::nullable` returns true, but `TryCastExpr::return_field` keeps
  the input field's nullability (`expressions/try_cast.rs`).
- A function (`ScalarFunctionExpr`, `HigherOrderFunctionExpr`, `AggregateExprBuilder`, `WindowUDFExpr`) that sets its
  output nullability from its input fields then declares a non-null column (or non-null `ListArray` item field), and
  Arrow refuses the batch or panics.

## Resolution (three seats, 2026-11-24)

1. **Physical planner (`crates/repark-core/src/stack/try_cast_nullable.rs`, `stack/planner.rs`, `session/df_guards.rs`):**
   `StackQueryPlanner::create_physical_plan` runs `shield_try_cast_plan` right before DataFusion builds physical
   expressions. When no `Expr::TryCast` exists, the plan is borrowed untouched. When a `TryCast` feeds a
   `ScalarFunction`, `HigherOrderFunction`, `AggregateFunction`, `WindowFunction`, or `Lambda` (including through
   `Cast`/`Negative`/`Not`), it wraps the `TryCast` in the zero-copy `__repark_nullable_field__` scalar UDF whose
   `return_field_from_args` marks the output field nullable while preserving name, type, metadata, bounds, and
   ordering. Bare `WHERE`/`SELECT` `TryCast` predicates stay bare for scan pruning.
2. **Post-pushdown physical repair (`crates/repark-core/src/session/df_guards/skipping_limit.rs`):**
   `TryCastSafeSanityCheck` wraps DataFusion's `"SanityCheckPlan"` (keeping the rule name and exact stock rule count)
   and runs `repair_inlined_try_cast_plan` to wrap any raw `TryCastExpr` inlined into a consumer expression by
   DataFusion's physical `ProjectionPushdown` inside `ProjectionExec` or `FilterExec` with `NullableFieldPhysicalExpr`.
3. **Cross-ANSI temp-view predicate (`crates/repark-spark/src/view_ddl/temp_settings.rs`):**
   Applies `repark_core::shield_try_cast_expr` before `SessionState::create_physical_expr` compiles a rewritten temp-view
   filter predicate.

Pins: `python/repark/tests/sql/test_try_cast_nonnull_field_1.py` (183 tests: 181 red on main + 2 pruning controls; 183
green on head) and unit tests in `crates/repark-core/src/stack/try_cast_nullable/tests.rs`.

## Still unlike Spark (separate shapes, out of scope for the `TryCast` seat)

Every cell below has a wrong **logical** output schema on its own UDF (not `TryCastExpr::return_field`) and is
identical in both ANSI modes (`false` and `true`):

| Cell | Spark 4.1.2 | Main `514281bd4` & Head | Cause |
|---|---|---|---|
| `case_no_else\|coll/array_remove` (`sep/*/case_no_else_array_remove`) | `[[1, None], [2, None]]`, nulls `[False, True]` | crash (`Column 'r' is declared as non-nullable`), nulls `[False, False]` | `array_remove` `return_field_from_args` ignores arg 1 nullability |
| `dfbug/array_remove_null_elem` (`sep/*/array_remove_null_elem`) | `[[1, None], [2, [2]]]`, nulls `[False, True]` | crash (`Column 'r' is declared as non-nullable`), nulls `[False, False]` | `array_remove` `return_field_from_args` ignores arg 1 nullability |
| `left_join\|coll/array_remove` | `[[1, None], [2, [1]]]`, nulls `[False, True]` | crash (`Column 'r' is declared as non-nullable`), nulls `[False, False]` | `array_remove` `return_field_from_args` ignores arg 1 nullability |
| `nullif_self\|coll/array_remove` | `[[1, None], [2, None]]`, nulls `[False, True]` | crash (`Column 'r' is declared as non-nullable`), nulls `[False, False]` | `array_remove` `return_field_from_args` ignores arg 1 nullability |
| `subq_empty\|coll/array_remove` | `[[1, None], [2, None]]`, nulls `[False, True]` | crash (`Column 'r' is declared as non-nullable`), nulls `[False, False]` | `array_remove` `return_field_from_args` ignores arg 1 nullability |
| `try_int\|coll/array_remove` (`sep/*/array_remove_try_int`, native `sep/array_remove_try_int`) | `[[1, None], [2, [1]]]`, nulls `[False, True]` | crash (`Column 'r' is declared as non-nullable`), nulls `[False, False]` | `array_remove` `return_field_from_args` ignores arg 1 nullability (even with nullable arg 1) |
| `neg_try_int\|coll/array_remove` | `[[1, None], [2, [1, 7]]]`, nulls `[False, True]` | crash (`Column 'r' is declared as non-nullable`), nulls `[False, False]` | `array_remove` `return_field_from_args` ignores arg 1 nullability (even with nullable arg 1) |
| `dfbug/make_dt_interval_nan_col` (`sep/ice/make_dt_interval_nan_col`) | `[[1, None], [2, '7 days, 0:00:01.500000']]`, `intervaldaytosecond`, nulls `[False, True]` | crash (`Column 'r' is declared as non-nullable`), `string`, nulls `[False, False]` | `make_dt_interval` UDF declares non-null output despite NaN producing NULL (and returns `string` on nullable inputs: `sep/pq/`, `sep/mem/`) |
| `dfbug/make_dt_interval_ovf_col` (`sep/*/make_dt_interval_ovf_col`) | `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION` | Iceberg: crash (`Column 'r' is declared as non-nullable`); Parquet/mem: returns `NULL` instead of raising | `make_dt_interval` UDF emits NULL on overflow instead of raising |

Note also (pre-existing, unchanged by this unit): `spark.createDataFrame(rows, schema)` with explicit `nullable=False`
`StructField`s currently emits `nullable=True` columns on RePark (`door/*/mem/*` col 0 `id` is `True` vs Spark
`False`).

Up: [map.md](map.md).
