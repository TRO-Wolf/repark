# SUM-DEC-I128WRAP-1 — the insertion inventory (2026-11-17)

By the insertion-inventory rule (owner, 2026-11-09): every seat that sets a decimal `sum`'s result type,
accumulates it, checks it or evaluates it, with "seen at the seat: yes / no, because". Read on main `6f80793b`, the
DataFusion 54.1.0 and datafusion-spark 54.1.0 sources in the cargo registry, and the iceberg-rust fork at
`b6e578dc` (the workspace patch). "Measured" names cells in [domain-table.md](domain-table.md).

**How searched.** `grep -rn '"sum"\|sum_udaf\|try_sum\|TrySum\|SparkSum'` over `crates/` and `python/repark/src`;
`grep -rn 'register_all\|all_default_aggregate_functions\|with_default_features'` for the registration lists;
`grep -rn 'simplify_expr_op_literal\|add_wrapping'` over the DataFusion sources; `EXPLAIN` of eight cells on both
engines (`raw/*-true.jsonl`, keys `explain/*`); the jar by `javap` ([evidence/jar/](evidence/jar/map.md)).

## R — which `sum` each door reaches

| id | seat | seen at the seat |
|---|---|---|
| R-1 | Spark-door session: `SessionStateBuilder::with_default_features()` (`crates/repark-core/src/session/df_guards.rs:173-181`) registers DataFusion's default aggregates, **`sum` among them** | **yes** — this is the `sum` every SQL cell runs. Then `register_all` (`crates/repark-functions/src/lib.rs:106-115`, called from `crates/repark-spark/src/extension.rs:176`) registers datafusion-spark's aggregates (`lib.rs:110`: `avg`, `try_sum`, `collect_list`, `collect_set`) and RePark's (`lib.rs:113`, `aggregate.rs:31-45`: `avg`, `mean`, `try_avg`, `count_if`, grouping, bitmap, two signed wrappers). **No list registers a `sum` over DataFusion's.** Measured: the wrap is exactly `i128` wrapping (`grouped/g6` = `2·10^38 − 2 − 2^128`) |
| R-2 | `try_sum` on the SQL door: datafusion-spark's `SparkTrySum` (`lib.rs:110`) | **yes** — the only `try_sum` registered; RePark's `aggregate::functions()` has none |
| R-3 | DataFrame door: `PyColumn::aggregate` (`crates/repark-python/src/column/mod.rs:886-891`) → `unary_aggregate_udaf` (`column/function_dispatch.rs:936-939`): `"sum" => sum_udaf()` (DataFusion's, by value, **not by name**), `"try_sum" => repark_functions::aggregate::try_sum_udaf()` (`aggregate.rs:47-51`, datafusion-spark's) | **yes** — a registration over the name `sum` would **not** reach this door; the arm must change too. Facade callers: `F.sum` (`python/repark/src/repark/spark/functions.py:515-531`), `F.try_sum` (`functions_try.py:85-97`), `GroupedData.sum` and `_shortcut_aggregate` (`dataframe/joins_columns.py:772-774`, `:799`), `agg({c: "sum"})` (`joins_columns.py:835`). Measured: every DataFrame form answers the SQL door's values (§5) |
| R-4 | `F.expr` / `selectExpr`: `build_expr_context` (`crates/repark-python/src/column/expr_build.rs:335-351`): `with_default_features()` then `register_all` | **yes** — resolves `sum` by name to DataFusion's, as R-1; a registration over the name reaches it (it calls `register_all`). Measured: `df/F.expr/*`, `df/selectExpr/*` |
| R-5 | Native door `repark.sql()`: `PyReparkSession::native` (`crates/repark-python/src/session.rs:151-167`): DataFusion defaults plus `register_native_door_functions` (`session_runtime.rs:452-461`), which registers no aggregate | **yes, for DataFusion's `sum`**; `try_sum` is not registered there (measured `UNRESOLVED_ROUTINE`). A Spark-door fix does not reach it unless it is registered here too |
| R-6 | `F.sum_distinct` / `F.sumDistinct` | **no**, because the facade has neither (measured `AttributeError`; FNP-AGG-1, PR #625 draft) |
| R-7 | The session-window rewrite: `running_sum_over` (`crates/repark-functions/src/analyzer/time_window/session_window.rs:228-231`) builds DataFusion's `sum_udaf()` directly | **no** decimal input reaches it: it sums the session-start markers, an integer; listed because a by-name registration would not replace it and need not |

## A — DataFusion's `Sum` (datafusion-functions-aggregate 54.1.0, `src/sum.rs`): type, accumulators, check

| id | seat | seen at the seat |
|---|---|---|
| A-1 | `Sum::return_type` (`sum.rs:211-238`): `Decimal128(min(38, p + 10), s)` | **yes**, and it equals Spark's `bounded(p + 10, s)` at every measured precision (no `TYPE` cell on the Spark door) |
| A-2 | `SumAccumulator` (ungrouped; `sum.rs:391-437`): `update_batch` = `arrow::compute::sum` then `add_wrapping` (`:420`); `merge_batch` = `update_batch`; `evaluate` = the native value with the declared type, **no precision check** | **yes** — the `X`/`W`/`w` cells ungrouped |
| A-3 | `create_groups_accumulator` (`sum.rs:286-299`): `PrimitiveGroupsAccumulator` with `add_wrapping` (`:294`) | **yes** — every grouped cell, the `DISTINCT` cells after the rewrite (A-6), `HAVING`, CTE, subquery, view, Iceberg sources |
| A-4 | `create_sliding_accumulator` (`sum.rs:301-322`): `SlidingSumAccumulator` (`:441-510`), `add_wrapping`/`sub_wrapping`, `count` for NULL | **yes** — `ROWS`/`RANGE` sliding frames; retraction is modular, so a frame whose true sum fits comes back right |
| A-5 | `DistinctSumAccumulator` (`datafusion-functions-aggregate-common-54.1.0/src/aggregate/sum_distinct/numeric.rs:64-76`): a hash set, summed with `add_wrapping` at `evaluate` (`:74`) | **no** in the measured single-distinct cells, because A-6 rewrites them; **yes** for a `DISTINCT` beside a non-distinct aggregate of another argument (not measured; same wrap by reading) |
| A-6 | Optimizer `SingleDistinctToGroupBy`: `sum(DISTINCT x)` → group by `(g, x)`, then `sum(alias1)` | **yes** — EXPLAIN `explain/distinct`: the outer `sum(alias1)` is A-3 with a clone of the same `AggregateUDF`, so a RePark UDAF is carried through |
| A-7 | `Sum::simplify_expr_op_literal` (`sum.rs:352-395`) via `rewrite_multiple_linear_aggregates` (`datafusion-optimizer-54.1.0/src/simplify_expressions/linear_aggregates.rs:27-120`, from `simplify_exprs.rs:188`): `sum(x + c1), sum(x + c2)` → `sum(x) + c·count(x)` | **no** for decimals on the Spark door, because RePark's decimal `+` is the UDF `__repark_spark_decimal_add__` (EXPLAIN `explain/linear`), not a `BinaryExpr`; it can fire for integer arguments. A RePark UDAF that does not implement the hook switches the rewrite off for every type it owns; delegating the hook for non-decimal types keeps it |
| A-8 | `ParallelWindowExec` (`crates/repark-core/src/parallel_window/exec.rs:229-356`, rule `rule.rs`): runs the plan's `WindowExpr`s on the blocking pool | **yes**, through A-2/A-4 — it evaluates the same expressions (measured `win_parallel`) |

## T — `try_sum`

| id | seat | seen at the seat |
|---|---|---|
| T-1 | `SparkTrySum::return_type` (`datafusion-spark-54.1.0/src/function/aggregate/try_sum.rs:258-275`): `Decimal128(min(38, p + 10), s)` | **yes**, equal to Spark's |
| T-2 | `TrySumAccumulator::update_decimal128` (`try_sum.rs:189-220`): `checked_add`, then `exceeds_decimal128_precision` against the **result** precision after **every row**; `failed` sticks; `merge_batch` (`:99-112`) ORs the partials' `failed` and re-adds | **yes** — per-row sticky in every path: equal to Spark's grouped buffer, NULL where Spark's ungrouped or whole-window sum comes back (§9) |
| T-3 | No `GroupsAccumulator`, no `retract_batch` | grouped `try_sum` runs one row accumulator per group (a cost case); sliding frames re-scan (WIN-SLIDE-1) |

## C — Spark's check, mirrored in RePark today

| id | seat | seen at the seat |
|---|---|---|
| C-1 | `SparkDecimalAverage` (`crates/repark-functions/src/avg_decimal.rs:168-301`) | **no** — `avg` only; it already carries Spark's sum check for `avg` (`sum_limit = 10^min(38, p + 10)`, `sum_overflow_error` with the `try_avg` hint), which `sum` needs with the `try_sum` hint |
| C-2 | `SparkAvgAnsiMode` (`aggregate.rs:297-349`): swaps `avg` to its legacy mode when ANSI is off | **no** — `legacy_avg_udaf` (`:341`) downcasts to `SparkAvgWithRetract` only; `sum` has no ANSI arm anywhere |
| C-3 | `SparkDecimalPrecision` / `__repark_spark_decimal_add__` (RePark's decimal `+`) | **partly**: a 39-digit `sum` fed into `+ 1` is caught there (`NUMERIC_VALUE_OUT_OF_RANGE` on, NULL off), a 38-digit wrap is not (`plus1/g7`) |

## I — the Iceberg store and DML paths

| id | seat | seen at the seat |
|---|---|---|
| I-1 | The CTAS / `INSERT … SELECT` / `MERGE` source plan | **yes** — the source is A-3's value; the store receives `decimal(38, s)` values already wrong |
| I-2 | `StoreOverflowCast` (`crates/repark-iceberg/src/write/store_overflow.rs`), `is_overflow_store_pair` (`write/store_cast.rs:53-60`) | **no**, because it guards float or decimal into an **integer** column only; a `decimal(38, s)` source into a `decimal(38, s)` column has no store cast at all |
| I-3 | The fork's Parquet writer: per-file min/max statistics (`crates/iceberg/src/writer/file_writer/parquet_writer.rs:340-351`) → `get_parquet_stat_min/max_as_datum` (`arrow/schema.rs:1050`, `:1196`) → `Datum::decimal_with_precision` (`spec/values/datum.rs:1044-1059`) → `validate_decimal_value` (`datum.rs:70-84`): digit count of the unscaled value | **yes, after the fact**: a value past 38 digits is always a file's min or max, so the file's statistics refuse it (`DataInvalid`), but only after the file is written: the 39-digit values stay in uncommitted files (§6, 2 per mode); a wrapped value inside 38 digits passes and commits (8 per mode) |
| I-4 | `FloatDecimalStoreRule` (`repark-iceberg/src/write/conform_stage.rs`), `InsertStoreAssignment` | **no** — float/decimal conformance and the ANSI store-assignment gate; neither checks a decimal value against its own declared precision (a reading of the rule names and the measured commits) |

## S — stored plans and other analyzer lists

| id | seat | seen at the seat |
|---|---|---|
| S-1 | Temp views (SQL door re-plans the text per read; DataFrame door inlines the plan) | **yes** — the view body's `sum` is the same UDAF (measured `temp_view`); a UDAF-level fix needs nothing here, an analyzer-level fix does (the view family, F1) |
| S-2 | CTE and scalar subquery | **yes** (measured `cte`, `scalar_sub`) |
| S-3 | Other analyzer lists: `build_expr_context` (R-4), the native door's | a UDAF-level fix is reached through registration on each; an analyzer-level fix must be listed in each list (the avg sketch's lesson) |

## Summary

- One accumulator family decides every Spark-door value: DataFusion's `Sum` (A-2, A-3, A-4, and A-5 by reading),
  reached by name (R-1, R-4) and by value (R-3); `try_sum` is datafusion-spark's (T-2) on both doors.
- No seat checks a decimal `sum` against `decimal(38, s)`: the first check is the Iceberg writer's (I-3), after the
  file is written; RePark's `+` (C-3) catches only 39-digit operands.
- The avg unit's machinery (C-1, `WideDecimal` in `avg_decimal.rs:22-147`, `add_batch`/`sub_batch` `:319-357`,
  `DecimalAvgGroups` `:467-688`) is the reusable part; its ANSI rule (C-2) needs one more downcast arm.
- The native door (R-5) keeps DataFusion's `sum` and has no `try_sum` unless the fix registers there.
