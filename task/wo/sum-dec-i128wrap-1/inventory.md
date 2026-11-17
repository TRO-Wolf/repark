# SUM-DEC-I128WRAP-1 — the insertion inventory (2026-11-17)

By the insertion-inventory rule (owner, 2026-11-09): every seat that sets a decimal `sum`'s result type,
accumulates it, checks it or evaluates it, with "seen at the seat: yes / no, because". Read on main `6f80793b`, the
DataFusion 54.1.0 and datafusion-spark 54.1.0 sources in the cargo registry, and the iceberg-rust fork at
`b6e578dc` (the workspace patch). "Measured" names cells in [domain-table.md](domain-table.md).

**How searched.** `grep -rn '"sum"\|sum_udaf\|try_sum\|TrySum\|SparkSum'` over `crates/` and `python/repark/src`;
`grep -rn 'register_all\|all_default_aggregate_functions\|with_default_features'` for the registration lists;
`grep -rn 'simplify_expr_op_literal\|add_wrapping'` over the DataFusion sources; `EXPLAIN` of eight cells on both
engines (`raw/*-true.jsonl`, keys `explain/*`); the jar by `javap` ([evidence/jar/](evidence/jar/map.md)).
**Revision (after the critic):** each seat the critic named was read at its cited lines and `grep`ped
(`_agg_via_sql_group`, `_grouping_sets_grouped`, `_pivot_aggregate_builder`, `_resolve_routine`, `needs_frame_rescan`,
`analyzer_rules_with_sliding_rescan`, `collects_bounds`, `limit_field_ids`, `aggregate`/`pushdown` in
`repark-connect`); the routes were measured (`raw/rev-*.jsonl`, keys `rev/df_*`, `route/scalar_sum`,
`explain/rescan/*`, `explain/distinct_mixed`); the Iceberg Java writer was read by `javap` from the runtime jar.

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
| R-8 | DataFrame `cube`, `rollup`, `groupingSets` (`python/repark/src/repark/spark/dataframe/core.py:2365-2383`, `_grouping_sets_grouped`) → `GroupedData.agg` (`dataframe/joins_columns.py:139-140`) → `_agg_via_sql_group` (`joins_columns.py:473-504`): a temp view of the plan, then `SELECT … sum(c) … GROUP BY <sets>` through `session.sql_built` | **yes, by name** (revision) — these forms reach R-1, not R-3, so the by-name registration (A1) covers them and the `:938` arm does not. Measured `rev/df_rollup/g1_7`, `rev/df_rollup_total/g1_4`, `rev/df_cube_total/g1_4`, `rev/df_gsets_total/g1_4`: silent, both modes. RePark's `groupingSets(*cols)` takes columns and always adds `()`; PySpark 4.1's takes a list of sets first (the cells call each engine's form) |
| R-9 | DataFrame `pivot`: `_pivot_aggregate_builder` (`dataframe/grouped_pivot.py:79-90`) returns the facade's `F.sum` for a `sum(` aggregate | **yes, by value** (revision) — R-3's route; the `:938` arm change covers it. Measured `rev/df_pivot/g1_7`, `rev/df_pivot_agg/g1_7`: the wrap of group 7 in the pivoted column, both modes |
| R-10 | `F.call_function("sum", …)` / `call_udf`: `_resolve_routine` (`python/repark/src/repark/spark/functions_byname.py:305-355`) tries `_scalar` first, which raises `call_scalar: unsupported function "sum"` (measured `route/scalar_sum`), then returns the facade's `F.sum` | **yes, by value** (revision) — R-3's route. Measured `rev/df_call_function/g7`, `rev/df_call_function_ungrouped/g5`: silent |
| R-11 | `repark-distributed` (`crates/repark-distributed/src/codec.rs:121-127` hands UDAF encode and decode to the inner codec; `session_provider.rs:23-38` builds executors from the RePark session's state), behind the `cluster` feature | **no** door uses it today (note only). A by-name `SparkSum` reaches its executors through the copied registry; the codec must carry the UDAF (and any sentinel state, A1) if the feature ships |
| R-12 | `repark-connect` (`crates/repark-connect/src/provider/table.rs:194-204`: filter pushdown only) | **no** — no aggregate pushdown, so no seat |

## A — DataFusion's `Sum` (datafusion-functions-aggregate 54.1.0, `src/sum.rs`): type, accumulators, check

| id | seat | seen at the seat |
|---|---|---|
| A-1 | `Sum::return_type` (`sum.rs:211-238`): `Decimal128(min(38, p + 10), s)` | **yes**, and it equals Spark's `bounded(p + 10, s)` at every measured precision (no `TYPE` cell on the Spark door) |
| A-2 | `SumAccumulator` (ungrouped; `sum.rs:391-437`): `update_batch` = `arrow::compute::sum` then `add_wrapping` (`:420`); `merge_batch` = `update_batch`; `evaluate` = the native value with the declared type, **no precision check** | **yes** — the `X`/`W`/`w` cells ungrouped |
| A-3 | `create_groups_accumulator` (`sum.rs:286-299`): `PrimitiveGroupsAccumulator` with `add_wrapping` (`:294`) | **yes** — every grouped cell, the `DISTINCT` cells after the rewrite (A-6), `HAVING`, CTE, subquery, view, Iceberg sources |
| A-4 | `create_sliding_accumulator` (`sum.rs:301-322`): `SlidingSumAccumulator` (`:441-510`), `add_wrapping`/`sub_wrapping`, `count` for NULL | **yes** — `ROWS`/`RANGE` sliding frames; retraction is modular, so a frame whose true sum fits comes back right |
| A-5 | `DistinctSumAccumulator` (`datafusion-functions-aggregate-common-54.1.0/src/aggregate/sum_distinct/numeric.rs:64-76`): a hash set, summed with `add_wrapping` at `evaluate` (`:74`) | **no** in the single-distinct cells, because A-6 rewrites them; **yes** for a `DISTINCT` beside an aggregate of another argument. Measured in the revision: `rev/distinct_mixed/g5`, `rev/distinct_two/g5`, `rev/distinct_mixed_ungrouped/g5` answer `10^38` (39 digits), both modes; EXPLAIN `explain/distinct_mixed` keeps `sum(DISTINCT s.c38_0)` in `AggregateExec` Partial and FinalPartitioned (no rewrite). The seat's own distinct accumulator (a set plus the exact sum) replaces it |
| A-6 | Optimizer `SingleDistinctToGroupBy`: `sum(DISTINCT x)` → group by `(g, x)`, then `sum(alias1)` | **yes** — EXPLAIN `explain/distinct`: the outer `sum(alias1)` is A-3 with a clone of the same `AggregateUDF`, so a RePark UDAF is carried through |
| A-7 | `Sum::simplify_expr_op_literal` (`sum.rs:352-395`) via `rewrite_multiple_linear_aggregates` (`datafusion-optimizer-54.1.0/src/simplify_expressions/linear_aggregates.rs:27-120`, from `simplify_exprs.rs:188`): `sum(x + c1), sum(x + c2)` → `sum(x) + c·count(x)` | **no** for decimals on the Spark door, because RePark's decimal `+` is the UDF `__repark_spark_decimal_add__` (EXPLAIN `explain/linear`), not a `BinaryExpr`; it can fire for integer arguments. A RePark UDAF that does not implement the hook switches the rewrite off for every type it owns; delegating the hook for non-decimal types keeps it |
| A-8 | `ParallelWindowExec` (`crates/repark-core/src/parallel_window/exec.rs:229-356`, rule `rule.rs`): runs the plan's `WindowExpr`s on the blocking pool | **yes**, through A-2/A-4 — it evaluates the same expressions (measured `win_parallel`) |
| A-9 | RePark's `SlidingFrameReScan` analyzer rule (`crates/repark-core/src/session/df_guards/window_rescan.rs:22-27` appends it to DataFusion's analyzer list; installed by `df_guards.rs:216`). `rewrite_expr` (`:60-90`) skips ever-expanding frames; otherwise `needs_frame_rescan` (`:184-197`) builds the aggregate's sliding accumulator and, when `!supports_retract_batch()`, swaps the window to a `WindowUDF` (`ReScanAggregateWindow`, named after the aggregate, `:234-236`) whose evaluator builds a fresh accumulator and re-sums the whole frame for every output row (`:330-352`) | **yes** (revision; step 0 credited it to DataFusion) — it is why sliding `try_sum` (T-3, no retract) works and agrees. **What the seat must provide:** a sliding accumulator with `retract_batch` and `supports_retract_batch() == true` for **every** arm the new `sum` owns, decimal and delegated (`BIGINT`, `DOUBLE`, …), and for `try_sum`. Otherwise (1) every sliding frame costs O(n·w); (2) measured rule order (`explain/rescan/*`, EXPLAIN VERBOSE): `sliding_frame_rescan` runs **before** `spark_avg_ansi_mode`, and the ANSI arm's downcast skips a `WindowUDF` (`aggregate.rs:327`), so a re-scanned window would keep the ANSI mode with ANSI off. EXPLAIN cannot show the swap: today's re-scanned `try_sum` prints `SAME TEXT AS ABOVE` after `sliding_frame_rescan` and the same `BoundedWindowAggExec` text as `sum` |

## T — `try_sum`

| id | seat | seen at the seat |
|---|---|---|
| T-1 | `SparkTrySum::return_type` (`datafusion-spark-54.1.0/src/function/aggregate/try_sum.rs:258-275`): `Decimal128(min(38, p + 10), s)` | **yes**, equal to Spark's |
| T-2 | `TrySumAccumulator::update_decimal128` (`try_sum.rs:189-220`): `checked_add`, then `exceeds_decimal128_precision` against the **result** precision after **every row**; `failed` sticks; `merge_batch` (`:99-112`) ORs the partials' `failed` and re-adds | **yes** — per-row sticky in every path: equal to Spark's grouped buffer, NULL where Spark's ungrouped or whole-window sum comes back (§9) |
| T-3 | No `GroupsAccumulator`, no `retract_batch` | grouped `try_sum` runs one row accumulator per group (a cost case); sliding frames re-scan through RePark's `SlidingFrameReScan` (A-9; WIN-SLIDE-1) |

## C — Spark's check, mirrored in RePark today

| id | seat | seen at the seat |
|---|---|---|
| C-1 | `SparkDecimalAverage` (`crates/repark-functions/src/avg_decimal.rs:168-301`) | **no** — `avg` only; it already carries Spark's sum check for `avg` (`sum_limit = 10^min(38, p + 10)`, `sum_overflow_error` with the `try_avg` hint), which `sum` needs with the `try_sum` hint |
| C-2 | `SparkAvgAnsiMode` (`aggregate.rs:297-349`): swaps `avg` to its legacy mode when ANSI is off | **no** — `legacy_avg_udaf` (`:341`) downcasts to `SparkAvgWithRetract` only; `sum` has no ANSI arm anywhere. It reads the flag at analysis (`spark_ansi_enabled_from_options(config)`, `:300`), runs after `sliding_frame_rescan` (measured order, A-9), and skips a `WindowUDF` (`:327`). Measured on `avg` (revision, `rev/view_flip_avg/g5`): a view created in one mode and read in the other follows the **reading** session, where Spark keeps the creating one — the TEMP-VIEW-SESSION-SETTINGS-1 class, which A3 inherits |
| C-3 | `SparkDecimalPrecision` / `__repark_spark_decimal_add__` (RePark's decimal `+`) | **partly**: a 39-digit `sum` fed into `+ 1` is caught there (`NUMERIC_VALUE_OUT_OF_RANGE` on, NULL off), a 38-digit wrap is not (`plus1/g7`) |

## I — the Iceberg store and DML paths

| id | seat | seen at the seat |
|---|---|---|
| I-1 | The CTAS / `INSERT … SELECT` / `MERGE` source plan | **yes** — the source is A-3's value; the store receives `decimal(38, s)` values already wrong |
| I-2 | `StoreOverflowCast` (`crates/repark-iceberg/src/write/store_overflow.rs`), `is_overflow_store_pair` (`write/store_cast.rs:53-60`) | **no**, because it guards float or decimal into an **integer** column only; a `decimal(38, s)` source into a `decimal(38, s)` column has no store cast at all |
| I-3 | The fork's Parquet writer: per-file min/max statistics (`crates/iceberg/src/writer/file_writer/parquet_writer.rs:340-351`) → `get_parquet_stat_min/max_as_datum` (`arrow/schema.rs:1050`, `:1196`) → `Datum::decimal_with_precision` (`spec/values/datum.rs:1044-1059`) → `validate_decimal_value` (`datum.rs:70-84`): digit count of the unscaled value | **only from bounds, and after the fact** (corrected in the revision; step 0 said "always"). The check runs from `MinMaxColAggregator::update`, which `parquet_to_data_file_builder` calls only when the column's metrics mode collects bounds (`parquet_writer.rs:463-466` skips a `None` column; `:480-482` gates bounds on `collects_bounds()`; doc `:430-432`). Under `truncate` (the default) or `full`, a value past 38 digits is a file's min or max and is refused (`DataInvalid`) after the file is written: the 39-digit values stay in uncommitted files (§6, 2 per mode; `ice/insert/metrics_full/*` and `ice/insert/wide100` in the revision). Under **`none` or `counts`** nothing checks, and the 39-digit values **commit**: `ice/{insert,ctas,merge_cow,merge_mor}/metrics_{none,counts}/c38_0` and `ice/{insert,ctas}/metrics_{none,counts}/c38_38`, both modes (domain table §12). With no metrics property, a table wider than 100 columns gives every column past the 100th `MetricsMode::None` (`spec/metrics_config.rs:336-347`, `limit_field_ids` `:494-513`, `METRICS_MAX_INFERRED_DEFAULT = 100` `:445`): measured `ice/{ctas,insert}/wide101/c38_0` commit, the 100-column controls refuse. A wrapped value inside 38 digits passes every mode and commits (8 per mode) |
| I-5 | The reference writer, Iceberg 1.11.0 Java (`iceberg-spark-runtime` jar, `javap`): `SparkParquetWriters$FixedDecimalWriter.write(int, Decimal)` → `DecimalUtil.toReusedFixLengthBytes(precision, scale, value, bytes)`, which checks every value: `Preconditions.checkArgument` with "Cannot write value as decimal(%s,%s), wrong scale: %s" and "… too large: %s" (`value.precision() <= precision`) | **not RePark's seat; the reference** — Java refuses an out-of-precision value per value, whatever the metrics mode. The fork's bounds-only check (I-3) is a divergence from it; a per-value check at the fork's writer (or before RePark hands it the batch) closes I-3's `none` / `counts` / wide-table hole for every producer, not only `sum` |
| I-4 | `FloatDecimalStoreRule` (`repark-iceberg/src/write/conform_stage.rs`), `InsertStoreAssignment` | **no** — float/decimal conformance and the ANSI store-assignment gate; neither checks a decimal value against its own declared precision (a reading of the rule names and the measured commits) |

## S — stored plans and other analyzer lists

| id | seat | seen at the seat |
|---|---|---|
| S-1 | Temp views (SQL door re-plans the text per read; DataFrame door inlines the plan) | **yes** — the view body's `sum` is the same UDAF (measured `temp_view`; revision `rev/view_same`, `rev/view_flip`, `rev/dfview_flip`: the wrap in every mode, since `sum` has no ANSI arm today); a UDAF-level fix needs nothing here, an analyzer-level fix does (the view family, F1). Spark keeps the creating session's mode for both SQL and DataFrame-made temp views (measured) |
| S-2 | CTE and scalar subquery | **yes** (measured `cte`, `scalar_sub`) |
| S-3 | Other analyzer lists: `build_expr_context` (R-4), the native door's | a UDAF-level fix is reached through registration on each; an analyzer-level fix must be listed in each list (the avg sketch's lesson) |

## Summary

- One accumulator family decides every Spark-door value: DataFusion's `Sum` (A-2, A-3, A-4, and A-5 by reading),
  reached by name (R-1, R-4) and by value (R-3); `try_sum` is datafusion-spark's (T-2) on both doors.
- No seat checks a decimal `sum` against `decimal(38, s)`: the first check is the Iceberg writer's (I-3), after the
  file is written and only for a column that collects bounds; under `none`, `counts` or past the 100th column of a
  table with no metrics property, nothing checks and the value commits (Spark then cannot scan it). RePark's `+`
  (C-3) catches only 39-digit operands.
- The DataFrame door reaches `sum` three ways: by value through the `:938` arm (R-3: `F.sum`, `GroupedData.sum`,
  `agg({…})`, `pivot` R-9, `call_function` R-10), by name through SQL text (R-8: `cube`, `rollup`, `groupingSets`),
  and by name through `F.expr` / `selectExpr` (R-4). The seat needs both the registration and the arm.
- RePark's `SlidingFrameReScan` (A-9) decides whether a sliding window keeps the accumulator path; every arm of the
  new `sum` must retract.
- The avg unit's machinery (C-1, `WideDecimal` in `avg_decimal.rs:22-147`, `add_batch`/`sub_batch` `:319-357`,
  `DecimalAvgGroups` `:467-688`) is the reusable part; its ANSI rule (C-2) needs one more downcast arm.
- The native door (R-5) keeps DataFusion's `sum` and has no `try_sum` unless the fix registers there.
