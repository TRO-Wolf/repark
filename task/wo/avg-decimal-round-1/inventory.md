# AVG-DECIMAL-ROUND-1 — inventory of the seats (with DECIMAL-DIVIDE-LITERAL-1)

The insertion-inventory rule: every seat that sets a decimal `avg`'s result type, scale or rounding, or a decimal
division's operand or result type, answers "seen at the seat: yes / no, because", with file:line. "Seen" means:
does the value or the type of a measured cell pass through this seat, so that a fix placed there changes it? Every
"yes" is confirmed by a cell in the [domain table](domain-table.md) or by the EXPLAIN in
`evidence/raw/explain-paths-repark.txt`. Lines are RePark main `a792ce47` and DataFusion 54.1.0 (the crates.io
sources in the cargo registry).

**How searched:** `grep` over `crates/` and `python/repark/src` for `avg_udaf`, `DecimalAverager`, `"avg"`,
`"mean"`, `is_distinct`, `div_wrapping`, `spark_div_result_type`, `spark_result_type`, `is_decimal_arithmetic`,
`min_precision_integer_literal`, `DECIMAL_DIV_NAME`, `__truediv__`, `fn div`, `coerce_pair`; the analyzer rule
lists read from `crates/repark-functions/src/registration.rs`, `lambda_rebind.rs`,
`crates/repark-spark/src/spark_literal_typing.rs` and `crates/repark-core/src/session/df_guards.rs`; DataFusion's
`DecimalAverager`, `average.rs`, `single_distinct_to_groupby.rs` and `type_coercion/binary.rs` read directly; the
plans of the `avg`, `avg(DISTINCT …)`, window, `/`, `*` and `%` cells read with `EXPLAIN` (`explain_paths.py`).

## `avg` — result type, rounding, overflow

| seat | file:line | sets | seen? |
|---|---|---|---|
| `SparkAvgWithRetract::return_type` | `crates/repark-functions/src/aggregate.rs:119-145` | the result type `(min(38, p+4), min(38, s+4))` | **yes, and right**: every measured `avg` type equals Spark's `DecimalType.bounded(p + 4, s + 4)` (`Average.resultType`) |
| `DecimalAverager::avg` | `aggregate.rs:303-327` (`div_wrapping` at `:315`, `mul_checked` at `:309`) | the value: `sum × 10^(ts−ss)` in the native type, then **truncating** division by the count; precision checked at the target | **yes — the rounding seat.** Every `t` cell; the `R` cells g20 and g22 at p ≥ 34 are the `i128` `mul_checked` failing before the division; the condition-less error text is this function's `exec_err!` |
| `DecimalAvgAccumulator` (`update_batch`, `merge_batch`, `evaluate`, `retract_batch`) | `aggregate.rs:331-440` (`evaluate` `:358`) | the sum (`add_wrapping`, no 38-digit check, state typed as the input column) and the per-group call of the averager | **yes**: ungrouped cells, the sliding, running, `RANGE` and whole-partition window frames (DataFusion's window operator calls `evaluate` after `retract_batch`), `try_avg` (`null_on_overflow`) |
| `SparkAvgGroupsAccumulator` and `decimal_average` | `crates/repark-functions/src/avg_groups.rs:158`, `:94-139` (averager call `:138`) | the grouped path: `add_wrapping` sums, the same averager | **yes**: every grouped cell, `t4` (four files: the `merge_batch` path), `repartition(4)` |
| `accumulator` refuses `is_distinct` | `aggregate.rs:148-150`; `groups_supported` `avg_groups.rs:19-28` | `not_impl_err!("DistinctAvgAccumulator")` | **yes**: `avg(DISTINCT …)` beside a non-distinct aggregate, and as a window, raise this |
| DataFusion `single_distinct_aggregation_to_group_by` | `datafusion-optimizer-54.1.0/src/single_distinct_to_groupby.rs:65-100` | rewrites an all-distinct aggregate list into a two-level `GROUP BY`; the outer `avg` is RePark's | **yes**: `avg(DISTINCT x)` alone reaches the averager (EXPLAIN shows `Aggregate: groupBy=[[g, c10_2 AS alias1]]` under `avg(alias1)`), so the `DISTINCT` cells are `t`, not refusals |
| DataFusion's own `avg` and `DecimalAverager` | `datafusion-functions-aggregate-54.1.0/src/average.rs:177-215`; `datafusion-functions-aggregate-common-54.1.0/src/utils.rs:157-176` | the same truncating division | **no** on the Spark door: `aggregate::functions()` (`aggregate.rs:28-36`) overwrites the name `avg`; RePark's copy has the same `div_wrapping`. On the native door, not settled which `avg` answers — both truncate identically, so the measured cells do not tell them apart |
| `try_avg_udaf` | `aggregate.rs:44-47` | the same struct with `null_on_overflow` | **yes**: `try_avg` cells `t` |
| ANSI flag | `AccumulatorArgs` (`datafusion-functions-aggregate-common-54.1.0/src/accumulator.rs`) carries no config | Spark's `Average` sets `nullOnOverflow = evalMode != ANSI` | **no**: the accumulator cannot read ANSI. So ANSI off, g20 and g24 at p = 38 raise where Spark answers NULL. A fix needs an analyzer arm that hands the flag in (for example the `try_avg` struct under the name `avg` when ANSI is off) |

## `avg` — the doors

| door | file:line | path | seen? |
|---|---|---|---|
| SQL `avg`, `mean` | registered by `aggregate::functions()` (`aggregate.rs:28`) | the UDAF above | **yes** (every SQL cell) |
| `F.avg`, `F.mean` | `python/repark/src/repark/spark/functions.py:604`, `mean = avg` `:620` | `PyColumn.aggregate("avg")` → `crates/repark-python/src/column/mod.rs:886` → `unary_aggregate_udaf` `crates/repark-python/src/column/function_dispatch.rs:940` → `avg_udaf()` | **yes** |
| `GroupedData.avg`, `.mean`, `agg({x: "avg"})`, `agg({x: "mean"})` | `python/repark/src/repark/spark/dataframe/joins_columns.py:776-781`, `:631` | `F.avg` / `F.mean` | **yes** |
| `describe`, `summary` | `python/repark/src/repark/spark/dataframe/statistics.py:109` | `"mean": spark_functions.avg`, then the stacked string cast | **yes**: the `mean` strings truncate (`0.152352` against `0.152353`) |
| `F.expr("avg(x)")` | `crates/repark-python/src/column/expr_build.rs:317` (`build_expr_context`) | parses only; the aggregate resolves in the session | **yes** |
| native door `repark.sql()` | `crates/repark-python/src/session_runtime.rs:414` (`prepare_session_sql`) and stock DataFusion planning | a registered `avg` | **yes** (truncates; out of the label's scope) |
| Iceberg CTAS, `INSERT`, `MERGE` | the store casts in `crates/repark-iceberg/src/write/store_cast.rs:478` (`store_cast_udf_for_target`) | the store rounds the already-truncated `avg` half up to the target scale | **yes, downstream**: it rounds correctly; the value it receives is wrong. No store fix is needed |

## Division — operand and result type

| seat | file:line | sets | seen? |
|---|---|---|---|
| `SparkIntegralLiteral` | `crates/repark-spark/src/spark_literal_typing.rs:17`, inserted before `type_coercion` (`:36-47`) | narrows a provisional `Int64` literal to `Int32` | **yes**: `3` reaches coercion as `Int32` (so `decimal(10,0)`, not `(20,0)`: EXPLAIN `Decimal128(Some(3),10,0)`) |
| `SparkDecimalPrecision`, U3 literal step | `crates/repark-functions/src/decimal_precision.rs:65-70` (`is_decimal_arithmetic`), `:138-166` (`min_precision_integer_literal`), `:168-183` (`integer_literal_value`), `:186-199` (`is_default_integer_to_decimal`); installed before `type_coercion` (`lambda_rebind.rs:51-54`) and again after it (`registration.rs:31`) | casts a bare integer literal beside a decimal to `DECIMAL(digits, 0)` | **no — the literal seat.** `is_decimal_arithmetic` lists `Plus`, `Minus`, `Multiply` only, so `/` and `%` never reach U3. EXPLAIN: `d10_2 * Decimal128(Some(3),1,0)` against `__repark_spark_decimal_div__(d10_2, Decimal128(Some(3),10,0))` and `d10_2 % Decimal128(Some(3),10,0)`. Also **no for suffixed literals** on every operator: `3S` and `3L` are not bare `Int16`/`Int64` literals when U3 runs (measured `d * 3S` `decimal(16,2)`, `d * 3L` `decimal(31,2)`); how they are spelled at that point is not read |
| `integer_literal_value`'s `Int8`/`UInt8` arms | `decimal_precision.rs:173`, `:177` | would narrow a `TINYINT` literal to digits | **not reached today** (`d * 3Y` agrees because the suffixed literal is not seen); a fix that makes suffixes visible must drop the `Int8` arm, since Spark's `fromLiteral` keeps `TINYINT` at `decimal(3,0)` |
| DataFusion `coerce_numeric_type_to_decimal128` | `datafusion-expr-common-54.1.0/src/type_coercion/binary.rs:1227-1242` | `Int32` → `decimal(10,0)`, `Int16` → `(5,0)`, `Int64` → `(20,0)`, `Int8` → `(3,0)` | **yes**: every wrong `/` literal type is this table (Spark `DecimalType.forType`), applied where Spark would use `fromLiteral` |
| `SparkDecimalRewrite::rewrite_decimal_div` | `crates/repark-functions/src/decimal_spark.rs:129`, `:135-149` | wraps a decimal-by-decimal `/` in `__repark_spark_decimal_div__` | **yes, and right given its operands**: the UDF's type is `spark_div_result_type` (`decimal_precision.rs:246-252`, Spark's `Divide.resultDecimalType` with precision loss allowed) and its kernel `checked_div` / `div_half_up` (`decimal_spark.rs:492-500`, `:527-561`) rounds half up once at the result scale, as Spark's `checkDecimalOverflow` does. Typed operands and `sum(x) / count(x)` agree, so only the operand type is wrong |
| `SparkDecimalExprPlanner::plan_binary_op` | `decimal_spark.rs:60-89` | SQL-text `*` past Arrow's limit (DEC-8) | **no**: returns `Original` for every operator but `*`, and the DataFrame door never plans SQL text |
| Modulo | no Spark rule: DataFusion's `%` with the coerced literal | `d % 3` type | **yes** (wrong type `decimal(10,2)` against Spark's `decimal(3,2)`, values equal), the same missing U3 arm |
| `PyColumn::div` (DataFrame `/`) | `crates/repark-python/src/column/mod.rs:697-703`; Python `Column.__truediv__` / `__rtruediv__` `python/repark/src/repark/spark/column.py:322`, `:345` | casts **both** operands to `Float64` before `/` | **yes — a second division seat.** Every DataFrame `/` with a decimal operand answers `double` (`0.30 / 3` = `0.09999999999999999`); the comment there states the integer case only |
| `try_divide` (and `try_add`, `try_subtract`, `try_multiply`, `try_mod`) | `crates/repark-functions/src/try_invert/arith.rs:263-315` (`coerce_pair`), `:187-210` (`result_type`) | operand types | **yes — a third seat.** A decimal beside an integer matches neither the both-decimal arm (`:266`) nor the both-non-decimal arm (`:269-276`) and falls to `integer_width` (`:306-314`): the decimal is cast to `INT` (`try_divide(2.51, 3)` = `0.6666…`, `try_add(2.51, 1)` = `3`) |
| F.expr context | `crates/repark-python/src/column/expr_build.rs:317-345` (`build_expr_context`, `spark_door_post_coercion_rules`) | parses `F.expr` text; the session analyzer types it | **yes**: `F.expr("d / 3")` shows the SQL literal width (`decimal(21,13)`), not `double` |
| Iceberg DML | `crates/repark-iceberg/src/write/store_overflow.rs:29`, `:888-895` (swaps the divisor guard inside `__repark_spark_decimal_div__`); the store casts `store_cast.rs:478` | the stored value is the store cast of the UDF's result | **yes, downstream**: CTAS stores the wrong type; `INSERT`, `UPDATE`, `MERGE` round the longer quotient once, where Spark rounds twice |
| native door | stock DataFusion `BinaryExpr` division (EXPLAIN `d10_2 / Decimal128(Some(3),20,0)`) and its truncating Arrow kernel | type and value | **yes** (out of the label's scope): Spark's type at `d10_2 / 3` but truncated digits; not diagnosed further |

## The confs

| conf | Spark seat (4.1.2) | RePark | seen? |
|---|---|---|---|
| `spark.sql.legacy.literal.pickMinimumPrecision` | `DecimalPrecisionTypeCoercion.nondecimalAndDecimal(literalPickMinimumPrecision)` | accepted and read back; no rule reads it | **no**: RePark answers as if `false` for `/`, `%` and suffixed literals, and as if `true` for a plain `INT` literal in `*`, `+`, `-` |
| `spark.sql.decimalOperations.allowPrecisionLoss` | `Divide.resultDecimalType` (and every `BinaryArithmetic`) | accepted and read back; `spark_result_type` / `spark_div_result_type` are the `true` formulas only | **no**: every wide cell answers the precision-loss type |

Up: [map.md](map.md).
