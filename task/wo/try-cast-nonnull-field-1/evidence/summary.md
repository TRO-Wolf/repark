# task/wo/try-cast-nonnull-field-1/evidence/summary.md — TRY-CAST-NONNULL-FIELD-1 measurement evidence

## Provenance

- **RePark:** `main` at `514281bd4` (tree-identical prebuilt wheel from `ea94eedc8`) and `fix/try-cast-nonnull-field-1` head (`b38cef08c` product commit), release wheels built from each commit.
- **Spark:** live PySpark 4.1.2 (`local[2]`, Java 17, `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, local Hadoop catalog), both ANSI modes (`spark.sql.ansi.enabled=false` and `true`), session timezone `America/New_York`.
- **Scripts:** [probe_nullability_sweep.py](probe_nullability_sweep.py) (542-cell nullability sweep across both ANSI modes), [probe_step0_extra.py](probe_step0_extra.py) (109 Spark door/source/edge cells, 8 Iceberg write cells, 12 Iceberg/Parquet pushdown/pruning cells per ANSI mode, plus 16 native-door cells).
- **Full cell-by-cell table:** [domain-table.md](domain-table.md).

## Root defect (DataFusion 54.1.0; not patched)

- `datafusion-physical-expr-54.1.0/src/expressions/try_cast.rs:83-101`: `TryCastExpr::nullable()` returns `Ok(true)`, while `TryCastExpr::return_field()` clones `self.expr.return_field(input_schema)` with `.with_data_type(...)` and omits `.with_nullable(true)`.
- **Propagators** delegating `return_field` to child `return_field`: `CastExpr` (`cast.rs:153-166, 235-253`), `NegativeExpr` (`negative.rs:86-104`), `NotExpr` (`not.rs:72-101`).
- **Consumers** passing child `return_field` into `return_field_from_args` / `return_field` (both plan-time and runtime `evaluate`): `ScalarFunctionExpr` (`scalar_function.rs:99, 246`), `HigherOrderFunctionExpr` (`higher_order_function.rs:140, 144, 302, 307`), `AggregateExprBuilder` (`aggregate.rs:275, 303`), `WindowUDFExpr` (`windows/mod.rs:70, 180`), `AsyncFuncExpr` (`async_scalar_function.rs:77, 139`).

## Seat inventory (`path:line`)

| Seat | `path:line` | Role / Defect on main | Resolution |
|---|---|---|---|
| **Seat 1 (primary): physical planner** | `crates/repark-core/src/stack/try_cast_nullable.rs`, `crates/repark-core/src/stack/planner.rs:14-31`, `crates/repark-core/src/session/df_guards.rs:190,207-211`, `crates/repark-core/src/lib.rs:117` | `DefaultPhysicalPlanner` compiles `Expr::TryCast` inside `ScalarFunction` / `HigherOrderFunction` / `AggregateFunction` / `WindowFunction` / `Lambda` (including through `Cast`/`Negative`/`Not` and `CastShield`) with non-nullable `TryCastExpr::return_field`; shared by Spark SQL, `selectExpr`, `F.expr`, `Column.try_cast`, native `repark.sql()`, Iceberg DML/CTAS, and `EXPLAIN ANALYZE` | `StackQueryPlanner::create_physical_plan` runs `shield_try_cast_plan` right before `DefaultPhysicalPlanner::create_physical_plan`. Fast-path borrows untouched plans with no `Expr::TryCast`. Wraps `Expr::TryCast` ONLY beneath a consumer with zero-copy immutable `__repark_nullable_field__` (`return_field_from_args` marks field nullable while preserving name, type, metadata, bounds, and ordering). Bare `WHERE`/`SELECT` `TryCast` stays untouched for static and dynamic scan pruning. |
| **Seat 2: post-pushdown physical repair** | `crates/repark-core/src/session/df_guards/skipping_limit.rs:17-109`, `crates/repark-core/src/stack/try_cast_nullable.rs` (`repair_inlined_try_cast_plan`, `NullableFieldPhysicalExpr`) | Physical `ProjectionPushdown` merges an inner bare `ProjectionExec(TRY_CAST(s AS INT) AS x)` into an outer consumer (`map('k', x)['k']`), inlining raw `TryCastExpr` into `ScalarFunctionExpr` and crashing at runtime | Wraps stock `"SanityCheckPlan"` with `TryCastSafeSanityCheck` (same name `"SanityCheckPlan"`, exact stock rule count `stock.len() + 3`, `schema_check() == true`), wrapping any raw `TryCastExpr` inside `ScalarFunctionExpr`/`HigherOrderFunctionExpr`/`LambdaExpr` on `ProjectionExec` or `FilterExec` with transparent `NullableFieldPhysicalExpr`. |
| **Seat 3: cross-ANSI temp-view predicate** | `crates/repark-spark/src/view_ddl/temp_settings.rs:260` | Calls `reader.create_physical_expr(predicate, below.as_ref())` directly, bypassing `StackQueryPlanner` (crashes on `WHERE map('k', TRY_CAST(s AS INT))['k'] > 0`) | Calls `repark_core::shield_try_cast_expr(predicate)?` right before `create_physical_expr`. |

Inventoried sites intentionally unchanged (already run before Seat 1 or read-only): `crates/repark-python/src/column/{mod.rs,expr_build.rs,display.rs}`, `python/repark/src/repark/spark/column.py`, `crates/repark-sql/src/dialect.rs`, `crates/repark-spark/src/extension.rs`, `crates/repark-core/src/session/df_guards/{cast_shield.rs,cast_shield/stats_barrier.rs,scalar_nullable.rs,case_bind.rs,signed_zero/*,struct_setop.rs,plan_canonical.rs,pre_execute/setop_all.rs}`, `crates/repark-functions/src/{decimal_precision/literal_step.rs,zone_seam.rs,java_double.rs,spark_string_timestamp.rs,analyzer.rs,int_to_binary.rs,bool_decimal.rs,timestamp_ns_cast.rs,time_family.rs,collection/key_by_position.rs}`, `crates/repark-spark/src/{normalize/struct_position.rs,keyword_lower/ntz_cast_lower.rs}`, and `crates/repark-iceberg/src/write/{conform_stage.rs,store_fold.rs}`.

### Rejected alternatives

1. **Global logical analyzer/optimizer rewrite wrapping every `Expr::TryCast`:** breaks Parquet and Iceberg `PruningPredicate` and `unwrap_cast` (which downcast directly to `TryCastExpr`) on both static predicates (`WHERE TRY_CAST(k_int AS BIGINT) > 500`) and dynamic join filters, and changes `EXPLAIN VERBOSE` rule lists on queries without `TRY_CAST`.
2. **Pure `PhysicalOptimizerRule` patching schemas post-`create_initial_plan`:** `create_initial_plan` caches non-nullable `SchemaRef` / `PlanProperties` across every ancestor operator (`ProjectionExec`, `HashJoinExec`, `SortExec`, `UnionExec`, `AggregateExec`, `WindowAggExec`, `IcebergWriteExec`); reconstructing every operator variant is fragile and adding a new rule name alters `EXPLAIN VERBOSE` and breaks `guarded_rule_list_is_stock_datafusion_in_order_then_the_repark_rules`.
3. **Patching DataFusion in cargo registry or `[patch.crates-io]`:** out of scope.
4. **Fixing `array_remove` / `make_dt_interval` UDFs here:** separate UDF logical-schema bugs (wrong `df.schema` even when inputs are nullable); recorded on the card's `Still unlike Spark` list.

## Measurement summary (Spark 4.1.2 vs main `514281bd4` vs head)

| Suite | ANSI mode | Total | Main crashes (`TryCast` scope) | Fixed on head (== Spark) | Regressed vs main | Residual separate-shape cells |
|---|---|---:|---:|---:|---:|---:|
| Nullability sweep (`probe_nullability_sweep.py`) | `false` | 542 | 63 (+9 separate UDF) | **63 / 63** | 0 | 9 (`array_remove` × 7, `make_dt_interval` × 2) |
| Nullability sweep (`probe_nullability_sweep.py`) | `true` | 542 | 63 (+9 separate UDF) | **63 / 63** | 0 | 9 (`array_remove` × 7, `make_dt_interval` × 2) |
| Extra Spark doors & sources (`probe_step0_extra.py`) | `false` | 109 | 29 (incl. 1 Rust `ListArray` panic) | **29 / 29** | 0 | 15 `sep/*` (`array_remove`/`make_dt_interval`) |
| Extra Spark doors & sources (`probe_step0_extra.py`) | `true` | 109 | 29 (incl. 1 Rust `ListArray` panic) | **29 / 29** | 0 | 15 `sep/*` (`array_remove`/`make_dt_interval`) |
| Iceberg writes (`INSERT`/`CTAS`/`UPDATE`/`MERGE`) | `false` | 8 | 3 (`insert_nullable_abs_try`, `ctas_abs`, `ctas_add`) | **3 / 3** (`insert_nonnull_with_null` still refuses) | 0 | 0 |
| Iceberg writes (`INSERT`/`CTAS`/`UPDATE`/`MERGE`) | `true` | 8 | 3 (`insert_nullable_abs_try`, `ctas_abs`, `ctas_add`) | **3 / 3** (`insert_nonnull_with_null` still refuses) | 0 | 0 |
| Native door (`PyReparkSession.native()`) | n/a | 16 | 7 (+1 `sep/array_remove_try_int`) | **7 / 7** | 0 | 1 (`sep/array_remove_try_int`) |
| WS6 seam corpus (`pre_handback_check.py --profile full`) | both | 219 | 4 (`N01_try_cast_not_null_wrappers`) | **4 / 4** | 0 | 0 |

Note on card's 87 vs main `514281bd4`'s 72 sweep crashes: between `e80e6bc8` and `514281bd4`, 14 cells were incidentally masked by `#1417` (`StatsBarrier`/`SparkNarrowIntegral`) and 2 (`subq_empty`) were closed by `#1423` (`SCALAR-SUBQUERY-EMPTY-NULL-1`), leaving 71 Spark-OK crashes plus `dfbug/make_dt_interval_ovf_col` (where Spark raises `INTERVAL_ARITHMETIC_OVERFLOW`). All 63 `TryCast`-scope crashes are fixed on head; the remaining 9 are separate UDF logical-nullability bugs.

## File pruning and `EXPLAIN` invariance

Measured over a 3-file Iceberg table (`ice.sw.nn_prune`: files `[1..100]`, `[101..500]`, `[501..1000]`) and a 3-file Parquet directory (`pq_prune`) in both ANSI modes, plus native `pq_prune_nat`:

- **Control queries without `TRY_CAST`** (`ice/control_k_gt_500`, `pq/control_k_gt_500`, `pq/prune_control`): normalized `EXPLAIN` is **byte-identical** between main and head; Iceberg prunes to `N=1` file and Parquet prunes `3 total -> 1 matched`.
- **Bare `TRY_CAST` pushdown and dynamic join filters** (`ice/try_widening_gt_500`, `pq/try_widening_gt_500`, `ice/try_str_gt_500`, `pq/try_str_gt_500`, `ice/dynamic_join_bare_try`, `pq/dynamic_join_bare_try`, native `pq/prune_try_widening`): normalized `EXPLAIN` is **byte-identical** between main and head; widening `TRY_CAST(k_int AS BIGINT) > 500` prunes Iceberg to `N=1` and Parquet to `3 total -> 1 matched`, and dynamic join filters keep `3 total -> 1 matched` on Parquet.
- **Compound predicate with function over `TRY_CAST`** (`ice/compound_prune_and_abs`, `pq/compound_prune_and_abs`): Iceberg still prunes to `N=1` (`predicate:[k_int > 500]`) and Parquet still prunes `3 total -> 1 matched`, while shielding `abs(TRY_CAST(s AS INT))` safely.

## Pins

- **Facade pin (`python/repark/tests/sql/test_try_cast_nonnull_field_1.py` + `_try_cast_nonnull_field_1_cells.py`):** 183 tests covering all 63 sweep cells, all doors (`spark.sql`, `selectExpr`, `F.expr`, `F.col(...).try_cast(...)`, native `repark.sql()`), Iceberg/Parquet/in-memory sources, `CastShield`, HOF `transform`/`aggregate`, merged projections, cross-ANSI temp view, `EXPLAIN ANALYZE`, Iceberg `INSERT`/`CTAS`/`UPDATE`/`MERGE`, and pruning/`EXPLAIN` hygiene in both ANSI modes. On main `514281bd4`: **181 failed, 2 passed** (only the 2 pruning control tests pass). On head: **183 passed**.
- **Rust unit tests (`crates/repark-core/src/stack/try_cast_nullable/tests.rs`):** 6 unit tests covering `shield_try_cast_plan` borrowing untouched non-`TryCast` plans, preserving bare `WHERE`/`SELECT` `TryCast`, shielding consumer arguments across `ScalarFunction`/`Cast`/`Negative`/`Not`, idempotence, and `repair_inlined_try_cast_plan` on `ProjectionExec` and `FilterExec`.
