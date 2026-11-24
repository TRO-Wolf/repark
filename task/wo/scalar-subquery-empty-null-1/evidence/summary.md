# SCALAR-SUBQUERY-EMPTY-NULL-1 — Evidence Summary

**Date:** 2026-11-23  
**Base:** `fa7d75e446e6f0a3edf48f6538c6a42c376515e7` (`main`)  
**Card:** [scalar-subquery-empty-null-1-card-2026-11-23.md](../../../roadmap/mid-term/cards-2026-11-23/scalar-subquery-empty-null-1-card-2026-11-23.md)  
**Pins:** `python/repark/tests/sql/test_scalar_subquery_empty_null_1.py` (`_scalar_subquery_empty_null_1_cells.py`), `crates/repark-core/src/session/df_guards/scalar_nullable.rs`, `crates/repark-core/src/pre_execute/tests.rs`  
**Scripts:** [probe_step0.py](probe_step0.py)  
**Domain table:** [domain-table.md](domain-table.md)

## Seat Inventory

1. **Session analyzer list (`crates/repark-core/src/session/df_guards.rs`):** `ReparkScalarSubqueryNullability` seated immediately before `type_coercion` after `extension.configure_analyzer_rules` and `dialect.configure_analyzer_rules`.
2. **Other production analyzer lists (`crates/repark-python/src/column/expr_build.rs`):** `build_expr_context` seats `repark_core::scalar_nullable::insert_before_coercion` immediately before `type_coercion` and registers `repark_core::scalar_nullable::udf()`. `crates/repark-spark/src/view_ddl/temp_settings.rs` only parses `SET`/`RESET`.
3. **DataFusion off-list runs (`crates/repark-core/src/pre_execute.rs`):** `PreExecute::execute` wraps `Statement::Prepare` and `DdlStatement::CreateView` inputs before DataFusion's bare optimizer and `apply_type_coercion`; `ReparkScalarSubqueryNullability` also runs as an `OptimizerRule` before `simplify_expressions`.
4. **SQL-planner-time coercions (`datafusion-sql` 54.1.0):** `parse_scalar_subquery` does not fold on nullability; casts preserve child nullability once wrapped before `type_coercion`.
5. **Optimizer rules:** `ReparkScalarSubqueryNullability` precedes `simplify_expressions`; `StripScalarNullable` runs immediately after `scalar_subquery_to_join` and strips `__repark_scalar_nullable__` outside surviving subqueries while recomputing ancestor schemas, preserving `EliminateOuterJoin`, `PushDownFilter`, `OptimizeProjections`, `CommonSubexprEliminate`, and `enable_physical_uncorrelated_scalar_subquery=true`.
6. **Stored plans re-entering:** Spark SQL temp views (`ReplanningTempView`), DataFrame temp views (`FrameView` including cross-ANSI `closed_scan`), cached tables (`LazyCacheTable`), native `CREATE VIEW` (`ViewTable` + `DESCRIBE`), and `PREPARE`/`EXECUTE` all preserve `nullable=true`.

## Design Choice

Refined Option A+ (`ReparkScalarSubqueryNullability` + `StripScalarNullable`), chosen over naive Option A (missed `Sort.fetch`, `Projection` unnest/generator, `Prepare`/`CreateView` off-list runs, and ancestor `recompute_schema`), Option B (no strip; blocks `EliminateOuterJoin`/`PushDownFilter`/CSE and leaks synthetic UDF), and Option C (reordering `ScalarSubqueryToJoin` before `SimplifyExpressions`; misses `TypeCoercion`, `SparkNullability` casts, `CREATE VIEW`, and `enable_physical_uncorrelated_scalar_subquery=true`).

## Measured Reach (Main vs Head vs Spark 4.1.2)

- **WS3 121-cell read matrix:** **57/57** S1 cells fixed in each ANSI mode and on native `repark.sql()` (**0 regressions**, **0 `NONNULL_WITH_NULLS`** down from 44). `inline_utf8view_subq`, `df_api_scalar_isnull`, and all 8 `enable_physical_uncorrelated_scalar_subquery=true` cells fixed.
- **Extended Step 0.3 cells:** 0 diffs across CTEs, `UNION ALL`, `CAST(DATE AS TIMESTAMP)`, nested subqueries, `HAVING`, `JOIN ON`, `PREPARE`/`EXECUTE`, `F.expr`, `filter(Column)`, `filter(str)` (inline), `selectExpr`, `withColumn`, `.scalar()`, SQL/DF temp views (incl. ANSI flip), cache, and native `CREATE VIEW` (`DESCRIBE` now reports `YES`).
- **DML / writes:** `INSERT ... WHERE (subquery) IS NULL` and `CTAS ... WHERE (subquery) IS NULL` now write Spark's 2 rows on both doors; `DELETE`/`UPDATE` with subquery keep their refusal.
- **MERGE (out of scope, recorded unchanged):** on Spark door, `dml_merge_on_isnull` moved from `[[1, 10], [2, 20]]` (folded false) to `[[1, 99], [2, 99]]` (evaluates true), and `dml_merge_when_isnull` moved from `[[1, 10], [2, 20]]` to `UnsupportedOperationException` (Spark refuses both with `[UNSUPPORTED_MERGE_CONDITION.SUBQUERY]`; native door refuses both with `UnsupportedOperationException`).
- **Plan quality (`EXPLAIN`):** all 5 non-subquery queries, `count(*)`, and `(SELECT 1)` are byte-for-byte identical (logical and physical) to main; `subq = 1` keeps `EliminateOuterJoin` (`CrossJoinExec`, byte-identical physical plan); zero `__repark_scalar_nullable__` markers survive in lowered plans.
- **Red-first / Green-head:** `test_scalar_subquery_empty_null_1.py` measures **173 failed / 20 passed** on main (`fa7d75e44`) and **193 passed / 0 failed** on head.
