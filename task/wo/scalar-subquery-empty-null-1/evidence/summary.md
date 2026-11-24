# SCALAR-SUBQUERY-EMPTY-NULL-1 — Evidence Summary

**Date:** 2026-11-23  
**Base:** `fa7d75e446e6f0a3edf48f6538c6a42c376515e7` (`main`)  
**Card:** [scalar-subquery-empty-null-1-card-2026-11-23.md](../../../roadmap/mid-term/cards-2026-11-23/scalar-subquery-empty-null-1-card-2026-11-23.md)  
**Pins:** `python/repark/tests/sql/test_scalar_subquery_empty_null_1.py` (`_scalar_subquery_empty_null_1_cells.py`), `crates/repark-core/src/session/df_guards/scalar_nullable.rs`, `crates/repark-core/src/pre_execute/tests.rs`

## Seat Inventory

1. **Session analyzer list (`crates/repark-core/src/session/df_guards.rs`):** `ReparkScalarSubqueryNullability` seated immediately before `type_coercion` after `extension.configure_analyzer_rules` and `dialect.configure_analyzer_rules`.
2. **Other production analyzer lists (`crates/repark-python/src/column/expr_build.rs`):** `build_expr_context` seats `repark_core::scalar_nullable::insert_before_coercion` immediately before `type_coercion` and registers `repark_core::scalar_nullable::udf()`. `crates/repark-spark/src/view_ddl/temp_settings.rs` only parses `SET`/`RESET`.
3. **DataFusion off-list runs (`crates/repark-core/src/pre_execute.rs`):** `PreExecute::execute` wraps `Statement::Prepare` and `DdlStatement::CreateView` inputs before DataFusion's bare optimizer and `apply_type_coercion`; `ReparkScalarSubqueryNullability` also runs as an `OptimizerRule` before `simplify_expressions`.
4. **SQL-planner-time coercions (`datafusion-sql` 54.1.0):** `parse_scalar_subquery` does not fold on nullability; casts preserve child nullability once wrapped before `type_coercion`.
5. **Optimizer rules:** `ReparkScalarSubqueryNullability` precedes `simplify_expressions`; `StripScalarNullable` runs immediately after `scalar_subquery_to_join` and strips `__repark_scalar_nullable__` outside surviving subqueries while recomputing ancestor schemas, preserving `EliminateOuterJoin`, `PushDownFilter`, `OptimizeProjections`, `CommonSubexprEliminate`, and `enable_physical_uncorrelated_scalar_subquery=true`.
6. **Stored plans re-entering:** Spark SQL temp views (`ReplanningTempView`), DataFrame temp views (`FrameView` including cross-ANSI `closed_scan`), cached tables (`LazyCacheTable`), native `CREATE VIEW` (`ViewTable` + `DESCRIBE`), and `PREPARE`/`EXECUTE` all preserve `nullable=true`.

## Design Choice

Refined Option A+ (`ReparkScalarSubqueryNullability` + `StripScalarNullable`), chosen over naive Option A (missed `Sort.fetch`, `Projection` unnest/generator, `Prepare`/`CreateView` off-list runs, and ancestor `recompute_schema`), Option B (no strip; blocks `EliminateOuterJoin`/`PushDownFilter`/CSE and leaks synthetic UDF), and Option C (reordering `ScalarSubqueryToJoin` before `SimplifyExpressions`; misses `TypeCoercion`, `SparkNullability` casts, `CREATE VIEW`, and `enable_physical_uncorrelated_scalar_subquery=true`).

## Red-First Verification on Main (`fa7d75e44`)

- `test_scalar_subquery_empty_null_1.py` on main: **173 failed (100% of bug cells), 20 passed (100% of control & plan-quality cells)**.
- Domain table: [domain-table.md](domain-table.md) (HEAD column added once rebuilt at HEAD).
