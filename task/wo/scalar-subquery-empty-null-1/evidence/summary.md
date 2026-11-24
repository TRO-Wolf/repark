# SCALAR-SUBQUERY-EMPTY-NULL-1 — Evidence Summary

**Date:** 2026-11-23 (Delta 1 updated 2026-11-24)  
**Base:** `fa7d75e446e6f0a3edf48f6538c6a42c376515e7` (`main`), merged with `ed637ffb7250b4d1ada60cb52cd7e7d8dffa4aa2` (`main`)  
**Card:** [scalar-subquery-empty-null-1-card-2026-11-23.md](../../../roadmap/mid-term/cards-2026-11-23/scalar-subquery-empty-null-1-card-2026-11-23.md)  
**Pins:** `python/repark/tests/sql/test_scalar_subquery_empty_null_1.py` (`_scalar_subquery_empty_null_1_cells.py`), `crates/repark-core/src/session/df_guards/scalar_nullable_tests.rs`, `crates/repark-core/src/pre_execute/tests.rs`  
**Scripts:** [probe_step0.py](probe_step0.py)  
**Domain table:** [domain-table.md](domain-table.md)

## Seat Inventory

1. **Session analyzer list (`crates/repark-core/src/session/df_guards.rs`):** `ReparkScalarSubqueryNullability` runs twice: Pass 1 immediately before `type_coercion` (after `extension.configure_analyzer_rules` and `dialect.configure_analyzer_rules`), and Pass 2 at the end of `finish_session_build` (after `SparkNullability`, `GeneratorRewrite`, `StackRewrite`, time-window rules, `FloatDecimalStoreRule`, `signed_zero`, and `CastShieldRule`).
2. **Other production analyzer lists (`crates/repark-python/src/column/expr_build.rs`):** `build_expr_context` seats Pass 1 immediately before `type_coercion`, seats Pass 2 at the end after `spark_door_post_coercion_rules()`, and registers `repark_core::scalar_nullable::udf()`. `crates/repark-spark/src/view_ddl/temp_settings.rs` only parses `SET`/`RESET`.
3. **DataFusion off-list runs (`crates/repark-core/src/pre_execute.rs`):** `PreExecute::execute` wraps `Statement::Prepare` and `DdlStatement::CreateView` inputs before DataFusion's bare optimizer and `apply_type_coercion`; `ReparkScalarSubqueryNullability` also runs as an `OptimizerRule` before `simplify_expressions`.
4. **SQL-planner-time coercions (`datafusion-sql` 54.1.0):** `parse_scalar_subquery` does not fold on nullability; casts preserve child nullability once wrapped before `type_coercion`.
5. **Optimizer rules:** `ReparkScalarSubqueryNullability` precedes `simplify_expressions`; `StripScalarNullable` runs immediately after `scalar_subquery_to_join` and strips `__repark_scalar_nullable__` outside surviving subqueries while recomputing ancestor schemas, preserving `EliminateOuterJoin`, `PushDownFilter`, `OptimizeProjections`, `CommonSubexprEliminate`, and `enable_physical_uncorrelated_scalar_subquery=true`.
6. **Stored plans re-entering:** Spark SQL temp views (`ReplanningTempView`), DataFrame temp views (`FrameView` including cross-ANSI `closed_scan`), cached tables (`LazyCacheTable`), native `CREATE VIEW` (`ViewTable` + `DESCRIBE`), and `PREPARE`/`EXECUTE` all preserve `nullable=true`, including expressions forced non-null after `type_coercion` (`b <=> 1`, `named_struct`, `map`, `array`, aliased/bare `explode`, `stack`, and `window`).

## Design Choice

Refined Option A+ (`ReparkScalarSubqueryNullability` + `StripScalarNullable`), updated in Delta 1:
- **Unexpanded generator deferral (F-001/F-005):** `scalar_subquery_needs_wrap` skips wrapping while the subquery plan contains an unexpanded generator placeholder (`explode`, `explode_outer`, `posexplode`, `posexplode_outer`, `inline`, `inline_outer`, `json_tuple`, `stack`, `__repark_gen_alias`, `__repark_gen_bare`), so `GeneratorRewrite` and `StackRewrite` expand cleanly before Pass 2 wraps the expanded output.
- **Metadata-free synthetic aliases (F-002):** `wrap_subquery_output` sets `metadata: None` on newly synthesized `Expr::Alias` nodes while preserving `alias_metadata` on existing `Expr::Alias` expressions, so downstream cast-to-UDF rewrites never mismatch Iceberg `PARQUET:field_id` metadata between logical and physical schemas while plain column subqueries still inherit field metadata dynamically.
- **Idempotent post-coercion Pass 2 (F-003):** trailing analyzer seats in `finish_session_build` and `build_expr_context` wrap subqueries turned non-null after `type_coercion` and peel stale outer `__repark_spark_nonnull__` cast wrappers (`Cast`, `to_timestamp`, `__repark_cast_timestamp_ntz__`, `__repark_zone_seam__`) whose cast operand became nullable.

## Measured Reach (Spark 4.1.2 / Old Main / Current Main / Merged / Delta 1)

- **WS3 121-cell read matrix:** **57/57** S1 cells fixed in each ANSI mode and on native `repark.sql()` (**0 regressions**, **0 `NONNULL_WITH_NULLS`** down from 44, **0 moved cells vs merged**). `inline_utf8view_subq` (`arrow_cast('x', 'Utf8View')`), `df_api_scalar_isnull`, and all 8 `enable_physical_uncorrelated_scalar_subquery=true` cells pass.
- **Extended Step 0.3 cells (`probe_step0.py`):** produces 100% of `domain-table.md` cells (57/57); `spark_explode_empty_isnull` now answers `[[True]]` matching Spark 4.1.2 in both ANSI modes (was `[UNRESOLVED_ROUTINE]` on old main, `[[False]]` on current main, and `[UNSUPPORTED_GENERATOR]` on merged).
- **Delta 1 findings (F-001 to F-005):** 20/20 verifier `repro_repark.py` cells match Spark 4.1.2 in both ANSI modes; 50/50 generator + Iceberg/Parquet SQL cells, 14/14 `selectExpr` + temp-view cells, 5/5 Iceberg/Parquet `INSERT ... SELECT` cells (both ANSI modes), and 11/11 native SQL/INSERT cells match Spark with **0 regressions vs current main**.
- **DML / writes:** `INSERT ... WHERE (subquery) IS NULL`, `INSERT ... SELECT` with Iceberg/Parquet cast subqueries, and `CTAS ... WHERE (subquery) IS NULL` match Spark on both doors; `DELETE`/`UPDATE` with subquery keep their refusal.
- **MERGE (out of scope, recorded unchanged):** on Spark door, `dml_merge_on_isnull` answers `[[1, 99], [2, 99]]` and `dml_merge_when_isnull` raises `UnsupportedOperationException` (Spark refuses both with `[UNSUPPORTED_MERGE_CONDITION.SUBQUERY]`; native door refuses both with `UnsupportedOperationException`).
- **Plan quality (`EXPLAIN`):** all 5 non-subquery queries, `count(*)`, and `(SELECT 1)` are byte-for-byte identical (logical and physical) to current main (`ed637ffb`); `subq = 1` keeps `EliminateOuterJoin` (`CrossJoinExec`, byte-identical physical plan); zero `__repark_scalar_nullable__` markers survive in lowered plans.
- **Red-first / Green-head:** `test_scalar_subquery_empty_null_1.py` measures **50 failed / 217 passed** on `pkg-merged-19bb3e3b` (every new F-001/F-002/F-003 pin red for its defect) and **267 passed / 0 failed** on Delta 1 head.
