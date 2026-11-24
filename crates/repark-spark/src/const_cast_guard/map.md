# `crates/repark-spark/src/const_cast_guard`

Write-door constant-cast folding guard for Spark ANSI parity (`INSERT-OVERWRITE-CONST-CAST-1`, 2026-11-24).

## Why this module exists

DataFusion's `ConstEvaluator` only propagates runtime errors for `Expr::Cast(Literal)` and keeps failing Spark cast UDFs (`__repark_parse_java_double__`, `to_timestamp`, `__repark_cast_timestamp_ntz__`, `__repark_float_to_decimal__`, `__repark_bool_to_decimal__`, `__repark_string_to_*__`, `__repark_narrow_to_*__`, `__repark_cast_map__`) for execution, and `EliminateFilter` drops `WHERE 1 = 0` before constant propagation. On Spark 4.1.2 under `spark.sql.ansi.enabled = true`, Catalyst's `CollapseProject` + `SimplifyConditionals` + `ConstantFolding` folds constant casts before writing and raises Spark's error class without mutating the target table.

This module runs only on Spark write doors (`insert_overwrite.rs`, `insert_by_name.rs`, `ctas.rs`, `spark_ast.rs`, `router/insert_positional/replace_where.rs`, `merge.rs`) right after eager analysis and before any empty-probe wipe, table creation, staging, or commit. The global constant evaluator, read path, and existing `logical_plan_has_unsafe_cast` W03 guard remain unchanged.

## Files

| File | Role |
|---|---|
| [`../const_cast_guard.rs`](../const_cast_guard.rs) | Write-door entry points (`validate_write_plan`, `validate_write_query`, `validate_where_predicate`, `validate_identity_dml_predicate`, `validate_merge_spec`) and relational constant-propagation walker (`walk_plan`: `Projection` column pruning, `Limit(0)` elimination, `Union` multi-branch envs, `Join`, `Aggregate`, `Values` non-propagation, `Dml(Update)` WHERE-only unwrapping, `ReplanningTempView`/`FrameView`/`FrameSettingsNode` settings overlay). |
| [`eval.rs`](eval.rs) | Catalyst-equivalent expression evaluator (`eval_expr`): dead-branch pruning for `Case`/`if`/`nvl2`/`coalesce`/`nvl`/`ifnull`, non-foldable `ConditionalExpression` stop rule, `And`/`Or` foldability + short-circuiting, `TryCast` & safe-cast UDF handling, 1-row batch evaluation via `create_physical_expr`, `FloatToDecimal` NaN/Inf NULL + finite overflow formatting, and Spark error-class formatting. |
| [`tests.rs`](tests.rs) | Unit tests covering types, indirections, dead branches, short-circuit ordering, DML doors (`INSERT OVERWRITE`, `INSERT INTO`, `CTAS`, `MERGE`, `UPDATE`, `DELETE`), ANSI off, and temp-view ANSI-mode capture. |

## Invariants

- Only cast expressions (`Expr::Cast` and recognized Spark cast UDFs) can fail validation; non-cast constant expressions return `Ok(None)` on evaluation error so non-cast expressions never regress.
- `spark.sql.ansi.enabled = false` (or an unwrapped view whose captured `TempViewSettings` has ANSI off) never raises a value-level cast error.
- `UPDATE` validates only its `WHERE` predicate (`unwrap_update_filter`), matching Spark Iceberg's measured `UPDATE SET ... WHERE 1 = 0` acceptance vs `UPDATE ... WHERE <bad const cast>` refusal.
- `FloatToDecimal` non-finite inputs (`NaN`, `±Infinity`) yield `NULL` without raising, matching Spark 4.1.2.
