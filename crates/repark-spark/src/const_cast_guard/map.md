# `crates/repark-spark/src/const_cast_guard`

Write-door constant-cast folding guard for Spark ANSI parity (`INSERT-OVERWRITE-CONST-CAST-1`, 2026-11-24).

## Why this module exists

DataFusion's `ConstEvaluator` only propagates runtime errors for `Expr::Cast(Literal)` and keeps failing Spark cast UDFs (`__repark_parse_java_double__`, `to_timestamp`, `__repark_cast_timestamp_ntz__`, `__repark_float_to_decimal__`, `__repark_bool_to_decimal__`, `__repark_string_to_*__`, `__repark_narrow_to_*__`, `__repark_cast_map__`) for execution, and `EliminateFilter` drops `WHERE 1 = 0` before constant propagation. On Spark 4.1.2 under `spark.sql.ansi.enabled = true`, Catalyst's `CollapseProject` + `SimplifyConditionals` + `ConstantFolding` folds constant casts before writing and raises Spark's error class without mutating the target table.

This module runs only on Spark write doors (`insert_overwrite.rs`, `insert_by_name.rs`, `ctas.rs`, `spark_ast.rs`, `router/insert_positional/replace_where.rs`, `merge.rs`) right after eager analysis and before any empty-probe wipe, table creation, staging, or commit. The global constant evaluator, read path, read-path cast functions, and existing `logical_plan_has_unsafe_cast` W03 guard remain unchanged.

## Files

| File | Role |
|---|---|
| [`../const_cast_guard.rs`](../const_cast_guard.rs) | `validate_write_plan`, live `ReplanningTempView` pre-resolution (`resolve_live_views`, `ViewPlanCache`), and relational constant-propagation walker (`walk_plan`: `Projection` column pruning, `Limit(0)` elimination, `Union` multi-branch envs, ungrouped `Aggregate` over `LIMIT 0`, `Values` non-propagation, `Dml(Update)` WHERE-only unwrapping, view settings overlay). |
| [`join.rs`](join.rs) | Join constant-propagation walker (`walk_join`, `side_guaranteed_non_empty`, `split_join_needed`, `combine_join_envs`, `project_join_output`): suppresses constants from outer-join null-supplying sides unless both sides are provably non-empty and the join condition is statically `true`, and drops `INNER JOIN ... ON false`. |
| [`probes.rs`](probes.rs) | SQL-probe write-door entry points (`validate_write_query`, `sql_expr_has_explicit_cast`, `validate_identity_dml_predicate`, `validate_merge_spec` with non-propagating `USING` `LEFT JOIN ... ON false` for `WHEN` clauses, and cast-only error propagation `is_cast_error`). |
| [`eval.rs`](eval.rs) | Catalyst-equivalent expression evaluator (`eval_expr`): dead-branch pruning for `Case`/`if`/`nvl2`/`coalesce`/`nvl`/`ifnull`, non-foldable `ConditionalExpression` stop rule, `And`/`Or` foldability + short-circuiting, `TryCast` & safe-cast UDF handling, 1-row batch evaluation via `create_physical_expr`, `FloatToDecimal` NaN/Inf NULL + finite overflow formatting, 2-arg `to_timestamp_{ltz,ntz}` `[CANNOT_PARSE_TIMESTAMP]` formatting, and Spark error-class formatting. |
| [`spark_grammar.rs`](spark_grammar.rs) | Self-contained Spark 4.1.2 `STRING -> T` constant-cast grammar parsers (`DATE`, `TIMESTAMP_NTZ`, `TIMESTAMP` LTZ validator, `DOUBLE`/`FLOAT` including Java hex floats & suffixes, `DECIMAL` with Java `BigDecimal` scientific notation + 38-digit range + `HALF_UP` rounding, and `TINYINT`/`SMALLINT`/`INT`/`BIGINT`) so pre-existing read-path cast gaps never cause false refusals on empty writes. |
| [`tests.rs`](tests.rs) | Unit tests covering types, indirections, dead branches, short-circuit ordering, DML doors, outer joins, `MERGE` action non-propagation, replaced temp views, Spark grammar boundary tables, 2-arg `to_timestamp_ltz`, ungrouped aggregates over `LIMIT 0`, ANSI off, and temp-view ANSI capture. |

## Invariants

- Only cast expressions (`Expr::Cast` and recognized Spark cast UDFs) can fail validation; non-cast constant expressions return `Ok(None)` on evaluation error so non-cast expressions never regress.
- `spark.sql.ansi.enabled = false` (or an unwrapped view whose captured `TempViewSettings` has ANSI off) never raises a value-level cast error.
- Outer joins carry no constant out of their null-supplying side (`LEFT` right, `RIGHT` left, `FULL` both) unless both sides are provably non-empty and the join condition evaluates statically to `true`.
- `MERGE` `WHEN` predicates and actions do not inherit constants from the `USING` source (`LEFT JOIN ... ON false`); only self-contained constant casts written in the `WHEN` clause itself fold.
- `ReplanningTempView` scans are resolved live via `live_replanning_plan` under each view's captured `TempViewSettings` and bounded by `MAX_PLAN_DEPTH` / `MAX_VIEW_EXPANSION_DEPTH`.
- `UPDATE` validates only its `WHERE` predicate (`unwrap_update_filter`), matching Spark Iceberg's measured `UPDATE SET ... WHERE 1 = 0` acceptance vs `UPDATE ... WHERE <bad const cast>` refusal.
- `FloatToDecimal` non-finite inputs (`NaN`, `±Infinity`) yield `NULL` without raising, matching Spark 4.1.2.
- SQL-probe helpers propagate only cast error classes (`is_cast_error`) so non-cast planning errors (e.g., `DATATYPE_MISMATCH.INVALID_ORDERING_TYPE` in `MERGE ON`) reach the DML executor's own formatter untouched.
