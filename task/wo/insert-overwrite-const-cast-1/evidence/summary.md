# task/wo/insert-overwrite-const-cast-1/evidence/summary.md — INSERT-OVERWRITE-CONST-CAST-1 measurement evidence

## Provenance

- **RePark:** `main` at `514281bd4` (`pkg-d1merged-ea94eedc`, tree-equal to `514281bd4`), and `fix/insert-overwrite-const-cast-1` release wheel (`896af53a3` / rebuilt at final HEAD).
- **Spark:** live PySpark 4.1.2 + Iceberg 1.11.0 (local hadoop catalog, Java 17), both ANSI modes (`spark.sql.ansi.enabled=true` and `false`).
- **Scripts:** [probe_part1.py](probe_part1.py) (102 shapes × 2 ANSI modes × read/overwrite = 204 cells), [probe_part2.py](probe_part2.py) (28 write doors × 7 shapes + 6 non-empty multi-row/20k-row multi-batch cases × 4 doors, both ANSI modes = 420 cells), [probe_edge.py](probe_edge.py) (24 edge and view/DataFrame ANSI-capture cells).
- **Full cell-by-cell table:** [domain-table.md](domain-table.md).

## Spark 4.1.2 measured constant-cast folding rule

1. **ANSI off (`spark.sql.ansi.enabled = false`):** no value-level constant cast raises; only analysis-time illegal type-pair casts raise `DATATYPE_MISMATCH`.
2. **ANSI on (`spark.sql.ansi.enabled = true`):** Catalyst folds constant casts during `CollapseProject` + `PushProjectionThroughUnion` + `SimplifyConditionals` + `ConstantFolding` before `EliminateFilters` drops `WHERE 1 = 0`:
   - Propagates constants through subqueries, nested subqueries, CTEs, temp views, `UNION ALL` branches, `JOIN`/`LEFT JOIN`, `GROUP BY ... HAVING 1 = 0`, compound expressions (`concat(s, 'y')`), `TRY_CAST(CAST(...))`, and `VALUES (CAST(...))`.
   - Does **not** raise on `LIMIT 0` (`EliminateLimits`), unprojected subquery columns (`ColumnPruning`), `FROM VALUES ('x') AS t(s)` (`LocalRelation`), real empty/filtered table columns, `TRY_CAST`, valid casts, `CAST(NULL AS T)`, `NaN`/`Infinity -> DECIMAL` (returns `NULL`), dead conditional branches (`CASE WHEN false`, `IF(false, ...)`, `coalesce(non_null_const, ...)`), non-foldable `ConditionalExpression` (`CASE WHEN col ...`), fully-foldable short-circuited booleans (`WHERE false AND CAST(s AS T) = v`, `WHERE (true OR CAST(s AS T) = v) AND 1 = 0`), or `UPDATE t SET x = CAST('x' AS T) WHERE 1 = 0`.
   - **Does** raise on `WHERE false AND col = CAST('x' AS T)` (because `And` is non-foldable and `ConstantFolding` runs before `BooleanSimplification`, recursing into children), on `WHERE CAST(s AS T) = v AND false` (LHS evaluated first), on `UPDATE ... WHERE <bad const cast>`, on `DELETE ... WHERE <bad const cast>`, and on `MERGE` source / `ON` / `WHEN` clauses.
   - **ANSI capture across views/DataFrames:** outer casts over a temp view created with ANSI=off raise under ANSI=on; casts compiled inside a temp view or DataFrame follow creation-time ANSI mode.

## Seat inventory

| Seat | Role | Resolution |
|---|---|---|
| `crates/repark-core/src/session/df_guards.rs:165-197` | Session analyzer/optimizer wiring | Unchanged (read path out of scope) |
| `crates/repark-core/src/session/df_guards/cast_shield.rs:27-60` | Narrowing-cast filter pushdown shield | Unchanged |
| `crates/repark-functions/src/analyzer/cast_legality.rs:24-28` | Analysis-time illegal type-pair deny list | Unchanged |
| `crates/repark-python/src/column/expr_build.rs:325` | `F.expr` expression context | Unchanged (writes execute through Spark router) |
| `datafusion-optimizer-54.1.0/.../expr_simplifier.rs:558-570` | `ConstEvaluator::f_up` swallows non-`Expr::Cast` UDF errors; `EliminateFilter` runs before `OptimizeProjections` | Untouched (write-door guard handles Spark write doors) |
| `crates/repark-spark/src/const_cast_guard.rs` & `eval.rs` | New write-door ANSI constant-cast folding guard | Added (`validate_write_plan`, `validate_write_query`, `validate_identity_dml_predicate`, `validate_merge_spec`) |
| `crates/repark-spark/src/view_ddl/{temp_view,frame_settings}.rs` | View plan + captured `TempViewSettings` | Added `plan_and_settings`, `frame_plan_and_settings`, `extension_settings` |
| `crates/repark-spark/src/insert_overwrite.rs:40,190,701-852` & `store.rs:12` | Whole-table & partition `INSERT OVERWRITE`; W03 guard | Wired `const_cast_guard` before empty probe/wipe & staging; W03 guard preserved untouched |
| `crates/repark-spark/src/insert_by_name.rs:49` | `INSERT INTO/OVERWRITE BY NAME` | Wired `validate_write_query` before evolution/wipe/staging |
| `crates/repark-spark/src/ctas.rs:156` | `CTAS`, `RTAS`, `CRTAS`, `writeTo.create/replace`, `saveAsTable` | Wired `validate_write_plan` right after `analyze_eagerly` |
| `crates/repark-spark/src/spark_ast.rs:191,377` | `INSERT INTO`, `append_with_options`, `DELETE`, `UPDATE`, local & S3 `COPY`, identity DML | Wired `validate_write_plan` (`is_eager_command || mode.insert_source`) and `validate_identity_dml_predicate` |
| `crates/repark-spark/src/router/insert_positional/replace_where.rs:186,391` | `REPLACE WHERE` & `writeTo.overwrite(cond)` | Source via `execute_insert_source`; predicate via `validate_write_plan` when explicit cast present |
| `crates/repark-spark/src/merge.rs:81` | `MERGE INTO` (`USING`, `ON`, `WHEN`) | Wired `validate_merge_spec` before `repark_iceberg::write::merge::execute_merge` |
| `crates/repark-sql/src/{router,insert_overwrite}.rs` | Native ANSI SQL door | Unchanged (no Spark ANSI toggle; out of scope) |

## Measurement summary

| Suite | Cells | Equal on main | Equal on head | Improved to exact Spark | Regressed vs main |
|---|---|---|---|---|---|
| Part 1 (`INSERT OVERWRITE`, both ANSI modes) | 204 | 109 | 152 | 43 | 0 |
| Part 1 (Read controls, both ANSI modes) | 204 | 146 | 146 | 0 (out of scope) | 0 |
| Part 2 (Write doors + non-empty atomicity, both ANSI modes) | 420 | 262 | 379 | 117 | 0 |
| Part 3 (Edge cases + ANSI capture) | 24 | 16 | 24 | 8 | 0 |
| `pre_handback_check.py --profile full` seam probes | 219 | 219 (baseline) | 219 | 0 diffs | 0 |

- **Non-empty S1 check:** NOT S1 (stays S2). All 2-row and 20,000-row multi-batch writes under ANSI=true fail atomically before commit and preserve prior rows.
- **Pins:** Rust unit tests in `crates/repark-spark/src/const_cast_guard/tests.rs`; table-driven facade pins in `python/repark/tests/writes/test_insert_overwrite_const_cast_1.py` (161/161 fixed pins red on main for the right reason; 260/260 pins + controls green on head).
