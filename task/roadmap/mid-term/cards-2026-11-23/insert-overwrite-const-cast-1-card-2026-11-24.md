# Card INSERT-OVERWRITE-CONST-CAST-1: an empty `INSERT OVERWRITE` with an invalid constant cast wipes the table under ANSI

**Date:** 2026-11-24. **Filed by:** the orchestrator (`claude-opus-5-5`), from an outside review, measured here.
**Severity:** S2 (owner, 2026-11-24; Step 0 non-empty multi-row and 20,000-row multi-batch check confirmed no S1 escalation).
**Status:** fixed on `fix/insert-overwrite-const-cast-1` (2026-11-24). Evidence:
[task/wo/insert-overwrite-const-cast-1/evidence/summary.md](../../../wo/insert-overwrite-const-cast-1/evidence/summary.md).

## Measured

Measured on a build of main at `fa7d75e4` (re-verified at `514281bd4`) and on Spark 4.1.2 with Iceberg 1.11.0, on a hadoop catalog. The table holds one row first.

| Statement | ANSI | Spark 4.1.2 | Main (`514281bd4`) | Head (`fix/insert-overwrite-const-cast-1`) |
|---|---|---|---|---|
| `INSERT OVERWRITE t SELECT CAST(s AS DOUBLE) FROM (SELECT 'x' AS s) src WHERE 1 = 0` | on | `[CAST_INVALID_INPUT]`; table keeps 1 row | **succeeds; 0 rows** | `[CAST_INVALID_INPUT]`; table keeps 1 row |
| the same, `CAST(s AS TIMESTAMP)` | on | `[CAST_INVALID_INPUT]`; table keeps 1 row | **succeeds; 0 rows** | `[CAST_INVALID_INPUT]`; table keeps 1 row |
| the same, `CAST(s AS TIMESTAMP_NTZ)` | on | `[CAST_INVALID_INPUT]`; table keeps 1 row | **succeeds; 0 rows** | `[CAST_INVALID_INPUT]`; table keeps 1 row |
| `INSERT OVERWRITE t SELECT CAST(d AS DECIMAL(5, 0)) FROM (SELECT 1e30 AS d) src WHERE 1 = 0` | on | `[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]`; 1 row | **succeeds; 0 rows** | `[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]`; 1 row |
| any of the above | off | succeeds; 0 rows | succeeds; 0 rows | succeeds; 0 rows |

## Resolution (2026-11-24)

- Added a narrow write-door constant-cast folding guard (`crates/repark-spark/src/const_cast_guard.rs` and `crates/repark-spark/src/const_cast_guard/eval.rs`), wired after eager analysis and before any probe/wipe, table create/replace, staging, or commit across every Spark write door:
  - `insert_overwrite.rs` (`execute_insert_overwrite`, `execute_partition_overwrite`),
  - `insert_by_name.rs` (`execute_insert_by_name`),
  - `ctas.rs` (`execute_ctas` — covering `CTAS`, `RTAS`, `CRTAS`, `writeTo.create/replace`, `saveAsTable`),
  - `spark_ast.rs` (`execute_passthrough_inner` for `Dml`/`Copy`/`insert_source` and `try_execute_identity_dml` — covering `INSERT INTO`, `append_with_options`, `DELETE`, `UPDATE` WHERE predicates, local & S3 `COPY` Parquet/CSV/JSON),
  - `router/insert_positional/replace_where.rs` (`refuse_non_deterministic` for `REPLACE WHERE` / `writeTo.overwrite(cond)`),
  - `merge.rs` (`execute_merge` — validating `USING` source, `ON`, and `WHEN` predicates/actions).
- Exposed view definition plans and captured `TempViewSettings` from `crates/repark-spark/src/view_ddl/{temp_view,frame_settings}.rs` so temp views and DataFrames (`FrameView`, `ReplanningTempView`, `FrameSettingsNode`) honor creation-time ANSI and timezone capture while propagating constants into outer queries.
- Matches Spark 4.1.2's measured Catalyst rule: ANSI-gated, relational constant propagation (`Projection` with column pruning, `Limit(0)` elimination, `Union` multi-branch envs, `Join`, `Aggregate`, `Values` non-propagation, `UPDATE` WHERE-only unwrapping), dead-branch pruning (`CASE`/`IF`/`NVL2`/`COALESCE`), non-foldable `ConditionalExpression` stop rule, `AND`/`OR` foldability + short-circuiting, `TRY_CAST` safety, and `NaN`/`Infinity` -> `DECIMAL` NULL handling.
- The global constant evaluator, read path, `SparkFloatStringify`, and existing `logical_plan_has_unsafe_cast` (W03) guard remain untouched (`0` guards relaxed, `0` read control moves, `219/219` `pre_handback_check` seam probes unchanged).
- Measured against Spark 4.1.2 across 648 cells (204 Part 1 shapes × read/overwrite, 420 Part 2 write doors + non-empty atomicity, 24 edge/ANSI-capture cells): **160/160** in-scope write cells (43 Part 1 + 117 Part 2) plus all 8 fixed edge/capture cells moved to exact Spark parity with **0 regressions**.
- **Pins:** Rust unit tests in `crates/repark-spark/src/const_cast_guard/tests.rs`; table-driven facade pin in `python/repark/tests/writes/test_insert_overwrite_const_cast_1.py` (161/161 fixed pins red on main for the exact right reason; 260/260 pins + controls green on head).

## Still unlike Spark

Measured gaps remaining on head (all out of scope for this write-door unit or pre-existing on main):

1. **Read path (`SELECT ... WHERE 1 = 0`)** — out of scope for this write-door fix. Plain reads with an invalid constant cast through a subquery, CTE, temp view, or `TIMESTAMP`/`TIMESTAMP_NTZ`/`DOUBLE->DECIMAL`/`BOOL->DECIMAL`/integral/complex casts answer empty on RePark where Spark 4.1.2 raises `[CAST_INVALID_INPUT]`, `[CAST_OVERFLOW]`, or `[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]` (all Part 1 read-control rows in `task/wo/insert-overwrite-const-cast-1/evidence/domain-table.md`).
2. **`SparkFloatStringify` eager analyzer folding on `STRING -> DOUBLE/FLOAT`** — pre-existing global analyzer rule (`crates/repark-functions/src/java_double.rs`) folds literal/subquery `CAST('x' AS DOUBLE)` during analysis even inside dead branches (`CASE WHEN false THEN CAST('x' AS DOUBLE)`, `WHERE false AND CAST(s AS DOUBLE) = 1.0`, `WHERE (true OR CAST(s AS DOUBLE) = 1.0) AND 1 = 0`, `LIMIT 0`, unprojected subquery, and `UPDATE SET x = CAST('x' AS DOUBLE) WHERE 1 = 0`), refusing where Spark accepts (17 Part 1 / Part 2 cells).
3. **Pre-existing `logical_plan_has_unsafe_cast` W03 guard on empty whole-table `INSERT OVERWRITE`** — retained untouched (`guards_relaxed = []`; pinned by `W03_empty_insert_overwrite_no_wipe` and `crates/repark-spark/src/tests/normalize.rs`), so empty `INSERT OVERWRITE` refuses under ANSI=false for string-to-integral/date/decimal/bool/complex casts, and under ANSI=true for valid/dead-branch/real-column `CAST(... AS INT)` shapes where Spark wipes (29 Part 1 cells, 5 Part 2 `str_int` ANSI=false overwrite doors).
4. **`FloatToDecimal` runtime behavior (`crates/repark-functions/src/bool_decimal.rs`)** — pinned on the read path by `bool_decimal/tests.rs:299`; on non-empty writes and non-folded predicates it emits a generic Arrow error under ANSI=true instead of `[NUMERIC_VALUE_OUT_OF_RANGE.WITH_SUGGESTION]` and refuses under ANSI=false instead of returning `NULL` (Part 2 `ne_dbl_dec_2row` and `false|dbl_dec|{merge_on_cond,update_where_cast,delete_where_cast}`).
5. **Non-empty ANSI=false `CASE`/`CAST` nullability on `INSERT` / `OVERWRITE` / `RTAS` / `Parquet`** — pre-existing Arrow non-null column check fails on multi-row/multi-batch sources when ANSI=false produces `NULL` from an invalid cast (`false|ne_str_ntz_2row|insert_into`, `false|ne_str_int_2row|insert_into`, `false|ne_str_double_multibatch|*`).
6. **Table-assignment store-cast overflow class on empty `INSERT OVERWRITE` (`1e19 -> BIGINT`)** — `ctrl_store_implicit_1e19_empty` and `ctrl_store_implicit_1e19_subq_false` refuse before wiping (preserving rows) with generic `AnalysisException` where Spark emits `[CAST_OVERFLOW_IN_TABLE_INSERT]`.
7. **Dead `named_struct(...).a` sibling cast (`cast_str_double_named_struct_dead`)** — RePark's SQL pre-parser drops unused `named_struct` fields before planning (carded separately under `SPARK-DOOR-OVER-ACCEPT-SYNTAX-1`).
8. **Analysis-time `DATE <-> INT` cast legality under ANSI=false** — `illegal_date_to_int_subq` / `illegal_date_to_int_tv` refuse with `DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION` where Spark returns empty under ANSI=false; `illegal_int_to_date_subq` emits `CAST_WITH_FUNC_SUGGESTION` where Spark emits `CAST_WITHOUT_SUGGESTION`.
9. **Spark Iceberg `MERGE ... ON false AND ...` under ANSI=false** — Spark refuses constant-false `MERGE ON` with `Py4JJavaError` even under ANSI=false; RePark accepts when no invalid ANSI cast fires.

Up: [map.md](map.md).
