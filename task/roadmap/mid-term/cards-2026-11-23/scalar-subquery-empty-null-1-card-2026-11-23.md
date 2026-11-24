# Card SCALAR-SUBQUERY-EMPTY-NULL-1: an empty scalar subquery over a NOT NULL column is treated as never NULL

**Date:** 2026-11-23. **Filed by:** the orchestrator (`claude-opus-5-5`), from an outside risk review that named the
hazard, and a measurement here. **Severity:** S1 (owner, 2026-11-23): a silent wrong answer. A filter on the subquery
drops every row, or keeps every row. **Status:** built on `fix/scalar-subquery-empty-null-1` (2026-11-23,
`gemini-4-neon-xhigh`). Evidence: [task/wo/scalar-subquery-empty-null-1/evidence/summary.md](../../../wo/scalar-subquery-empty-null-1/evidence/summary.md).

## Measured on main (`fa7d75e44`, unchanged from `e80e6bc8`)

- **Read matrix (121 cells per ANSI mode against live Spark 4.1.2):** 60 silent wrong answers in each ANSI mode (57
  caused by this defect + 3 empty-table `GROUP BY ROLLUP`/`CUBE`/`GROUPING SETS (())` divergence), 3 pre-existing
  unaggregated correlated non-LIMIT errors, 44 `NONNULL_WITH_NULLS` Arrow schema violations, plus runtime Arrow errors
  on `inline_utf8view_subq` (`SELECT (SELECT 'hello' WHERE false)`) and `CAST((SELECT d FROM empty_nn) AS TIMESTAMP)`.
  Native `repark.sql()` reproduced the same 60 wrong answers against DuckDB/Trino.
- **DataFrame / expression / stored-plan doors:** `F.expr`, `filter(Column)` from `F.expr`, `filter(str)`,
  `selectExpr`, `withColumn`, `.scalar()`, SQL and DataFrame temp views (including ANSI-flip), cached DataFrames,
  `PREPARE`/`EXECUTE`, CTEs, `CREATE VIEW` (`DESCRIBE` reported `NO`), and
  `enable_physical_uncorrelated_scalar_subquery=true` all reproduced the defect.
- **DML / writes:** `INSERT ... WHERE (subquery) IS NULL` and `CTAS ... WHERE (subquery) IS NULL` silently wrote 0 rows
  on both doors where Spark writes 2 rows.

## Fix

Implemented refined Option A+ in `crates/repark-core/src/session/df_guards/scalar_nullable.rs`:

- `ReparkScalarSubqueryNullability` (`AnalyzerRule` immediately before `type_coercion` on every session and in
  `crates/repark-python/src/column/expr_build.rs` `build_expr_context`, `OptimizerRule` immediately before
  `simplify_expressions`, and invoked in `PreExecute::execute` for `Statement::Prepare` and `DdlStatement::CreateView`)
  wraps single-column non-null `Expr::ScalarSubquery` outputs lacking `guaranteed_at_least_one_row` in `Volatile` UDF
  `__repark_scalar_nullable__` (`schema_name` passthrough, `nullable=true`), recomputing ancestor schemas (including
  `Union` and `Values`).
- `guaranteed_at_least_one_row` keeps provably-1-row subqueries (`COUNT(*)` without `GROUP BY`, global aggregates,
  `(SELECT 1)`, non-empty `VALUES`) non-null so constant folding is preserved, while guarding `Sort { fetch: Some(0) }`,
  `Limit`, `Projection` with `Unnest`/generator UDFs, and grouping sets.
- `StripScalarNullable` runs immediately after `scalar_subquery_to_join` to remove markers outside surviving subqueries
  and recompute ancestor schemas, preserving `EliminateOuterJoin`, filter pushdown, CSE, and
  `enable_physical_uncorrelated_scalar_subquery=true`.
- **Pins:** Rust unit tests in `crates/repark-core/src/session/df_guards/scalar_nullable.rs` and
  `crates/repark-core/src/pre_execute/tests.rs`; table-driven facade pins across both doors and Iceberg Hadoop writes
  in `python/repark/tests/sql/test_scalar_subquery_empty_null_1.py` (`_scalar_subquery_empty_null_1_cells.py`).

## Still unlike Spark (outside this unit)

1. **Empty-table `GROUP BY ROLLUP(b)` / `CUBE(b)` / `GROUPING SETS (())` (3 cells):** Spark 4.1.2 returns 0 rows over
   an empty table (`IS NULL` = `true`), whereas DataFusion 54.1.0 (`init_empty_grouping_sets`), DuckDB, and Trino follow
   standard SQL and return 1 row (`0`, so `0 IS NULL` = `false`). `GROUP BY b, ROLLUP(b)` has no empty grouping set and
   matches Spark.
2. **Unaggregated correlated scalar subqueries without `LIMIT` (3 cells):** `corr_nonagg_where_isnull`,
   `corr_nonagg_select_val`, and `corr_nonagg_coalesce` fail with DataFusion's pre-existing `Invalid (non-executable)
   plan after Analyzer` error.
3. **Catalog-table references inside `F.expr` and `filter(str)` (5 cells):** `F.expr` parses against static
   `EXPR_CONTEXT` and `filter(str)` uses DataFusion's empty-table expression provider, so catalog-table subqueries fail
   with `table not found` (inline subqueries work on both; `selectExpr` and `.scalar()` work on catalog tables).
4. **`(subquery) + 1` types `bigint` where Spark types `int`.** This was measured after merging main and is the same on
   main; nullability now matches Spark.
5. **Subqueries in `DELETE`, `UPDATE`, and `MERGE`:** `DELETE` and `UPDATE` keep their refusal; `MERGE` with a subquery
   in `ON`/`WHEN` (which Spark refuses with `[UNSUPPORTED_MERGE_CONDITION.SUBQUERY]`) is unchanged and out of scope
   (post-fix on Spark door: `MERGE ... ON ... (subq) IS NULL` updates rows `[[1, 99], [2, 99]]` instead of silently
   matching 0 rows `[[1, 10], [2, 20]]`, and `MERGE ... WHEN MATCHED AND (subq) IS NULL` raises
   `UnsupportedOperationException`; native door still raises `UnsupportedOperationException` for both).
6. **Output schema nullability on provably-1-row subqueries (`COUNT(*)`, `(SELECT 1)`):** Spark Catalyst marks all
   `ScalarSubquery` outputs nullable even when provably non-null; RePark intentionally keeps provably-1-row subqueries
   non-null to preserve constant folding (never producing `NONNULL_WITH_NULLS`).
7. **Related, separate family:** `TRY_CAST` over a NOT NULL column crashing a function whose output nullability follows
   its input (`abs(TRY_CAST(s AS INT))`). Carded as TRY-CAST-NONNULL-FIELD-1
   ([try-cast-nonnull-field-1-card-2026-11-24.md](try-cast-nonnull-field-1-card-2026-11-24.md)).

## Reach on main before the fix (measured 2026-11-24)

An outside review wrote a 121-cell probe; it was run here against Spark 4.1.2 and main `e80e6bc8`.

**Reads.** In each ANSI mode, 60 of the 121 cells answer in both engines but differ.
- **Shapes:**
  - an inline empty subquery (23 cells);
  - a subquery over an empty table (14);
  - an aggregate whose `HAVING`, `GROUP BY`, `LIMIT 0`, `OFFSET`, `ROLLUP`, `CUBE` or `GROUPING SETS` leaves no rows
    (14);
  - a correlated subquery with no row for some outer row (9).
- **Functions:** `IS NULL`, `IS NOT NULL`, `coalesce`, `ifnull`, `nvl`, `nvl2`, `CASE`, `x = x` and `LIKE '%'`.

**Writes**, Iceberg hadoop catalog, ANSI off:

| Statement shape | Spark 4.1.2 | RePark main |
|---|---|---|
| `INSERT INTO … SELECT … WHERE (subquery) IS NULL` | inserts 2 rows | **inserts none** |
| `CREATE TABLE … AS SELECT id, coalesce((subquery), 42) … WHERE (subquery) IS NULL` | 2 rows, both 42 | **an empty table** |
| `DELETE` / `UPDATE … WHERE (subquery) IS NULL` | answers | refuses (a subquery in DML) |
| `MERGE … ON` / `WHEN … (subquery) IS NULL` | `[UNSUPPORTED_MERGE_CONDITION.SUBQUERY]` | **runs** |

So the S1 writes wrong data through `INSERT` and `CTAS`, and `MERGE` runs a condition Spark refuses.

**Suffixed literals (PR #1417).** Before that PR, every suffixed literal such as `1L` was declared nullable. That hid
the defect in `(subquery) + 1L` while `(subquery) + 1` already showed it. Now both spellings declare the field
non-null on main, where Spark declares it nullable. The rows are right (NULL) and no export path crashes (measured on
`collect`, `toArrow` and `toPandas`). With this unit merged onto main, both spellings declare it nullable, as Spark
does, in both ANSI modes.

**Not affected:** `WHERE (id IN (SELECT v …)) IS NULL` over a list that holds NULL. Spark and RePark both answer no
rows, so the review's IN-subquery claim does not hold.

Up: [map.md](map.md).
