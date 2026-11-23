# Card SCALAR-SUBQUERY-EMPTY-NULL-1: an empty scalar subquery over a NOT NULL column is treated as never NULL

**Date:** 2026-11-23. **Filed by:** the orchestrator (`claude-opus-5-5`), from an outside risk review that named the
hazard, and a measurement here. **Severity:** S1 (owner, 2026-11-23): a silent wrong answer. A filter on the subquery
drops every row, or keeps every row. **Status:** filed, queued after DATE-PART-SESSION-ZONE-1 (owner, 2026-11-23).

## Measured

These were measured on RePark main `e80e6bc8` and Spark 4.1.2, in both ANSI modes, with the same answers in each mode.
`t` is `(SELECT 1 AS id, 'abc' AS s UNION ALL SELECT 2, '7')`. `empty` is `(SELECT 1 AS b WHERE 1 = 0)`, so its one
column is NOT NULL and it has no rows.

| Statement | Spark 4.1.2 | RePark `spark.sql` | RePark `repark.sql` | DuckDB 1.5.5 |
|---|---|---|---|---|
| `SELECT (SELECT b FROM empty) IS NULL` | true | **false** | **false** | — |
| `SELECT id FROM t WHERE (SELECT b FROM empty) IS NULL` | 1, 2 | **no rows** | **no rows** | 1, 2 |
| `SELECT id FROM t WHERE (SELECT b FROM empty) IS NOT NULL` | no rows | **1, 2** | — | — |
| `SELECT id, coalesce((SELECT b FROM empty), 42) FROM t` | 42, 42 | **NULL, NULL** | **NULL, NULL** | 42, 42 |
| `SELECT id, (CASE WHEN id = 1 THEN (SELECT b FROM empty) ELSE 1 END) IS NULL FROM t` | true, false | **false, false** | — | — |

A scalar subquery with no rows is NULL in Spark, DuckDB and Trino. So both doors are wrong, each against its own
oracle.

## Cause (read in the pinned sources, not yet traced through a plan)

- In `datafusion-expr` 54.1.0, `Expr::ScalarSubquery` reports the nullability of the subquery's column
  (`expr_schema.rs`, `subquery.schema().field(0).is_nullable()`). It ignores that an empty subquery yields NULL.
- The expression simplifier then folds `IS NULL` to false and `IS NOT NULL` to true for any expression it believes
  cannot be NULL. `coalesce` drops its later arguments by the same belief.

## Not yet known

- Which seat fixes it without changing plans that answer Spark today. Candidates: a rule that marks scalar subqueries
  nullable before the simplifier runs, or a change at the decorrelation that turns them into joins.
- The reach: correlated subqueries, `IN` lists holding a scalar subquery, `CASE` branches, DML predicates (`DELETE` /
  `UPDATE … WHERE`), `MERGE` conditions, and DataFrame filters built from SQL fragments.
- Related, not the same: a `TRY_CAST` over a NOT NULL column can crash a function whose output nullability follows its
  input (`abs(TRY_CAST(s AS INT))`). That is loud and is a separate family.

Up: [map.md](map.md).
