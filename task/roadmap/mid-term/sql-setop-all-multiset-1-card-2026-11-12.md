# Card SQL-SETOP-ALL-MULTISET-1 — `INTERSECT ALL` and `EXCEPT ALL` answer Spark's multiset

**Date:** 2026-11-12 · **Filed by:** Claude Opus 5.5 (`claude-opus-5-5`), the second builder of
SQL-SETOP-ALL-REFUSE-1 · **Severity:** S2 · **Source:** owner ruling of the same day, verbatim "Go
with the defaults" (signed-zero part B, Q2): refuse now, fix under this card. **Status:** filed, not
dispatched.

## Why

DataFusion plans `INTERSECT ALL` as a left-semi join and `EXCEPT ALL` as a left-anti join, both
without the left `DISTINCT`, so every left duplicate survives or every copy goes. Spark keeps the
minimum multiplicity and subtracts counts. Measured on main `a3f8c7d9` against Spark 4.1.2, with
`l = {1, 1, 1, 2, 2, 4}` and `r = {1, 2, 2, 3}`
([step-0 table](../../wo/sql-setop-all-refuse-1/evidence/step0/table.md)):

| operator | Spark 4.1.2 | RePark, both SQL doors |
|---|---|---|
| `l INTERSECT ALL r` | `1, 2, 2` | `1, 1, 1, 2, 2` |
| `l EXCEPT ALL r` | `1, 1, 4` | `4` |
| `l MINUS ALL r` | `1, 1, 4` | DataFusion refuses (`MINUS ALL not implemented`); `MINUS` refuses too |

The wrong multiset reaches `INSERT`, CTAS and `MERGE` writes. Fable measured the same on 1.5.6 for
INT and the decimal twin (signed-zero part B, SO-intersectall, SO-exceptall).

Until this card lands, SQL-SETOP-ALL-REFUSE-1 refuses every SQL spelling at the session's statement
seat, and in `F.expr` / `filter` / `selectExpr` fragments and stored view bodies, and the facade
refuses `DataFrame.intersectAll` / `exceptAll` (`octo C1-L-005`, `-006`).

## Candidates

| id | candidate | status |
|---|---|---|
| A | Number each side's duplicates, `row_number() OVER (PARTITION BY <every column>)`, apply the distinct `INTERSECT` / `EXCEPT` to `(columns, n)`, and project the columns away (Fable's idea). | Untested as a rewrite. As user SQL it answers Spark's rows on both doors on the step-0 data only (the `workaround` rows); the refusal hint names it. |
| B | Spark's own rewrite (`RewriteIntersectAll`, `RewriteExceptAll`): `UNION ALL` of the two sides tagged with a marker column, an aggregate of the per-side counts, a filter, and a row replication by the kept count. | Not measured. Needs a replicate-rows operator (an `unnest` of a repeated array, or a generator node). |

Either is a logical rewrite of the semi and anti join DataFusion builds, at the seat the refusal
holds now. The candidate must agree with the set operation's key semantics: NULLs equal, and signed
zero and NaN as SIGNED-ZERO-NORMALIZE-1 part B rules them for set-operation keys.

## Done when

- Every step-0 shape answers Spark's rows on both SQL doors: top level, CTE, scalar and `IN`
  subqueries, temporary and persistent views, Iceberg sources, `INSERT`, CTAS, `MERGE USING`, nested
  in `UNION ALL`, and `MINUS ALL` as `EXCEPT ALL` (Spark accepts it).
- The DataFrame `intersectAll` / `exceptAll` answer Spark's rows.
- The cells cover INT, decimal, float (with `-0.0` and NaN), string, NULL and struct columns, and
  several columns at once.
- The refusals retire: `pre_execute::setop_all::refuse_in_session_sql`, the fragment and view-body calls, the
  DataFrame refusal, and the parity doc's SQL-SETOP-ALL-REFUSE-1 row. The refusal pins in
  `python/repark/tests/sql/test_sql_setop_all_refuse_1.py` turn into answer pins.
