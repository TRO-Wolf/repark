# Card SQL-MINUS-REFUSE-1: `MINUS` refuses on both RePark doors where Spark answers it as `EXCEPT`

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** SQL-SETOP-ALL-REFUSE-1 step 0
(`task/wo/sql-setop-all-refuse-1/evidence/step0/`, PR #1314).

**Status:** filed 2026-11-12, not dispatched. Severity **S3, over-refusal**: loud, no table changes. Pre-existing on main.

**Measured on main `a3f8c7d9` (release build) against Spark 4.1.2 with Iceberg 1.11.0:**

- Data: `l = {1, 1, 1, 2, 2, 4}`, `r = {1, 2, 2, 3}`.
- `MINUS`, Spark's synonym for the distinct `EXCEPT`, refuses on the Spark door and on the native door with
  `UnsupportedOperationException: This feature is not implemented: MINUS not implemented`.
- Spark answers `[4]` at the top level and in a CTE; an `INSERT` of it stores `[4]`, and a CTAS of it creates a table
  holding `[4]`.
- On RePark the `INSERT` stores nothing and adds no snapshot, and the CTAS creates no table.

**Not this card:** `MINUS ALL` is now refused deliberately, with a hint, by SQL-SETOP-ALL-REFUSE-1 (PR #1314), beside
`INTERSECT ALL` and `EXCEPT ALL`, until their multiset semantics are Spark-correct (SQL-SETOP-ALL-MULTISET-1). Only the
distinct `MINUS` belongs here.

**Fix direction:** none in the source. Spark treats `MINUS` as `EXCEPT`, so the natural shape is to plan it as the
distinct `EXCEPT`, which answers Spark's rows on both doors, with the four cells above as pins.

**Close:** `MINUS` answers Spark's rows on both doors, pinned, or the card is declined by a recorded ruling.

Up: [map.md](map.md).
