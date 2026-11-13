# Card NOT-IN-MULTICOLUMN-1: `(d, k) NOT IN (SELECT d, k …)` refuses on both doors

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`), under owner ruling Q8 of 2026-11-12 ("Go with
the defaults"). **Source:** SIGNED-ZERO-NORMALIZE-1 part B evidence (`task/wo/signed-zero-normalize-1/evidence/part-b/`,
filed with that unit): item 1's addendum on main `276dc503`, cell N2-two-column (group Q1).

**Status:** filed 2026-11-12, not dispatched. Severity **S3, over-refusal**: loud, no table changes. Pre-existing (item 1).

**Measured on main `276dc503` against Spark 4.1.2:**

- `SELECT id FROM na WHERE (d, k) NOT IN (SELECT d, k FROM nb)` (table names shortened) refuses on the Spark door and on native with
  `Error during planning: Too many columns! The subquery should only return one column`.
- Spark answers `[2]`, with IEEE equality on the float column.
- The one-column form, N1-one-column, is EQUAL on both doors.

**Bearing on part B:** owner ruling Q1 of 2026-11-12 makes IEEE equality the truth for `NOT IN (subquery)` and names
this cell the clause's second witness. Today the witness exists only on Spark's side.

**Fix direction:** plan a multi-column `NOT IN (subquery)` as a null-aware anti join on the column tuple, with
Spark's NULL rules for a row-valued `NOT IN`, and let part B's seat 2 wrap its float keys as it wraps the
one-column form. Pin N2 on both doors with IEEE equality as the truth.

**Close:** fixed with that pin, or declined by a recorded ruling.
