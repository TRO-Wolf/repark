# Card STRUCT-COMPARE-REVIEW-RESIDUE-1: the class (c) items the PR #1292 verdicts did not fold into a unit

**Date:** 2026-11-10. **Source:** the frontier review memo of 2026-11-10, and the Opus 5.5 and Muse verifier verdicts on PR #1292. **Filed by:** the records clerk (`claude-haiku-5-5`).

**Severity:** S3. **Pre-existing:** pre-existing (base and head agree).

**Measured:**

- The `IN (subquery)` result column is named `IN`; Spark names it `(p IN (listquery()))`.
- An out-of-bounds array index returns NULL; Spark raises `INVALID_ARRAY_INDEX`. This is the array-index domain.
- The DataFrame string door says "Dot access not supported" on `d['k'].l[0].inner`; the SQL door answers.
- A duplicate-named literal `IN (subquery)` refuses: `named_struct('a',1,'a',2) IN (SELECT named_struct('b',1,'c',2))`. Spark answers true.
- The `d['k'][0].x` comparison refuses on both sides.
- An `INSERT` into a `STRUCT<b INT NOT NULL, …>` column from `named_struct` refuses on both sides ("Unsupported CAST … non-null"). Spark's answer is not recorded.
- A `USING` join on a duplicate-named key refuses on both sides ("duplicate unqualified field name").

**Status:** filed 2026-11-10, not dispatched.

**Close:** fixed, or declined by a recorded ruling.
