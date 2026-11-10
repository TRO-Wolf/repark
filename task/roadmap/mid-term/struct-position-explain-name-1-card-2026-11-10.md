# Card STRUCT-POSITION-EXPLAIN-NAME-1: EXPLAIN shows the internal struct-by-position function

**Date:** 2026-11-10. **Source:** the frontier review memo of 2026-11-10, and the Opus 5.5 and Muse verifier verdicts on PR #1292. **Filed by:** the records clerk (`claude-haiku-5-5`).

**Severity:** S3. **Pre-existing:** no; introduced by PR #1292 (the Opus 5.5 verdict's F-3, class (a)).

**Measured:**

- `EXPLAIN SELECT * FROM vcmp3` over `l.s = r.s AS e` prints the physical plan `s@1 = __repark_struct_by_position__(s@3)`. Spark's EXPLAIN text differs from RePark's anyway; the internal name is what this card removes.

**Status:** filed 2026-11-10, not dispatched.

**Close:** fixed, or declined by a recorded ruling.
