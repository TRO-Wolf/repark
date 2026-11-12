# Card SET-COMPARISON-ANY-1: `= ANY (subquery)` is accepted where Spark has no such syntax

**Date:** 2026-11-10. **Source:** the frontier review memo of 2026-11-10, and the Opus 5.5 and Muse verifier verdicts on PR #1292. **Filed by:** the records clerk (`claude-haiku-5-5`).

**Severity:** S3. **Pre-existing:** pre-existing (main and 1.5.6 agree).

**Measured:**

- `lid … WHERE s = ANY (SELECT s FROM rk)` answers by name on the head build. Spark gives `PARSE_SYNTAX_ERROR`. The same class of door fault as SPARK-DOOR-PREPARE-1.

**Appended 2026-11-12 (records clerk, `claude-opus-5-5`, owner ruling Q8 of 2026-11-12):** SIGNED-ZERO-NORMALIZE-1
part B evidence (`task/wo/signed-zero-normalize-1/evidence/part-b/`, filed with that unit) measured the same finding
as its `ANY-SUBQUERY-ACCEPTED-1`, which is not filed separately; this card covers it.

- Cell SQ-any (seeds `up` and `un`; DOUBLE, FLOAT and the decimal twin), step 0 on main `ae247767`, re-measured by
  item 1 on main `276dc503` with no label changed: `SELECT id FROM t WHERE d = ANY (SELECT d FROM up)` answers on
  the Spark door and on native (`[1, 3, 5]` on the DOUBLE `up` seed). Spark 4.1.2 refuses with
  `[PARSE_SYNTAX_ERROR] Syntax error at or near '('` for every type: ACCEPTED-WHERE-SPARK-REFUSED, 12 cells.
- Part B's drafter and Fable rated it S4; the card's filed S3 stands until a ruling changes it. It does not block the
  scoped label.

**Status:** filed 2026-11-10, not dispatched.

**Close:** fixed, or declined by a recorded ruling.
