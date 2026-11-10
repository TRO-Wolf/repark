# Card SPARK-DOOR-PREPARE-1: the Spark door accepts PREPARE and EXECUTE, and cell 04376 names the wrong refusal class

**Date:** 2026-11-10. **Source:** the frontier review memo of 2026-11-10, and the Opus 5.5 and Muse verifier verdicts on PR #1292. **Filed by:** the records clerk (`claude-haiku-5-5`).

**Severity:** S3. **Pre-existing:** pre-existing (base and head agree).

**Measured:**

- Part 1: cell 04376's refusal class differs from Spark's (class only).
- Part 2: `PREPARE p1 AS SELECT 1` answers on the Spark door; Spark gives `PARSE_SYNTAX_ERROR` (SQLSTATE 42601). EXECUTE pairs struct fields by position on the head build, so no wrong answer comes from it.
- Whether PREPARE is a kept extension or a refusal is the owner's to rule.

**Status:** filed 2026-11-10, not dispatched.

**Close:** fixed, or declined by a recorded ruling.
