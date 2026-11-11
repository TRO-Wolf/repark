# Card SET-COMPARISON-ANY-1: `= ANY (subquery)` is accepted where Spark has no such syntax

**Date:** 2026-11-10. **Source:** the frontier review memo of 2026-11-10, and the Opus 5.5 and Muse verifier verdicts on PR #1292. **Filed by:** the records clerk (`claude-haiku-5-5`).

**Severity:** S3. **Pre-existing:** pre-existing (main and 1.5.6 agree).

**Measured:**

- `lid … WHERE s = ANY (SELECT s FROM rk)` answers by name on the head build. Spark gives `PARSE_SYNTAX_ERROR`. The same class of door fault as SPARK-DOOR-PREPARE-1.

**Status:** filed 2026-11-10, not dispatched.

**Close:** fixed, or declined by a recorded ruling.
