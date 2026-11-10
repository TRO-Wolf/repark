# Card MAP-IN-SUBQUERY-ORDERING-1: `c IN (SELECT m ...)` over a map answers where Spark refuses

**Date:** 2026-11-10. **Source:** the frontier review memo of 2026-11-10, and the Opus 5.5 and Muse verifier verdicts on PR #1292. **Filed by:** the records clerk (`claude-haiku-5-5`).

**Severity:** S3. **Pre-existing:** pre-existing; main accepts it too.

**Measured:**

- `c IN (SELECT m …)` over a map answers `[[0]]`. Spark refuses.

**Status:** filed 2026-11-10, not dispatched.

**Close:** fixed, or declined by a recorded ruling.
