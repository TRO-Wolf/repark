# Card STRUCT-MAP-FIELD-COMPARE-1: a struct of map fields compares where Spark refuses

**Date:** 2026-11-10. **Source:** the frontier review memo of 2026-11-10, and the Opus 5.5 and Muse verifier verdicts on PR #1292. **Filed by:** the records clerk (`claude-haiku-5-5`).

**Severity:** S3. **Pre-existing:** pre-existing; main accepts it too.

**Measured:**

- `named_struct('k', c) = named_struct('k', d)` over maps answers `[[0], [1]]`. Spark refuses.

**Status:** filed 2026-11-10, not dispatched.

**Close:** fixed, or declined by a recorded ruling.
