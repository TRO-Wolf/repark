# Card STRUCT-MAP-SUBSCRIPT-JOIN-1: a map subscript on a struct key in a DataFrame join is ambiguous

**Date:** 2026-11-10. **Source:** the frontier review memo of 2026-11-10, and the Opus 5.5 and Muse verifier verdicts on PR #1292. **Filed by:** the records clerk (`claude-haiku-5-5`).

**Severity:** S3. **Pre-existing:** pre-existing; 1.5.6 refuses too.

**Measured:**

- 10 `df_typed` cells: 02498 02517 02536 02555 02574 02593 02612 02631 02650 02669. RePark refuses with `AMBIGUOUS_REFERENCE`. Spark's answer for these cells is not restated in the records read; the step-0 cells are the reference.

**Status:** filed 2026-11-10, not dispatched.

**Close:** fixed, or declined by a recorded ruling.
