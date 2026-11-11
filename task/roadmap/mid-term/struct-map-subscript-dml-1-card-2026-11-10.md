# Card STRUCT-MAP-SUBSCRIPT-DML-1: a map subscript on a struct key fails in DML with an internal field name

**Date:** 2026-11-10. **Source:** the frontier review memo of 2026-11-10, and the Opus 5.5 and Muse verifier verdicts on PR #1292. **Filed by:** the records clerk (`claude-haiku-5-5`).

**Severity:** S3. **Pre-existing:** pre-existing.

**Measured:**

- 20 cells: 02491 02492 02510 02511 02529 02530 02548 02549 02567 02568 02586 02587 02605 02606 02624 02625 02643 02644 02662 02663. RePark refuses with `No field named __datafusion_extracted_1`. Spark's answer for these cells is not restated in the records read; the step-0 cells are the reference.

**Status:** filed 2026-11-10, not dispatched.

**Close:** fixed, or declined by a recorded ruling.
