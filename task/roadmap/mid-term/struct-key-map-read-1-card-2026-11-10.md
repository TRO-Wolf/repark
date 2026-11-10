# Card STRUCT-KEY-MAP-READ-1: reading a map with struct keys fails in Python

**Date:** 2026-11-10. **Source:** the frontier review memo of 2026-11-10, and the Opus 5.5 and Muse verifier verdicts on PR #1292. **Filed by:** the records clerk (`claude-haiku-5-5`).

**Severity:** unrated. **Pre-existing:** not stated in the records.

**Measured:**

- `map(A, 1, B, 2)` and `map_concat` with struct keys run on RePark, where Spark refuses. Reading the result fails with Python's `TypeError: unhashable type: 'dict'`.
- This is a reading of two cells, not a confirmed defect. A cell of its own comes first. The facade may build a Python dict with struct keys; that is not measured.

**Status:** filed 2026-11-10, not dispatched.

**Close:** fixed, or declined by a recorded ruling.
