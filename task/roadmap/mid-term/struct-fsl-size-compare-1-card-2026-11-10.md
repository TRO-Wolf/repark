# Card STRUCT-FSL-SIZE-COMPARE-1: a FixedSizeList of structs with different sizes is refused where Spark answers

**Date:** 2026-11-10. **Source:** the frontier review memo of 2026-11-10, and the Opus 5.5 and Muse verifier verdicts on PR #1292. **Filed by:** the records clerk (`claude-haiku-5-5`).

**Severity:** S3, over-refusal. **Pre-existing:** pre-existing.

**Measured:**

- Extension cells: P34-00386, 00388, 00390 (3); P31-00098, 00100, 00102, 00128, 00130, 00132 (6); P33-00314 to 00330, even numbers (9); P35-00422, 00424, 00426 (3).
- Seat C cells: P10-01577, 01579, 01581, 01583, 01585, 01587, 01637, 01639, 01641, 01643, 01645, 01647 (12).
- RePark refuses each pair of different sizes; Spark answers.

**Status:** filed 2026-11-10, not dispatched.

**Close:** fixed, or declined by a recorded ruling.
