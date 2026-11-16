# Card VALUES-INLINE-REFUSALS-1: two `VALUES` cells answer where Spark refuses, a string beside a double and a `NULLIF` of decimals

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from the re-check verdict of DECIMAL-DOUBLE-UNION-COERCION-1 part 2 (PR #1341, merged to main as `cb7d3943`; the verdict names the delta head `12101e9b`), held outside the repository, "Findings" c1 and c3. Both are pre-existing notes in that verdict: class c, each reproduced in both ANSI modes.

**Status:** filed 2026-11-16, not dispatched. **Severity: S3.**

## Measured

- **c1.** `VALUES ('0.1'), (double)`: Spark refuses with `INCOMPATIBLE_TYPES_IN_INLINE_TABLE`. Both repark builds answer `double`. The verdict calls it a pre-existing string and double `VALUES` gap.
- **c3.** A `NULLIF(decimal, decimal)` cell beside a double: Spark refuses with `CANNOT_EVALUATE_EXPRESSION_IN_INLINE_TABLE`. The base answered `decimal(30,15)`; the #1341 head answers `double`, which follows the unit's widening. The head still answers where Spark refuses.

The same verdict's c2 is an append to [DECIMAL-DECIMAL-UNION-WIDEN-1](decimal-decimal-union-widen-1-card-2026-11-16.md), and its c4 is already on [SHOW-FUNCTIONS-1](show-functions-1-card-2026-10-26.md).

Up: [map.md](map.md).
