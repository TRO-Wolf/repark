# Card USING-SCALAR-STRUCT-KEY-CLASS-1: an INT key against a struct key in USING refuses without a class, in both orders

**Date:** 2026-11-11. **Filed by:** the records clerk (`claude-haiku-5-5`), from the verdicts of PR #1295 (Opus 5.5 round 1 and re-check, Muse MAX round 1 and re-check).

**Status:** filed 2026-11-11, not dispatched. Severity **S3**, class only: both sides refuse. Pre-existing: base equals head, and the unit did not widen it.

**Measured:**

- An `INT` key against a struct key in `USING` refuses with a classless "Cannot infer common argument type" in both orders: DK20 (the struct on the left, as the family pin names it), J37 (Muse), W05 (the sides-swapped mirror, Muse re-check).
- Spark answers `BINARY_OP_DIFF_TYPES`.
- K06 (Opus) is the same shape and is classless on base, PR and head.

**Close:** the cells refuse with Spark's `BINARY_OP_DIFF_TYPES` class, in both orders, with a pin.
