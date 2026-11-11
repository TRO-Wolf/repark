# Card MAP-ORDERING-FORMS-1: map operands in BETWEEN, nullif and CASE are accepted or refused without Spark's class

**Date:** 2026-11-11. **Filed by:** the records clerk (`claude-haiku-5-5`), from the Opus 5.5 round 1 and re-check, and the Muse MAX round 1 and re-check, on PR #1295.

**Status:** filed 2026-11-11, not dispatched. Severity **S3**. Pre-existing: the forms were accepted or refused the same way on base.

**Measured:**

- Map operands in `BETWEEN`, `NOT BETWEEN`, `nullif` and single-`WHEN` simple `CASE` are accepted where Spark refuses: Q1198 to Q1237 and X0090.
- Simple `CASE` over a plain `MAP<STRING, INT>` operand refuses without a class: B01 to B09, B12 to B14. Spark gives `INVALID_ORDERING_TYPE` or `BINARY_OP_DIFF_TYPES`.
- Simple `CASE` class gaps from the re-check: C01 (Spark `INVALID_ORDERING_TYPE` on `(m = NULL)`, head `BINARY_OP_DIFF_TYPES`) and C06 (Spark `INVALID_ORDERING_TYPE` on `(m = m)`, head classless). Muse V09 to V11: a scalar `WHEN` or a clash-first `WHEN` over a map operand refuses without Spark's class.
- C07, a single `WHEN` over a map of reordered structs, is accepted where Spark refuses.

**Close:** each form refuses with Spark's class, or accepts as Spark does, with a pin per form.
