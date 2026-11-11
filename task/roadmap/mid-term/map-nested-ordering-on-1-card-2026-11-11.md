# Card MAP-NESTED-ORDERING-ON-1: a nested map inside an ON or literal = operand is accepted where Spark refuses

**Date:** 2026-11-11. **Filed by:** the records clerk (`claude-haiku-5-5`), from Muse MAX round 1 finding F5 and its re-check item V14 on PR #1295.

**Status:** filed 2026-11-11, not dispatched. Severity **S3**. Class (c) for the `ON` door: the answer is accepted, so no wrong write is measured. Pre-existing: it is on #1292's `=` arm and base equals head.

**Measured:**

- `ON l.s = r.s` over `STRUCT<a, ARRAY<MAP>>` keys answers `[[1, 10]]` on head; Spark refuses with `INVALID_ORDERING_TYPE` on `(s = s)`, SQLSTATE 42K09 (V14).
- Literal `=` on a nested map inside a struct: cells J33c and J33e, accepted on base and head, refused by Spark.
- The `ON` twin J22b is identical on base and head.

**Close:** the `ON` and literal `=` forms refuse with Spark's class on nested maps, with a pin.
