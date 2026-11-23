# map — task/roadmap/mid-term/cards-2026-11-23/

Cards filed by the orchestrator on 2026-11-23, during the owner's overnight window, from the verifiers and builders
of PRs #1411 and #1413 and the review of 2026-11-23. The owner rated each on 2026-11-23. None is dispatched.

## Cards

- [PARQUET-PARTITION-DISCOVERY-1](parquet-partition-discovery-1-card-2026-11-23.md) (S1) — **card:**
  `read.parquet` of a `key=value/` directory drops the partition columns, so a read-then-write copy loses them.
- [ARROW-ERROR-NUL-ABORT-1](arrow-error-nul-abort-1-card-2026-11-23.md) (S2) — **card:** a cast error over
  text holding a NUL byte aborts the Python process.
- [VALUES-EXPLICIT-CAST-1](values-explicit-cast-1-card-2026-11-23.md) (S2 proposed) — **card:** explicit user `CAST`
  inside SQL `VALUES` and `INSERT ... VALUES` refuses with Arrow's error for both string-to-integer and narrowing numeric
  casts where Spark returns rows (ANSI off) or raises `INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION` (ANSI on).
- [SCALAR-SUBQUERY-EMPTY-NULL-1](scalar-subquery-empty-null-1-card-2026-11-23.md) (S1) — **card:** an empty scalar
  subquery over a NOT NULL column folds `IS NULL` to false, so a filter on it drops every row.

Up: [../map.md](../map.md).
