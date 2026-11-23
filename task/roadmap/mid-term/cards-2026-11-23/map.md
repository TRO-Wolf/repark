# map — task/roadmap/mid-term/cards-2026-11-23/

Cards filed by the orchestrator on 2026-11-23, during the owner's overnight window, from the verifiers and builders
of PRs #1411 and #1413, and by the builder of SQL-EXPLODE-1. Each severity is proposed; the owner rates it.
of PRs #1411 and #1413 and the review of 2026-11-23. The owner rated each on 2026-11-23. None is dispatched.

## Cards

- [PARQUET-PARTITION-DISCOVERY-1](parquet-partition-discovery-1-card-2026-11-23.md) (S1) — **card:**
  `read.parquet` of a `key=value/` directory drops the partition columns, so a read-then-write copy loses them.
- [ARROW-ERROR-NUL-ABORT-1](arrow-error-nul-abort-1-card-2026-11-23.md) (S2) — **card:** a cast error over
  text holding a NUL byte aborts the Python process.
- [SQL-EXPLODE-1](sql-explode-1-card-2026-11-23.md) (S2 proposed) — **card:** `explode` / `explode_outer` in a SQL
  select list are unresolved; fixed on a branch with EXTRACT-FIELD-ALIASES-1.
- [SCALAR-SUBQUERY-EMPTY-NULL-1](scalar-subquery-empty-null-1-card-2026-11-23.md) (S1) — **card:** an empty scalar
  subquery over a NOT NULL column folds `IS NULL` to false, so a filter on it drops every row.

Up: [../map.md](../map.md).
