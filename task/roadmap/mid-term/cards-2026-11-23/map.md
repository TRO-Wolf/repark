# map — task/roadmap/mid-term/cards-2026-11-23/

Cards filed by the orchestrator on 2026-11-23, during the owner's overnight window, from the verifiers and builders
of PRs #1411 and #1413 and the review of 2026-11-23. The owner rated each on 2026-11-23. None is dispatched.

## Cards

- [PARQUET-PARTITION-DISCOVERY-1](parquet-partition-discovery-1-card-2026-11-23.md) (S1) — **card:**
  `read.parquet` of a `key=value/` directory drops the partition columns, so a read-then-write copy loses them.
- [ARROW-ERROR-NUL-ABORT-1](arrow-error-nul-abort-1-card-2026-11-23.md) (S2) — **card:** a cast error over
  text holding a NUL byte aborts the Python process.
- [SETOP-MIXED-TYPE-COERCION-1](setop-mixed-type-coercion-1-card-2026-11-23.md) (S3 proposed) — **card:** mixed-type set operations keep left schemas and refuse lossy multiset round-trips (`STRING` vs numeric/`DATE`/`TIMESTAMP`, lossy int/float, nested containers).
- [SCALAR-SUBQUERY-EMPTY-NULL-1](scalar-subquery-empty-null-1-card-2026-11-23.md) (S1) — **built (2026-11-23):** an empty scalar
  subquery over a NOT NULL column now stays nullable on both doors and all expression/stored-plan seats (`ReparkScalarSubqueryNullability` + `StripScalarNullable`), keeping folds on provably-1-row subqueries.

Up: [../map.md](../map.md).
