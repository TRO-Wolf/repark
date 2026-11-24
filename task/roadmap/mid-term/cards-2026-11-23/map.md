# map — task/roadmap/mid-term/cards-2026-11-23/

Cards filed by the orchestrator on 2026-11-23 and 2026-11-24, from the verifiers and builders of PRs #1411 and
#1413, the builder of SQL-EXPLODE-1, and the outside reviews of 2026-11-23 and 2026-11-24. A severity marked proposed
awaits the owner's rating. Each card row records its own status.

## Cards

- [PARQUET-PARTITION-DISCOVERY-1](parquet-partition-discovery-1-card-2026-11-23.md) (S1) — **fixed (2026-11-24):**
  `read.parquet` and `format("parquet").load` now discover Hive partition columns with Spark 4.1.2 type inference,
  `partitionColumnTypeInference.enabled`, overlap/conflict/case rules, hidden-file skipping, and partition-only file
  pruning (residual options/globs/CSV/JSON/S3 list on the card); evidence in
  [task/wo/parquet-partition-discovery-1/evidence/](../../../wo/parquet-partition-discovery-1/evidence/).
- [ARROW-ERROR-NUL-ABORT-1](arrow-error-nul-abort-1-card-2026-11-23.md) (S2) — **card:** a cast error over
  text holding a NUL byte aborts the Python process.
- [SQL-EXPLODE-1](sql-explode-1-card-2026-11-23.md) (S2 proposed) — **card:** `explode` / `explode_outer` in a SQL
  select list are unresolved; fixed on a branch with EXTRACT-FIELD-ALIASES-1.
- [VALUES-EXPLICIT-CAST-1](values-explicit-cast-1-card-2026-11-23.md) (S2 proposed) — **card:** explicit user `CAST`
  inside SQL `VALUES` and `INSERT ... VALUES` refuses with Arrow's error for both string-to-integer and narrowing numeric
  casts where Spark returns rows (ANSI off) or raises `INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION` (ANSI on).
- [SETOP-MIXED-TYPE-COERCION-1](setop-mixed-type-coercion-1-card-2026-11-23.md) (S3 proposed) — **card:** mixed-type set operations keep left schemas and refuse lossy multiset round-trips (`STRING` vs numeric/`DATE`/`TIMESTAMP`, lossy int/float, nested containers).
- [SCALAR-SUBQUERY-EMPTY-NULL-1](scalar-subquery-empty-null-1-card-2026-11-23.md) (S1) — **built (2026-11-23):** an empty scalar
  subquery over a NOT NULL column now stays nullable on both doors and all expression/stored-plan seats (`ReparkScalarSubqueryNullability` + `StripScalarNullable`), keeping folds on provably-1-row subqueries.
- [FILTER-STRING-ESCAPED-QUOTE-1](filter-string-escaped-quote-1-card-2026-11-24.md) (S1) — **fixed (2026-11-24):**
  aligned the inventoried Python and Rust SQL string/raw/backtick/comment scanners with Spark 4.1.2 (residual
  still-unlike-Spark list on the card); evidence in
  [task/wo/filter-string-escaped-quote-1/evidence/](../../../wo/filter-string-escaped-quote-1/evidence/).
- [TRY-CAST-NONNULL-FIELD-1](try-cast-nonnull-field-1-card-2026-11-24.md) (S2) — **card:** 87 cells crash where a
  function consumes `TRY_CAST` of a NOT NULL column.
- [CAST-SHIELD-INT-DECIMAL-OVERFLOW-1](cast-shield-int-decimal-overflow-1-card-2026-11-24.md) (S2) — **card:** an
  integer-to-decimal overflow in a filter is silently false under ANSI.
- [INSERT-OVERWRITE-CONST-CAST-1](insert-overwrite-const-cast-1-card-2026-11-24.md) (S2) — **card:** an empty
  `INSERT OVERWRITE` with an invalid constant cast wipes the table where Spark refuses.
- [SPARK-DOOR-OVER-ACCEPT-SYNTAX-1](spark-door-over-accept-syntax-1-card-2026-11-24.md) (S3) — **card:** `1e2foo`,
  `named_struct(…).a` with a bad sibling, `* EXCLUDE` and `DELETE` without `FROM` answer where Spark refuses.

Up: [../map.md](../map.md).
