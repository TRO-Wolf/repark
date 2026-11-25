# map — task/roadmap/mid-term/cards-2026-11-23/

Cards filed by the orchestrator from 2026-11-23 to 2026-11-25, from the verifiers and builders of PRs #1411, #1413,
#1429, #1430 and #1431, the builder of SQL-EXPLODE-1, and the outside reviews of 2026-11-23 and 2026-11-24. A severity marked proposed
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
- [TRY-CAST-NONNULL-FIELD-1](try-cast-nonnull-field-1-card-2026-11-24.md) (S2) — **merged as #1429 (`14d2404a`, 2026-11-24):**
  shields `TryCast` consumers in physical planning (`__repark_nullable_field__`), post-`ProjectionPushdown`/`FilterPushdown`
  (`TryCastSafeSanityCheck`, incl. `DataSourceExec` `ParquetSource` pushed predicates under `pushdown_filters = true`),
  and cross-ANSI temp views while keeping pruning and `EXPLAIN` byte-identical on non-`TRY_CAST` queries; 9 separate
  `array_remove`/`make_dt_interval` cells remain on the card's still-unlike-Spark list; evidence in
  [task/wo/try-cast-nonnull-field-1/evidence/](../../../wo/try-cast-nonnull-field-1/evidence/).
- [CAST-SHIELD-INT-DECIMAL-OVERFLOW-1](cast-shield-int-decimal-overflow-1-card-2026-11-24.md) (S2) — **card:** an
  integer-to-decimal overflow in a filter is silently false under ANSI.
- [INSERT-OVERWRITE-CONST-CAST-1](insert-overwrite-const-cast-1-card-2026-11-24.md) (S2) — **card:** an empty
  `INSERT OVERWRITE` with an invalid constant cast wipes the table where Spark refuses. Narrow fix built 2026-11-25
  (replaces parked #1431); the card lists what stays unlike Spark.
- [SPARK-DOOR-OVER-ACCEPT-SYNTAX-1](spark-door-over-accept-syntax-1-card-2026-11-24.md) (S3) — **card:** `1e2foo`,
  `named_struct(…).a` with a bad sibling, `* EXCLUDE` and `DELETE` without `FROM` answer where Spark refuses.
  **Fixed on branch** for four seats (2026-11-24, PR #1434); the struct seat stays as on main (delta 1).
- [CSV-JSON-PARTITION-DISCOVERY-1](csv-json-partition-discovery-1-card-2026-11-24.md) (S1) — **card:** CSV and JSON
  reads of a `key=value/` directory drop the partition columns.
- [MAKE-DT-INTERVAL-OVERFLOW-1](make-dt-interval-overflow-1-card-2026-11-25.md) (S1) — **card:** `make_dt_interval`
  answers NULL on overflow, where Spark raises or answers, and types its result as a string.
- [FILE-EXTENSION-FILTER-1](file-extension-filter-1-card-2026-11-25.md) (S1) — **built** on
  `fix/file-extension-filter-1`, not merged: Parquet path reads list every name Spark lists and skip Spark's hidden
  names; delta 1: append validation skips the same names; integrated with #1430's discovery listing.
- [CAST-STRING-GRAMMAR-1](cast-string-grammar-1-card-2026-11-25.md) (S2 proposed) — **card:** string casts to `DATE`,
  `TIMESTAMP_NTZ`, `DOUBLE` and `DECIMAL` refuse text Spark accepts.
- [TRY-CAST-PUSHED-PROJECTION-1](try-cast-pushed-projection-1-card-2026-11-25.md) (S2 proposed) — **card:** a shared
  `TRY_CAST` pushed into the Parquet scan's projection crashes.
- [ARRAY-REMOVE-NULLABILITY-1](array-remove-nullability-1-card-2026-11-25.md) (S2 proposed) — **card:** `array_remove`
  with a nullable element declares a non-null result and crashes.
- [SQL-PATH-PARQUET-DOORS-1](sql-path-parquet-doors-1-card-2026-11-25.md) (S2 proposed) — **card:** SQL reads of a
  Parquet path refuse where Spark reads them.
- [FILTER-PROJECTION-SWAP-RAISE-1](filter-projection-swap-raise-1-card-2026-11-25.md) (S2 proposed) — **card:** under
  ANSI, a Parquet read raises an error from a row its `WHERE` removes.
- [PARTITIONBY-NULL-WRITE-1](partitionby-null-write-1-card-2026-11-25.md) (S1, owner 2026-11-25) — **card:** a
  partitioned Parquet, CSV or JSON path write stored a NULL partition as the type's default (`p=0`); fixed on its
  branch, PR open; #1430 is parked on it.

Up: [../map.md](../map.md).
