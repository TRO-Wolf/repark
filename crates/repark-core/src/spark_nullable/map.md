# map — repark-core/src/spark_nullable

## Purpose

The test cohort of `../spark_nullable.rs`'s float statistics seat (NAN-MINMAX-1 part 1, 2026-11-13). Parquet's
footer min and max leave NaN out, and DataFusion takes them as exact, so two RePark-only consumers lost NaN rows: the
opener's constant-column replacement (a file whose other float values are one value read every NaN as that value)
and the `max`/`min` statistics shortcut. `FloatBoundsInexactFormat` wraps DataFusion's Parquet format and demotes to
`Precision::Inexact`:

- **per file** (`infer_stats`, `infer_stats_and_ordering`): a FLOAT or DOUBLE column whose bounds are equal, the
  constant replacement's only trigger, or with a row group that holds a non-null value but no footer min or max (all
  NaN, or NULL and NaN). DataFusion merges the other row groups' bounds into an exact file bound
  (`datafusion-datasource-parquet-54.1.0/src/metadata.rs:609-641`, `:748`), and the file pruner then dropped the
  whole file. `unbounded_float_columns` reads the footer from DataFusion's file-metadata cache; a footer it cannot
  read demotes every float bound of the file;
- **per scan** (`create_physical_plan`): every float bound in the table and file-group statistics, which the
  shortcut reads.

A spread file whose row groups all carry bounds keeps them exact, so DataFusion's file pruner (exact bounds only)
still prunes it, as row-group pruning by the same footers would. Those bounds also feed DataFusion's aggregate dynamic
filter, which can skip a NaN file under a multi-file `max` (NAN-MINMAX-1 part 2, I8) once a session turns it on; RePark sessions keep it off by default (`../session/df_guards.rs`). Row counts, null counts and
every non-float statistic are untouched. `read_parquet_nullable` builds its listing with the wrapper;
`FloatBoundsInexactFactory` replaces the session's `parquet` file-format factory (registered in
`../session/df_guards.rs`), which the native `CREATE EXTERNAL TABLE … STORED AS PARQUET` uses. Evidence:
`task/wo/nan-minmax-1/`.

**CAST-STATS-INTERVAL-1 (2026-11-22):** the same seats drop a 64-bit bound that sits at its type's extreme.
DataFusion 54.1.0 counts an interval's values as `distance + 1` in `u64` (`interval_arithmetic.rs` `cardinality`), so
a BIGINT column whose statistics span `[i64::MIN, i64::MAX]` counts zero values and every filter above it failed with
`Selectivity is out of limit: inf` (or `NaN`). Dropping the extreme bound loses nothing: an absent bound of a 64-bit
type already means that extreme. Dropped per file, per file group and on the scan (also when a filter is pushed into
it), a full range cannot form when DataFusion merges files whose own ranges are not full
([card](../../../../task/roadmap/mid-term/cast-stats-interval-1-card-2026-11-22.md)).

**PARQUET-INT96-AS-NTZ-1 (2026-11-22):** `with_int96_as_instants` sets DataFusion's `execution.parquet.coerce_int96`
to `us` and `coerce_int96_tz` to `UTC` on every RePark session config (`../session/df_guards.rs`
`context_with_df_54_1_rule_guards`, after the explicit `datafusion.*` keys, which it keeps), so an `INT96` leaf without an embedded Arrow schema (every Spark-written file)
top-level or nested, infers and decodes as `Timestamp(Microsecond, "UTC")`, Spark's `timestamp` shown in the
session zone, on every plain Parquet route; it was `Timestamp(Nanosecond, None)`, read as `timestamp_ntz` holding
the UTC wall time, and wrapped past 2262. `INT64` leaves keep their footer's `isAdjustedToUTC`. Arrow's statistics
converter reads no `INT96` bounds, so the leaf is never pruned, as before. SQL `RESET` of the key returns DataFusion's
unset value for the session (`conf.unset` leaves the live value)
([card](../../../../task/roadmap/mid-term/parquet-int96-as-ntz-1-card-2026-11-22.md)).

**PARQUET-PARTITION-DISCOVERY-1 (2026-11-24):** `partition.rs` adds Spark-compatible Hive partition discovery to the
Parquet path-read seat (`ReparkSession::read_parquet` / `read_parquet_with_case_sensitivity`) over both local paths
and S3 prefixes (shared `ObjectStore` listing; `plan_introspect.rs` recognizes `PartitionedParquetProvider` beside
`ListingTable`). It reuses `../partition_discovery.rs`'s `__HIVE_DEFAULT_PARTITION__`, URL unescaping, and
`CONFLICTING_PARTITION_COLUMN_NAMES` formatter while leaving text and ORC discovery untouched. Rules pinned to live
Spark 4.1.2:
- Hidden path segments (leading `.` or `_` in any relative directory or filename, e.g. `_SUCCESS`, `_temporary/`,
  `.crc`, `.hidden`, `p=1/.dot`, `p=1/_under`) and 0-byte files are skipped before discovery.
- Root-level files alongside `k=v` subdirectories are ignored (`keep_partitioned_only`). Leaf reads (`root/p=1`) and
  root directory names holding `=` have no relative `k=v` segments, so they yield no partition columns.
- Empty partition values (`p=`) and empty partition keys (`=1`) refuse. Mixed depths or conflicting partition column
  names refuse with `CONFLICTING_PARTITION_COLUMN_NAMES` (`KD009`); mixed-case names unify under `caseSensitive=false`
  and refuse under `caseSensitive=true`. `file://` directory URIs without a trailing slash normalize to a collection.
- Type inference follows Spark 4.1.2 order: `Int32` -> `Int64` -> `Decimal128(p, 0)` (Java integral `BigDecimal` with
  `scale <= 0` and precision up to 38, including `1e5`, `1.5e2`, `5.`, and integers outside `i64`) -> `Float64`
  (Java `Double.parseDouble`, including case-sensitive `NaN`/`Infinity`/`-Infinity`/`+Infinity`, trailing `d`/`f`,
  and whitespace-padded numbers; decimal+float widens to `Utf8`) -> `Date32` -> `Timestamp(Microsecond, session_tz)`
  (`yyyy-MM-dd HH:mm:ss` and single fractional digit `.S`; `.SS` and `T` stay `Utf8`) -> `Utf8`, with all-null
  `__HIVE_DEFAULT_PARTITION__` inferring `DataType::Null` (`void`) even when
  `spark.sql.sources.partitionColumnTypeInference.enabled=false` (`PartitionColumnTypeInferenceConfig`).
- Non-colliding partition columns append in directory order; when a partition column matches a data-file column
  (case-insensitively when `caseSensitive=false`, exact when `true`), the colliding file column is omitted from the
  Parquet scan schema and a logical `Projection` places the partition column at the original file-column position
  using the partition name, inferred type, and directory value.
- `PartitionedParquetProvider` wraps `FileScanConfigBuilder` + `FloatBoundsInexactFormat` + `TableSchema`, sets
  `PartitionedFile.partition_values`, supports `Inexact` filter pushdown, and prunes files soundly on partition-only
  filters (conservatively keeping a file when filter evaluation errors, and returning `EmptyExec` when all files prune).
([card](../../../../task/roadmap/mid-term/cards-2026-11-23/parquet-partition-discovery-1-card-2026-11-23.md)).

## Files

- `extreme_bounds.rs` — `without_type_extreme_bounds`, `drop_type_extreme_bounds`, `has_type_extreme_bound`: a min at
  the type's minimum or a max at its maximum becomes `Precision::Absent` for BIGINT, UNSIGNED BIGINT, `Date64` and
  every TIMESTAMP unit; every other bound, the row and null counts are kept.
- `extreme_bounds_tests.rs` — the extreme table (a full range, a range one inside it, a full INT range kept, a
  TIMESTAMP and an UNSIGNED bound), and `x > 0`, `x = 5`, a filter on another column, `max(x)` and `min(x)` over one
  file, row groups, and two files `[MIN, -1]` and `[0, 5, MAX]` whose merged range is full.

- `float_bounds_tests.rs` — the seat's pins: a constant file's float bounds turn inexact while every other statistic
  (row count, byte size, null counts, the integer and string columns) is unchanged; a spread file's bounds stay exact;
  a row group of NaN, or of NULL and NaN, turns the file's float bounds inexact while an all-NULL row group does not,
  and `read_parquet` keeps those NaN rows under `> 5.0` and `= NaN`; `read_parquet` and the external table read
  every NaN back, `max` answers NaN and `min` the smallest number, on a
  constant and a spread file, one and two row groups. Removing the per-file demotion, the per-scan demotion or the
  factory registration, or the row-group check, each turns a pin red. The aggregate dynamic filter (NAN-MINMAX-1 N1): three files `[3,4]`, `[NULL,NULL | 1,2]` and `[5,6]` answer `max(d), min(d)` 6.0, 1.0 and `min(d), max(id)` 1.0, 8 in 10 of 10 runs (red with the default removed: `min` 3.0); the umbrella key at `true` is refused on four `SET` forms and at build, `false` is accepted, and the specific key turns the filter on at runtime and at build.

- `int96_tests.rs` — a low-level `INT96` file (2025, year 2999 and 1960 instants, a NULL) beside an `INT64`
  not-adjusted leaf: `read_parquet` and `CREATE EXTERNAL TABLE … STORED AS PARQUET` read `Timestamp(Microsecond,
  "UTC")` with the exact microseconds, the `INT64` leaf stays zone-less; the config sets `us`/`UTC` and keeps
  an explicit setting.

- `partition.rs` — `PARTITION_COLUMN_TYPE_INFERENCE_KEY`, `PartitionColumnTypeInferenceConfig`,
  `PartitionedParquetProvider`, `read_parquet_with_case_sensitivity`, and `set_partition_column_type_inference`.
- `partition_tests.rs` — Rust pins for 1-level and 2-level discovery, partition-only file pruning (`p=2` scans 1 file)
  and `EmptyExec`, leaf read, all inferred types (`Decimal128(30,0)`, `1e5` -> `Decimal128(6,0)`, decimal+float ->
  `Utf8`, `NaN`/`-Infinity` -> `Float64`, `nan` -> `Utf8`, `1.50`, `01`, `.5` vs `.12` timestamps in NY, all-null
  `Null`, empty value and empty key refusals, `file://` directory URI without trailing slash),
  `partitionColumnTypeInference.enabled=false`, file/directory overlap (both `caseSensitive` modes and
  all-columns-shadowed), hidden-file skipping, mixed-case directory names, and in-memory S3 + `plan_reads_s3_prefix`.

Up: [../map.md](../map.md).
