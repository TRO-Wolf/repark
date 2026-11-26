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

**FILE-EXTENSION-FILTER-1 (2026-11-25):** a Parquet path read lists files as Spark's `InMemoryFileIndex` does: every
suffix (main's `.parquet` filter lost `b`, `b.PARQUET`, `b.bin` silently), skipping a name that starts with `_` and
holds no `=`, starts with `.`, or ends in `._COPYING_`, on every segment below the root; a single-file path is judged
by its own name (`_b.parquet` read alone refuses, as on Spark); `_root/` as the root is read. `_metadata` and
`_common_metadata` are skipped (Spark lists but never scans them). `hidden_files.rs` is the rule's one home: the
discovery listing (`partition.rs`) and the fallback both use `spark_hidden_below`. The fallback
(`read_parquet_spark_listing`, for globs, single files and unpartitioned directories) scans through
`SparkListingTable`, which hands DataFusion's `ListingTable` a session whose stores drop hidden names from `list`
and answer `NotFound` to a `head` of a hidden file (DataFusion 54.1.0 filters only by suffix and glob,
`url.rs:296-302`); statistics, pruning, grouping, `inputFiles()` and execution stay DataFusion's own. The list-files
cache is off (`../session.rs`), so no cached listing bypasses the rule. Evidence: `task/wo/file-extension-filter-1/`.

**PARQUET-PARTITION-DISCOVERY-1 (2026-11-24; Delta 2):** `partition.rs` and `partition_infer.rs` add Spark-compatible
Hive partition discovery to the Parquet path-read seat (`ReparkSession::read_parquet` /
`read_parquet_with_case_sensitivity`) over both local paths and S3 prefixes (shared `ObjectStore` listing;
`plan_introspect.rs` recognizes `PartitionedParquetProvider` beside `ListingTable`). They reuse
`../partition_discovery.rs`'s `__HIVE_DEFAULT_PARTITION__` and `CONFLICTING_PARTITION_COLUMN_NAMES` formatter while
leaving text and ORC discovery untouched. Rules pinned to live Spark 4.1.2:
- Hidden path rule: `hidden_files::spark_hidden_below` (FILE-EXTENSION-FILTER-1) on every segment, so `_p=1/` is a
  partition directory, `_temporary/`, `.p=1/`, `_SUCCESS`, `.crc` and `_zero.parquet` are skipped, and every other
  file name is listed whatever its suffix.
- Footer order (Spark `ParquetUtils.inferSchema` reads one footer, the first of `allFiles()` by path string): the
  first visible file in byte order (root and base-directory files included when partitioned, root files only when
  not) refuses with `[FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER]` (`SQLSTATE: KD001`) when it holds 0 bytes; any
  other 0-byte file is skipped from the scan (`isNonEmptyFile`) but still names its partition and its value feeds type
  inference. The refusal comes after both conflict checks.
- When no visible file has a partition spec, the root files' footer rule runs and the read takes the Spark-listing
  fallback, which reads root files only.
- Partition spec extraction (`parse_relative_specs`) takes the trailing contiguous run of `key=value` directories
  above the file, scanning in reverse (`a=1/b=2/sub/` skips `sub/`; `a=1/sub/b=2/` -> `b` only). Leaf reads (`root/p=1`) and root names holding
  `=` (`tag=foo`) yield no partition columns.
- Base paths (Spark `PartitioningUtils.parsePartitions`): a file's directory has the base path where that walk stops
  (the non-`=` segment, or the root); a directory without `key=value` segments is its own base path. When any file is
  partitioned, every visible file's base path must match case-insensitively, or the read refuses with
  `[CONFLICTING_DIRECTORY_STRUCTURES]` (`KD009`) before `CONFLICTING_PARTITION_COLUMN_NAMES`; files in the common base
  directory (root files beside `k=v/`) are then left out, as Spark leaves them out.
- Both partition column names and partition values unescape `%XX` sequences per byte (`char::from(byte)` in
  `partition_infer::unescape_spark_path_name`, matching Spark's `ExternalCatalogUtils.unescapePathName`: `a%2Fb=1` ->
  column `a/b`, `p=%C3%A9` -> `Ã©`, while raw UTF-8 `p=é` stays `é`).
- Empty partition values (`p=`) and empty partition keys (`=1`) refuse. Mixed depths or conflicting partition column
  names refuse with `CONFLICTING_PARTITION_COLUMN_NAMES` (`KD009`); mixed-case names unify under `caseSensitive=false`
  and refuse under `caseSensitive=true`. `file://` directory URIs without a trailing slash normalize to a collection.
- Type inference (`partition_infer.rs`) follows Spark 4.1.2 order: `Int32` -> `Int64` -> `Decimal128(p, 0)` (Java
  integral `BigDecimal` with `scale <= 0` and precision up to 38, including zero-mantissa `0e5` -> `decimal(6,0)`,
  `0.0e5` -> `decimal(5,0)`, `0e37` -> `decimal(38,0)`, `0e38` -> `double`, `1e5`, `1.5e2`, `5.`, and integers outside
  `i64`) -> `Float64` (Java `Double.parseDouble`, including exact `NaN`/`+NaN`/`-NaN`/`Infinity`/`+Infinity`/`-Infinity`,
  Java hex floats `0x1.0p0` with IEEE-754 round-to-nearest-even, trailing `d`/`f`, and whitespace-padded numbers;
  decimal+float widens to `Utf8`) -> `Date32` -> `Timestamp(Microsecond, session_tz)` (`yyyy-MM-dd HH:mm:ss` and single
  fractional digit `.S`; mixed `Date32` + `Timestamp` widens to `Timestamp` with dates parsed at local midnight in the
  session zone; `.SS` and `T` stay `Utf8`) -> `Utf8`, with all-null `__HIVE_DEFAULT_PARTITION__` inferring
  `DataType::Null` (`void`) even when `spark.sql.sources.partitionColumnTypeInference.enabled=false`
  (`PartitionColumnTypeInferenceConfig`).
- Non-colliding partition columns append in directory order; when a partition column matches a data-file column
  (case-insensitively when `caseSensitive=false`, exact when `true`), the colliding file column is omitted from the
  Parquet scan schema and a logical `Projection` places the partition column at the original file-column position
  using the partition name, inferred type, and directory value.
- `PartitionedParquetProvider` wraps `FileScanConfigBuilder` + `FloatBoundsInexactFormat` + `TableSchema`, sets
  `PartitionedFile.partition_values`, supports `Inexact` filter pushdown, and prunes files in `partition_prune.rs`:
  it first models Spark's `ConstantPropagation` (a top-level `column = literal` or `<=>` conjunct binds that column,
  partition or data,
  the last binding wins, bindings reach `AND` children and the `AND`s inside `OR` but not `NOT`; bound columns are
  replaced inside comparisons other than the binding itself, with `!=` read as `NOT (=)`), then its `ConstantFolding`
  (a conjunct left constant is evaluated once: the leading run short-circuits on `false`, later ones always run and
  raise), then evaluates the remaining partition-only conjuncts sequentially per file (short-circuiting on
  `false`/`NULL`). Under
  `PartitionPruningAnsiConfig { enabled: true }` (mirrored from `SparkAnsiConfig` in `extension.rs`,
  `session_runtime.rs`, and `temp_settings.rs`), runtime evaluation errors propagate even on 0-row partitions; under
  `enabled: false` or when `create_physical_expr` cannot lower an expression, files are kept conservatively. Returns
  `EmptyExec` when all files prune
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

- `hidden_files.rs` — `spark_hidden_name` and `spark_hidden_below` (the name rule over each segment below the root),
  `SparkListingTable`, `SparkListingSession`, and the registry and store wrappers that apply the rule to a listing.
- `hidden_files_tests.rs` — the name table (Spark's writer markers, `_metadata`, `_k=1.parquet`, `._COPYING_`), the
  segment table (partition directories, hidden roots, single files), and `read_parquet` over a flat directory with
  every marker, partition directories (`_q=3/` beside `p=1/` conflicts, as on Spark), single files with any suffix and hidden names, a hidden root directory, a
  glob, and the wrapped listing's S3 overwrite check. The S3 append pins are `../session/tests/hidden_append.rs`.

- `int96_tests.rs` — a low-level `INT96` file (2025, year 2999 and 1960 instants, a NULL) beside an `INT64`
  not-adjusted leaf: `read_parquet` and `CREATE EXTERNAL TABLE … STORED AS PARQUET` read `Timestamp(Microsecond,
  "UTC")` with the exact microseconds, the `INT64` leaf stays zone-less; the config sets `us`/`UTC` and keeps
  an explicit setting.

- `partition.rs` — `PARTITION_COLUMN_TYPE_INFERENCE_KEY`, `PartitionColumnTypeInferenceConfig`,
  `PartitionPruningAnsiConfig`, `PartitionedParquetProvider`, `read_parquet_with_case_sensitivity`,
  `set_partition_column_type_inference`, `partition_column_names`. **CSV-JSON-PARTITION-DISCOVERY-1:**
  `read_discovered` over a `DiscoveryFormat`; footer, relax and all-suffix rules are Parquet's; a CSV
  column named like a partition goes to main; root files stay in `inputFiles` via a never-run `ListedOnlyExec`;
  `refuse_unreadable_root_files` byte-probes the ones Spark opens.
- `partition_layout_tests.rs` — CSV/JSON pins (CSV-JSON-PARTITION-DISCOVERY-1, unreadable roots).
  Delta 2 Rust pins: the base-path rule (two sub-bases, root beside `sub/`, a `sub/`
  file or 0-byte file, `a=1/sub/b=2` beside `a=2/sub/b=3`, directory conflict before names; root, hidden and
  base-directory files answer; same mid base and case-only base differences answer), footer order (root, `Z` before
  `a`, unpartitioned first refuse; later 0-byte files skip; a 0-byte leaf types `p` as string; a root file's schema
  with only 0-byte partitions; names before footer), and binding-first pruning (answers and refusals, two columns).
- `partition_prune.rs` — `prune_partitioned_files`: conjunct flattening, the `ConstantPropagation` and
  `ConstantFolding` model, and per-file evaluation under `PartitionPruningAnsiConfig`.
- `partition_infer.rs` — `unescape_spark_path_name`, Java integral `BigDecimal` / `Double` (incl. hex float and
  signed NaN) parsers, Spark 4.1.2 inference ladder (incl. `Date32`+`Timestamp` widening), and partition scalar
  materialization.
- `partition_tests.rs` — 11 Rust pins for 1-level and 2-level discovery, partition-only file pruning (`p=2` scans 1
  file) and `EmptyExec`, leaf read, all inferred types (`Decimal128(30,0)`, `0e5`/`0.0e5`/`0e37`/`0e38`, `1e5`,
  decimal+float -> `Utf8`, `NaN`/`+NaN`/`-NaN`/`-Infinity`/hex floats -> `Float64`, `nan`/`0x1.0` -> `Utf8`, `1.50`,
  `01`, `.5` vs `.12` timestamps, mixed `Date32`+`Timestamp` in UTC and NY, all-null `Null`, empty value and empty
  key refusals, `file://` directory URI without trailing slash), `partitionColumnTypeInference.enabled=false`,
  file/directory overlap, Spark hidden dir/file rules (`_p=1/`, `_hidden.parquet`), root-only unpartitioned nested
  filter, trailing-run specs (`a=1/sub/b=2/`, `a=1/b=2/sub/`), URL-escaped keys and per-byte `%XX` unescaping, 0-byte
  `.parquet` refusal, ANSI pruning error propagation and short-circuiting on 0-row partitions, mixed-case conflicts,
  and in-memory S3 + `plan_reads_s3_prefix`.

Up: [../map.md](../map.md).
