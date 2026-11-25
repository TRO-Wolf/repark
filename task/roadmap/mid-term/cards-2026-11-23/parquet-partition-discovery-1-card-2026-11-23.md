# Card PARQUET-PARTITION-DISCOVERY-1: `read.parquet` of a partitioned directory drops the partition columns

**Date:** 2026-11-23. **Filed by:** the orchestrator (`claude-opus-5-5`), from both verifiers of PR #1411 (each saw
the column missing on main and on the branch) and a re-measurement here. **Severity:** S1 (owner, 2026-11-23):
a read-then-write copy stores the table without its partition columns, silently. **Status:** fixed on
`fix/parquet-partition-discovery-1` (2026-11-24; Delta 2). Evidence:
[task/wo/parquet-partition-discovery-1/evidence/summary.md](../../../wo/parquet-partition-discovery-1/evidence/summary.md)
and [domain-table.md](../../../wo/parquet-partition-discovery-1/evidence/domain-table.md).

## Measured (base `514281bd4`)

A Spark 4.1.2 `partitionBy("p")` write of `(id BIGINT, p INT, s STRING)`, four rows over `p=1/` and `p=2/`, local
paths, session zone `America/New_York`; RePark main `5caea2a5` / `514281bd4` (the 1.5.7 product tree).

| Read | Spark 4.1.2 | RePark main | RePark head |
|---|---|---|---|
| `read.parquet(root)` schema | `struct<id:bigint,s:string,p:int>` | `struct<id:bigint,s:string>` | `struct<id:bigint,s:string,p:int>` |
| `read.parquet(root).collect()` | four rows with `p` | four rows without `p` | four rows with `p` |
| `read.parquet(root).where("p = 2")` | two `p = 2` rows (1 file read) | `AnalysisException: Schema error: No field named p` | two `p = 2` rows (1 file scanned) |
| `read.format("parquet").load(root)` schema | with `p` | without `p` | with `p` |
| `read.parquet(root + "/p=1")` (a leaf) | `struct<id:bigint,s:string>` | the same | the same |
| `option("basePath", root).parquet(root + "/p=1")` | with `p` | refuses: `reader option 'basePath' is not supported by repark yet` | refuses (unchanged) |
| `read.parquet(root).write.parquet(copy)`, read back | the copy keeps `p` | the copy has no `p` | the copy keeps `p` |

## Resolution (2026-11-24; Delta 2)

- Added Hive partition discovery to the Parquet path-read seat in
  `crates/repark-core/src/spark_nullable/partition.rs` and `partition_infer.rs` (invoked by
  `ReparkSession::read_parquet` / `read_parquet_with_case_sensitivity` for both `read.parquet` and
  `read.format("parquet").load`, sharing the same `ObjectStore` listing for local paths and S3 prefixes):
  - Follows Spark's hidden path rule: a directory segment is hidden iff it starts with `.`, or starts with `_` and
    holds no `=` (so `_p=1/` and `_=1/` are partition directories while `_temporary/` is hidden); a file name is
    hidden iff it starts with `.` or `_` (skipping `_SUCCESS`, `.crc`, `.hidden.parquet`, `_p=1/_hidden.parquet`,
    and hidden 0-byte files).
  - Reads one footer, as Spark's `ParquetUtils.inferSchema` does: the first visible file in path byte order (root and
    base-directory files included when partitioned, root files only when not). When that file holds 0 bytes the read
    refuses with `[FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER]` (`SQLSTATE: KD001`), after both conflict checks; every
    other 0-byte file is left out of the scan, while its directory still names a partition and types the column.
  - When no visible file has a partition spec, unpartitioned directories without hidden `.parquet` files take main's
    `ListingTable` fallback; when hidden `.parquet` files exist, only root-level files (`depth == 1`) are kept (nested
    non-`=` subdirectories are not read, and nested-only directories refuse as main and Spark do).
  - Extracts only the trailing contiguous run of `key=value` directories above each file (skipping trailing non-`=`
    subdirectories like `a=1/b=2/sub/`, stopping before a non-`=` separator like `a=1/sub/b=2/` -> `b` only). Leaves
    leaf reads (`root/p=1`) and root paths holding `=` (`tag=foo`) unpartitioned; normalizes `file://` directory URIs
    without trailing slash to collections.
  - Applies Spark's base-path rule (`PartitioningUtils.parsePartitions`): each file's directory has the base path where
    that walk stops, a directory without `key=value` segments is its own base path, and once any file is partitioned
    every base path must match case-insensitively or the read refuses with `[CONFLICTING_DIRECTORY_STRUCTURES]`
    (`SQLSTATE: KD009`), before `CONFLICTING_PARTITION_COLUMN_NAMES`. Files in the common base directory (root files
    beside `k=v/`) are left out of the scan, as Spark leaves them out.
  - Unescapes `%XX` sequences per byte (`char::from(byte)`) on both partition column names and values (`a%2Fb=10` ->
    `a/b`, `p=%C3%A9` -> `Ã©`, raw UTF-8 `p=é` -> `é`). Refuses empty partition values (`p=`) and empty
    partition keys (`=1`), and refuses conflicting partition column names or mixed depths with
    `[CONFLICTING_PARTITION_COLUMN_NAMES]` (`SQLSTATE: KD009`), respecting live `spark.sql.caseSensitive`.
  - Implements Spark 4.1.2 type inference (`Int32`, `Int64`, Java integral `BigDecimal` up to precision 38 incl.
    zero-mantissa `0e5` -> `decimal(6,0)` and `0.0e5` -> `decimal(5,0)`, Java `Double` incl. exact
    `NaN`/`+NaN`/`-NaN`/`Infinity`/`-Infinity` and Java hex floats `0x1.0p0`, `Date32`,
    `Timestamp(Microsecond, session_tz)` for `yyyy-MM-dd HH:mm:ss[.S]` with mixed `DATE`+`TIMESTAMP` widening to
    `TIMESTAMP` at local midnight in the session zone, `Utf8`; `__HIVE_DEFAULT_PARTITION__` -> NULL and all-null ->
    `void`) and wires `spark.sql.sources.partitionColumnTypeInference.enabled` (default `true`) across builder,
    `spark.conf.set`/`unset`, and SQL `SET`/`RESET`.
  - Resolves data-file vs partition-directory column overlap by replacing the colliding file column at its original
    position with the partition column's name, inferred type, and directory value (`caseSensitive`-aware).
  - Executes partitioned Parquet scans via `PartitionedParquetProvider` over DataFusion `FileScanConfigBuilder` +
    `FloatBoundsInexactFormat` + `TableSchema`, with sound partition-only file pruning (`where("p = 2")` scans 1 file
    instead of 2), Spark's `ConstantPropagation` and `ConstantFolding` modelled first (`partition_prune.rs`: a top-level
    `column = literal` or `<=>` conjunct binds the column; bound columns are replaced inside the other comparisons; a
    conjunct left constant is evaluated once and raises or empties the scan), then sequential top-level `AND`
    short-circuiting, ANSI runtime error propagation even on 0-row partitions (`PartitionPruningAnsiConfig` mirrored
    from `SparkAnsiConfig`), and `plan_introspect.rs` integration
    (`plan_reads_s3_prefix`, `write_table_source`, `inputFiles`).
- Measured over 719 Step 0 cells against live Spark 4.1.2 (UTC and `America/New_York`, ANSI `true` and `false`):
  `parquet` moves 6/104 -> 89/104 (+83 fixed), `parquet_load` moves 5/103 -> 88/103 (+83 fixed), 0 regressions across
  all controls (`leaf_read`, `ICEBERG` 20/20, `orc` 73/99, `text` 71/94); all Delta 1 verifier findings F1–F9 closed.
- **Delta 2 (re-check findings D1-1 to D1-4):** a 522-cell layout, footer-order and propagation matrix
  ([delta2-matrix.md](../../../wo/parquet-partition-discovery-1/evidence/delta2-matrix.md)) equals Spark on 96 cells on
  main, 360 on `9656eec2`, 334 on `f514246d9` and 512 on the Delta 2 head. Replaying the 719 Step 0 cells and the
  re-check's seven probe suites (681 cells) on the Delta 2 head moves 51 cells toward Spark and none away (Step 0 stays
  at 356/719).
- **S3 status:** `ReparkSession::read_parquet` shares the local `ObjectStore` discovery seat with `s3://`/`s3a://` and
  is verified locally via an in-memory object store (`s3_in_memory_partitioned_parquet_shares_discovery_and_introspection`
  in `crates/repark-core/src/spark_nullable/partition_tests.rs`). **Not measured on live AWS S3.**
- **Pins:** Rust unit tests in `crates/repark-core/src/spark_nullable/partition_tests.rs` (11 async tests) and facade
  pins in `python/repark/tests/io/test_parquet_partition_discovery_1.py` (128 original defect pins red on main
  `514281bd4`; 86 Delta 1 F1–F9 pins red on `9656eec2`; all green on head; 4 control pins green throughout). Delta 2
  adds `partition_layout_tests.rs` (3 Rust tests) and 508 facade cases recorded from Spark in
  `python/repark/tests/io/test_parquet_partition_discovery_1_delta2.py` (178 red on `f514246d9`, the rest controls
  green on both).

## Still unlike Spark

Measured gaps remaining on head (all pre-existing on main; no guard relaxed):

1. **Parquet `basePath`, `recursiveFileLookup`, and `mergeSchema` options (16 cells across `parquet` and
   `parquet_load`)** — `OPT/*/basePath_leaf`, `OPT/*/basePath_glob`, `OPT/*/recursive_true`, `OPT/*/recursive_false`,
   `OPT/*/mergeSchema_true_one`, `OPT/*/mergeSchema_false_one`, `OPT/*/mergeSchema_true_evolved`, and
   `OPT/*/mergeSchema_false_evolved` remain refused loud (`AnalysisException`: reader option is not supported yet).
2. **Parquet glob paths and multi-path varargs / lists (6 cells across `parquet` and `parquet_load`)** —
   `OPT/*/glob_dir` (`p=*`), `OPT/*/glob_files` (`p=*/*.parquet`), `OPT/parquet/varargs_dirs`
   (`read.parquet(p1, p2)`), and `OPT/parquet_load/path_list_dirs` (`format("parquet").load([p1, p2])`) fail on RePark
   where Spark reads the matched partitions.
3. **`input_file_name()` on Parquet plain-path reads (8 cells across `parquet` and `parquet_load`, both ANSI modes)**
   — `Q/*/input_file_name_df` (`UnsupportedOperationException`, disclosed R-FN-BATCH4) and `Q/*/input_file_name_sql`
   (`AnalysisException` `[UNRESOLVED_ROUTINE]`).
4. **SQL path-read doors over Parquet directories (C1, pre-existing)** — `SELECT * FROM parquet.\`dir\`` (`table not
   found`), `CREATE TEMPORARY VIEW v USING parquet OPTIONS (path '...')` (`expected AS with the view query`), and
   `CREATE TABLE t USING parquet LOCATION '...'` (`CREATE TABLE without AS SELECT requires a column list`) refuse
   identically on main and head where Spark 4.1.2 discovers `struct<id:bigint,s:string,p:int>`.
5. **Parquet files without `.parquet` suffix or uppercase `.PARQUET` (C2, pre-existing)** — `part_no_ext_mix`
   (`p=1/part-00000` + `p=2/part-00001.parquet` reads only `p=2` vs Spark 2 rows), `part_no_ext_only`,
   `unpart_no_ext_*`, and `type_uppercase_ext` skip non-`.parquet` files identically on main and head where Spark reads
   them. For the same reason a stray non-`.parquet` file does not make its directory a leaf: `p=1/good.parquet` +
   `sub/notes.txt` (`LAYOUT/L21_part_sub_txt`, 4 cells) answers `struct<id:bigint,s:string,p:int>` on head (as on
   `9656eec2` and `f514246d9`) where Spark refuses `CONFLICTING_DIRECTORY_STRUCTURES`.
6. **DataFrame read-time session timezone snapshot & ANSI-off string casts (C3, pre-existing)** — after
   `df = spark.read.parquet(timestamps)` in `UTC` followed by `spark.conf.set("spark.sql.session.timeZone", "America/New_York")`,
   `df.where("hour(p) = 3")` / `df.where("p = TIMESTAMP '2024-01-02 03:04:05'")` evaluate in the read-time `UTC` zone
   (same as unpartitioned Parquet on main; SQL over a temp view uses the live zone and matches Spark 100%). Under
   `spark.sql.ansi.enabled=false`, `p = 1` over non-empty string `p=x` (`CAST_ANSI/ansi=false/str_eq_int_1`) and
   `CAST(p AS DATE)` over `p=notadate` (`CAST_ANSI/ansi=false/cast_date_bad`) raise Arrow cast errors (same as
   unpartitioned DataFrames on main).
7. **Local self-overwrite, single-slash `file:/` URI, root-only hidden files, and exception subclass (C4,
   pre-existing)** — local `df = spark.read.parquet(d); df.write.mode("overwrite").parquet(d)` succeeds via local
   staging where Spark fails `FAILED_READ_FILE.FILE_NOT_EXIST`; single-slash `file:/<dir>` URI (`a13_file_single_slash_uri`)
   fails identically on main and head where Spark reads `struct<id:bigint,s:string,p:int>`; root-only hidden files
   (`unpart_only_dot_hidden`, `unpart_only_under_hidden`) read via `ListingTable` fallback on main and head where Spark
   refuses `UNABLE_TO_INFER_SCHEMA`; partition conflicts raise `PySparkException` (carrying exact condition
   `CONFLICTING_PARTITION_COLUMN_NAMES` and SQLSTATE `KD009`) where PySpark raises subclass `SparkRuntimeException`.
8. **CSV partitioned reads (`csv_header` 5/100 equal Spark; `csv_noheader` 5/99 equal Spark)** — separate reader seat
   (`read_csv_path` + `_apply_reader_schema_semantics`); CSV reads over partitioned directories do not yet discover
   partition columns (95 cells differing for `csv_header`, 94 for `csv_noheader`).
9. **JSON partitioned reads (`json` 5/100 equal Spark)** — separate reader seat (`context.read_json` +
   `_apply_reader_schema_semantics`); JSON reads over partitioned directories do not yet discover partition columns
   (95 cells differing).
10. **Live AWS S3 measurement** — shared local/S3 `ObjectStore` seat verified with in-memory object store only; **not
    measured on S3** (live AWS not contacted).
11. **A single 0-byte file read directly (`SINGLE/zero_file`, 2 cells)** — `read.parquet(".../empty.parquet")` takes
    the `ListingTable` fallback and answers `struct<>` `[]` on main, `9656eec2`, `f514246d9` and head where Spark refuses
    `FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER`.
12. **A comparison with `true` over a raising cast (`PRUNE/P28_cast_bool_cmp_sibling`, 4 cells, ANSI on)** —
    `CAST(p AS BOOLEAN) = true AND p = '1'` over `p=1`, `p=x`: Spark propagates `p = '1'` into the comparison and
    answers `[(1,'one','1')]` (as `9656eec2` did); DataFusion simplifies `x = true` to `x` before the scan, so the
    pruning seat receives the same filter as `CAST(p AS BOOLEAN) AND p = '1'`, which Spark raises on and head raises on
    (as `f514246d9`). The two cannot both match at this seat; head keeps the Delta 1 answer for both.
13. **Pre-existing ORC (26/99) and text (23/94) control gaps** — untouched by this unit (e.g.,
    `partitionColumnTypeInference.enabled=false`, `caseSensitive` directory unification,
    `mergeSchema`/`recursiveFileLookup`/`input_file_name`, decimal/scientific/single-fractional-digit timestamp
    nuances); zero control cells moved.
14. **DataFusion folds some ANSI filters away before the pruning seat (6 cells, ANSI on)** — `p = 1 AND false AND
    1 / 0 = 1` answers `[]`, and `p = 0 AND (1 / p = 1 OR true)` answers the `p=0` row, where Spark raises
    `DIVIDE_BY_ZERO`. DataFusion's simplifier removes `x AND false` and `x OR true` before the scan, so the seat never
    sees the raising side. Main refuses these cells because it discovers no `p` column.

Up: [map.md](map.md).
