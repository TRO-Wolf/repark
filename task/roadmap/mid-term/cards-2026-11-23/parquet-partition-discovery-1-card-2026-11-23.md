# Card PARQUET-PARTITION-DISCOVERY-1: `read.parquet` of a partitioned directory drops the partition columns

**Date:** 2026-11-23. **Filed by:** the orchestrator (`claude-opus-5-5`), from both verifiers of PR #1411 (each saw
the column missing on main and on the branch) and a re-measurement here. **Severity:** S1 (owner, 2026-11-23):
a read-then-write copy stores the table without its partition columns, silently. **Status:** fixed on
`fix/parquet-partition-discovery-1` (2026-11-24). Evidence:
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

## Resolution (2026-11-24)

- Added Hive partition discovery to the Parquet path-read seat in `crates/repark-core/src/spark_nullable/partition.rs`
  (invoked by `ReparkSession::read_parquet` / `read_parquet_with_case_sensitivity` for both `read.parquet` and
  `read.format("parquet").load`, sharing the same `ObjectStore` listing for local paths and S3 prefixes):
  - Skips hidden path segments (leading `.` or `_`, e.g. `_SUCCESS`, `_temporary/`, `.crc`, `.hidden.parquet`,
    `p=1/.dot.parquet`, `p=1/_under.parquet`) and 0-byte files; applies `keep_partitioned_only` so root-level files
    beside `k=v/` subdirectories are ignored.
  - Leaves leaf reads (`root/p=1`) and root paths holding `=` (`tag=foo`) unpartitioned; normalizes `file://`
    directory URIs without trailing slash to collections.
  - Refuses empty partition values (`p=`) and empty partition keys (`=1`), and refuses conflicting partition column
    names or mixed depths with `[CONFLICTING_PARTITION_COLUMN_NAMES]` (`SQLSTATE: KD009`), respecting live
    `spark.sql.caseSensitive` (`false` unifies `p=1`/`P=2`; `true` refuses).
  - Implements Spark 4.1.2 type inference (`Int32`, `Int64`, Java integral `BigDecimal` up to precision 38, Java
    `Double` incl. exact `NaN`/`Infinity`/`-Infinity`, `Date32`, `Timestamp(Microsecond, session_tz)` for
    `yyyy-MM-dd HH:mm:ss[.S]`, `Utf8`; `__HIVE_DEFAULT_PARTITION__` -> NULL and all-null -> `void`) and wires
    `spark.sql.sources.partitionColumnTypeInference.enabled` (default `true`) across builder, `spark.conf.set`/`unset`,
    and SQL `SET`/`RESET`.
  - Resolves data-file vs partition-directory column overlap by replacing the colliding file column at its original
    position with the partition column's name, inferred type, and directory value (`caseSensitive`-aware).
  - Executes partitioned Parquet scans via `PartitionedParquetProvider` over DataFusion `FileScanConfigBuilder` +
    `FloatBoundsInexactFormat` + `TableSchema`, with sound partition-only file pruning (`where("p = 2")` scans 1 file
    instead of 2) and `plan_introspect.rs` integration (`plan_reads_s3_prefix`, `write_table_source`, `inputFiles`).
- Measured over 719 Step 0 cells against live Spark 4.1.2 (UTC and `America/New_York`, ANSI `true` and `false`):
  `parquet` moves 6/104 -> 89/104 (+83 fixed), `parquet_load` moves 5/103 -> 88/103 (+83 fixed), 0 regressions across
  all controls (`leaf_read`, `ICEBERG` 20/20, `orc` 73/99, `text` 71/94).
- **S3 status:** `ReparkSession::read_parquet` shares the local `ObjectStore` discovery seat with `s3://`/`s3a://` and
  is verified locally via an in-memory object store (`s3_in_memory_partitioned_parquet_shares_discovery_and_introspection`
  in `crates/repark-core/src/spark_nullable/partition_tests.rs`). **Not measured on live AWS S3.**
- **Pins:** Rust unit tests in `crates/repark-core/src/spark_nullable/partition_tests.rs` (7 async tests) and facade
  pins in `python/repark/tests/io/test_parquet_partition_discovery_1.py` (128 defect pins red on main `514281bd4` and
  green on head; 4 control pins green on both).

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
4. **CSV partitioned reads (`csv_header` 5/100 equal Spark; `csv_noheader` 5/99 equal Spark)** — separate reader seat
   (`read_csv_path` + `_apply_reader_schema_semantics`); CSV reads over partitioned directories do not yet discover
   partition columns (95 cells differing for `csv_header`, 94 for `csv_noheader`).
5. **JSON partitioned reads (`json` 5/100 equal Spark)** — separate reader seat (`context.read_json` +
   `_apply_reader_schema_semantics`); JSON reads over partitioned directories do not yet discover partition columns
   (95 cells differing).
6. **Live AWS S3 measurement** — shared local/S3 `ObjectStore` seat verified with in-memory object store only; **not
   measured on S3** (live AWS not contacted).
7. **Pre-existing ORC (26/99) and text (23/94) control gaps** — untouched by this unit (e.g., `partitionColumnTypeInference.enabled=false`, `caseSensitive` directory unification, `mergeSchema`/`recursiveFileLookup`/`input_file_name`, decimal/scientific/single-fractional-digit timestamp nuances); zero control cells moved.

Up: [map.md](map.md).
