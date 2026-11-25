# PARQUET-PARTITION-DISCOVERY-1 — Evidence Summary

- **Unit:** `PARQUET-PARTITION-DISCOVERY-1` (S1, 2026-11-24; Delta 2).
- **Card:** [parquet-partition-discovery-1-card-2026-11-23.md](../../../roadmap/mid-term/cards-2026-11-23/parquet-partition-discovery-1-card-2026-11-23.md).
- **Base:** `514281bd4` (tree-equal to prebuilt main package `pkg-d1merged-ea94eedc`).
- **Oracle:** live Spark 4.1.2 + Iceberg 1.11.0 (`local[2]`, Java 17), measured in both `UTC` and `America/New_York` and both ANSI modes (`true` and `false`).
- **Full 719-cell table:** [domain-table.md](domain-table.md).
- **Scripts:** [generate_fixtures.py](generate_fixtures.py), [generate_repark_fixtures.py](generate_repark_fixtures.py), [probe_step0.py](probe_step0.py), [render_step0.py](render_step0.py).

## Parity summary (719 Step 0 cells)

| Reader / control | Total | Main == Spark | Head == Spark | Fixed | Regressions |
|---|---:|---:|---:|---:|---:|
| `parquet` | 104 | 6 | 89 | 83 | 0 |
| `parquet_load` | 103 | 5 | 88 | 83 | 0 |
| `csv_header` (out of narrow seat) | 100 | 5 | 5 | 0 | 0 |
| `csv_noheader` (out of narrow seat) | 99 | 5 | 5 | 0 | 0 |
| `json` (out of narrow seat) | 100 | 5 | 5 | 0 | 0 |
| `orc` (control) | 99 | 73 | 73 | 0 | 0 |
| `text` (control) | 94 | 71 | 71 | 0 | 0 |
| `ICEBERG` (control, 4 doors) | 20 | 20 | 20 | 0 | 0 |
| **Total** | **719** | **190** | **356** | **166** | **0** |

All 89/89 in-scope `parquet` cells and 88/88 in-scope `parquet_load` cells match live Spark 4.1.2 on schema, `dtypes`, collected values/types, error classes, and partition pruning.

## Partition pruning measurement

Over `one_level` (`p=1/part-00000-...parquet`, `p=2/part-00000-...parquet`), `where("p = 2")` across both doors and both ANSI modes:

| Door / ANSI | Spark 4.1.2 files read | RePark main | RePark head files scanned |
|---|---|---|---|
| `read.parquet` (`ansi=true`) | 1 (`p=2/...`) | `ERR` (`No field named p`) | 1 (`p=2/...`) |
| `read.parquet` (`ansi=false`) | 1 (`p=2/...`) | `ERR` (`No field named p`) | 1 (`p=2/...`) |
| `format("parquet").load` (`ansi=true`) | 1 (`p=2/...`) | `ERR` (`No field named p`) | 1 (`p=2/...`) |
| `format("parquet").load` (`ansi=false`) | 1 (`p=2/...`) | `ERR` (`No field named p`) | 1 (`p=2/...`) |

## Seat inventory

| Door / layer | Seat (`path:line`) | Role | Shares Parquet local seat? |
|---|---|---|---|
| Spark facade (`read.parquet`) | `python/repark/src/repark/spark/session/reader.py:56-80` | `DataFrameReader.parquet`: semantic/schema gates, calls `self._session.read_parquet(path)`, attaches `CONFLICTING_PARTITION_COLUMN_NAMES` (`KD009`) and `FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER` (`KD001`). | Yes (entry) |
| Spark facade (`format("parquet").load`) | `python/repark/src/repark/spark/session/reader.py:430-470` | `DataFrameReader.load`: merges kwargs, semantic gate, routes `fmt == "parquet"` to `self.parquet(effective_path)`. | Yes (entry) |
| Spark facade (`ReparkSession.read_parquet`) | `python/repark/src/repark/spark/session/session_core.py:892-895` | Calls `inner.read_parquet(str(path))` and wraps `DataFrame`. | Yes |
| Spark facade session conf | `python/repark/src/repark/spark/session/session_configuration.py`, `builder_conf.py`, `sql_set_statements.py` | Default (`true`), `conf.set`/`unset`, and SQL `SET`/`RESET` forwarding for `spark.sql.sources.partitionColumnTypeInference.enabled`. | Yes |
| PyO3 binding (`read_parquet` & runtime conf) | `crates/repark-python/src/session.rs:225-244` (the `read_parquet_with_case_sensitivity` call at `237-238`), `crates/repark-python/src/session_runtime.rs:247-250,298-328` | Reads live `spark.sql.caseSensitive`, calls `read_parquet_with_case_sensitivity`; routes `spark.sql.sources.partitionColumnTypeInference.enabled` and mirrors `spark.sql.ansi.enabled` into `PartitionPruningAnsiConfig`. | Yes |
| Rust core (`ReparkSession::read_parquet`) | `crates/repark-core/src/session.rs:771-773`, `crates/repark-core/src/spark_nullable/partition.rs:230-249` (`read_parquet_with_case_sensitivity`) | Registers S3 bucket store if `s3://`/`s3a://`, reads session timezone and `PartitionColumnTypeInferenceConfig`, runs `read_parquet_discovered`. | Yes (local + S3 share this seat) |
| Rust core Parquet discovery, inference & scan | `crates/repark-core/src/spark_nullable/partition.rs`, `partition_infer.rs`, `partition_prune.rs` | Object-store listing, Spark hidden dir/file filtering, root-only unpartitioned filter, base-path conflict refusal, first-footer 0-byte refusal, trailing-run specs, per-byte `%XX` unescaping, conflict checks, Spark 4.1.2 type inference (incl. mixed `DATE`+`TIMESTAMP` and Java zero-decimal/hex-float/signed-NaN), overlap resolution, `PartitionedParquetProvider` with `FloatBoundsInexactFormat`, constant propagation and folding before sequential conjunct pruning, and ANSI error propagation. | Yes (fix seat) |
| Rust core S3 path seat | `crates/repark-core/src/spark_nullable/partition.rs:235-237`, `crates/repark-core/src/object_store_s3.rs:224-272` | Resolves slashless S3 prefix and lists via the same `ObjectStore` seat; verified via in-memory object store (`s3_in_memory_partitioned_parquet_shares_discovery_and_introspection`). **Not measured on live AWS S3.** | Yes |
| Rust core plan introspection | `crates/repark-core/src/plan_introspect.rs:84-88`, `477-484` | Recognizes `PartitionedParquetProvider` alongside `ListingTable` in `plan_reads_s3_prefix` and `write_table_source`; `input_files` reads `FileScanConfig` directly. | Yes |
| Rust Spark extension & temp views | `crates/repark-spark/src/extension.rs:85,105-110`, `crates/repark-spark/src/view_ddl/temp_settings.rs:77-86` | Installs `with_partition_pruning_ansi` and `with_partition_column_type_inference` on session build; mirrors captured ANSI setting in temp-view overlays. | Yes |
| Rust core path-append validation | `crates/repark-core/src/session/path_write/append.rs:98-100` | Calls `read_parquet` on single part files (not directories; takes `ListingTable` fallback). | Single-file fallback |
| CSV seat | `reader.py`, `session_core.py`, `crates/repark-core/src/session.rs`, `read_options.rs` | Separate seat (`read_csv_path` + `_apply_reader_schema_semantics`); out of narrow fix. | No |
| JSON seat | `reader.py`, `session_core.py`, `crates/repark-core/src/session.rs` | Separate seat (`context.read_json` + `_apply_reader_schema_semantics`); out of narrow fix. | No |
| ORC seat (control) | `reader_orc.py`, `crates/repark-python/src/orc_io.rs`, `crates/repark-core/src/orc_scan.rs` | Dedicated `OrcTableProvider` using `partition_discovery::discover_partitions`; untouched. | No (control) |
| Text seat (control) | `reader_text.py`, `crates/repark-python/src/text_io.rs`, `crates/repark-core/src/text_scan.rs` | Dedicated `TextTableProvider` using `partition_discovery::discover_partitions`; untouched. | No (control) |
| Iceberg doors (control) | `reader.py`, `reader_iceberg_path.py`, `crates/repark-core/src/iceberg_path/` | `IcebergTableProvider` over Iceberg metadata; untouched. | No (control) |
| Native ANSI SQL `CREATE EXTERNAL TABLE` | `crates/repark-core/src/local_fs_policy.rs`, `spark_nullable.rs` | Explicit DDL schema via `FloatBoundsInexactFactory`; no plain-path `read_parquet` SQL table function. | No |

## Adversarial edge probes & Delta 1 findings closed (measured against live Spark 4.1.2)

- **25/25 initial edge probe cells closed (including F9 `pct_utf8` in Delta 1):**
  - Java `BigDecimal` integral inference (`scale <= 0`, precision <= 38): `1e5` -> `decimal(6,0)`, `1.5e2` -> `decimal(3,0)`, `5.` -> `decimal(1,0)`, `1e5` + `2.5e-2` -> `string`, 39-digit integer -> `double`; Delta 1 (F8) adds zero-mantissa precision (`0e5` -> `decimal(6,0)`, `0.0e5` -> `decimal(5,0)`, `0e37` -> `decimal(38,0)`, `0e38` -> `double`).
  - Java `Double.parseDouble` inference: exact `NaN`, `+NaN`, `-NaN`, `Infinity`, `-Infinity`, `+Infinity`, and Java hex floats (`0x1.0p0`, `-0x1.8p2`, `0x1p-1074`, `0x1p1024`) -> `double`; lowercase `nan`/`inf` and `0x1.0` -> `string`; trailing `d`/`f` and whitespace-padded numbers -> `double`.
  - Empty partition key (`=1`) and empty partition value (`p=`) both refuse loudly.
  - `file://` directory URIs without a trailing slash normalize to a collection URL.
  - Subdirectories under or above partitions (`p=1/sub`, `sub/p=1`), empty partition directories, spaces in root paths, literal `+` and `=` in values, `%25` / dangling `%`, and per-byte `%XX` unescaping (`p=%C3%A9` -> `Ã©`, raw `p=é` -> `é`) match Spark 4.1.2.
- **Delta 1 verifier findings F1–F9 closed:**
  - **F1:** unpartitioned directories with hidden `.parquet` files keep only root-level files (`depth == 1`), refusing nested-only layouts (`a2_nested_sub_temp`) and never reading nested non-`=` subdirectories (`a1_root_sub_temp`).
  - **F2:** Spark hidden rule — directory hidden iff leading `.` or leading `_` without `=`; file hidden iff leading `.` or `_` (so `_p=1/` and Spark `partitionBy("_p")` discover `_p:int`, while `_p=1/_hidden.parquet` is hidden).
  - **F3 & F9:** per-byte `%XX` unescaping applied to both partition column names (`a%2Fb=10` -> `a/b`) and partition values (`p=%C3%A9` -> `Ã©`) inside the Parquet seat (`partition_infer::unescape_spark_path_name`), leaving ORC and text untouched.
  - **F4:** reverse-scan trailing contiguous run of `key=value` directories (`a=1/sub/b=2/` -> `b` only; `a=1/b=2/sub/` -> `a, b`).
  - **F5:** mixed `DATE` + `TIMESTAMP` partition values widen to `TIMESTAMP` with dates at local midnight in the session zone (verified in both `UTC` and `America/New_York` across winter/summer/DST).
  - **F6:** partition-only `AND` conjuncts flattened and evaluated sequentially per file (short-circuiting `p = '1' AND CAST(p AS INT) = 1`), propagating runtime errors under `spark.sql.ansi.enabled=true` even on 0-row partitions while keeping files conservatively under `ansi=false` or unlowerable expressions.
  - **F7:** non-hidden 0-byte `.parquet` files refuse with `[FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER]` (`KD001`) while hidden 0-byte files (`_zero.parquet`) remain skipped.
  - **F8:** zero-mantissa decimal precision, `+NaN`/`-NaN`, and Java hex floats match Spark 4.1.2.

## Pins

- Rust unit pins: `crates/repark-core/src/spark_nullable/partition_tests.rs` (11 async tests covering layouts, pruning, `EmptyExec`, type inference, `partitionColumnTypeInference.enabled`, overlap, Spark hidden rules, root-only unpart filter, trailing-run specs, URL-escaped keys/values, mixed `DATE`+`TIMESTAMP` in UTC/NY, ANSI pruning error propagation/short-circuiting, 0-byte `.parquet` refusal, Java zero-decimal/signed-NaN/hex-float, conflicts, `caseSensitive`, and in-memory S3).
- Facade pins: `python/repark/tests/io/test_parquet_partition_discovery_1.py` (218 test cases across `read.parquet`, `format("parquet").load`, ANSI `true`/`false`, `UTC`/`America/New_York`: 128 original defect pins red on main `514281bd4`; 86 Delta 1 F1–F9 pins red on `9656eec2`; all 218 green on head).
