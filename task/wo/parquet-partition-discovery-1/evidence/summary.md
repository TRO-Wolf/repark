# PARQUET-PARTITION-DISCOVERY-1 — Evidence Summary

- **Unit:** `PARQUET-PARTITION-DISCOVERY-1` (S1, 2026-11-24).
- **Card:** [parquet-partition-discovery-1-card-2026-11-23.md](../../../roadmap/mid-term/cards-2026-11-23/parquet-partition-discovery-1-card-2026-11-23.md).
- **Base:** `514281bd4` (tree-equal to prebuilt main package `pkg-d1merged-ea94eedc`).
- **Oracle:** live Spark 4.1.2 + Iceberg 1.11.0 (`local[2]`, Java 17), measured in both `UTC` and `America/New_York` and both ANSI modes (`true` and `false`).
- **Full 719-cell table:** [domain-table.md](domain-table.md).
- **Scripts:** [generate_fixtures.py](generate_fixtures.py), [generate_repark_fixtures.py](generate_repark_fixtures.py), [probe_step0.py](probe_step0.py), [render_step0.py](render_step0.py).

## Parity summary (719 cells)

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
| Spark facade (`read.parquet`) | `python/repark/src/repark/spark/session/reader.py:56-75` | `DataFrameReader.parquet`: semantic/schema gates, calls `self._session.read_parquet(path)`, attaches `CONFLICTING_PARTITION_COLUMN_NAMES` (`KD009`). | Yes (entry) |
| Spark facade (`format("parquet").load`) | `python/repark/src/repark/spark/session/reader.py:425-465` | `DataFrameReader.load`: merges kwargs, semantic gate, routes `fmt == "parquet"` to `self.parquet(effective_path)`. | Yes (entry) |
| Spark facade (`ReparkSession.read_parquet`) | `python/repark/src/repark/spark/session/session_core.py:892-895` | Calls `inner.read_parquet(str(path))` and wraps `DataFrame`. | Yes |
| Spark facade session conf | `python/repark/src/repark/spark/session/session_configuration.py`, `builder_conf.py`, `sql_set_statements.py` | Default (`true`), `conf.set`/`unset`, and SQL `SET`/`RESET` forwarding for `spark.sql.sources.partitionColumnTypeInference.enabled`. | Yes |
| PyO3 binding (`read_parquet` & runtime conf) | `crates/repark-python/src/session.rs:224-233`, `crates/repark-python/src/session_runtime.rs:412-415` | Reads live `spark.sql.caseSensitive`, calls `read_parquet_with_case_sensitivity`; routes `spark.sql.sources.partitionColumnTypeInference.enabled`. | Yes |
| Rust core (`ReparkSession::read_parquet`) | `crates/repark-core/src/session.rs:771-775`, `crates/repark-core/src/spark_nullable/partition.rs:184-203` | Registers S3 bucket store if `s3://`/`s3a://`, reads session timezone and `PartitionColumnTypeInferenceConfig`, runs `read_parquet_discovered`. | Yes (local + S3 share this seat) |
| Rust core Parquet discovery & scan | `crates/repark-core/src/spark_nullable/partition.rs` | Object-store listing, hidden-segment filtering, `keep_partitioned_only`, conflict checks, Spark 4.1.2 type inference, overlap resolution, `PartitionedParquetProvider` with `FloatBoundsInexactFormat` and partition-only file pruning. | Yes (fix seat) |
| Rust core S3 path seat | `crates/repark-core/src/spark_nullable/partition.rs:189-191`, `crates/repark-core/src/object_store_s3.rs:224-272` | Resolves slashless S3 prefix and lists via the same `ObjectStore` seat; verified via in-memory object store (`s3_in_memory_partitioned_parquet_shares_discovery_and_introspection`). **Not measured on live AWS S3.** | Yes |
| Rust core plan introspection | `crates/repark-core/src/plan_introspect.rs:84-88`, `477-484` | Recognizes `PartitionedParquetProvider` alongside `ListingTable` in `plan_reads_s3_prefix` and `write_table_source`; `input_files` reads `FileScanConfig` directly. | Yes |
| Rust Spark extension | `crates/repark-spark/src/extension.rs:58` | Installs `with_partition_column_type_inference` on session build. | Yes |
| Rust core path-append validation | `crates/repark-core/src/session/path_write/append.rs:98-100` | Calls `read_parquet` on single part files (not directories; takes `ListingTable` fallback). | Single-file fallback |
| CSV seat | `reader.py`, `session_core.py`, `crates/repark-core/src/session.rs`, `read_options.rs` | Separate seat (`read_csv_path` + `_apply_reader_schema_semantics`); out of narrow fix. | No |
| JSON seat | `reader.py`, `session_core.py`, `crates/repark-core/src/session.rs` | Separate seat (`context.read_json` + `_apply_reader_schema_semantics`); out of narrow fix. | No |
| ORC seat (control) | `reader_orc.py`, `crates/repark-python/src/orc_io.rs`, `crates/repark-core/src/orc_scan.rs` | Dedicated `OrcTableProvider` using `partition_discovery::discover_partitions`; untouched. | No (control) |
| Text seat (control) | `reader_text.py`, `crates/repark-python/src/text_io.rs`, `crates/repark-core/src/text_scan.rs` | Dedicated `TextTableProvider` using `partition_discovery::discover_partitions`; untouched. | No (control) |
| Iceberg doors (control) | `reader.py`, `reader_iceberg_path.py`, `crates/repark-core/src/iceberg_path/` | `IcebergTableProvider` over Iceberg metadata; untouched. | No (control) |
| Native ANSI SQL `CREATE EXTERNAL TABLE` | `crates/repark-core/src/local_fs_policy.rs`, `spark_nullable.rs` | Explicit DDL schema via `FloatBoundsInexactFactory`; no plain-path `read_parquet` SQL table function. | No |

## Adversarial edge checks measured against live Spark 4.1.2

An additional 25-case edge probe against live Spark 4.1.2 verified and closed:
- Java `BigDecimal` integral inference (`scale <= 0`, precision <= 38): `1e5` -> `decimal(6,0)`, `1.5e2` -> `decimal(3,0)`, `5.` -> `decimal(1,0)`, `1e5` + `2.5e-2` -> `string`, 39-digit integer -> `double`.
- Java `Double.parseDouble` inference: exact `NaN`, `Infinity`, `-Infinity`, `+Infinity` -> `double`; lowercase `nan`/`inf` -> `string`; trailing `d`/`f` and whitespace-padded numbers -> `double`.
- Empty partition key (`=1`) and empty partition value (`p=`) both refuse loudly.
- `file://` directory URIs without a trailing slash normalize to a collection URL.
- Subdirectories under or above partitions (`p=1/sub`, `sub/p=1`), empty partition directories, spaces in root paths, literal `+` and `=` in values, and `%25` / dangling `%` match Spark 4.1.2.

## Pins

- Rust unit pins: `crates/repark-core/src/spark_nullable/partition_tests.rs` (5 async tests covering layouts, pruning, `EmptyExec`, type inference, `partitionColumnTypeInference.enabled`, overlap, hidden files, conflicts, `caseSensitive`, numeric/URI edges, and in-memory S3).
- Facade pins: `python/repark/tests/io/test_parquet_partition_discovery_1.py` (132 test cases across `read.parquet`, `format("parquet").load`, ANSI `true`/`false`, `UTC`/`America/New_York`: 128/128 defect pins red on main `514281bd4` and green on head; 4/4 control pins green on both).
