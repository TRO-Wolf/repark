# NAN-MINMAX-1 step 0: every read path to the constant-column replacement and the statistics shortcut

Main `631d283c`, DataFusion 54.1.0. Two RePark-only consumers need `Precision::Exact` float bounds:

- **the constant-column replacement:** `datafusion-datasource-parquet-54.1.0/src/opener/mod.rs:620-637`
  (`constant_columns_from_stats` → `replace_columns_with_literals`) and `:1325-1347` (`constant_value_from_stats`:
  exact `min == max`, `null_count == 0`);
- **the `max`/`min` statistics shortcut:** `datafusion-functions-aggregate-54.1.0/src/min_max.rs:195` (`max`) and
  `:478` (`min`), applied by `AggregateStatistics`.

Both read the per-file statistics a `ListingTable` collects through its `FileFormat`:
`datafusion-catalog-listing-54.1.0/src/table.rs:830-836` (`infer_stats_and_ordering`). Parquet's footer min and max
leave NaN out, so a file whose other float values are one value reports `min == max`.

| # | Path | Reaches the consumers? | Through `spark_nullable.rs`'s format? | Measured on main |
|---|---|---|---|---|
| 1 | `spark.read.parquet(p)`: `reader.py:63` → `session_core.py:898-901` → `PyReparkSession.read_parquet` (`crates/repark-python/src/session.rs:223-226`) → `ReparkSession::read_parquet` (`crates/repark-core/src/session.rs:771-776`) → `read_parquet_nullable` (`spark_nullable.rs:19-33`) → `SessionContext::read_parquet` | yes | yes | wrong (`read_parquet`) |
| 2 | `spark.read.format("parquet").load(p)`: `reader.py:450` → path 1 | yes | yes | wrong (`format_load`) |
| 3 | SQL over a path, `` SELECT … FROM parquet.`p` `` | no: RePark refuses it (`table 'datafusion.parquet.<p>' not found`) | — | refused; Spark answers |
| 4 | `CREATE TABLE t USING parquet LOCATION p` | no: refused (`CREATE TABLE without AS SELECT requires a column list`) | — | refused; Spark answers |
| 5 | Spark door `CREATE EXTERNAL TABLE … STORED AS PARQUET LOCATION p`, with or without a column list and with `repark.sql.allowLocalFilesystemDDL` | no: refused (`crates/repark-spark/src/create_table.rs:101`) | — | refused |
| 6 | Native door `PyReparkSession.native().read_parquet(p)` | yes | yes (path 1's Rust entry) | wrong (`native read_parquet`) |
| 7 | Native door `repark.sql("CREATE EXTERNAL TABLE … STORED AS PARQUET LOCATION p")`: DataFusion's `ListingTableFactory` takes the session's `"parquet"` file-format factory, DataFusion's stock `ParquetFormatFactory` (installed by `with_default_features`, `crates/repark-core/src/session/df_guards.rs:156`). Local `p` is allowed under a registered warehouse root; remote `s3://` is always allowed (`crates/repark-core/src/local_fs_policy.rs:179`) | yes | **no** | wrong (`create_external_under_warehouse`); a local path outside every warehouse is refused |
| 8 | DataFusion's `register_parquet` / `register_listing_table` | no: RePark's Python and Rust sessions expose neither | — | — |
| 9 | The path writer's append (`crates/repark-core/src/session/path_write/append.rs:99`) reads the destination through `ReparkSession::read_parquet` | yes | yes | not measured separately |
| 10 | `crates/repark-distributed/src/iceberg_provider.rs:617`: `context.read_parquet(files, ParquetReadOptions::default())` over an Iceberg scan's files | yes, in that crate | no | not reachable from the wheel: `repark-distributed` is not a dependency of `repark-python`; left as is |
| 11 | Iceberg reads (Spark door, DataFrame doors, native door) | no: the fork's scan reports only `num_rows` (`integrations/datafusion/src/physical_plan/scan.rs:300-318`) | — | right on every build (sketch §9) |

**So two seats reach every path the wheel exposes:** path 1's `read_parquet_nullable` (paths 1, 2, 6, 9) and the
session's `"parquet"` file-format factory (path 7).
