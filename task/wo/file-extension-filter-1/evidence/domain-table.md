# FILE-EXTENSION-FILTER-1 step-0 and head table

## Provenance

- **RePark:** main's package (BUILT_FROM `57c92b411eb6`, #1429's tested head; main `30288badc` adds only Markdown and
  #1434, which touches no listing) and the branch head (`fix/file-extension-filter-1`), release wheels.
- **Spark:** live PySpark 4.1.2 (`local[2]`, Java 17, `iceberg-spark-runtime-4.1_2.13-1.11.0.jar`, a local Hadoop
  catalog), both ANSI modes, session zone `America/New_York`.
- **Scripts:** [probe_step0.py](probe_step0.py) (every cell and door in a fresh synthetic directory;
  `--engine spark|repark`, `--peer` reads the other engine's written directories, `--cross-only` reads them alone),
  [render_step0.py](render_step0.py) (this table), [probe_listing_freshness.py](probe_listing_freshness.py) (main
  re-lists a directory on every action: a frame read before an append counts the appended file).
- Every Parquet file holds one `id bigint` row; `n` is `count()`, `files` is `inputFiles()` relative to the cell's
  directory.

## Spark's rule, measured

- A name starting with `_` that holds no `=`, a name starting with `.`, or a name ending in `._COPYING_` is skipped on
  every level below the read root (`_temporary/`, `.hidden/` and `.p=3/` directories too). `_k=1.parquet` and `_p=2/`
  are read. Every other name is read as Parquet, whatever its suffix; a non-Parquet file with an ordinary name
  (`README.txt`) fails the read.
- A single file path is judged by its own name: `_b.parquet`, `.b.parquet` and `b.parquet._COPYING_` read directly,
  and the `_*` glob, are refused with `UNABLE_TO_INFER_SCHEMA`. A root directory named `_root/` or `.root/` is read.
- `_metadata`, `_common_metadata` and 0-byte files are listed by `inputFiles()` and contribute no rows.
- A `.X.crc` whose content is not `X`'s checksum fails Spark's read of `X` (Hadoop's checksum file system); the probe
  names its synthetic `.crc` files after no data file, so the name rule is measured alone.
- Every Spark door answers alike, and both ANSI modes answer alike.

## Rejected designs

- A glob such as `[!_.]*` on the table URL: DataFusion matches a glob against the first non-`key=value` segment only,
  so `sub/x` would be read, `_k=1.parquet` skipped, and `._COPYING_` cannot be written as a glob.
- One URL per listed file: a `HEAD` per file per execution, and a listing frozen at read time where main re-lists.
- Filtering in the file format: hidden files would still reach the statistics merge, the limit's row count and the
  file grouping.
- Patching DataFusion or a `[patch.crates-io]` entry: out of scope.

## Seat inventory

Every place a path read lists files or filters them by name or extension, on every door (line numbers on main
`30288badc` unless the path names a registry crate).

| # | Seat | Door | What it does | Why it exists | This unit |
|---|---|---|---|---|---|
| 1 | `crates/repark-core/src/spark_nullable.rs:48` `context.read_parquet(&resolved, ParquetReadOptions::default())` | `spark.read.parquet`, `format("parquet").load`, native `read_parquet` | schema inference listing, `.parquet` suffix filter; a single file path without the suffix is refused by DataFusion's extension check (#3) | the default `ParquetReadOptions` extension | **fixed**: inference lists with an empty suffix through the Spark listing session |
| 2 | `crates/repark-core/src/spark_nullable.rs:60` `ParquetReadOptions::default().schema(..).to_listing_options(..)` | same | the scan's `ListingTable` filters by the `.parquet` suffix at every execution | same default | **fixed**: empty suffix, the table wrapped in `SparkListingTable` |
| 3 | `datafusion-54.1.0/src/execution/context/mod.rs:1724` (`_read_type`) | same (through #1) | refuses a non-collection path whose name does not end in the suffix | DataFusion guards `read_*` against wrong-format single files | bypassed: #1 no longer calls `read_parquet` |
| 4 | `datafusion-datasource-54.1.0/src/url.rs:296-302` `list_prefixed_files` | every DataFusion listing (Parquet, CSV, JSON, native DDL) | keeps a listed object only when it ends in `file_extension` and matches the URL glob (`contains`, which also skips subdirectories that are not `key=value`) | DataFusion's only listing filter; it knows no hidden-name rule | unpatched (out of scope); the rule sits under it, in the object store the listing reads |
| 5 | `datafusion-catalog-listing-54.1.0/src/helpers.rs:414` and `options.rs:302` | same | drops 0-byte files from the scan and from inference | an empty file has no footer | unchanged: `inputFiles()` omits empty files Spark lists (rows equal) |
| 6 | `crates/repark-core/src/session.rs:314` `with_object_list_cache_limit(0)` | every RePark session | turns DataFusion's list-files cache off | fresh listings after writes | unchanged; it is why a cached listing cannot bypass the rule |
| 7 | `crates/repark-core/src/read_options.rs:56` (CSV) and `:86` (JSON), used by `session.rs` `read_csv` / `read_json` | `spark.read.csv`, `spark.read.json`, their `format().load` | DataFusion listing with the compression-derived suffix (`.csv`, `.json`, `.csv.gz`, …) and no hidden-name rule | DataFusion matches listing files by suffix only (the seat's own map note) | **not fixed** (a different seat): `b`, `b.CSV`, `b.bin` lose rows and `_b.csv`, `.b.csv` are read; listed on the card |
| 8 | `crates/repark-core/src/text_glob.rs:5` `is_hidden_name`, used at `text_scan.rs:63`, `orc_scan.rs:160`, `text_glob.rs:297` | `spark.read.text`, `spark.read.orc` (own listings) | skips names starting with `_` or `.`, any suffix | Spark's hidden rule, ported for the RePark-owned scans | unchanged: reads `b.orc._COPYING_` and `b.txt._COPYING_` (measured) and, by the code, skips `_k=1.orc` (no `=` exception; not measured), both unlike Spark; `inputFiles()` is empty; listed on the card |
| 9 | `crates/repark-core/src/session/path_write.rs:479-495` `list_part_keys` | `write.parquet` / `write.csv` / `write.json` to S3 | lists existing part keys by suffix for numbering, overwrite and append validation (`path_write/append.rs:99` reads each part through `read_parquet`) | the S3 commit protocol | unchanged (write side); a part key is a single non-hidden file, read as before |
| 10 | `crates/repark-distributed/src/iceberg_provider.rs:617` `read_parquet(files, ParquetReadOptions::default())` | Iceberg reads | reads the manifest's explicit data files | manifests list their files | unchanged (control): every cell equal |
| 11 | `crates/repark-core/src/spark_nullable.rs:92` `FloatBoundsInexactFactory` registration | native `CREATE EXTERNAL TABLE … STORED AS PARQUET LOCATION` | DataFusion's `ListingTableFactory` keeps the `.parquet` suffix | native DDL keeps DataFusion semantics | unchanged (native-only, no Spark contract) |
| 12 | `python/repark/src/repark/spark/session/reader.py:56` `DataFrameReader.parquet(self, path)` | `spark.read.parquet(a, b)` | one path only: a list of paths raises `TypeError` | the native read takes one path | unchanged (loud); listed on the card |
| 13 | `python/repark/src/repark/spark/session/reader_support.py:13` `_UNSUPPORTED_SEMANTIC_READER_OPTIONS` | `.option(...)` on Parquet | refuses `pathGlobFilter`, `recursiveFileLookup`, `ignoreCorruptFiles`, `ignoreMissingFiles`, `mergeSchema` | they would change the listing if ignored | unchanged (loud); listed on the card |
| 14 | `python/repark/src/repark/spark/session/reader.py:451` | `spark.read.load(p)` | refuses the empty default format | disclosed divergence | unchanged (loud) |
| 15 | the Spark door's SQL `parquet.\`p\`` and `CREATE TEMPORARY VIEW … USING parquet OPTIONS (path …)` | Spark SQL | refused (`table … not found`, `could not parse`) | not wired | unchanged (loud) |
| 16 | `crates/repark-core/src/plan_introspect.rs:78,465` | `inputFiles`-adjacent: the S3 overwrite check and `semanticHash` | read a `ListingTable`'s paths | write-over-read guard and plan identity | **updated** to read through `SparkListingTable` |

## Table (Spark vs main; the head column is added by `render_step0.py --head`)

| cell | door | ANSI | Spark 4.1.2 | main | main = Spark |
|---|---|---|---|---|---|
| `glob/p-star` | spark.read.format("parquet").load(p) | false | ids [1, 2, 3, 4]; n=4; files [p=1/part-00000, p=2/part-00000.parquet, p=3/part-00000.snappy.parquet, p=4/data.bin]; struct<id:bigint> | refuses: AnalysisException Error during planning: No files found at file://… | **no** |
| `glob/p-star` | native read_parquet(p) | false | ids [1, 2, 3, 4]; n=4; files [p=1/part-00000, p=2/part-00000.parquet, p=3/part-00000.snappy.parquet, p=4/data.bin]; struct<id:bigint> | refuses: AnalysisException Error during planning: No files found at file://… | **no** |
| `glob/p-star` | spark.read.parquet(p) | false | ids [1, 2, 3, 4]; n=4; files [p=1/part-00000, p=2/part-00000.parquet, p=3/part-00000.snappy.parquet, p=4/data.bin]; struct<id:bigint> | refuses: AnalysisException Error during planning: No files found at file://… | **no** |
| `glob/star-flat` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `glob/star-flat` | native read_parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `glob/star-flat` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `glob/star-hidden-explicit` | spark.read.format("parquet").load(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [_b.parquet]; struct<id:bigint> | **no** |
| `glob/star-hidden-explicit` | native read_parquet(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [_b.parquet]; id:int64 | **no** |
| `glob/star-hidden-explicit` | spark.read.parquet(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [_b.parquet]; struct<id:bigint> | **no** |
| `glob/star-parquet` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `glob/star-parquet` | native read_parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `glob/star-parquet` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `iceberg/control` | - | false | ids [1, 2]; n=2; files []; struct<id:bigint> | ids [1, 2]; n=2; files []; struct<id:bigint> | yes |
| `layout/flat-no-ext` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `layout/flat-no-ext` | native read_parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `layout/flat-no-ext` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `layout/hidden-dirs` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `layout/hidden-dirs` | native read_parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `layout/hidden-dirs` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `layout/hidden-partition-dirs` | spark.read.format("parquet").load(p) | false | refuses: SparkRuntimeException CONFLICTING_PARTITION_COLUMN_NAMES | ids [1, 2, 3]; n=3; files [.p=3/c.parquet, _p=2/b.parquet, p=1/a.parquet]; struct<id:bigint> | **no** |
| `layout/hidden-partition-dirs` | native read_parquet(p) | false | refuses: SparkRuntimeException CONFLICTING_PARTITION_COLUMN_NAMES | ids [1, 2, 3]; n=3; files [.p=3/c.parquet, _p=2/b.parquet, p=1/a.parquet]; id:int64 | **no** |
| `layout/hidden-partition-dirs` | spark.read.parquet(p) | false | refuses: SparkRuntimeException CONFLICTING_PARTITION_COLUMN_NAMES | ids [1, 2, 3]; n=3; files [.p=3/c.parquet, _p=2/b.parquet, p=1/a.parquet]; struct<id:bigint> | **no** |
| `layout/nested-mixed` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `layout/nested-mixed` | native read_parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `layout/nested-mixed` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `layout/nested-plain` | spark.read.format("parquet").load(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | refuses: AnalysisException Error during planning: No files found at file://… | yes |
| `layout/nested-plain` | native read_parquet(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | refuses: AnalysisException Error during planning: No files found at file://… | yes |
| `layout/nested-plain` | spark.read.parquet(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | refuses: AnalysisException Error during planning: No files found at file://… | yes |
| `layout/partitioned-card` | spark.read.format("parquet").load(p) | false | ids [(1, 1), (2, 2), (3, 3), (4, 4)]; n=4; files [p=1/part-00000, p=2/part-00000.parquet, p=3/part-00000.snappy.parquet, p=4/data.bin]; struct<id:bigint,p:int> | ids [2, 3]; n=2; files [p=2/part-00000.parquet, p=3/part-00000.snappy.parquet]; struct<id:bigint> | **no** |
| `layout/partitioned-card` | native read_parquet(p) | false | ids [(1, 1), (2, 2), (3, 3), (4, 4)]; n=4; files [p=1/part-00000, p=2/part-00000.parquet, p=3/part-00000.snappy.parquet, p=4/data.bin]; struct<id:bigint,p:int> | ids [2, 3]; n=2; files [p=2/part-00000.parquet, p=3/part-00000.snappy.parquet]; id:int64 | **no** |
| `layout/partitioned-card` | spark.read.parquet(p) | false | ids [(1, 1), (2, 2), (3, 3), (4, 4)]; n=4; files [p=1/part-00000, p=2/part-00000.parquet, p=3/part-00000.snappy.parquet, p=4/data.bin]; struct<id:bigint,p:int> | ids [2, 3]; n=2; files [p=2/part-00000.parquet, p=3/part-00000.snappy.parquet]; struct<id:bigint> | **no** |
| `layout/partitioned-markers` | spark.read.format("parquet").load(p) | false | ids [(1, 1), (2, 1)]; n=2; files [p=1/a.parquet, p=1/b]; struct<id:bigint,p:int> | ids [1]; n=1; files [p=1/a.parquet]; struct<id:bigint> | **no** |
| `layout/partitioned-markers` | native read_parquet(p) | false | ids [(1, 1), (2, 1)]; n=2; files [p=1/a.parquet, p=1/b]; struct<id:bigint,p:int> | ids [1]; n=1; files [p=1/a.parquet]; id:int64 | **no** |
| `layout/partitioned-markers` | spark.read.parquet(p) | false | ids [(1, 1), (2, 1)]; n=2; files [p=1/a.parquet, p=1/b]; struct<id:bigint,p:int> | ids [1]; n=1; files [p=1/a.parquet]; struct<id:bigint> | **no** |
| `list/file-and-no-ext` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | refuses: AnalysisException Error during planning: No files found at file://… | **no** |
| `list/file-and-no-ext` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | refuses: TypeError DataFrameReader.parquet() takes 2 positional arguments but 3 | **no** |
| `list/two-dirs` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [d1/a.parquet, d2/b]; struct<id:bigint> | refuses: AnalysisException Error during planning: No files found at file://… | **no** |
| `list/two-dirs` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [d1/a.parquet, d2/b]; struct<id:bigint> | refuses: TypeError DataFrameReader.parquet() takes 2 positional arguments but 3 | **no** |
| `marker/.part-00000.parquet.crc` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/.part-00000.parquet.crc` | native read_parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `marker/.part-00000.parquet.crc` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/.z.parquet.crc` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/.z.parquet.crc` | native read_parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `marker/.z.parquet.crc` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/README.txt` | spark.read.format("parquet").load(p) | false | refuses: Py4JJavaError An error occurred while calling o615.load. | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/README.txt` | native read_parquet(p) | false | refuses: Py4JJavaError An error occurred while calling o603.parquet. | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `marker/README.txt` | spark.read.parquet(p) | false | refuses: Py4JJavaError An error occurred while calling o603.parquet. | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/_SUCCESS` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/_SUCCESS` | native read_parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `marker/_SUCCESS` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/_committed_123` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/_committed_123` | native read_parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `marker/_committed_123` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/_common_metadata` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [_common_metadata, a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/_common_metadata` | native read_parquet(p) | false | ids [10]; n=1; files [_common_metadata, a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `marker/_common_metadata` | spark.read.parquet(p) | false | ids [10]; n=1; files [_common_metadata, a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/_metadata` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [_metadata, a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/_metadata` | native read_parquet(p) | false | ids [10]; n=1; files [_metadata, a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `marker/_metadata` | spark.read.parquet(p) | false | ids [10]; n=1; files [_metadata, a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/_started_123` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/_started_123` | native read_parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `marker/_started_123` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/all-writer-markers` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [part-00000-x.snappy.parquet, part-00001-x.snappy.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [part-00000-x.snappy.parquet, part-00001-x.snappy.parquet]; struct<id:bigint> | yes |
| `marker/all-writer-markers` | native read_parquet(p) | false | ids [10, 11]; n=2; files [part-00000-x.snappy.parquet, part-00001-x.snappy.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [part-00000-x.snappy.parquet, part-00001-x.snappy.parquet]; id:int64 | yes |
| `marker/all-writer-markers` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [part-00000-x.snappy.parquet, part-00001-x.snappy.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [part-00000-x.snappy.parquet, part-00001-x.snappy.parquet]; struct<id:bigint> | yes |
| `marker/c.parquet` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [a.parquet, c.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/c.parquet` | native read_parquet(p) | false | ids [10]; n=1; files [a.parquet, c.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `marker/c.parquet` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.parquet, c.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/c` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [a.parquet, c]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/c` | native read_parquet(p) | false | ids [10]; n=1; files [a.parquet, c]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `marker/c` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.parquet, c]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/.b.parquet` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [.b.parquet, a.parquet]; struct<id:bigint> | **no** |
| `name/.b.parquet` | native read_parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [.b.parquet, a.parquet]; id:int64 | **no** |
| `name/.b.parquet` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [.b.parquet, a.parquet]; struct<id:bigint> | **no** |
| `name/_b.parquet` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [_b.parquet, a.parquet]; struct<id:bigint> | **no** |
| `name/_b.parquet` | native read_parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [_b.parquet, a.parquet]; id:int64 | **no** |
| `name/_b.parquet` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [_b.parquet, a.parquet]; struct<id:bigint> | **no** |
| `name/_k=1.parquet` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [_k=1.parquet, a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [_k=1.parquet, a.parquet]; struct<id:bigint> | yes |
| `name/_k=1.parquet` | native read_parquet(p) | false | ids [10, 11]; n=2; files [_k=1.parquet, a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [_k=1.parquet, a.parquet]; id:int64 | yes |
| `name/_k=1.parquet` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [_k=1.parquet, a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [_k=1.parquet, a.parquet]; struct<id:bigint> | yes |
| `name/b.PARQUET` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [a.parquet, b.PARQUET]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b.PARQUET` | native read_parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b.PARQUET]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `name/b.PARQUET` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b.PARQUET]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b.bin` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [a.parquet, b.bin]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b.bin` | native read_parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b.bin]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `name/b.bin` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b.bin]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b.parquet._COPYING_` | spark.read.format("parquet").load(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `name/b.parquet._COPYING_` | native read_parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `name/b.parquet._COPYING_` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `name/b.parquet` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [a.parquet, b.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [a.parquet, b.parquet]; struct<id:bigint> | yes |
| `name/b.parquet` | native read_parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [a.parquet, b.parquet]; id:int64 | yes |
| `name/b.parquet` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [a.parquet, b.parquet]; struct<id:bigint> | yes |
| `name/b.parq` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [a.parquet, b.parq]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b.parq` | native read_parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b.parq]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `name/b.parq` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b.parq]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b.snappy.parquet` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [a.parquet, b.snappy.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [a.parquet, b.snappy.parquet]; struct<id:bigint> | yes |
| `name/b.snappy.parquet` | native read_parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b.snappy.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [a.parquet, b.snappy.parquet]; id:int64 | yes |
| `name/b.snappy.parquet` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b.snappy.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [a.parquet, b.snappy.parquet]; struct<id:bigint> | yes |
| `name/b.x.y.z` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [a.parquet, b.x.y.z]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b.x.y.z` | native read_parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b.x.y.z]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `name/b.x.y.z` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b.x.y.z]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b` | native read_parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `name/b` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `neighbour/csv/.b.csv` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.csv]; struct<_c0:string> | ids [10, 11]; n=2; files [.b.csv, a.csv]; struct<_c0:string> | **no** |
| `neighbour/csv/_b.csv` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.csv]; struct<_c0:string> | ids [10, 11]; n=2; files [_b.csv, a.csv]; struct<_c0:string> | **no** |
| `neighbour/csv/b.CSV` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.csv, b.CSV]; struct<_c0:string> | ids [10]; n=1; files [a.csv]; struct<_c0:string> | **no** |
| `neighbour/csv/b.bin` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.csv, b.bin]; struct<_c0:string> | ids [10]; n=1; files [a.csv]; struct<_c0:string> | **no** |
| `neighbour/csv/b.csv._COPYING_` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.csv]; struct<_c0:string> | ids [10]; n=1; files [a.csv]; struct<_c0:string> | yes |
| `neighbour/csv/b` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.csv, b]; struct<_c0:string> | ids [10]; n=1; files [a.csv]; struct<_c0:string> | **no** |
| `neighbour/csv/markers` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.csv]; struct<_c0:string> | ids [10]; n=1; files [a.csv]; struct<_c0:string> | yes |
| `neighbour/json/.b.json` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.json]; struct<id:bigint> | ids [10, 11]; n=2; files [.b.json, a.json]; struct<id:bigint> | **no** |
| `neighbour/json/_b.json` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.json]; struct<id:bigint> | ids [10, 11]; n=2; files [_b.json, a.json]; struct<id:bigint> | **no** |
| `neighbour/json/b.JSON` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.json, b.JSON]; struct<id:bigint> | ids [10]; n=1; files [a.json]; struct<id:bigint> | **no** |
| `neighbour/json/b.bin` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.json, b.bin]; struct<id:bigint> | ids [10]; n=1; files [a.json]; struct<id:bigint> | **no** |
| `neighbour/json/b.json._COPYING_` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.json]; struct<id:bigint> | ids [10]; n=1; files [a.json]; struct<id:bigint> | yes |
| `neighbour/json/b` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.json, b]; struct<id:bigint> | ids [10]; n=1; files [a.json]; struct<id:bigint> | **no** |
| `neighbour/json/markers` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.json]; struct<id:bigint> | ids [10]; n=1; files [a.json]; struct<id:bigint> | yes |
| `neighbour/orc/.b.orc` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.orc]; struct<id:bigint> | ids [10]; n=1; files []; struct<id:bigint> | **no** |
| `neighbour/orc/_b.orc` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.orc]; struct<id:bigint> | ids [10]; n=1; files []; struct<id:bigint> | **no** |
| `neighbour/orc/b.ORC` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.orc, b.ORC]; struct<id:bigint> | ids [10, 11]; n=2; files []; struct<id:bigint> | **no** |
| `neighbour/orc/b.bin` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.orc, b.bin]; struct<id:bigint> | ids [10, 11]; n=2; files []; struct<id:bigint> | **no** |
| `neighbour/orc/b.orc._COPYING_` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.orc]; struct<id:bigint> | ids [10, 11]; n=2; files []; struct<id:bigint> | **no** |
| `neighbour/orc/b` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.orc, b]; struct<id:bigint> | ids [10, 11]; n=2; files []; struct<id:bigint> | **no** |
| `neighbour/orc/markers` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.orc]; struct<id:bigint> | ids [10]; n=1; files []; struct<id:bigint> | **no** |
| `neighbour/text/.b.txt` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.txt]; struct<value:string> | ids [10]; n=1; files []; struct<value:string> | **no** |
| `neighbour/text/_b.txt` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.txt]; struct<value:string> | ids [10]; n=1; files []; struct<value:string> | **no** |
| `neighbour/text/b.TXT` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.txt, b.TXT]; struct<value:string> | ids [10, 11]; n=2; files []; struct<value:string> | **no** |
| `neighbour/text/b.bin` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.txt, b.bin]; struct<value:string> | ids [10, 11]; n=2; files []; struct<value:string> | **no** |
| `neighbour/text/b.txt._COPYING_` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.txt]; struct<value:string> | ids [10, 11]; n=2; files []; struct<value:string> | **no** |
| `neighbour/text/b` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.txt, b]; struct<value:string> | ids [10, 11]; n=2; files []; struct<value:string> | **no** |
| `neighbour/text/markers` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.txt]; struct<value:string> | ids [10]; n=1; files []; struct<value:string> | **no** |
| `option/ignoreCorruptFiles` | spark.read.parquet(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | refuses: AnalysisException reader option 'ignoreCorruptFiles' is not supported by repar | yes |
| `option/ignoreMissingFiles` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | refuses: AnalysisException reader option 'ignoreMissingFiles' is not supported by repar | **no** |
| `option/mergeSchema` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | refuses: AnalysisException reader option 'mergeSchema' is not supported by repark yet ( | **no** |
| `option/pathGlobFilter` | spark.read.parquet(p) | false | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | refuses: AnalysisException reader option 'pathGlobFilter' is not supported by repark ye | **no** |
| `option/recursiveFileLookup` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | refuses: AnalysisException reader option 'recursiveFileLookup' is not supported by repa | **no** |
| `root/dot-dir` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [.root/a.parquet, .root/b]; struct<id:bigint> | ids [10]; n=1; files [.root/a.parquet]; struct<id:bigint> | **no** |
| `root/dot-dir` | native read_parquet(p) | false | ids [10, 11]; n=2; files [.root/a.parquet, .root/b]; struct<id:bigint> | ids [10]; n=1; files [.root/a.parquet]; id:int64 | **no** |
| `root/dot-dir` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [.root/a.parquet, .root/b]; struct<id:bigint> | ids [10]; n=1; files [.root/a.parquet]; struct<id:bigint> | **no** |
| `root/underscore-dir` | spark.read.format("parquet").load(p) | false | ids [10, 11]; n=2; files [_root/a.parquet, _root/b]; struct<id:bigint> | ids [10]; n=1; files [_root/a.parquet]; struct<id:bigint> | **no** |
| `root/underscore-dir` | native read_parquet(p) | false | ids [10, 11]; n=2; files [_root/a.parquet, _root/b]; struct<id:bigint> | ids [10]; n=1; files [_root/a.parquet]; id:int64 | **no** |
| `root/underscore-dir` | spark.read.parquet(p) | false | ids [10, 11]; n=2; files [_root/a.parquet, _root/b]; struct<id:bigint> | ids [10]; n=1; files [_root/a.parquet]; struct<id:bigint> | **no** |
| `single/bin` | spark.read.format("parquet").load(p) | false | ids [11]; n=1; files [b.bin]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/bin` | native read_parquet(p) | false | ids [11]; n=1; files [b.bin]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/bin` | spark.read.parquet(p) | false | ids [11]; n=1; files [b.bin]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/copying` | spark.read.format("parquet").load(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | yes |
| `single/copying` | native read_parquet(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | yes |
| `single/copying` | spark.read.parquet(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | yes |
| `single/hidden-dot` | spark.read.format("parquet").load(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [.b.parquet]; struct<id:bigint> | **no** |
| `single/hidden-dot` | native read_parquet(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [.b.parquet]; id:int64 | **no** |
| `single/hidden-dot` | spark.read.parquet(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [.b.parquet]; struct<id:bigint> | **no** |
| `single/hidden-underscore` | spark.read.format("parquet").load(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [_b.parquet]; struct<id:bigint> | **no** |
| `single/hidden-underscore` | native read_parquet(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [_b.parquet]; id:int64 | **no** |
| `single/hidden-underscore` | spark.read.parquet(p) | false | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [_b.parquet]; struct<id:bigint> | **no** |
| `single/no-ext` | spark.read.format("parquet").load(p) | false | ids [11]; n=1; files [b]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/no-ext` | native read_parquet(p) | false | ids [11]; n=1; files [b]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/no-ext` | spark.read.parquet(p) | false | ids [11]; n=1; files [b]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/upper` | spark.read.format("parquet").load(p) | false | ids [11]; n=1; files [b.PARQUET]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/upper` | native read_parquet(p) | false | ids [11]; n=1; files [b.PARQUET]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/upper` | spark.read.parquet(p) | false | ids [11]; n=1; files [b.PARQUET]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `glob/p-star` | spark.read.format("parquet").load(p) | true | ids [1, 2, 3, 4]; n=4; files [p=1/part-00000, p=2/part-00000.parquet, p=3/part-00000.snappy.parquet, p=4/data.bin]; struct<id:bigint> | refuses: AnalysisException Error during planning: No files found at file://… | **no** |
| `glob/p-star` | native read_parquet(p) | true | ids [1, 2, 3, 4]; n=4; files [p=1/part-00000, p=2/part-00000.parquet, p=3/part-00000.snappy.parquet, p=4/data.bin]; struct<id:bigint> | refuses: AnalysisException Error during planning: No files found at file://… | **no** |
| `glob/p-star` | spark.read.parquet(p) | true | ids [1, 2, 3, 4]; n=4; files [p=1/part-00000, p=2/part-00000.parquet, p=3/part-00000.snappy.parquet, p=4/data.bin]; struct<id:bigint> | refuses: AnalysisException Error during planning: No files found at file://… | **no** |
| `glob/star-flat` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `glob/star-flat` | native read_parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `glob/star-flat` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `glob/star-hidden-explicit` | spark.read.format("parquet").load(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [_b.parquet]; struct<id:bigint> | **no** |
| `glob/star-hidden-explicit` | native read_parquet(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [_b.parquet]; id:int64 | **no** |
| `glob/star-hidden-explicit` | spark.read.parquet(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [_b.parquet]; struct<id:bigint> | **no** |
| `glob/star-parquet` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `glob/star-parquet` | native read_parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `glob/star-parquet` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `iceberg/control` | - | true | ids [1, 2]; n=2; files []; struct<id:bigint> | ids [1, 2]; n=2; files []; struct<id:bigint> | yes |
| `layout/flat-no-ext` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `layout/flat-no-ext` | native read_parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `layout/flat-no-ext` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `layout/hidden-dirs` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `layout/hidden-dirs` | native read_parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `layout/hidden-dirs` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `layout/hidden-partition-dirs` | spark.read.format("parquet").load(p) | true | refuses: SparkRuntimeException CONFLICTING_PARTITION_COLUMN_NAMES | ids [1, 2, 3]; n=3; files [.p=3/c.parquet, _p=2/b.parquet, p=1/a.parquet]; struct<id:bigint> | **no** |
| `layout/hidden-partition-dirs` | native read_parquet(p) | true | refuses: SparkRuntimeException CONFLICTING_PARTITION_COLUMN_NAMES | ids [1, 2, 3]; n=3; files [.p=3/c.parquet, _p=2/b.parquet, p=1/a.parquet]; id:int64 | **no** |
| `layout/hidden-partition-dirs` | spark.read.parquet(p) | true | refuses: SparkRuntimeException CONFLICTING_PARTITION_COLUMN_NAMES | ids [1, 2, 3]; n=3; files [.p=3/c.parquet, _p=2/b.parquet, p=1/a.parquet]; struct<id:bigint> | **no** |
| `layout/nested-mixed` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `layout/nested-mixed` | native read_parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `layout/nested-mixed` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `layout/nested-plain` | spark.read.format("parquet").load(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | refuses: AnalysisException Error during planning: No files found at file://… | yes |
| `layout/nested-plain` | native read_parquet(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | refuses: AnalysisException Error during planning: No files found at file://… | yes |
| `layout/nested-plain` | spark.read.parquet(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | refuses: AnalysisException Error during planning: No files found at file://… | yes |
| `layout/partitioned-card` | spark.read.format("parquet").load(p) | true | ids [(1, 1), (2, 2), (3, 3), (4, 4)]; n=4; files [p=1/part-00000, p=2/part-00000.parquet, p=3/part-00000.snappy.parquet, p=4/data.bin]; struct<id:bigint,p:int> | ids [2, 3]; n=2; files [p=2/part-00000.parquet, p=3/part-00000.snappy.parquet]; struct<id:bigint> | **no** |
| `layout/partitioned-card` | native read_parquet(p) | true | ids [(1, 1), (2, 2), (3, 3), (4, 4)]; n=4; files [p=1/part-00000, p=2/part-00000.parquet, p=3/part-00000.snappy.parquet, p=4/data.bin]; struct<id:bigint,p:int> | ids [2, 3]; n=2; files [p=2/part-00000.parquet, p=3/part-00000.snappy.parquet]; id:int64 | **no** |
| `layout/partitioned-card` | spark.read.parquet(p) | true | ids [(1, 1), (2, 2), (3, 3), (4, 4)]; n=4; files [p=1/part-00000, p=2/part-00000.parquet, p=3/part-00000.snappy.parquet, p=4/data.bin]; struct<id:bigint,p:int> | ids [2, 3]; n=2; files [p=2/part-00000.parquet, p=3/part-00000.snappy.parquet]; struct<id:bigint> | **no** |
| `layout/partitioned-markers` | spark.read.format("parquet").load(p) | true | ids [(1, 1), (2, 1)]; n=2; files [p=1/a.parquet, p=1/b]; struct<id:bigint,p:int> | ids [1]; n=1; files [p=1/a.parquet]; struct<id:bigint> | **no** |
| `layout/partitioned-markers` | native read_parquet(p) | true | ids [(1, 1), (2, 1)]; n=2; files [p=1/a.parquet, p=1/b]; struct<id:bigint,p:int> | ids [1]; n=1; files [p=1/a.parquet]; id:int64 | **no** |
| `layout/partitioned-markers` | spark.read.parquet(p) | true | ids [(1, 1), (2, 1)]; n=2; files [p=1/a.parquet, p=1/b]; struct<id:bigint,p:int> | ids [1]; n=1; files [p=1/a.parquet]; struct<id:bigint> | **no** |
| `list/file-and-no-ext` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | refuses: AnalysisException Error during planning: No files found at file://… | **no** |
| `list/file-and-no-ext` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | refuses: TypeError DataFrameReader.parquet() takes 2 positional arguments but 3 | **no** |
| `list/two-dirs` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [d1/a.parquet, d2/b]; struct<id:bigint> | refuses: AnalysisException Error during planning: No files found at file://… | **no** |
| `list/two-dirs` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [d1/a.parquet, d2/b]; struct<id:bigint> | refuses: TypeError DataFrameReader.parquet() takes 2 positional arguments but 3 | **no** |
| `marker/.part-00000.parquet.crc` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/.part-00000.parquet.crc` | native read_parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `marker/.part-00000.parquet.crc` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/.z.parquet.crc` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/.z.parquet.crc` | native read_parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `marker/.z.parquet.crc` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/README.txt` | spark.read.format("parquet").load(p) | true | refuses: Py4JJavaError An error occurred while calling o2278.load. | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/README.txt` | native read_parquet(p) | true | refuses: Py4JJavaError An error occurred while calling o2266.parquet. | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `marker/README.txt` | spark.read.parquet(p) | true | refuses: Py4JJavaError An error occurred while calling o2266.parquet. | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/_SUCCESS` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/_SUCCESS` | native read_parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `marker/_SUCCESS` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/_committed_123` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/_committed_123` | native read_parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `marker/_committed_123` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/_common_metadata` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [_common_metadata, a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/_common_metadata` | native read_parquet(p) | true | ids [10]; n=1; files [_common_metadata, a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `marker/_common_metadata` | spark.read.parquet(p) | true | ids [10]; n=1; files [_common_metadata, a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/_metadata` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [_metadata, a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/_metadata` | native read_parquet(p) | true | ids [10]; n=1; files [_metadata, a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `marker/_metadata` | spark.read.parquet(p) | true | ids [10]; n=1; files [_metadata, a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/_started_123` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/_started_123` | native read_parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `marker/_started_123` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `marker/all-writer-markers` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [part-00000-x.snappy.parquet, part-00001-x.snappy.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [part-00000-x.snappy.parquet, part-00001-x.snappy.parquet]; struct<id:bigint> | yes |
| `marker/all-writer-markers` | native read_parquet(p) | true | ids [10, 11]; n=2; files [part-00000-x.snappy.parquet, part-00001-x.snappy.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [part-00000-x.snappy.parquet, part-00001-x.snappy.parquet]; id:int64 | yes |
| `marker/all-writer-markers` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [part-00000-x.snappy.parquet, part-00001-x.snappy.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [part-00000-x.snappy.parquet, part-00001-x.snappy.parquet]; struct<id:bigint> | yes |
| `marker/c.parquet` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [a.parquet, c.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/c.parquet` | native read_parquet(p) | true | ids [10]; n=1; files [a.parquet, c.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `marker/c.parquet` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.parquet, c.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/c` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [a.parquet, c]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `marker/c` | native read_parquet(p) | true | ids [10]; n=1; files [a.parquet, c]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `marker/c` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.parquet, c]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/.b.parquet` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [.b.parquet, a.parquet]; struct<id:bigint> | **no** |
| `name/.b.parquet` | native read_parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [.b.parquet, a.parquet]; id:int64 | **no** |
| `name/.b.parquet` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [.b.parquet, a.parquet]; struct<id:bigint> | **no** |
| `name/_b.parquet` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [_b.parquet, a.parquet]; struct<id:bigint> | **no** |
| `name/_b.parquet` | native read_parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [_b.parquet, a.parquet]; id:int64 | **no** |
| `name/_b.parquet` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [_b.parquet, a.parquet]; struct<id:bigint> | **no** |
| `name/_k=1.parquet` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [_k=1.parquet, a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [_k=1.parquet, a.parquet]; struct<id:bigint> | yes |
| `name/_k=1.parquet` | native read_parquet(p) | true | ids [10, 11]; n=2; files [_k=1.parquet, a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [_k=1.parquet, a.parquet]; id:int64 | yes |
| `name/_k=1.parquet` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [_k=1.parquet, a.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [_k=1.parquet, a.parquet]; struct<id:bigint> | yes |
| `name/b.PARQUET` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [a.parquet, b.PARQUET]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b.PARQUET` | native read_parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b.PARQUET]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `name/b.PARQUET` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b.PARQUET]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b.bin` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [a.parquet, b.bin]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b.bin` | native read_parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b.bin]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `name/b.bin` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b.bin]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b.parquet._COPYING_` | spark.read.format("parquet").load(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `name/b.parquet._COPYING_` | native read_parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | yes |
| `name/b.parquet._COPYING_` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | yes |
| `name/b.parquet` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [a.parquet, b.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [a.parquet, b.parquet]; struct<id:bigint> | yes |
| `name/b.parquet` | native read_parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [a.parquet, b.parquet]; id:int64 | yes |
| `name/b.parquet` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [a.parquet, b.parquet]; struct<id:bigint> | yes |
| `name/b.parq` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [a.parquet, b.parq]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b.parq` | native read_parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b.parq]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `name/b.parq` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b.parq]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b.snappy.parquet` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [a.parquet, b.snappy.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [a.parquet, b.snappy.parquet]; struct<id:bigint> | yes |
| `name/b.snappy.parquet` | native read_parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b.snappy.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [a.parquet, b.snappy.parquet]; id:int64 | yes |
| `name/b.snappy.parquet` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b.snappy.parquet]; struct<id:bigint> | ids [10, 11]; n=2; files [a.parquet, b.snappy.parquet]; struct<id:bigint> | yes |
| `name/b.x.y.z` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [a.parquet, b.x.y.z]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b.x.y.z` | native read_parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b.x.y.z]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `name/b.x.y.z` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b.x.y.z]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `name/b` | native read_parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; id:int64 | **no** |
| `name/b` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | **no** |
| `neighbour/csv/.b.csv` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.csv]; struct<_c0:string> | ids [10, 11]; n=2; files [.b.csv, a.csv]; struct<_c0:string> | **no** |
| `neighbour/csv/_b.csv` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.csv]; struct<_c0:string> | ids [10, 11]; n=2; files [_b.csv, a.csv]; struct<_c0:string> | **no** |
| `neighbour/csv/b.CSV` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.csv, b.CSV]; struct<_c0:string> | ids [10]; n=1; files [a.csv]; struct<_c0:string> | **no** |
| `neighbour/csv/b.bin` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.csv, b.bin]; struct<_c0:string> | ids [10]; n=1; files [a.csv]; struct<_c0:string> | **no** |
| `neighbour/csv/b.csv._COPYING_` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.csv]; struct<_c0:string> | ids [10]; n=1; files [a.csv]; struct<_c0:string> | yes |
| `neighbour/csv/b` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.csv, b]; struct<_c0:string> | ids [10]; n=1; files [a.csv]; struct<_c0:string> | **no** |
| `neighbour/csv/markers` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.csv]; struct<_c0:string> | ids [10]; n=1; files [a.csv]; struct<_c0:string> | yes |
| `neighbour/json/.b.json` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.json]; struct<id:bigint> | ids [10, 11]; n=2; files [.b.json, a.json]; struct<id:bigint> | **no** |
| `neighbour/json/_b.json` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.json]; struct<id:bigint> | ids [10, 11]; n=2; files [_b.json, a.json]; struct<id:bigint> | **no** |
| `neighbour/json/b.JSON` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.json, b.JSON]; struct<id:bigint> | ids [10]; n=1; files [a.json]; struct<id:bigint> | **no** |
| `neighbour/json/b.bin` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.json, b.bin]; struct<id:bigint> | ids [10]; n=1; files [a.json]; struct<id:bigint> | **no** |
| `neighbour/json/b.json._COPYING_` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.json]; struct<id:bigint> | ids [10]; n=1; files [a.json]; struct<id:bigint> | yes |
| `neighbour/json/b` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.json, b]; struct<id:bigint> | ids [10]; n=1; files [a.json]; struct<id:bigint> | **no** |
| `neighbour/json/markers` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.json]; struct<id:bigint> | ids [10]; n=1; files [a.json]; struct<id:bigint> | yes |
| `neighbour/orc/.b.orc` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.orc]; struct<id:bigint> | ids [10]; n=1; files []; struct<id:bigint> | **no** |
| `neighbour/orc/_b.orc` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.orc]; struct<id:bigint> | ids [10]; n=1; files []; struct<id:bigint> | **no** |
| `neighbour/orc/b.ORC` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.orc, b.ORC]; struct<id:bigint> | ids [10, 11]; n=2; files []; struct<id:bigint> | **no** |
| `neighbour/orc/b.bin` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.orc, b.bin]; struct<id:bigint> | ids [10, 11]; n=2; files []; struct<id:bigint> | **no** |
| `neighbour/orc/b.orc._COPYING_` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.orc]; struct<id:bigint> | ids [10, 11]; n=2; files []; struct<id:bigint> | **no** |
| `neighbour/orc/b` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.orc, b]; struct<id:bigint> | ids [10, 11]; n=2; files []; struct<id:bigint> | **no** |
| `neighbour/orc/markers` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.orc]; struct<id:bigint> | ids [10]; n=1; files []; struct<id:bigint> | **no** |
| `neighbour/text/.b.txt` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.txt]; struct<value:string> | ids [10]; n=1; files []; struct<value:string> | **no** |
| `neighbour/text/_b.txt` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.txt]; struct<value:string> | ids [10]; n=1; files []; struct<value:string> | **no** |
| `neighbour/text/b.TXT` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.txt, b.TXT]; struct<value:string> | ids [10, 11]; n=2; files []; struct<value:string> | **no** |
| `neighbour/text/b.bin` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.txt, b.bin]; struct<value:string> | ids [10, 11]; n=2; files []; struct<value:string> | **no** |
| `neighbour/text/b.txt._COPYING_` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.txt]; struct<value:string> | ids [10, 11]; n=2; files []; struct<value:string> | **no** |
| `neighbour/text/b` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.txt, b]; struct<value:string> | ids [10, 11]; n=2; files []; struct<value:string> | **no** |
| `neighbour/text/markers` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.txt]; struct<value:string> | ids [10]; n=1; files []; struct<value:string> | **no** |
| `option/ignoreCorruptFiles` | spark.read.parquet(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | refuses: AnalysisException reader option 'ignoreCorruptFiles' is not supported by repar | yes |
| `option/ignoreMissingFiles` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | refuses: AnalysisException reader option 'ignoreMissingFiles' is not supported by repar | **no** |
| `option/mergeSchema` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | refuses: AnalysisException reader option 'mergeSchema' is not supported by repark yet ( | **no** |
| `option/pathGlobFilter` | spark.read.parquet(p) | true | ids [10]; n=1; files [a.parquet]; struct<id:bigint> | refuses: AnalysisException reader option 'pathGlobFilter' is not supported by repark ye | **no** |
| `option/recursiveFileLookup` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [a.parquet, b]; struct<id:bigint> | refuses: AnalysisException reader option 'recursiveFileLookup' is not supported by repa | **no** |
| `root/dot-dir` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [.root/a.parquet, .root/b]; struct<id:bigint> | ids [10]; n=1; files [.root/a.parquet]; struct<id:bigint> | **no** |
| `root/dot-dir` | native read_parquet(p) | true | ids [10, 11]; n=2; files [.root/a.parquet, .root/b]; struct<id:bigint> | ids [10]; n=1; files [.root/a.parquet]; id:int64 | **no** |
| `root/dot-dir` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [.root/a.parquet, .root/b]; struct<id:bigint> | ids [10]; n=1; files [.root/a.parquet]; struct<id:bigint> | **no** |
| `root/underscore-dir` | spark.read.format("parquet").load(p) | true | ids [10, 11]; n=2; files [_root/a.parquet, _root/b]; struct<id:bigint> | ids [10]; n=1; files [_root/a.parquet]; struct<id:bigint> | **no** |
| `root/underscore-dir` | native read_parquet(p) | true | ids [10, 11]; n=2; files [_root/a.parquet, _root/b]; struct<id:bigint> | ids [10]; n=1; files [_root/a.parquet]; id:int64 | **no** |
| `root/underscore-dir` | spark.read.parquet(p) | true | ids [10, 11]; n=2; files [_root/a.parquet, _root/b]; struct<id:bigint> | ids [10]; n=1; files [_root/a.parquet]; struct<id:bigint> | **no** |
| `single/bin` | spark.read.format("parquet").load(p) | true | ids [11]; n=1; files [b.bin]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/bin` | native read_parquet(p) | true | ids [11]; n=1; files [b.bin]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/bin` | spark.read.parquet(p) | true | ids [11]; n=1; files [b.bin]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/copying` | spark.read.format("parquet").load(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | yes |
| `single/copying` | native read_parquet(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | yes |
| `single/copying` | spark.read.parquet(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | yes |
| `single/hidden-dot` | spark.read.format("parquet").load(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [.b.parquet]; struct<id:bigint> | **no** |
| `single/hidden-dot` | native read_parquet(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [.b.parquet]; id:int64 | **no** |
| `single/hidden-dot` | spark.read.parquet(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [.b.parquet]; struct<id:bigint> | **no** |
| `single/hidden-underscore` | spark.read.format("parquet").load(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [_b.parquet]; struct<id:bigint> | **no** |
| `single/hidden-underscore` | native read_parquet(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [_b.parquet]; id:int64 | **no** |
| `single/hidden-underscore` | spark.read.parquet(p) | true | refuses: AnalysisException UNABLE_TO_INFER_SCHEMA | ids [11]; n=1; files [_b.parquet]; struct<id:bigint> | **no** |
| `single/no-ext` | spark.read.format("parquet").load(p) | true | ids [11]; n=1; files [b]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/no-ext` | native read_parquet(p) | true | ids [11]; n=1; files [b]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/no-ext` | spark.read.parquet(p) | true | ids [11]; n=1; files [b]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/upper` | spark.read.format("parquet").load(p) | true | ids [11]; n=1; files [b.PARQUET]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/upper` | native read_parquet(p) | true | ids [11]; n=1; files [b.PARQUET]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |
| `single/upper` | spark.read.parquet(p) | true | ids [11]; n=1; files [b.PARQUET]; struct<id:bigint> | refuses: PySparkException datafusion engine error: Execution error: File path 'file:// | **no** |

- spark.read.load(p): refuses on main in all 82 cells (both modes); Spark answers as `spark.read.parquet`.
- SELECT * FROM parquet.`p`: refuses on main in all 82 cells (both modes); Spark answers as `spark.read.parquet`.
- CREATE TEMPORARY VIEW v USING parquet OPTIONS (path p): refuses on main in all 82 cells (both modes); Spark answers as `spark.read.parquet`.

Writes and round trips (ids only; Spark's own part names differ per run):

| case | ANSI | Spark | main | head |
|---|---|---|---|---|
| append beside `b`, read by the same engine | false | ids [10, 11, 20]; n=3 | ids [10, 20]; n=2 | - |
| overwrite beside `b`, read by the same engine | false | ids [20]; n=1 | ids [20]; n=1 | - |
| Spark's append dir read by RePark | false | ids [10, 11, 20]; n=3 | ids [10, 20]; n=2 | - |
| Spark's overwrite dir read by RePark | false | ids [20]; n=1 | ids [20]; n=1 | - |
| RePark's append dir read by Spark | false | ids [10, 11, 20]; n=3 | - | - |
| RePark's overwrite dir read by Spark | false | ids [20]; n=1 | - | - |
| Spark-written flat dir (markers) read by RePark | false | ids [1, 2, 3] written | ids [1, 2, 3]; n=3 | - |
| Spark-written partitioned dir read by RePark | false | ids [1, 2, 3] written | ids [1, 2, 3]; n=3 | - |
| append beside `b`, read by the same engine | true | ids [10, 11, 20]; n=3 | ids [10, 20]; n=2 | - |
| overwrite beside `b`, read by the same engine | true | ids [20]; n=1 | ids [20]; n=1 | - |
| Spark's append dir read by RePark | true | ids [10, 11, 20]; n=3 | ids [10, 20]; n=2 | - |
| Spark's overwrite dir read by RePark | true | ids [20]; n=1 | ids [20]; n=1 | - |
| RePark's append dir read by Spark | true | ids [10, 11, 20]; n=3 | - | - |
| RePark's overwrite dir read by Spark | true | ids [20]; n=1 | - | - |
| Spark-written flat dir (markers) read by RePark | true | ids [1, 2, 3] written | ids [1, 2, 3]; n=3 | - |
| Spark-written partitioned dir read by RePark | true | ids [1, 2, 3] written | ids [1, 2, 3]; n=3 | - |
