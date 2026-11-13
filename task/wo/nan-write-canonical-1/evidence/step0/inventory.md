# NAN-WRITE-CANONICAL-1 step 0 — the write-door inventory and the seat (rule 3)

Main `1d9d2808`, fork pin `92242cb4`. Line numbers are on those trees. "Measured" names the table in
[table.md](table.md) that holds the door's stored bits; `u_`/`p_` are the unpartitioned and partitioned forms.

## How the inventory was built (repeatable)

1. **Doors from the router and the facade.** `crates/repark-spark/src/write_door.rs` enumerates the Spark-door and
   native write doors (`WriteDoor::ALL`, D1–D21); `crates/repark-spark/src/router.rs` `execute_inner` (line 291)
   dispatches each statement; `python/repark/src/repark/spark/dataframe/writer_readwriter.py` holds the DataFrame
   doors, each of which issues SQL into that router.
2. **Writers from the bottom up.** Every Iceberg data file is written by the fork's `DataFileWriter`. The closure:
   - `rg -n 'DataFileWriterBuilder::new' crates --glob '!**/tests/**' --glob '!*_tests.rs'` gives the four RePark
     sites below; the fork's DataFusion insert path builds its own (fork `physical_plan/write.rs:330`).
   - `rg -n 'ParquetWriterBuilder::new|ArrowWriter::try_new|EqualityDelete' crates` finds no other data-file
     writer: `position_delete.rs:293` writes position-delete files (no float column), `writer_props.rs:100` and
     `write_options.rs:330` feed the four sites, `microbatch/provider.rs:226,351` are inside `#[cfg(test)]`, and
     `repark-core/src/session/path_write.rs:483` is the plain-file (non-Iceberg) empty-part writer.
3. **Partition values from the bottom up.** `rg -n 'PartitionValueCalculator|RecordBatchPartitionSplitter|partition_value_exprs'`
   over both trees gives every place a partition value is computed or a row is routed by it (section "Partition
   values" below).
4. **Measured.** `write_doors_probe.py` drives each door on both engines and reads the bits back from the Parquet
   files and the Avro manifests (`avro_bits.py`), never through an engine.

## Data-file writer sites (every door ends in one)

| site | builds `DataFileWriter` for |
|---|---|
| fork `crates/integrations/datafusion/src/physical_plan/write.rs:330` (`IcebergWriteExec::execute`) | DataFusion `insert_into` on the fork's `IcebergTableProvider` (`table/mod.rs:244`): plain INSERT (D1), native INSERT (D14/D15) |
| `crates/repark-iceberg/src/write/write_options.rs:364` | the staged unpartitioned writers (`write_data_files*`, `stage_overwrite_files_with`, `append_staged_with_options`) |
| `crates/repark-iceberg/src/write/append_fanout_serial.rs:90` | every partitioned RePark fan-out (`fanout_conformed_stream_serial_with_abort`, line 40; splitter at line 54) |
| `crates/repark-iceberg/src/write/merge/session_staging.rs:130` | MERGE and predicate UPDATE/DELETE staging (`write_new_data_files_from_stream_with`, line 26) |
| `crates/repark-iceberg/src/write/merge/row_lineage.rs:205` | format-v3 row-lineage MERGE writes (`split_lineage_batch`, line 227) |
| fork `crates/integrations/datafusion/src/physical_plan/row_lineage.rs:309` (`StreamingDataFileWriter`) | the fork's DataFusion DELETE / UPDATE (`IcebergDeleteExec`, `IcebergUpdateExec`): native UPDATE and DELETE, and any Spark-door DELETE or UPDATE that falls through to DataFusion |

Every site builds its file writer through the fork's `AnyFileWriterBuilder` (`writer/file_writer/any_writer.rs:50-84`), with the format from the table's `write.format.default` or the per-write `write-format` option (`write/data_format.rs`): Parquet, ORC or Avro. The step 0 table measured Parquet; [formats.md](formats.md) measures all three.

## Doors

| door | entry (file:line) | executor (file:line) | reaches | measured |
|---|---|---|---|---|
| `INSERT INTO … SELECT` (D1) | `repark-spark/src/router.rs:486` | `catalog_ops.rs:85` `passthrough_after_p11` → DataFusion → fork `IcebergWriteExec` | fork `write.rs:330` | `insert` |
| `INSERT` with options / session write conf (D2) | `router.rs:465,474` | `append_with_options.rs:10` → `append_staged_with_options` / `stage_overwrite_files_with` | `write_options.rs:364`, `append_fanout_serial.rs:90` | via `writeTo().append()` / `saveAsTable` append (`writeto_append`, `saveastable_append`) |
| `INSERT … BY NAME` (D3) | `router.rs:312,446` | `insert_by_name.rs:49` | as D2 / D4 | `writeto_append` (by-name append) |
| `INSERT OVERWRITE` (D4) | `router.rs:460` | `insert_overwrite.rs:40` (static: `commit_overwrite_replace_all_with_summary`; dynamic: `partition_overwrite`) | `write_options.rs:364`, `append_fanout_serial.rs:90` | `insert_overwrite`, `writeto_overwrite_partitions` |
| `INSERT … REPLACE WHERE` (D5) | `router.rs:307` | `router/insert_positional/replace_where.rs:186` | `write_options.rs:364`, `append_fanout_serial.rs:90` | `writeto_overwrite` (`overwrite(cond)`) |
| CTAS, RTAS (D6) | `router.rs:345` | `ctas.rs:156` `execute_ctas` → `ctas.rs:441` `write_data_files_from_plan` | `write_options.rs:364`, `append_fanout_serial.rs:90` | `ctas`, `rtas` |
| MERGE insert / update (D7–D9) | `router.rs` `Statement::Merge` → `merge.rs:46` | `repark-iceberg/src/write/merge/` (`copy_on_write.rs:83`, `merge_on_read.rs:88`) | `session_staging.rs:130`; v3 lineage `row_lineage.rs:205` | `merge_insert`, `merge_update_cow`, `merge_update_mor` |
| UPDATE (D10), DELETE | `router.rs` → `router/delete_update.rs:11,60` | `predicate_dml` (`cow_commit.rs:62,134`, `mor_commit.rs`) or the fork's provider | `session_staging.rs:130` | `update_cow`, `update_mor`, `delete_cow`, `delete_mor` |
| `writeTo().create()` / `createOrReplace()` / `replace()` | `writer_readwriter.py:798,814,824` | `_run_ctas` → CTAS / RTAS (D6) | as D6 | `writeto_create`, `writeto_create_or_replace` |
| `writeTo().append()` | `writer_readwriter.py:852` | by-name append (D3/D2) | as D2 | `writeto_append` |
| `writeTo().overwrite(cond)` | `writer_readwriter.py:868` | REPLACE WHERE (D5) | as D5 | `writeto_overwrite` |
| `writeTo().overwritePartitions()` | `writer_readwriter.py:859` | dynamic INSERT OVERWRITE (D4) | as D4 | `writeto_overwrite_partitions` |
| `saveAsTable` append / overwrite | `writer_readwriter.py:194` → `writer_save.py:70` `write_table` | by-name append (D2/D3) / RTAS (D6) | as D2 / D6 | `saveastable_append`, `saveastable_overwrite` |
| `save(<table name>)` | `writer_readwriter.py:336` → `writer_save.py:99` | `write_table` | as `saveAsTable` | `save_name` |
| `save(<path>)` (Iceberg path) | `writer_save.py:99` | refused on main (`TABLE_OR_VIEW_NOT_FOUND`): RePark has no Iceberg path write | — | `path_append` (refused) |
| `insertInto` | `writer_readwriter.py:215` | `INSERT INTO`/`INSERT OVERWRITE … SELECT * FROM <view>` (D1/D4) | as D1 / D4 | `insertinto` |
| native INSERT (D14, D15) | `repark.sql()`: `repark-core/src/dialect.rs:145` `DataFusionDialect` in `local_fs_policy.rs:51` (stock DataFusion; `repark-sql` is on no door) | DataFusion → fork `table/mod.rs:244` `insert_into` (partition values from fork `project.rs:93`) | fork `write.rs:330` | `native_insert` |
| native INSERT OVERWRITE (D16) | as D14 | DataFusion `InsertOp::Overwrite` → fork `insert_into`, committed at fork `physical_plan/commit.rs:412` | fork `write.rs:330` | not measured: the native door registers only memory catalogs and cannot create a partitioned table, so it cannot reach a partitioned table another door made |
| native CTAS (D17) | as D14 | refused by `repark-core/src/local_fs_policy/native_ctas.rs:14`, reached from `local_fs_policy.rs:102` | — | `native_ctas` (refused) |
| native UPDATE / DELETE (D20) | as D14 | DataFusion → fork `table/mod.rs:368` / `:311` → `update.rs:35-36,173` / `delete.rs:656,819,968` | fork `physical_plan/row_lineage.rs:309` `StreamingDataFileWriter` (partition values at `:338`) | `native_update`, `native_delete` |
| native MERGE (D19) | as D14 | refused by DataFusion (`Unsupported SQL statement: MERGE`) | — | `native_merge` (refused) |
| Rust `append()` (D18) | `repark-iceberg/src/write/append.rs:33` | `write_data_files_with_concurrency` / `fanout_data_files_with_concurrency` | `write_options.rs:364`, `append_fanout_serial.rs:90` | not measured (no Python door) |
| micro-batch sink (D21) | `repark-core/src/microbatch/run.rs:351,465,482` | `append` / `stage_overwrite_files_with` | as D18 | not measured (streaming track frozen) |
| `CALL … rewrite_data_files` (and `run_maintenance` `call/run_maintenance_apply.rs:122`, `apply_partitioning` `call/apply_partitioning.rs:199`, which reach it) | `repark-spark/src/router.rs` `Statement::Call` | fork `maintenance/rewrite_data_files_write.rs:265` (splitter), `:278,:358,:411` (`DataFileWriter` over `AnyFileWriterBuilder`) | yes | `rewrite_data_files` per format (`formats.md`) |
| `CALL add_files` | `repark-spark/src/call/add_files.rs:22` → fork `maintenance::AddFiles` | imports files as they are | writes data files: no | — |
| `CALL rewrite_manifests`, `CALL rewrite_table_path` | `Statement::Call` | copy existing manifests, partition values and files | writes data files: no | — |
| fork `RepairPartitionKeys` | fork `maintenance/partition_key_audit.rs:533` (bare `ParquetWriterBuilder`) | no RePark caller | not a door | — |

## Partition values (who computes them, who routes by them)

| site | role | NaN today |
|---|---|---|
| fork `crates/iceberg/src/arrow/partition_value_calculator.rs:212` `calculate_from_columns` | computes the partition struct for every caller below | raw bits |
| fork `arrow/record_batch_partition_splitter.rs:128` `split` (computed values: `append_fanout_serial.rs:54`, fork `rewrite_data_files_write.rs:265`; precomputed `_partition`: fork `task_writer.rs:138`) | groups rows: floats group row-wise by `Struct` equality (NaN equal to NaN), the first row's bits become the `PartitionKey` | first NaN's raw bits (NAN-PARTITION-RAW-BITS-1) |
| fork `integrations/datafusion/src/physical_plan/project.rs:93` | the `_partition` column of the fork insert path, then `repartition` hashes it | raw bits hashed |
| `crates/repark-iceberg/src/write/distribution/router.rs:22` | the stream router: each batch split to writer slots by a hash of the calculator's output | raw bits hashed (NAN-PARTITION-FILE-SPLIT-1) |
| `crates/repark-iceberg/src/write/merge/row_lineage.rs:165` | v3 lineage calculator, grouped by `Struct` | first NaN's raw bits |
| `crates/repark-iceberg/src/write/distribution.rs:129` `partition_value_exprs` (used by `hash_distribution`, line 157, from `partition_write.rs:229`) | plan-path hash distribution over `PartitionTransformExpr` (not the calculator) | raw bits hashed |

## The seat (corrected after the inventory critic, 2026-11-13)

The critic measured, and [formats.md](formats.md) confirms, that Spark's **Avro** data files keep the raw NaN bits
while its Parquet and ORC files canonicalise; Spark canonicalises partition values on all three formats. The
orchestrator ruled the seat as follows; the build follows it.

1. **Data values:** fork `writer/file_writer/any_writer.rs:158` `AnyFileWriter::write`, Parquet and ORC arms only;
   Avro keeps the bits it is given. Every site above writes through `AnyFileWriter`, after `DataFileWriter::write`'s
   `apply_write_defaults`.
2. **Partition values:** fork `arrow/partition_value_calculator.rs:281`, all formats; it feeds every consumer in the
   partition-value table.
3. **Plan-path routing:** RePark `write/distribution.rs:129` (used by `hash_distribution`, line 157), each float
   partition expression wrapped in the existing `CanonicalFloatExpr::wrap`, so the plan-path doors write one NaN
   partition file.

One function, `canonical_nan_batch` / `canonical_nan_array` in `partition_value_calculator.rs`, does the rewrite:
every NaN of a FLOAT or DOUBLE leaf, top-level or inside a STRUCT, LIST, LARGE LIST, FIXED SIZE LIST, DICTIONARY
value or MAP value, becomes `7ff8000000000000` / `7fc00000`; nothing else changes.

- **Map keys stay out.** Spark stores one NaN key canonical in Parquet and ORC (`k1_*`), but refuses a map with two
  NaN keys (`DUPLICATED_MAP_KEY`) where main writes it (`k2_*`). Rewriting keys would turn that map into one with two
  identical keys, so keys keep their bits (ruling item 6); a single sign-set or payload NaN key in a Parquet or ORC
  map stays unlike Spark.
- **FixedSizeList and Dictionary** are covered by the function rather than argued unreachable: every RePark door
  conforms columns to `schema_to_arrow_schema` types before the writer, but the fork's schema visitor accepts both
  (`arrow/schema.rs:315,362`), so the arms cost nothing to keep.
