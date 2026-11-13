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
| native INSERT (D14, D15) | `repark-sql/src/router.rs:163` | DataFusion `insert_into` → fork | fork `write.rs:330` | `native_insert` |
| native INSERT OVERWRITE / PARTITION (D16) | `repark-sql/src/router.rs:160` → `repark-sql/src/insert_overwrite.rs:23` | `stage_overwrite_files_with`, `stage_static_partition_overwrite_files_with` | `write_options.rs:364`, `append_fanout_serial.rs:90` | not measured (native has no partitioned `CREATE TABLE`) |
| native CTAS (D17) | `repark-sql/src/create_table.rs:621` | refused into Iceberg on main (NATIVE-CTAS-ICEBERG-REFUSE-1) | — | `native_ctas` (refused) |
| native UPDATE / DELETE (D20) | `repark-sql/src/router.rs:176-177` | `commit_identity_dml` / predicate DML | `session_staging.rs:130` | `native_update`, `native_delete` |
| native MERGE (D19) | `repark-sql/src/router.rs:158` | refused in the measured form (`Unsupported SQL statement: MERGE`) | `session_staging.rs:130` when it runs | `native_merge` (refused) |
| Rust `append()` (D18) | `repark-iceberg/src/write/append.rs:33` | `write_data_files_with_concurrency` / `fanout_data_files_with_concurrency` | `write_options.rs:364`, `append_fanout_serial.rs:90` | not measured (no Python door) |
| micro-batch sink (D21) | `repark-core/src/microbatch/run.rs:351,465,482` | `append` / `stage_overwrite_files_with` | as D18 | not measured (streaming track frozen) |
| `CALL … rewrite_data_files` | `repark-spark/src/router.rs` `Statement::Call` | fork `maintenance/rewrite_data_files_write.rs:265` | the fork's `DataFileWriter` | not measured |

## Partition values (who computes them, who routes by them)

| site | role | NaN today |
|---|---|---|
| fork `crates/iceberg/src/arrow/partition_value_calculator.rs:212` `calculate_from_columns` | computes the partition struct for every caller below | raw bits |
| fork `arrow/record_batch_partition_splitter.rs:128` `split` (computed values: `append_fanout_serial.rs:54`, fork `rewrite_data_files_write.rs:265`; precomputed `_partition`: fork `task_writer.rs:138`) | groups rows: floats group row-wise by `Struct` equality (NaN equal to NaN), the first row's bits become the `PartitionKey` | first NaN's raw bits (NAN-PARTITION-RAW-BITS-1) |
| fork `integrations/datafusion/src/physical_plan/project.rs:93` | the `_partition` column of the fork insert path, then `repartition` hashes it | raw bits hashed |
| `crates/repark-iceberg/src/write/distribution/router.rs:22` | the stream router: each batch split to writer slots by a hash of the calculator's output | raw bits hashed (NAN-PARTITION-FILE-SPLIT-1) |
| `crates/repark-iceberg/src/write/merge/row_lineage.rs:165` | v3 lineage calculator, grouped by `Struct` | first NaN's raw bits |
| `crates/repark-iceberg/src/write/distribution.rs:129` `partition_value_exprs` (used by `hash_distribution`, line 157, from `partition_write.rs:229`) | plan-path hash distribution over `PartitionTransformExpr` (not the calculator) | raw bits hashed |

## The seat I intend to use (for the critic; no product code is in this commit)

One canonicalising function in the fork, `canonical_nan_batch` / `canonical_nan_array` (every NaN of a FLOAT or
DOUBLE leaf, top-level or inside a STRUCT, LIST or MAP, becomes `7ff8000000000000` / `7fc00000`; nothing else
changes; a batch with no other NaN is returned untouched), applied at the two fork points every door crosses:

1. **`DataFileWriter::write`** (fork `writer/base_writer/data_file_writer.rs:207`): every data file of every door,
   before the file writer computes its statistics. Stored bits and NaN counts; also every copy-on-write rewrite,
   which matches Spark's rewrite cell.
2. **`PartitionValueCalculator::calculate_from_columns`** (line 281, before the partition struct is built): the
   partition value and the grouping of the fork splitter, the fork insert path's `_partition` hash, RePark's stream
   router and the v3 lineage split. This is "before the partition split".

Files: fork `partition_value_calculator.rs` and `data_file_writer.rs` (two product files), the offline tests in the
fork's `data_file_writer_tests.rs` and the calculator's test module, and the RePark fork pin.

**Open question for the critic.** The plan-path hash (`distribution.rs:129`, RePark) does not go through the
calculator, so the CTAS/RTAS-shaped doors would still route NaN patterns to different writers by their raw bits
until (1) has canonicalised them inside each writer: the stored values and partition values become canonical, but
the NaN partition can still be split across writer files (NAN-PARTITION-FILE-SPLIT-1, S4). Closing that too means
wrapping each float partition expression in the existing `CanonicalFloatExpr::wrap`
(`distribution/canonical_float.rs:17`) — a third product file. I lean to include it and will measure both ways.
EOF
