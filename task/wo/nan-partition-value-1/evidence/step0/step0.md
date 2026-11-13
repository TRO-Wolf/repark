# NAN-PARTITION-VALUE-1 step 0: the seat, its callers, the read half and the sizing

Model `claude-opus-5-5`, 2026-11-12. RePark main `a3f8c7d9`'s release module, fork `66901f02`,
Spark 4.1.2 with Iceberg 1.11.0 (`iceberg-spark-runtime-4.1_2.13-1.11.0.jar`), Hadoop catalogs
on local disk. Rows `(1, 0.0), (2, -0.0), (3, NaN), (4, NULL), (5, 1.5)` in `(id BIGINT, d T)`
for T in DOUBLE and FLOAT; `(6, Infinity), (7, -Infinity)` in a DOUBLE twin. The rendered
cells are in [table-a3f8c7d9.md](table-a3f8c7d9.md); raw legs in [raw/](raw/map.md); probes
in [probe/](probe/map.md).

## 1. The seat, to the line

| Step | Where | NaN after the step |
|---|---|---|
| Arrow batch → partition literal | fork `crates/iceberg/src/arrow/value.rs:324-332` (`Literal::double`, `Literal::float`) | NaN |
| Split into partition keys | fork `crates/iceberg/src/arrow/record_batch_partition_splitter.rs:251-300`, `PartitionKey::new` (`spec/partition.rs:393`) | NaN; the NaN and NULL rows land in two files |
| Data file's partition | fork `crates/iceberg/src/writer/base_writer/data_file_writer.rs:238-239` (`pk.data().clone()`) | NaN |
| **Write exec → commit exec** | fork `crates/integrations/datafusion/src/physical_plan/write.rs:370` calls `serialize_data_file_to_json` (`crates/iceberg/src/spec/manifest/mod.rs:121`): `serde_json::to_string` writes a non-finite `f64`/`f32` as `null` | **`null`** |
| Commit exec reads it back | `physical_plan/commit.rs:323`, `deserialize_data_file_from_json` (`mod.rs:137`): `null` → `RawLiteralEnum::Null` → a NULL slot | NULL |
| Avro manifest | `spec/manifest/writer.rs:470-476` → `RawLiteralEnum::Double` (`spec/values/serde.rs:241-242`) | whatever it is given; lossless |

The stored value, read with fastavro from the written manifests
([raw/manifests-repark-main.jsonl](raw/manifests-repark-main.jsonl)): the INSERT tables hold two
entries with partition `d: null`, one with `nan_value_counts {2: 1}`; the CTAS table holds
`d = NaN`, bits `7ff8000000000000`, the same bits Spark's own manifest holds
([raw/manifests-spark-own.jsonl](raw/manifests-spark-own.jsonl)). The infinity table holds two
`null` partitions; Spark's holds `7ff0…` and `fff0…`.

The doors confirm it: `INSERT`, `writeTo(...).append()` and `saveAsTable` plan through the
fork's `IcebergTableProvider::insert_into` (`crates/integrations/datafusion/src/table/mod.rs:244-300`)
and lose the value; CTAS writes through RePark's fan-out writer
(`crates/repark-iceberg/src/write/append_fanout_serial.rs:54-100`) with no JSON step, and keeps it.

**RePark's `.files` reader adds no NULL of its own.** On the same build it shows `d = NaN` for
the CTAS table and for Spark's own table (cells `files|… ctas`, `files|… repark-reads-spark`).

## 2. Every place the same conversion can lose a NaN

| Caller | Lossy? | RePark reaches it? |
|---|---|---|
| `serialize_data_file_to_json` / `deserialize_data_file_from_json` (`spec/manifest/mod.rs:115-146`), from `IcebergWriteExec` (`physical_plan/write.rs:370`) and `IcebergCommitExec` (`commit.rs:323`) | yes: partition values (the seat) | yes: SQL `INSERT`, `writeTo`, `saveAsTable` (measured); also `write_to_branch.rs:490` and `:602` build the provider directly |
| The same JSON for column bounds and counts | no: bounds travel as bytes, NaN counts as integers | — |
| Manifest Avro (`writer.rs:470-476`) | no | yes |
| Manifest-list partition summaries (`PartitionFieldStats::update`, `writer.rs:525-541`) | no: NaN sets `contains_nan`; on main it saw a NULL instead, a consequence of the seat | yes |
| `.partitions` / `.files` metadata tables | no: they read the manifest; on main they show the stored NULL (`ICEBERG-PARTITIONS-TABLE-FLOAT-1`'s NaN half) | yes |
| Partition statistics files (`maintenance/partition_stats.rs:992`, `arrow_struct_to_literal`) | no: Arrow, not JSON; they read the manifest's value | only through `compute_partition_stats` |
| `Literal::try_into_json` (`spec/values/literal.rs:630-640`), from `NestedField` default serialization (`spec/datatypes.rs:683, 688`), the Avro schema default (`avro/schema.rs:92`), the Parquet footer schema (`writer/file_writer/parquet_footer.rs:66, 72`) and the default check (`transaction/update_schema.rs:1412`) | yes, for a NaN column default; not a partition value | only for a FLOAT/DOUBLE column with a NaN `DEFAULT`; not measured, out of this unit |
| `Datum` JSON (`spec/values/datum.rs:128`, predicates) | yes, for a NaN literal in a serialized predicate | not on a RePark write path |
| Snapshot summary partition entries (`partition_to_path`) | no: text `d=NaN` | yes |

## 3. The read half (Spark wrote the table)

RePark adopted Spark's tables with `CALL … register_table` and ran `SELECT *`, `isnan`, `= NaN`,
`d > 1.0`, `IS NULL`, `count(*)` and `.files` (DOUBLE, FLOAT, and the infinity twin). Every cell
is EQUAL to Spark's answer on the same table. Note `d > 1.0` on the partitioned tables: Spark
itself answers `[5]` (its scan prunes the NaN partition), where the unpartitioned table
answers `[3, 5]`; RePark answers the same on both. Nothing on the read half is in this unit.

## 4. Controls (recorded)

- INT and STRING partitions with a NULL: `.files` `{7, NULL}` and `{'a', NULL}` on both engines.
- The unpartitioned DOUBLE and FLOAT tables: `isnan` `[3]`, `= NaN` `[3]`, `d > 1.0` `[3, 5]`.
- Signed zero: both engines store one `0.0` partition with 2 records (bits `0000…`). Part B owns it.

## 5. Java's JSON for a non-finite double

`SingleValueParser.toJson(DoubleType, NaN)` writes `"NaN"` (a JSON string), and `"Infinity"`,
`"-Infinity"` the same way. `SingleValueParser.fromJson` refuses all three ("Cannot parse
default as a double value") and refuses the bare token `NaN`. The spec's JSON single-value form
is a JSON number, which cannot hold a NaN; Java's strings are outside it. The fix keeps the value
off the spec's JSON entirely (the transport is internal to one plan) and uses Java's spelling
for the string, so nothing persisted changes form.

## 6. The sizing and the bar

- Fork: `crates/iceberg/src/spec/manifest/_serde.rs` (the transport and its tests) and
  `crates/iceberg/src/spec/manifest/mod.rs` (one line in each public function). Two files.
- RePark: the fork pin bump (`Cargo.toml`, `Cargo.lock` source lines). No RePark product seat.
- **Within the bar** (two fork files, no RePark product file): built.
