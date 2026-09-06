# map — docs/examples/io/

## Purpose

Worked examples for DataFrameReader, DataFrameWriter, and DataFrameWriterV2.
Examples construct the session as `repark = ReparkSession.builder…`; see
[../map.md](../map.md). The WriterV2 examples write to a local memory-catalog
Iceberg table (`register_memory_catalog("local", …)` plus
`CREATE NAMESPACE local.ns`, then `writeTo("local.ns.…")`) and read it back.
`DataFrameWriterV2.overwrite` stays on the backlog as a measured divergence
(EX-W2-1; EX-W2-2, EX-W2-3 and EX-W2-4 pin the empty-source, branch and unpartitioned arms of names
that are covered). The excel surface (`excel`, `sheet_names`, `read_excel`,
`excel_sheet_names`) stays too: the engine reader is deferred post-milestone-one (§7 EX-IO-7;
Spark has no excel reader, so there is no Spark oracle for these four). Examples keep the house
form: one module docstring, the `main()` one-liner, and bare helpers.

## Contents

- [parquet_roundtrip.py](parquet_roundtrip.py) — local Parquet write then read.
- [reader_csv_json.py](reader_csv_json.py) — `csv` (default all-string, header,
  null value, bare `_cN`) and `json` (default, explicit schema) through `option` /
  `schema` (EX-26). The infer-schema width arm is §7 `EX-IO-3`, schema-on-parquet
  `EX-IO-2`.
  pins: ex-26-io-session/C-002
- [reader_format_load.py](reader_format_load.py) — `format` / `load` over csv, json
  and parquet (plus the `path` option and `options`), and `table` on a temp view
  with the missing-name raise (EX-26). The bare-load default is §7 `EX-IO-1`, the
  missing-table text `EX-IO-8`.
  pins: ex-26-io-session/C-003
- [reader_smart_csv.py](reader_smart_csv.py) — `smartCsv` messy-file ingest: preamble
  skip, header detect, type inference (EX-26). Repark extension, no Spark analog.
  pins: ex-26-io-session/C-004
- [writer_csv.py](writer_csv.py) — `csv` explicit-header arms plus `format` /
  `option` / `options` / `save`, asserting file bytes and data-file counts
  (EX-26). The header default is §7 `EX-IO-4`, the save default `EX-IO-5`.
  pins: ex-26-io-session/C-005
- [writer_json.py](writer_json.py) — `json` shorthand and format spellings,
  byte-identical to Spark (EX-26).
  pins: ex-26-io-session/C-006
- [writer_partition.py](writer_partition.py) — `partitionBy` / `partition_by`
  hive layout: directory names and per-partition bytes (EX-26). The snake
  spelling is repark-only (`hasattr` False on live PySpark 4.1.2).
  pins: ex-26-io-session/C-007
- [writer_tables.py](writer_tables.py) — `saveAsTable` / `save_as_table` and
  `insertInto` / `insert_into`, positional insert included (EX-26). The snake
  spellings are repark-only; non-iceberg table formats are §7 `EX-IO-6`, the
  exists text `EX-IO-9`, the missing text `EX-IO-8`.
  pins: ex-26-io-session/C-008
- [writerv2_create.py](writerv2_create.py) — `using`, `tableProperty` /
  `table_property`, `partitionedBy` / `partitioned_by`, `create` (EX-22).
- [writerv2_replace.py](writerv2_replace.py) — `createOrReplace` /
  `create_or_replace`, `replace` (EX-22).
- [writerv2_append_overwrite.py](writerv2_append_overwrite.py) — by-name
  `append` (and a second append arm read back ordered by id),
  `overwritePartitions` / `overwrite_partitions`, `option` /
  `options` (EX-22).

## Pointers

- Up: [../map.md](../map.md)
