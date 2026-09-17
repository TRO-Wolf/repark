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
Spark has no excel reader, so there is no Spark oracle for these four) and `openpyxl` is
absent from the locked venv (measured by EX-29, 2026-09-11). Examples keep the house
form: one module docstring, the `main()` one-liner, and bare helpers — every example in this
directory carries the one-liner (verified by scan, EX-26 round 2).

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
- [text_read_write.py](text_read_write.py) — `DataFrameReader.text` /
  `DataFrameWriter.text` round trip (null writes an empty line), the `format("text")`
  spellings, the `wholetext` row-per-file arm (IO-TEXT-1), and the `partitionBy` hive
  layout with values read back (follow-up, 2026-09-15).
  pins: io-text-1/C-003, io-text-1/T-6
- [writer_csv.py](writer_csv.py) — `csv` explicit-header arms plus `format` /
  `option` / `options` / `save`, asserting file bytes and data-file counts
  (EX-26). The header default is §7 `EX-IO-4`, the save default `EX-IO-5`,
  the output listing `EX-IO-10`.
  pins: ex-26-io-session/C-005
- [writer_json.py](writer_json.py) — `json` shorthand and format spellings,
  byte-identical to Spark (EX-26). The output listing is §7 `EX-IO-10`.
  pins: ex-26-io-session/C-006
- [writer_partition.py](writer_partition.py) — `partitionBy` / `partition_by`
  hive layout: directory names and per-partition bytes (EX-26). The snake
  spelling is repark-only (`hasattr` False on live PySpark 4.1.2); the
  output listing is §7 `EX-IO-10`.
  pins: ex-26-io-session/C-007
- [writer_bucket_cluster.py](writer_bucket_cluster.py) — `bucketBy` / `bucket_by`,
  `sortBy` / `sort_by`, `clusterBy` / `cluster_by` (v1 and V2) chaining on a local
  memory-catalog frame, with the two declared Iceberg refusals pinned at the
  actions (§5 IO-BUCKET-1 Ruling R-1, IO-CLUSTER-1 Ruling R-2; the V2 create
  arm records where Spark answered `None`). IO-BUCKET-CLUSTER-1 (2026-09-14).
  pins: io-bucket-cluster-1/C-002
- [writer_tables.py](writer_tables.py) — `saveAsTable` / `save_as_table` and
  `insertInto` / `insert_into`, positional insert included (EX-26). The snake
  spellings are repark-only; non-iceberg table formats are §7 `EX-IO-6`, the
  exists text `EX-IO-9`, the missing text `EX-IO-8`, the arity text `EX-IO-8`.
  pins: ex-26-io-session/C-008
- [writerv2_create.py](writerv2_create.py) — `using`, `tableProperty` /
  `table_property`, `partitionedBy` / `partitioned_by`, `create` (EX-22).
- [writerv2_replace.py](writerv2_replace.py) — `createOrReplace` /
  `create_or_replace`, `replace` (EX-22).
- [writerv2_append_overwrite.py](writerv2_append_overwrite.py) — by-name
  `append` (and a second append arm read back ordered by id),
  `overwritePartitions` / `overwrite_partitions`, `option` /
  `options` (EX-22).
- [orc_read.py](orc_read.py) — **IO-ORC-1 (2026-09-16):** `DataFrameReader.orc`
  and `format("orc").load` over the committed Spark-written `m1` fixture, plus the
  `PATH_NOT_FOUND` missing-path shape. pins: io-orc-1/C-010
- [io_declared_refusals.py](io_declared_refusals.py) — the declared
  orc-write/`xml` refusals and the non-PostgreSQL-driver `jdbc` refusals
  (`DataFrameReader.xml`/`jdbc`, `DataFrameWriter.orc`/`xml`/`jdbc`)
  asserting each `NOT_IMPLEMENTED` shape and Spark's own `XML_ROW_TAG_MISSING`
  check **(IO-ORC-1 moves the `DataFrameReader.orc` arm to `orc_read.py`)**, plus
  the `DataFrameNaFunctions.replace`
  delegation with its `ARGUMENT_REQUIRED` / `MIXED_TYPE_REPLACEMENT` arms
  (IO-DECLARED-1; registry IO-ORC-1 / IO-XML-1 / IO-JDBC-1 — R-3 restores
  PostgreSQL reads on `DataFrameReader.jdbc`, pinned in
  `test_pg_jdbc_options.py`). `DataFrameReader.jdbc`
  leaves the exceptions list — the non-Postgres refusal is the example.

## Pointers

- Up: [../map.md](../map.md)
