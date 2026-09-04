# map — docs/examples/io/

## Purpose

Worked examples for DataFrameReader, DataFrameWriter, and DataFrameWriterV2.
Examples construct the session as `repark = ReparkSession.builder…`; see
[../map.md](../map.md). The WriterV2 examples write to a local memory-catalog
Iceberg table (`register_memory_catalog("local", …)` plus
`CREATE NAMESPACE local.ns`, then `writeTo("local.ns.…")`) and read it back.
`DataFrameWriterV2.overwrite` stays on the backlog as a measured divergence
(EX-W2-1; EX-W2-2 and EX-W2-3 pin the empty-source and branch arms of names
that are covered). Examples keep the house form: one module docstring, the
`main()` one-liner, and bare helpers.

## Contents

- [parquet_roundtrip.py](parquet_roundtrip.py) — local Parquet write then read.
- [writerv2_create.py](writerv2_create.py) — `using`, `tableProperty` /
  `table_property`, `partitionedBy` / `partitioned_by`, `create` (EX-22).
- [writerv2_replace.py](writerv2_replace.py) — `createOrReplace` /
  `create_or_replace`, `replace` (EX-22).
- [writerv2_append_overwrite.py](writerv2_append_overwrite.py) — by-name
  `append`, `overwritePartitions` / `overwrite_partitions`, `option` /
  `options` (EX-22).

## Pointers

- Up: [../map.md](../map.md)
