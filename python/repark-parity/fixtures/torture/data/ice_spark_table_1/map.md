# map — fixtures/torture/data/ice_spark_table_1 (a committed Iceberg table root)

## Purpose

A Spark-written Iceberg **format-version 2** copy-on-write table with the production
properties, checked in so JVM-free CI can pin repark writing *into Spark-created
metadata* — the ICE-SPARK-TABLE-1 scenario (ruling 6 in `docs/cutover/inventory.md` §8:
shadow tables are Spark-created, so RePark must meet Spark-stamped metadata before C4).
Written 2026-09-14 by PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 on a Hadoop
catalog at a local filesystem warehouse; total size 36,612 bytes. Repark follows the
absolute paths baked into the metadata — a copy at any other path is unreadable — so the
suite copies this tree onto exactly `/tmp/repark-ice-spark-table-1/ns/facts` under a
directory lock, the same contract the `v3_dv` fixture uses. The copy uses
`shutil.copy` (fresh mtimes) so the `remove_orphan_files` 24-hour protection floor
behaves identically on every run.

## Contents

- `data/ds=2026-09-13/`, `data/ds=2026-09-14/` — two Parquet data files (ten seeded
  rows each, zstd — Spark stamped `write.parquet.compression-codec`).
- `metadata/` — Hadoop `v1`…`v3.metadata.json` (create + two seed appends), two
  manifest Avro files and two manifest lists, and `version-hint.text` (`3`). Adopt
  `metadata/v3.metadata.json`.
- `truth.json` — the writer record: `seed_rows` verbatim, `true_rows` 20,
  `format_version` 2, `partition_spec` `identity(ds)`, the baked-in `table_location`,
  and the declared schema string (timestamptz `ingestion_timestamp`).
- `map.md` — this file. `.crc` sidecars never produced under the local FileIO.

Spark-stamped surface repark meets on adoption: `owner`, the four production `write.*`
copy-on-write properties plus `write.target-file-size-bytes`,
`write.parquet.compression-codec = zstd`, Spark snapshot-summary keys (`spark.app.id`,
`engine-name`, `app-id`, `iceberg-version`, …), Hadoop `vN` metadata naming, and a
`version-hint.text` repark never updates.

## Pointers

- Up: [../map.md](../map.md)
- Cells: [../../../../../repark/tests/test_ice_spark_table_1.py](../../../../../repark/tests/test_ice_spark_table_1.py)
- Ledger: `task/ledgers/staging/ice-spark-table-1-ledger.md`

pins: ice-spark-table-1/C-008
