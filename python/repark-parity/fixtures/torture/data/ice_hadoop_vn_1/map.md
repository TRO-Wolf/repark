# map — fixtures/torture/data/ice_hadoop_vn_1 (a committed Iceberg table root)

## Purpose

A Spark-written Iceberg **format-version 2** Hadoop-catalog table at `v2` with one
seed row, checked in so JVM-free CI can pin the stale-`vN`-writer contract: two
RePark memory catalogs adopt `v2`, one commits `v3`, and every later commit from
the stale pointer raises `PySparkException` with a `CatalogCommitConflicts`-leading
message while the winner's bytes stay intact (ICE-HADOOP-VN-1). Written 2026-09-17
by PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 on a Hadoop catalog at a
local filesystem warehouse. Repark follows the absolute paths baked into the
metadata, so the suite copies this tree onto exactly
`/tmp/repark-ice-hadoop-vn-1/ns/conc` under a directory lock, the same contract
the `ice_spark_table_1` fixture uses, and removes the copy when the test exits.
`.crc` sidecars Spark wrote beside the files are not checked in.

## Contents

- `data/` — one Parquet seed file (one row).
- `metadata/` — Hadoop `v1`…`v2.metadata.json` (create, seed append), one manifest
  Avro file and its manifest list, and `version-hint.text` (`2`). Adopt
  `metadata/v2.metadata.json`.
- `truth.json` — the writer record: `seed_rows`, `format_version` 2, the baked-in
  `table_location`, and the declared schema string.
- `map.md` — this file.

## Pointers

- Up: [../map.md](../map.md)
- Cells: [../../../../../repark/tests/test_ice_hadoop_vn_1.py](../../../../../repark/tests/test_ice_hadoop_vn_1.py)
- Ledger: `task/ledgers/staging/ice-hadoop-vn-1-ledger.md`

pins: ice-hadoop-vn-1/C-007
