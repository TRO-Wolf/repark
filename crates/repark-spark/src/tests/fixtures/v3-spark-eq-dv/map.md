# map — tests/fixtures/v3-spark-eq-dv

## Purpose

Checked-in **Spark-written** Iceberg format-v3 **partitioned** table that carries
**both** a Puffin deletion vector and an equality-delete file. V3E-3: CI can
adopt it with no JVM and pin "equality deletes alongside DVs" against Spark.

Written by PySpark 4.1.2 + Iceberg 1.11.0 (Hadoop catalog): identity partition
on `part`, four-row append, merge-on-read `DELETE WHERE id = 1` (Puffin DV),
then one Iceberg Java `RowDelta` equality-delete on `id = 4` in `part = 1`
(Spark SQL 4.1.2+1.11.0 does not write equality deletes — identifier-field
MERGE still writes DVs / position deletes). Spark live rows: `(2,b,0), (3,c,1)`.
Current snapshot `5751120093798556354`; summary `total-records = 4`,
`total-data-files = 2`, `total-delete-files = 2`, `total-equality-deletes = 1`,
`total-position-deletes = 1`. Adopt `metadata/v4.metadata.json`.

The table location baked into every metadata/Avro/Puffin/Parquet path is
`/tmp/repark-v3e3-eqdel/ns/v3eq`. Tests copy this tree onto that path under a
lock.

## Contents

- `data/part=0/`, `data/part=1/` — two Parquet data files (2 rows each).
- `data/*-deletes.puffin` — one Puffin deletion vector (`id = 1`).
- `data/eqdel-4.parquet` — one equality-delete file (`id = 4`, field id 1).
- `metadata/` — Hadoop `v1.metadata.json` … `v4.metadata.json`, Avro lists,
  `version-hint.text` (`4`).

Hadoop `.crc` sidecar files are omitted; LocalFs does not consult them.

## Pointers

- Tests: [../../v3e3.rs](../../v3e3.rs)
- Up: [../map.md](../map.md)
