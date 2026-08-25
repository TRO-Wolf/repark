# map — tests/fixtures/v3-spark-part-dv

## Purpose

Checked-in **Spark-written** Iceberg format-v3 **partitioned** table with live Puffin
deletion vectors. V3E-3: CI can adopt it with no JVM and pin partitioned DV
reads against Spark's numbers.

Written by PySpark 4.1.2 + Iceberg 1.11.0 (Hadoop catalog, local disk): identity
partition on `part`, six-row append, merge-on-read `DELETE WHERE id IN (2, 5)`.
Spark live rows: `(1,a,0), (3,c,0), (4,d,1), (6,f,1)`. Partition prune
`part = 0` → `(1,a), (3,c)`. Current snapshot
`8850248918634954095`; summary `total-records = 6`, `total-data-files = 2`,
`total-delete-files = 2`, `added-dvs = 2` (one Puffin file on disk; the files
table lists it once per partition). Adopt
`metadata/v3.metadata.json`.

The table location baked into every metadata/Avro/Puffin path is
`/tmp/repark-v3e3-partdv/ns/v3part`. Tests copy this tree onto that path
under a lock.

## Contents

- `data/part=0/`, `data/part=1/` — two Parquet data files (3 rows each).
- `data/*.puffin` — one Puffin deletion vector file.
- `metadata/` — Hadoop `v1.metadata.json` … `v3.metadata.json`, Avro lists,
  `version-hint.text` (`3`).

Hadoop `.crc` sidecar files are omitted; LocalFs does not consult them.

## Pointers

- Tests: [../../v3e3.rs](../../v3e3.rs)
- Up: [../map.md](../map.md)
