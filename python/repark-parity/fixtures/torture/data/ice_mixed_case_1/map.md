# map — fixtures/torture/data/ice_mixed_case_1 (a committed Iceberg table root)

## Purpose

A Spark-written Iceberg **format-version 2** copy-on-write table with
mixed-case columns, checked in so JVM-free CI can pin the Spark door resolving
column references case-insensitively — the ICE-MIXED-CASE-1 scenario (registry
`docs/spark-sql-iceberg-parity.md` §3 ID-1, rating row V2-27 claim C-11).

Written 2026-09-17 by PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0 on a Hadoop catalog at local filesystem
warehouse `/tmp/repark-ice-mixed-case-1`; total size 56K. Repark follows the
absolute paths baked into the metadata — a copy at any other path is
unreadable — so the suite copies this tree onto exactly
`/tmp/repark-ice-mixed-case-1/ns/mc` under a directory lock, the same contract
the `ice_spark_table_1` fixture uses, and adopts `metadata/v2.metadata.json`.
Each test re-copies fresh because the mutating cells (UPDATE / DELETE / MERGE
/ INSERT) move the table forward. `.crc` sidecars stripped at check-in.

## Contents

- `ns/mc/data/` — two seed Parquet files, one row each (Spark wrote one file
  per INSERT row), zstd.
- `ns/mc/metadata/` — Hadoop `v1.metadata.json` (create) and
  `v2.metadata.json` (seed append), one manifest-list, one manifest, and
  `version-hint.text` (`2`). Adopt `metadata/v2.metadata.json`.
- `truth.json` — beside this map: seed rows, stored schema, table location,
  and the create/seed statements.
- `twin_v3.metadata.json` — run 21b (2026-09-17): Spark's `v3.metadata.json` for
  `sc.ns.tw`, copied verbatim from the orchestrator's recording warehouse
  (`probe_mc.py`, PySpark 4.1.2 + Iceberg 1.11.0). Schema `id INT` then `ID INT`
  added through the Iceberg Java API `updateSchema().addColumn("ID", …)` — Spark
  allows ASCII case-twin fields. Only the metadata file is kept: RePark's fork
  refuses the file while parsing it (`Cannot build lower case index: id and ID
  collide`), before any manifest or data path is read, so the pin needs no data
  files and the baked `/tmp/kb-oracle-mc` location is never touched.
  pins: ice-mixed-case-1/C-013
- `map.md` — this file.
