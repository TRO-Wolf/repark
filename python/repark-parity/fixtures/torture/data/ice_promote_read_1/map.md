# map — fixtures/torture/data/ice_promote_read_1 (Spark oracle + two adopted Iceberg tables)

## Purpose

The recorded Spark 4.1.2 answers for ICE-PROMOTE-READ-1 — reads and DML after a legal
Iceberg type promotion (`int→bigint`, `float→double`, `decimal(9,2)→(12,2)`) — and two
Spark-created, Spark-promoted mixed-era tables the JVM-free tier adopts with
`register_table`. Recorded 2026-09-16 by
`python/repark/tests/_record_ice_promote_read_1.py` on PySpark 4.1.2 +
iceberg-spark-runtime-4.1_2.13:1.11.0 (ANSI on, Hadoop catalog); two independent
recordings produced identical answers. Re-recorded 2026-09-17 with the `inspect/*`
cases appended: the 126 prior answers are byte-identical, the two new cases record
Spark's Long-typed merged partitions. Re-recorded again the same day per ruling
Q-20a-5 with the three nested projections aliased to leaf names (values and types
unchanged; the 126 prior answers byte-identical across both recordings).
Never hand-edited: re-run the driver.

## Contents

- `truth.json` — `oracle` (pyspark, runtime GAV, ANSI), `catalog_sha256` (the SHA-256 of
  the driver's `build_cases()`; the pin module fails if the driver and the recording
  drift), and one `answers` line per case: per step Spark's SQL answer (`rows`, Arrow
  `types`, `columns`) or `{"ok": true}` for a statement, a `dataframe` answer only where
  Spark's DataFrame door differed from its SQL door (none did), `dataframe_steps` for the
  `writeTo(t).overwritePartitions()` twin, and `frozen` (baked table root + metadata file)
  for the adopted tables. 128 cases, 59,831 bytes.
- `adopt_v2/`, `adopt_v3/` — the Spark-written table roots (`data/p=…/` parquet,
  `metadata/v1…v7.metadata.json`, manifests, manifest lists, `version-hint.text`):
  `(id INT, f FLOAT, d DECIMAL(9,2), p INT, s STRING) PARTITIONED BY (p)`, two rows, all
  four numeric columns promoted, one post-promotion row. Metadata carries absolute paths
  under `/tmp/repark-ice-promote-read-1/ns/`, so the pins copy each root there under a
  directory lock and remove the copy on exit.
- `map.md` — this file.

## Pointers

- Up: [../map.md](../map.md)
- Cells: [../../../../../repark/tests/test_ice_promote_read_1.py](../../../../../repark/tests/test_ice_promote_read_1.py)
- Driver: [../../../../../repark/tests/_record_ice_promote_read_1.py](../../../../../repark/tests/_record_ice_promote_read_1.py)
- Ledger: `task/ledgers/staging/ice-promote-read-1-ledger.md`

pins: ice-promote-read-1/C-001, C-008, C-015
