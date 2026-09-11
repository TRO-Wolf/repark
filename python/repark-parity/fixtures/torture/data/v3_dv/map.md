# map — fixtures/torture/data/v3_dv (a committed Iceberg table root)

## Purpose

A Spark-written Iceberg **format-version 3** table under merge-on-read deletes, checked
in so JVM-free CI can pin repark's Puffin deletion-vector reads against a known true row
count. Written 2026-09-11 by PySpark 4.1.2 + iceberg-spark-runtime-4.1_2.13:1.11.0 via
`python -m repark_parity.torture generate v3_dv --rows 120 --seed 7 --out
/tmp/repark-torture-v3dv`; the warehouse argument is why the baked-in table location is
`/tmp/repark-torture-v3dv/ns/v3dv`. Repark follows the absolute paths baked into the
metadata — a copy at any other path is unreadable (measured 2026-09-11:
`Failed to read file <old path>` on manifest-list load) — so the suite copies this tree
onto exactly that path under a directory lock, the same contract the
`v3-spark-part-dv` fixture uses.

## Contents

- `data/part=0/`, `data/part=1/` — 24 Parquet data files (twelve seeded inserts of ten
  ids each, partitioned by `part = id % 2`).
- `data/*-deletes.puffin` — one Puffin deletion-vector file for the merge-on-read
  `DELETE WHERE id % 5 = 2` (24 rows).
- `metadata/` — Hadoop `v1`…`v14.metadata.json`, the manifest-list Avro files, and
  `version-hint.text` (`14`). Adopt `metadata/v14.metadata.json` — the newest file, which
  the suite locates by version sort rather than by name.
- `truth.json` — the generator's truth record: `rows_written` 120, `rows_deleted` 24,
  `true_rows` 96, the delete rule, the newest metadata file's relative path, the baked-in
  `table_location`, and the declared schema string.
- `map.md` — this file. Hadoop `.crc` sidecars are stripped (LocalFs never consults
  them; same treatment as `v3-spark-part-dv`).

## Pointers

- Up: [../map.md](../map.md)
- Generator: [../../v3_dv.py](../../v3_dv.py)
- Cells: [../../../../tests/torture/test_torture_v3_dv.py](../../../../tests/torture/test_torture_v3_dv.py)

pins: torture-1/C-025, C-026
