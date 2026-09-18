# map — python/repark-parity/fixtures/torture/data/ice_dyn_overwrite_1

## Purpose

Recorded Spark 4.1.2 + Iceberg 1.11.0 answers for ICE-DYN-OVERWRITE-1 (report row
V2-24b and the INSERT OVERWRITE half of V2-20a): the static/dynamic overwrite matrix
(SQL, `insertInto`, `overwritePartitions`, `PARTITION (p)`, v2 + v3, unpartitioned,
evolved spec, empty source, all three conf paths, `saveAsTable`) and the three
disk-verified overwrite-vs-concurrent-append interleaves. One JSON document, read by
`python/repark/tests/test_ice_dyn_overwrite_1.py` offline and on the live tier.

## Contents

- [spark_oracle.json](spark_oracle.json) — `meta` (Spark version, runtime GAV, record
  date, race staging note) plus `cells`: one row/table snapshot per oracle cell, recorded
  2026-09-17 by `/tmp/oc-worker/ja-dyn/record_spark_oracle.py` (matrix) and
  `/tmp/oc-worker/ja-dyn/record_spark_race4.py` (interleaves, two Hadoop-catalog sessions
  sharing one warehouse, order verified in metadata files and re-read from a fresh
  session). pins: ice-dyn-overwrite-1/C-002…C-011, C-018.
- [spark_byname_dyn_oracle.json](spark_byname_dyn_oracle.json) — `spark_version`,
  `iceberg_runtime` plus 24 `cells`: 20 recorded 2026-09-17 by `record_spark_byname_dyn.py` and four
  (`v2`/`v3` × `dynamic`/`static` empty `BY NAME` on an UNPARTITIONED table, the cell behind the unit's
  ruling that a dynamic empty source is a no-op on every table) recorded the same night by
  `record_spark_byname_empty_unpart.py`
  (one JVM, Spark 4.1.2 + Iceberg 1.11.0): a `(id BIGINT, k STRING, v STRING)` table
  partitioned by `k` holding `(1,'a','old'), (2,'b','old')`, overwritten through
  `BY NAME`, an explicit column list, and positionally, under `dynamic` and `static`,
  on v2 and v3, plus the empty-source and unpartitioned `BY NAME` shapes. Each cell
  carries `mode`, `setup`, `statement`, `rows` and `snapshot_operations` (the dynamic
  empty cell records no new snapshot). Read by
  `python/repark/tests/test_ice_dyn_overwrite_1_by_name.py`, which replays every cell.
  pins: ice-dyn-overwrite-1/C-019.

## Pointers

- Up: [../map.md](../map.md)
- Pins live in: [../../../../../repark/tests/test_ice_dyn_overwrite_1.py](../../../../../repark/tests/test_ice_dyn_overwrite_1.py)
  and [../../../../../repark/tests/test_ice_dyn_overwrite_1_by_name.py](../../../../../repark/tests/test_ice_dyn_overwrite_1_by_name.py)
- Ledger: [../../../../../../task/ledgers/staging/ice-dyn-overwrite-1-ledger.md](../../../../../../task/ledgers/staging/ice-dyn-overwrite-1-ledger.md)
