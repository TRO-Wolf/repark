# map — python/repark-parity/fixtures/torture/data

## Purpose

Committed fixture data for the torture suite — TORTURE-1's single committed-data
exception (card D-5): the `v3_dv` family needs Spark-written Puffin deletion vectors,
which no pure-Python generator can produce, so one ≤ 1 MB table instance is checked in
here. Every other byte the suite reads is generated at test time and never committed;
the generators' repository-output refusal does not apply here because this directory is
the committed landing zone, not a generation target.

## Contents

- [v3_dv/](v3_dv/map.md) — a Spark-written format-v3 merge-on-read Iceberg table with
  live deletion vectors and its `truth.json` record.
- [ice_spark_table_1/](ice_spark_table_1/map.md) — a Spark-written format-v2
  copy-on-write Iceberg table with the production properties, Spark's own CoW MERGE
  and a `hash` distribution stamp in its history, and its `truth.json` record
  (ICE-SPARK-TABLE-1; 67,799 bytes, second committed-data exception alongside
  `v3_dv`).
- [ice_hadoop_vn_1/](ice_hadoop_vn_1/map.md) — a Spark-written format-v2 Hadoop
  table at `v2` with one seed row, the adoption base for the stale-`vN`-writer
  pins (ICE-HADOOP-VN-1).
  pins: ice-hadoop-vn-1/C-007
- [ice_promote_read_1/](ice_promote_read_1/map.md) — the recorded Spark 4.1.2 answers for
  reads and DML after a legal type promotion (`truth.json`, 126 cases) and two
  Spark-created promoted mixed-era tables (v2, v3) for the adoption cells
  (ICE-PROMOTE-READ-1; third committed-data exception).
- [ice_nested_evo_1/](ice_nested_evo_1/map.md) — the recorded Spark 4.1.2 answers for
  nested schema evolution and nested DDL (`oracle.json`) and four Spark-written tables whose
  struct, list-element struct or map-value struct gained a child after the first write
  (ICE-NESTED-EVO-1, 2026-09-17; fourth committed-data exception).

## Pointers

- Up: [../map.md](../map.md)
- Suite: [../../../tests/torture/map.md](../../../tests/torture/map.md)
