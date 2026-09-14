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
  copy-on-write Iceberg table with the production properties and its `truth.json`
  record (ICE-SPARK-TABLE-1; 36,612 bytes, second committed-data exception alongside
  `v3_dv`).

## Pointers

- Up: [../map.md](../map.md)
- Suite: [../../../tests/torture/map.md](../../../tests/torture/map.md)
