# map — python/repark/tests/range_tvf_id_1

## Purpose

Recorded Spark 4.1.2 oracle for RANGE-TVF-ID-1: every `range(...)` table-function
form answers `struct<id:bigint>` with a non-nullable column, and the two refusal
cells carry Spark's exception class plus error-class token.

## Contents

- [range_tvf_id_1_spark_oracle.json](range_tvf_id_1_spark_oracle.json) — the 16
  recorded cells (13 SQL cells plus 3 `spark.range` DataFrame-door cells).
  pins: range-tvf-id-1/C-001, C-002, C-003, C-004
- [range_tvf_id_2_spark_oracle.json](range_tvf_id_2_spark_oracle.json) — the 19
  RANGE-TVF-ID-2 edge-case cells (NULL bounds, narrow widths, decimal and float
  bounds, overflow bounds, the near-max count, numPartitions refusals) in the
  compact shape (schema and rows, or exception class and first error line).
  pins: range-tvf-id-2/C-001, C-002, C-003, C-004, C-005, C-006

## Provenance

Recorded by the orchestrator on 2026-09-18 from live PySpark 4.1.2
(`record_range.py`, one short JVM, `local[2]`, UTC,
`spark.sql.session.timeZone=UTC`) and copied verbatim into this directory; the
repo-idiomatic re-deriver is
[../_record_range_tvf_id_1.py](../_record_range_tvf_id_1.py), which exits
non-zero on drift.

SHA-256 of the fixture file:

`21a6c87fe2f2d159fc4ba2929afccc42cf820fee68a85db424ae8fef90cfee7c`

Recorded by the orchestrator on 2026-09-19 from live PySpark 4.1.2
(`record_range2.py`, one short JVM, `local[2]`, UTC,
`spark.sql.session.timeZone=UTC`) and copied verbatim into this directory as
`range_tvf_id_2_spark_oracle.json`; the same re-deriver gains the ID-2 cell
table (`_SQL_CELLS_2`, compact shape without per-field nullability) and checks
both fixtures under `--check`.

SHA-256 of the ID-2 fixture file:

`4af911ff6f3e84ff0b4fa224839ae7b96b010090d5e20634d77688b17edb2314`

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../test_range_tvf_id_1.py](../test_range_tvf_id_1.py)
- Ledger: [../../../../task/ledgers/staging/range-tvf-id-1-ledger.md](../../../../task/ledgers/staging/range-tvf-id-1-ledger.md)
