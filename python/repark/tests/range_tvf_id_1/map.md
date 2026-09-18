# map — python/repark/tests/range_tvf_id_1

## Purpose

Recorded Spark 4.1.2 oracle for RANGE-TVF-ID-1: every `range(...)` table-function
form answers `struct<id:bigint>` with a non-nullable column, and the two refusal
cells carry Spark's exception class plus error-class token.

## Contents

- [range_tvf_id_1_spark_oracle.json](range_tvf_id_1_spark_oracle.json) — the 16
  recorded cells (13 SQL cells plus 3 `spark.range` DataFrame-door cells).
  pins: range-tvf-id-1/C-001, C-002, C-003, C-004

## Provenance

Recorded by the orchestrator on 2026-09-18 from live PySpark 4.1.2
(`record_range.py`, one short JVM, `local[2]`, UTC,
`spark.sql.session.timeZone=UTC`) and copied verbatim into this directory; the
repo-idiomatic re-deriver is
[../_record_range_tvf_id_1.py](../_record_range_tvf_id_1.py), which exits
non-zero on drift.

SHA-256 of the fixture file:

`21a6c87fe2f2d159fc4ba2929afccc42cf820fee68a85db424ae8fef90cfee7c`

## Pointers

- Up: [../map.md](../map.md)
- Pins: [../test_range_tvf_id_1.py](../test_range_tvf_id_1.py)
- Ledger: [../../../../task/ledgers/staging/range-tvf-id-1-ledger.md](../../../../task/ledgers/staging/range-tvf-id-1-ledger.md)
