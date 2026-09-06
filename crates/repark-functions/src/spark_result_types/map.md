# map — repark-functions/src/spark_result_types

## Purpose

Unit tests for `SparkIntegerLiteral` (plus the signed aggregate/window wrappers'
rule shape): narrowing lands on `SELECT`/`VALUES`/CTAS literals, `LIMIT` fetch/skip
stay `Int64`, `count(*)`/`regr_count`/`ntile`/`rank()` keep their signed widths.

## Files

- [`tests.rs`](tests.rs) contains the suite; the context mirrors the production rule
  order (DataFusion defaults, then narrowing).

## Contracts pinned

- Every `SELECT`-literal / `VALUES` / CTAS integer in `Int32` range analyzes `Int32`.
- `LIMIT` fetch/skip analyze `Int64` (physical-planner requirement).
- `count(*)` and `count(1)` answer `Int64`; `ntile(2)` and `rank()` answer `Int32`.

## Pointers

- Up: [src map](../map.md)
- Rule: [`spark_result_types.rs`](../spark_result_types.rs)
