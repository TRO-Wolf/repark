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
- FNP-8's HOF preparation reuses `narrow_provisional_integer_literals`; the direct rewrite
  tests exercise that shared helper. Explicit casts retain their declared type.
  pins: fnp-8/C-004, C-006
- **SQL-LITERAL-TYPING-1 round 3 (2026-09-16):** the `Negative` fold is gone
  from the shared helper — a parenthesized `-(2147483648)` stays `Int64`
  here exactly as on the top-level door; only the lexer-level negative token
  narrows. The renamed unit test pins the no-fold.
  pins: sql-literal-typing-1/V-001
- **ICE-COUNT-FOLD-1 (2026-09-19):** a non-distinct `count` whose only argument is the
  integer literal `1` (`Int32` or `Int64`) analyzes to DataFusion's count-star expansion
  `Int64(1)`, so `Count::value_from_stats` can fold it; the output name is preserved.
  `count(5)`, `count(DISTINCT 1)` and a `FILTER` literal still narrow (the `FILTER` pin
  reads the analyzed plan text, and fails when the filter is left un-narrowed).
  pins: ice-count-fold-1/C-002

## Pointers

- Up: [src map](../map.md)
- Rule: [`spark_result_types.rs`](../spark_result_types.rs)
