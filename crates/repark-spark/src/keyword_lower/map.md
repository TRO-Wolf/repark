# map — repark-spark/src/keyword_lower

## Purpose

WO NTZ-1 slice 1 support. The parent `keyword_lower.rs` holds the Spark-door AST keyword
lowering (`TIMESTAMP_NS` casts, empty `map()`, `RLIKE`, `TIMESTAMP_LTZ` targets) and the
`UNSUPPORTED_TIMESTAMP_NTZ` refusal mapper; this directory holds the `TIMESTAMP_NTZ` cast
lowering it calls first.

## Contents

- `ntz_cast_lower.rs` — `lower_timestamp_ntz_cast` rewrites `CAST` / `::` to `TIMESTAMP_NTZ`
  into `__repark_cast_timestamp_ntz__`, `TRY_CAST` / `SAFE_CAST` into
  `__repark_try_cast_timestamp_ntz__`, and a `TIMESTAMP_NTZ` typed string that escaped the
  token layer into the cast of its text. Nested targets (`ARRAY`, `STRUCT`, `MAP`) stay
  untouched for the R4 refusal. Reached from `lower_expression`,
  `TimestampNsCastLower::post_visit_expr` and `TimestampNsCastProbe`.
  pins: ntz-1/C-002, C-004, C-005
- `ltz_values_cast.rs` — **WO NTZ-STORE-DOORS-1 second re-verify fold (2026-09-29,
  RD3-1):** `mark_values_timestamp_casts` wraps each `VALUES` cell that is a written
  `CAST` / `::` to `TIMESTAMP` (or `TIMESTAMP_LTZ`), through parentheses, in one more
  `CAST(… AS TIMESTAMP)`. The planner's `VALUES` coercion emits the same naive
  `Timestamp(ns)` cast for a `DATE` or `TIMESTAMP_NTZ` cell, so without the mark
  `repark-functions` `timestamp_ns_cast` could not tell a written `CAST(ntz AS TIMESTAMP)`
  from a coerced `TIMESTAMP_NTZ` literal. The extra cast is same-typed, so every later
  rule treats the cell as before. A cell already wrapped is left alone, so the mark is
  idempotent. Casts outside `VALUES`, `TRY_CAST`, and casts inside a larger cell
  expression are untouched. `KeywordLower` and `TimestampNsCastLower` call it from
  `post_visit_query`; `has_values_timestamp_cast` lets the MERGE probe lower a source
  that needs it. 3 in-module tests.
  pins: ntz-store-doors-1/C-008

## Pointers

- Up: [../map.md](../map.md)
