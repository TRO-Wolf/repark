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

## Pointers

- Up: [../map.md](../map.md)
