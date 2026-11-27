# map — repark-spark/src/keyword_lower

## Purpose

WO NTZ-1 slice 1 support. The parent `keyword_lower.rs` holds the Spark-door AST keyword
lowering (empty `map()`, `RLIKE`, `TIMESTAMP_LTZ` targets; the `timestamp_ns` and
`timestamptz_ns` cast names are no longer lowered since ICE-TSNS-TYPE-PLANNER-1, the type
planner types them) and the
`UNSUPPORTED_TIMESTAMP_NTZ` refusal mapper; this directory holds the `TIMESTAMP_NTZ` cast
lowering it calls first.

## Contents

- `ntz_cast_lower.rs` — `lower_timestamp_ntz_cast` rewrites `CAST` / `::` to `TIMESTAMP_NTZ`
  into `__repark_cast_timestamp_ntz__`, `TRY_CAST` / `SAFE_CAST` into
  `__repark_try_cast_timestamp_ntz__`, and a `TIMESTAMP_NTZ` typed string that escaped the
  token layer into the cast of its text. Nested targets (`ARRAY`, `STRUCT`, `MAP`) stay
  untouched for the R4 refusal. Reached from `lower_expression`,
  `TimestampNsCastLower::post_visit_expr` and `TimestampNsCastProbe`.
  **FEXPR-TIMESTAMP-NTZ-1 narrow (2026-11-26), fragment door only:** `lower_fragment_timestamp_ntz_cast`
  lowers the same casts for `F.expr` and wraps a lowered non-`TRY` string-literal cast in
  `__repark_decimal_cast_nullable__` (outside, so a folded `'bad'` stays nullable under ANSI off). A string literal is
  lowered only where `timestamp_type::fragment_lowers_ntz_string_cast` holds; otherwise the cast keeps main's route
  (delta 1: the kernel cannot read year 1 or `2024-1-1`, and Spark answers them). `bare_timestamp_types_as_ntz`
  respells a bare `TIMESTAMP` as `TIMESTAMP_NTZ` where it names a type (a `CAST` / `TRY_CAST` target, a `::`
  target, or a typed literal whose text parses with no zone and is not time-only), for a fragment built under
  `spark.sql.timestampType=TIMESTAMP_NTZ`. A zoned literal stays an instant, as Spark's parser keeps it. A literal whose wall fits Int32 is respelled `CAST('…' AS TIMESTAMP_NTZ)`: main's `TIMESTAMP_NTZ '…'` rewrite emits `__repark_timestamp_ntz__(<wall>)`, the fragment door's `spark_integral_literal` narrows that argument to Int32 exactly when `i32::try_from` holds, and the wall UDF refuses an Int32 (delta 1; the edges `1969-12-31 23:24:12.516352` and `1970-01-01 00:35:47.483647` are pinned). The
  SQL door never calls either. Its cuts map token line and column to bytes by character (`spark_rewrites::byte_offset`),
  pinned over non-ASCII text, multi-line fragments, tabs and CRLF.
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

## Pointers

- Up: [../map.md](../map.md)
