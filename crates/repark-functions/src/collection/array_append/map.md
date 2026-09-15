# map — repark-functions/src/collection/array_append

## Purpose

The child module of [`../array_append.rs`](../array_append.rs), split out under the
Rust file-size ratchet. The parent declares it (`mod coerce;`) and keeps the UDF
registration, the invoke wrapper and the tests.

## Contents

- `coerce.rs` — **ARRAY-NULL-1 round 4 (2026-09-14):** Spark's recursive
  `findTightestCommonType` for the `(array element, element)` pair plus the
  invocation-side conversion machinery. `spark_common_element` resolves the
  numeric ladder (byte < short < int < long < float < double, higher of the two —
  `array<float>` + int stays float; `Float16` ranks as `Float32` via `ladder_type`
  because Spark has no half-float and the name is already FLOAT — only
  `Float16` + `Float16` keeps `halffloat`), recurses through list children, map
  keys and values, and struct fields (same count, positional order, names matched
  case-insensitively regardless of `spark.sql.caseSensitive`, result keeps the
  array's field names), and resolves the
  temporal pairs to microsecond timestamps (any zoned or LTZ timestamp pulls the
  pair to `Timestamp(Microsecond, UTC)`; NTZ stays NTZ only against date/NTZ).
  `spark_coerce_args` validates only and returns the argument types unchanged, so
  DataFusion's analyzer never inserts a plan CAST — a `List<Timestamp(ns)>`
  pre-cast would wrap year 0001, and a date array must be localized in the
  session zone, not read as UTC midnight. `convert_columnar`/`convert_array` do
  the conversion at invoke time instead (the zone is `Option<Tz>` — parsed only
  when a conversion is actually needed): `ZoneSpans` caches the zone offset per
  civil day / offset-transition interval and writes the output `Vec<i64>` +
  `NullBufferBuilder` directly for date→LTZ and NTZ→LTZ leaves, falling back to
  `localize_wall_micros_in_zone` on transition/gap/overlap days;
  `rescale_timestamp_column` rescales the i64 buffer directly for unit-only
  timestamp changes with an unchanged zone annotation (Arrow semantics:
  truncation toward zero, checked multiply → null); `convert_list`/`convert_map`
  convert only each record batch's offset window of the shared flat child
  (a batched `ListArray` keeps the whole child, so converting `values()` whole
  re-did ~16× the work); Arrow `cast` covers the rest, recursing through
  list/map/struct children so their null buffers survive. Every incompatible
  pair refuses with `DATATYPE_MISMATCH.ARRAY_FUNCTION_DIFF_TYPES` naming both
  Spark type names.
  pins: array-null-1/L-5, L-6, L-7, L-8, L-9, L-10

## I want to...

| ...do this | go to |
|---|---|
| change the append/prepend kernels or the null graft | [`../array_append.rs`](../array_append.rs) |
| change which type pairs Spark accepts | `spark_common_element` in `coerce.rs` |
| change how values reach the common type | `convert_columnar`/`convert_array` in `coerce.rs` |

## Pointers

- Up: [`../map.md`](../map.md)
