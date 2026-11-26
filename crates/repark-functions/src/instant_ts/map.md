# map — repark-functions/src/instant_ts

Submodules of [instant_ts.rs](../instant_ts.rs) (FEXPR-TIMESTAMP-NTZ-1, 2026-11-24).

## Files

- [ltz_cast.rs](ltz_cast.rs) — `__repark_cast_timestamp_ltz__` and `__repark_try_cast_timestamp_ltz__`
  marker UDFs (`schema_name`/`display_name` delegate to the operand so unaliased SQL projections
  never expose internal marker names) carrying explicit `TIMESTAMP_LTZ` casts, zoned
  `TIMESTAMP '...'` literals, and Iceberg conform-stage LTZ store casts across
  `build_expr_context` deferral and `spark.sql.timestampType=TIMESTAMP_NTZ`; rewritten in
  `SparkLtzTimestampCast` via the unified LTZ path regardless of `spark.sql.timestampType`.
- [ntz_cast.rs](ntz_cast.rs) — **FEXPR-TIMESTAMP-NTZ-1 Delta 1 (2026-11-24):** pre-coercion
  `SparkNtzTimestampCast` (`spark_ntz_timestamp_cast`, installed before `type_coercion` via
  `insert_ntz_timestamp_cast_before_coercion`). No-op under `TIMESTAMP_LTZ` or deferral. Under
  `TIMESTAMP_NTZ`, rewrites user bare `TIMESTAMP` `Cast`/`TryCast` (and `Values` cells/schemas +
  stale `Union` schemas) to NTZ via `timestamp_ntz_cast_expr` before `type_coercion` while
  preserving `Dml` LTZ target casts (both synthesized `Cast(Column, LTZ)` and re-wrapped NTZ
  inputs) and protecting Iceberg DML `arrow_cast(CAST(x AS TIMESTAMP), 'Timestamp(µs, "UTC")')`
  store casts as explicit LTZ markers; any post-coercion `Cast(Timestamp(µs, None) -> Timestamp(µs, UTC))`
  then localizes through `to_timestamp` in `SparkLtzTimestampCast`.
- [tests.rs](tests.rs) — **FEXPR-TIMESTAMP-NTZ-1 delta 2:** `to_timestamp_is_not_nullable_only_over_one_non_null_timestamp`.
  Unit pins moved out of `instant_ts.rs` plus deferral, explicit-LTZ,
  NTZ `TRY_CAST`, LTZ-to-NTZ wall conversion, stale-`Union` rebuild, NTZ-session DML-into-LTZ
  store-cast, Delta 1 mixed NTZ/LTZ comparison & `MERGE` `CASE` localization, gap/offset/ANSI
  string casts, Spark zone spellings, `date_trunc` LTZ return, and F-A9 nullability pins.

## Pointers

- Up: [../map.md](../map.md)
