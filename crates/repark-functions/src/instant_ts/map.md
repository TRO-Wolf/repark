# map — repark-functions/src/instant_ts

Submodules of [instant_ts.rs](../instant_ts.rs) (FEXPR-TIMESTAMP-NTZ-1, 2026-11-24).

## Files

- [ltz_cast.rs](ltz_cast.rs) — `__repark_cast_timestamp_ltz__` and `__repark_try_cast_timestamp_ltz__`
  marker UDFs carrying explicit `TIMESTAMP_LTZ` casts and zoned `TIMESTAMP '...'` literals across
  `build_expr_context` deferral and Iceberg `conform_stage` writes; rewritten in
  `SparkLtzTimestampCast` via the LTZ path regardless of `spark.sql.timestampType`.
- [tests.rs](tests.rs) — unit pins moved out of `instant_ts.rs` plus deferral, explicit-LTZ,
  NTZ `TRY_CAST`, LTZ-to-NTZ wall conversion, and stale-`Union` rebuild tests.

## Pointers

- Up: [../map.md](../map.md)
