# map — repark-functions/src/instant_ts

Submodules of [instant_ts.rs](../instant_ts.rs) (FEXPR-TIMESTAMP-NTZ-1, 2026-11-24).

## Files

- [ltz_cast.rs](ltz_cast.rs) — `__repark_cast_timestamp_ltz__` and `__repark_try_cast_timestamp_ltz__`
  marker UDFs plus `retarget_dml_ltz_store_casts` carrying explicit `TIMESTAMP_LTZ` casts, zoned
  `TIMESTAMP '...'` literals, and DML/Iceberg writes into LTZ columns across `build_expr_context`
  deferral and `spark.sql.timestampType=TIMESTAMP_NTZ`; rewritten in `SparkLtzTimestampCast` via
  the LTZ path regardless of `spark.sql.timestampType`.
- [tests.rs](tests.rs) — unit pins moved out of `instant_ts.rs` plus deferral, explicit-LTZ,
  NTZ `TRY_CAST`, LTZ-to-NTZ wall conversion, stale-`Union` rebuild, and NTZ-session DML-into-LTZ
  store-cast tests.

## Pointers

- Up: [../map.md](../map.md)
