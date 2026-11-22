# map — repark-functions/src/timestamp_cast

## Purpose

Children of [`../timestamp_cast.rs`](../timestamp_cast.rs), split out so the parent stays under its
size ceiling and the crate root under its `check_lib_rs` line ceiling.

## Contents

- `unix_floor.rs` — **UNIX-TIMESTAMP-TRUNCATE-1 (2026-11-22):** the registered `unix_seconds` and
  `unix_millis` that replace `datafusion-spark`'s cast chains, which truncate toward zero. Spark's
  `UnixSeconds` / `UnixMillis` divide the microseconds with `Math.floorDiv`, so
  `1969-12-31 23:59:59.5` is `-1` second and `-500` milliseconds. Every Arrow timestamp unit
  rescales with `div_euclid` (a coarser input multiplies, refusing an `i64` overflow); an untyped
  `NULL` answers `NULL`; a scalar answers a scalar. A `TIMESTAMP_NTZ` argument (an Arrow timestamp
  with no zone) refuses at planning with Spark's `[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE]` text,
  naming the column when the argument is one. Registered from `registration::register_udf_families`,
  after the `datafusion-spark` defaults, so the SQL door, `F.expr` and `F.unix_seconds` /
  `F.unix_millis` (which build the engine function) share it. `unix_micros` stays
  `datafusion-spark`'s. Pins: this file's `tests`, and
  `python/repark/tests/timestamps/test_unix_timestamp_truncate_1.py`.

Up: [../map.md](../map.md).
