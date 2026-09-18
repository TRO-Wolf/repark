# map — repark-functions/src/timestamp_ns_cast

## Purpose

ICE-TSNS-SQL-1 support. The parent `timestamp_ns_cast.rs` holds the two embedded Spark-door
casts to Iceberg `timestamp_ns` / `timestamptz_ns` and the `VALUES` timestamp-column conform the
`spark_ltz_timestamp_cast` analyzer rule calls.

## Contents

- `tests.rs` — the module's `#[cfg(test)]` suite: nine fraction digits from strings, an explicit
  offset versus the session zone, malformed strings under ANSI on and off, exact microsecond
  widening, an instant into a naive target as the session wall, a wall into a zoned target
  localized, the overflow refusal, the planning-time refusal of a non-temporal source, and the
  `VALUES` retype / mixed-row cast.
  pins: ice-tsns-sql-1/C-001, C-002

## Pointers

- Up: [../map.md](../map.md)
