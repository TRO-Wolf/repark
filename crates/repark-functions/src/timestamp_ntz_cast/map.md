# map — repark-functions/src/timestamp_ntz_cast

## Purpose

WO NTZ-1 slice 1 support. The parent `timestamp_ntz_cast.rs` holds the embedded Spark-door
`TIMESTAMP_NTZ` cast (`__repark_cast_timestamp_ntz__`, `__repark_try_cast_timestamp_ntz__`) and
the wall literal (`__repark_timestamp_ntz__`), registered through `instant_ts::functions()` and
reached only through the SQL door's literal and cast lowering and the DataFrame-door cast.

## Contents

- `tests.rs` — the module's `#[cfg(test)]` suite: the literal parse table (a zoned wall, a
  date-only wall, a T separator, a seven-digit fraction truncated to micros, three malformed
  refusals), strings / dates / NULLs through the cast, an instant as its session-zone wall in
  UTC and New York, malformed strings raising `CAST_INVALID_INPUT` under ANSI and NULL under
  try_cast, numeric sources refusing with Spark's `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`
  class and names, and the literal answering a naive wall named `TIMESTAMP_NTZ '<wall>'`.
  pins: ntz-1/C-001, C-002
  **WO NTZ-1 re-verify fold (2026-09-28):** the store-wrap idempotence pin
  (an already-wrapped expression keeps a single wrap; `wrap_bare=false`
  leaves the expression alone). pins: ntz-1/C-006

## Pointers

- Up: [../map.md](../map.md)
