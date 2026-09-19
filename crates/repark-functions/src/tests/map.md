# map — repark-functions/src/tests

## Purpose

Crate-root unit battery for `repark-functions` modules whose tests live outside the module file.
Only tests; `mod.rs` is the module manifest.

## Contents

- `mod.rs` — module manifest.
- `spark_string_timestamp.rs` — **CAST-TS-STRING-1 (2026-09-19):** the kernel against Spark
  4.1.2's measured answers: year-only and single-digit fields, fraction truncation and padding,
  time-only strings against a fixed clock, the zone position rule, the trim set, digit counts,
  calendar and clock range checks, Java offset forms and limits, the legacy padding, prefixes,
  short ids and regions, DST gap and overlap, the chrono-tz table end and the far-future
  proxy, local mean time, the i64 microsecond edges, and both failure modes (NULL, and
  `CAST_INVALID_INPUT` with Spark's `'…'` quoting). pins: cast-ts-string-1/C-001, C-002, C-003, C-008
- `spark_string_timestamp_sql.rs` — the same kernel through the analyzer: literal and column
  `CAST`, ANSI on and off, `TRY_CAST`, one-argument `to_timestamp` and `try_to_timestamp`, each
  as `Timestamp(µs, "UTC")`. pins: cast-ts-string-1/C-004, C-005, C-006

## Pointers

- Up: [../map.md](../map.md)
