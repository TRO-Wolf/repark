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
  **C-2d fold 1 (2026-10-07):** the horizon pins import `LAST_TABULATED_YEAR` and `proxy_year`
  from `repark_common::zone_horizon`, their new home; every assertion is unchanged.
- `spark_string_timestamp_sql.rs` — the same kernel through the analyzer: literal and column
  `CAST`, ANSI on and off, `TRY_CAST`, one-argument `to_timestamp` and `try_to_timestamp`, each
  as `Timestamp(µs, "UTC")`. pins: cast-ts-string-1/C-004, C-005, C-006
  A dictionary-encoded string column runs the kernel on every door, and the kernel battery
  pins the doubled-blank refusal (verification critic). pins: cast-ts-string-1/C-013
- `zone_horizon_render.rs` — **ZONE-HORIZON-RENDER-1 (2026-10-08):** the horizon in real
  zones, against answers recorded on live Spark 4.1.2. 40 recorded instants (New York, Sydney,
  Lord Howe, Kolkata, UTC; 2099 controls, 2100, 2104, 2500, 9999; both sides of each
  transition) render at Spark's wall clock; 12 recorded wall clocks, gap and overlap included,
  are placed at Spark's instant by `micros_from_local_datetime` and by the literal kernel; the
  round trip over eight zones (an instant every 11 days, 7 hours and 1861 seconds in
  2100–2500, more than 12 000 per zone) reads back as the instant with the preferred offset,
  agrees with the literal without it, and differs from the instant in fewer than 20 samples
  per zone (the overlaps); a time-only string takes today's date from the final rule; the
  card's three expressions answer noon in six zones and five years; 21 extractor and 7
  constructor expressions answer Spark's New York 2100 cells through the analyzer; and a batch
  reads Arrow's kernel only when every instant is inside the tables (a 2024 + 2099 batch, a
  2024 + 2100 batch and a 2100 + 2500 batch each answer the right hour, weekday and year).
  pins: zone-horizon-render-1/C-001, C-002, C-004, C-006, C-007, C-011
  The same unit rewrote `spark_string_timestamp.rs::chrono_tz_tables_stop_after_the_last_tabulated_year`:
  it read the table end through `micros_from_local_datetime`, which now reads the horizon, so
  it reads chrono-tz itself for the table end (−4 at 2099, −5 at 2100) and the funnel for the
  placement (−4 at both).
  **Fold 1 Item A (2026-10-08):** `an_offset_with_seconds_prints_main_text_before_2100`
  pins `to_json` of a seconds-bearing offset (New York, Paris, Kolkata, Sydney at
  1850, 1883 and 0001) to main's text, and the `from_json` round trip to a
  non-empty document.
  **Fold 1 Item B (2026-10-08):**
  `a_zone_without_a_transition_in_2099_reads_the_table_end` pins Casablanca and
  El_Aaiun at 2112-09-11 12:34:56 UTC to 13:34:56 (string cast and hour). The
  facade grid keeps its six zones: the two zones are pinned in Rust only.

## Pointers

- Up: [../map.md](../map.md)
