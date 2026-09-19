# map — repark-functions/src/spark_string_timestamp

## Purpose

CAST-TS-STRING-1 (2026-09-19): Spark 4.1.2's `SparkDateTimeUtils.stringToTimestamp` as one
Rust kernel. The parent [`../spark_string_timestamp.rs`](../spark_string_timestamp.rs) holds the
entry points every string → `TIMESTAMP` (LTZ) site calls; this directory holds the three halves
of the rule. Closes when the Spark grammar changes (a Spark bump re-records the oracle).

## Contents

- `grammar.rs` — `parse_timestamp_string`: Spark's `parseTimestampString` byte for byte. Trim
  bytes `<= 0x20` and `0x7F` from both ends; an optional `+` / `-` year sign; segments
  year (4–6 digits), month, day, hour, minute, second (1–2 digits each), fraction (any count,
  the first 6 kept, short ones padded). `-` separates date fields, a single ` ` or `T` separates
  date and time, `:` separates time fields. A `T` at byte 0 or a `:` after the first number (no
  sign) makes a time-only string. After the seconds or the fraction, the rest of the string is
  the zone id. A zone after the minutes, a second `.`, `t`, or a doubled blank fails.
- `zone.rs` — `spark_zone_id`: Spark's `getZoneId` then `ZoneId.of(id, SHORT_IDS)`. It pads the
  legacy `+h:mm` and `+hh:m` forms, maps the 28 Java short ids, reads `Z` and
  `+h` / `+hh` / `+hhmm` / `+hh:mm` / `+hhmmss` / `+hh:mm:ss` offsets (18 h cap), the
  `UTC` / `GMT` / `UT` prefixes, and case-sensitive region ids (`[A-Za-z][A-Za-z0-9~/._+-]+`,
  resolved through chrono-tz). Anything else is not a zone.
- `instant.rs` — `instant_micros`: `LocalDate.of` / `LocalTime.of` range checks (proleptic
  Gregorian, year 0 allowed), today's date in the zone for a time-only string, then
  `ZonedDateTime.of` (a DST gap shifts forward, an overlap takes the earlier offset) and an
  exact microsecond result (overflow → no value). chrono-tz tabulates transitions only up to
  `LAST_TABULATED_YEAR` (2099) and Java applies the final rule for ever, so a later wall reads
  its offset from the latest year in 2072–2099 with the same leap flag and January-1 weekday.
  A wall before 1200 reads its offset from the same day in 1200–1599 (local mean time), so the
  i64 range edge outside chrono's calendar still resolves.

Tests: [`../tests/spark_string_timestamp.rs`](../tests/map.md).
pins: cast-ts-string-1/C-001, C-002, C-003, C-008

## Pointers

- Up: [../map.md](../map.md)
- Oracle and facade pins: `python/repark/tests/test_cast_ts_string_1.py`
