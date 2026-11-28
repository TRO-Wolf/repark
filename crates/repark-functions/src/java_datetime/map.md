# map — repark-functions/src/java_datetime

## Purpose

Children of [`../java_datetime.rs`](../java_datetime.rs), the shared Java-pattern parser, split
out because the parent sits at its 1000-line ceiling.

## Contents

- `walk.rs` — **DATETIME-PATTERN-PARSE-1 (2026-11-28):** the token walk behind
  `parse_wall_text`, every `to_timestamp` / `to_date` / `unix_timestamp` / `to_unix_timestamp` /
  `to_timestamp_ltz` / `to_timestamp_ntz` / `try_to_timestamp` / `try_to_date` parse with a
  format, and the `from_csv` / `from_json` `timestampFormat` / `dateFormat` options.
  - An `S` run of width *n* takes 1 to *n* digits, as Spark's
    `appendFraction(NANO_OF_SECOND, 1, n, false)` does; the nanos scale by the digits taken.
    No digit, or more than *n*, fails at the same index main reported.
  - `[` opens and `]` closes an optional section (`link` pairs them; `JavaToken::Open` holds
    the index after its `]`). Sections nest; an unclosed `[` runs to the pattern end; a `[`
    inside quotes stays a literal; an empty `[]` is a no-op. A stray `]` refuses with
    `[INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION]`, `try_` forms
    included. `walk` saves the cursor at each `[`, commits it at `]`, and on a position
    failure inside a section restores it and resumes after the section, as Java's optional
    `CompositePrinterParser` does; a committed section is never re-entered. A field-range
    failure (`Invalid value for …`) is not rolled back.
  - Inside a section, a field that sets a value already parsed with a different value fails
    the section, as Java's `setParsedField` conflict does. Outside sections the later value
    still wins, as on main.
  - Two shapes keep main's literal brackets and exact-width fractions (`ParsedPattern::exact`):
    a pattern with a token this parser cannot read (`JavaToken::Unsupported`, the zone letters
    among them), and a pattern whose digit run crosses a section boundary after a field Spark
    reads without a width bound (`yyyy`, or a one-letter `M d H h m s`). Java's adjacent value
    parsing does not reach into a section, so Spark's greedy field there takes the section's
    digits and fails a text this walk would accept.
  - `try_dates_with_format` is `try_to_date`'s two-argument arm: `to_date_with_format` with ANSI
    off, so a time-bearing pattern parses its time and returns the date, and a pattern error
    still raises.
  - Pins: `java_datetime::walk::tests::*`, `python/repark/tests/test_datetime_pattern_parse.py`.
