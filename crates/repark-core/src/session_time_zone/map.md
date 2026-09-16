# map — repark-core/src/session_time_zone

## Purpose

File-backed test module of `../session_time_zone.rs` — the session timezone
(`spark.sql.session.timeZone`) as a *configuration surface*: parsing, validation, the one
authoritative key spelling, and the resolved value reaching engine session state.

## Contents

- `tests.rs` — eleven pins in three cohorts:
  - **parse/validate** — absent key → the `UTC` default; an IANA id is kept verbatim; a fixed
    offset (`+05:30`) is accepted; a padded value is trimmed rather than treated as a different
    zone; an unknown zone and a blank value each fail loud naming the conf key, as
    `Error::Config` (the variant that reaches Python as `IllegalArgumentException`).
  - **exactly one spelling** — `lookalike_spellings_are_not_a_second_way_to_set_the_zone` walks
    six near-misses (case variants, `snake_case`, `repark.`-namespaced) and asserts each leaves
    the default in place. This is the pin that turns "we have one spelling" from a claim into a
    gate: an alias added later reds here.
  - **reaches session state** — a bare session carries `UTC`; a builder conf reaches
    `ReparkSession::session_time_zone`; an invalid zone fails the BUILD (not a later query); a
    session clone shares the resolved zone.
  - **runtime values** — SET-ANSI-RUNTIME-1 (2026-09-15): `parse_runtime_session_zone_value`
    accepts what Java `ZoneId.of` accepts (IANA ids plus `+05`, `+5`, `+18:00`, `GMT+8`,
    `+08:00`, `Z`) and refuses what Java refuses (past ±18:00, unknown ids, blanks, quoted
    values) with Spark's `INVALID_CONF_VALUE.TIME_ZONE` as `Error::IllegalArgument` (the
    variant that reaches Python as `IllegalArgumentException`). The offset arm runs BEFORE
    the IANA check: Arrow alone would accept `+18:01`, and a sign-led value that fails the
    offset arm never falls through to it. Refusals echo the RAW value (batch-17 oracle).
    A stored runtime zone is what `session_time_zone` reports, on the session and its
    clones. pins: set-ansi-runtime-1/C-002
  - **canonical companion (R-17c-4, 2026-09-15; corrected by R-17c-6, oracle batch-17):**
    `canonical_session_zone_id` maps the raw snapshot text to the id every value-bearing
    consumer parses — measured against Arrow `Tz::from_str` and Python `ZoneInfo` (which
    takes IANA names plus `UTC`/`GMT` only, never an offset). `ZoneId` matching is
    case-sensitive (`GMT+8` yes, `gmt+8` no; `Z` yes, `z` no) and surrounding whitespace is
    not trimmed (padded refuses). Zero-seconds forms (`+18:00:00`, `+053000`, `+05:30:00`)
    canonicalise to `±HH:MM` with identical instants; nonzero-seconds (`+05:30:30`) has no
    Arrow form and stays a refusal as declared divergence SET-ANSI-RUNTIME-4. The single
    shared table is `canonical_zone_table.txt` in this directory, asserted by both crates'
    tests; the same mapping is duplicated in the functions-carrier fill
    (`repark-functions` cannot depend on this crate and the builder fill site is out of
    fence). pins: set-ansi-runtime-1/C-002

Deliberately NOT here: extraction implementation. H-1a split B owns extractor pins; this map covers
parsing, one spelling, and resolved session state.

## Pointers

- Up: [../map.md](../map.md)
- The user-visible half of the same knob: `python/repark/src/repark/session/session_time_zone.py`
- The recorded cross-engine rows: `python/repark/tests/test_session_timezone_parity.py`

## Debug

| Symptom | First check |
|---|---|
| A new zone string is refused | Validation is Arrow's zone database (`arrow::array::timezone::Tz`), which accepts IANA ids and fixed offsets — not Windows zone names or abbreviations like `EST5EDT` aliases that the database lacks. The refusal quotes the value. |
| EVERY IANA id is refused but `+05:00` still works | The `chrono-tz` feature on `repark-core`'s `arrow` dependency is gone. It is declared in `crates/repark-core/Cargo.toml` precisely so this validator does not ride `datafusion`'s feature graph; re-declare it there. |
| A conf key that "looks right" configures nothing | It is probably a lookalike, not the key: exactly one spelling exists (`spark.sql.session.timeZone`, case-sensitive). `lookalike_spellings_are_not_a_second_way_to_set_the_zone` enumerates the near-misses. |
| The zone is set but query results did not move | Check extractor parity pins; the resolved zone is consumed by extraction. |

First checks: `cargo test -p repark-core session_time_zone`. Escalate to:
[../map.md#debug](../map.md).
