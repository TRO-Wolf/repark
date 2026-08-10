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

Deliberately NOT here: extraction semantics. This unit does not change what `year`/`hour`/
`date_trunc` return, so there is nothing about them to pin at this layer (H-1a split B owns them,
with its own extractor pins).

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
| The zone is set but query results did not move | Expected in this unit: the value is carried, not yet consumed by extraction. See `../map.md` "Debug" and the recorded disclosure rows. |

First checks: `cargo test -p repark-core session_time_zone`. Escalate to:
[../map.md#debug](../map.md).
