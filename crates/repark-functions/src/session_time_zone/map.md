# map — repark-functions/src/session_time_zone

## Purpose

File-backed test module of `../session_time_zone.rs` — the **carrier** that brings the resolved
session timezone (`spark.sql.session.timeZone`) down to the calendar extractors in
`../datetime.rs`, and the pins that keep it a carrier rather than a second knob.

## Contents

- `tests.rs` — six pins in two cohorts:
  - **it carries** — an absent carrier falls back to `DEFAULT_EXTRACTION_TIME_ZONE` (`UTC`) so a
    bare DataFusion context still works; an installed zone reads back verbatim for IANA ids and
    fixed offsets alike; installing twice keeps the last value rather than leaving two truths.
  - **it is not a knob** — `the_carrier_refuses_to_be_set_and_names_the_one_authoritative_key`
    (every key spelling refuses, and the refusal names `spark.sql.session.timeZone`),
    `the_carrier_advertises_no_settable_entries` (`entries()` is empty, so it never surfaces in
    `information_schema` settings), and `the_sql_set_door_cannot_reach_the_carrier` (a real
    `SessionContext`: `SET repark.session.…` and `SET spark.sql.session.timeZone` both fail, and
    the session's zone is unchanged after every refused spelling).

Deliberately NOT here: the extraction SEMANTICS. What `year` / `hour` / `date_trunc` actually
answer under a zone is pinned end-to-end on real sessions in
`crates/repark-spark/tests/session_timezone.rs` and
`crates/repark-sql/tests/session_timezone_ansi_door.rs`; the coercion path's own properties
(idempotence, instant-vs-DATE) are pinned inside `../datetime.rs`'s test module.

## Pointers

- Up: [../map.md](../map.md)
- Who FILLS the carrier: `crates/repark-spark/src/extension.rs` (`SparkExtension::configure`) —
  the only crate that depends on both the engine that owns the key and the leaf that reads it
- Who OWNS the key, the spelling and the validation: `crates/repark-core/src/session_time_zone.rs`
- The recorded cross-engine rows: `python/repark/tests/test_session_timezone_parity.py`

## Debug

| Symptom | First check |
|---|---|
| Extraction ignores the session zone on a real session | The carrier was not installed. Only `SparkExtension::configure` installs it; a session built without the Spark extension is stock DataFusion and reads the stored zone (pinned by `crates/repark-sql/tests/session_timezone_ansi_door.rs::a_native_session_without_the_spark_extension_reads_the_stored_zone`). |
| `SET repark.session.… = '…'` errors | Correct and deliberate. DataFusion resolves an extension namespace on the text before the FIRST `.`, so the two-segment `PREFIX` makes this carrier unreachable from `SET`; the zone is set with `spark.sql.session.timeZone` on the builder, once, at session build. |
| The default zone disagrees with `repark-core`'s | `DEFAULT_EXTRACTION_TIME_ZONE` is a second constant by necessity (this crate has no `repark-core` edge). The two are pinned equal across the seam by `crates/repark-spark/tests/session_timezone.rs::default_session_extracts_in_the_core_default_zone`. |
| A zone is accepted here that the engine would refuse | This module does no validation on purpose — one validator, in `repark-core`, at session build. If an invalid zone reached the carrier, the session that built it should never have built. |

First checks: `cargo test -p repark-functions session_time_zone`. Escalate to:
[../map.md#debug](../map.md).
