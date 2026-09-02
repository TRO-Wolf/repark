# map — repark-sql/src/alter

## Purpose

File-backed tests for `../alter.rs` (`ALTER TABLE`).

The schema-evolution half needs a real Iceberg catalog, so it is pinned end to end in
`../tests.rs`; what lives here is the `SET PROPERTIES` recognizer and its curated vocabulary,
which are pure functions.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../alter.rs`.
  **V3-10:** `format_version` is no longer a reserved refusal here — the recognizer folds it to
  the Iceberg `format-version` key for the upgrade path (a bare number or a string literal), and
  only `format_version = DEFAULT` refuses, because a format version only moves up. The version
  itself is resolved against the table and the session opt-in at execute, not at parse.
  pins: v3-10-upgrade-v2-to-v3/C-003

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| The recognizer edited text inside a string literal | it cannot — it indexes `scan::word_spans` over SCRUBBED text; see `rewrite_does_not_fire_on_other_statements_or_inside_literals` |
| A property key was silently ignored | there is no ignore path — every key either maps, refuses as reserved, or refuses as unknown |

First checks: `cargo test -p repark-sql alter::`. Escalate to: [../map.md#debug](../map.md).
