# map — repark-core/src/time_travel

## Purpose

File-backed tests for the hoisted time-travel pins (`../time_travel.rs`): `TimeTravelSpec`
parsing (`parse_version_value` / `parse_timestamp_to_ms`) and snapshot resolution. Hoisted
MOVE-ONLY from the v1 SQL crate's `time_travel` module at the port-source pin; the SQL-text
rewrite half (and its tests) is deferred with the phase-2 statement router — see
`task/port/deferred-tests.md`.

## Contents

- `tests.rs` — parser + resolution pins (`#[cfg(test)] mod tests;` in `../time_travel.rs`).

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| `TIMESTAMP AS OF` string fails to parse | Accepted forms: epoch ms, RFC3339/Zulu, `YYYY-MM-dd[ HH:MM:SS][Z]` (UTC) — `parse_timestamp_to_ms` in `../time_travel.rs`. |

First checks: `cargo test -p repark-core time_travel`. Escalate to: [../map.md#debug](../map.md).
