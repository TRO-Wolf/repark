# map — repark-sql/src/properties

## Purpose

File-backed tests for `../properties.rs`. They cover the curated `WITH ( … )` vocabulary and each refusal class. The
refusals especially — a refusal that stops firing is a silent behavior change.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../properties.rs`.
  **V3-2:** `format_version = 3` is accepted at parse (execute still needs the session opt-in).
  **WO U5 PR2b (2026-09-24):** `'1'` is accepted as well; `'0'` and `'4'` refuse, naming v1, v2
  or v3. The V3-2 C-007 citation (`'1'` refuses) is superseded (errata in the archived v3-2
  ledger).
  pins: ice-nested-evo-1/C-052

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A new property silently ignored | it cannot be — unknown BARE keys refuse; check that the key was added to `CURATED_KEYS` AND handled |

First checks: `cargo test -p repark-sql properties::`. Escalate to: [../map.md#debug](../map.md).
