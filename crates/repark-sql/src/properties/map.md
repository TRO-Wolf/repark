# map — repark-sql/src/properties

## Purpose

File-backed tests for `../properties.rs`. The curated `WITH ( … )` vocabulary: every accepted key, and every refusal class. The
refusals especially — a refusal that stops firing is a silent behavior change.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../properties.rs`.
  **V3-2:** `format_version = 3` is accepted at parse (execute still needs the session opt-in);
  `'1'` and `'4'` still refuse.

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A new property silently ignored | it cannot be — unknown BARE keys refuse; check that the key was added to `CURATED_KEYS` AND handled |

First checks: `cargo test -p repark-sql properties::`. Escalate to: [../map.md#debug](../map.md).
