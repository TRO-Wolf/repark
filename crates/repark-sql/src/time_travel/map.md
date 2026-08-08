# map — repark-sql/src/time_travel

## Purpose

File-backed tests for `../time_travel.rs` — the `FOR … AS OF` scanner.

The v1 span pin set ported as double-quote ANSI variants (graft G7): span extraction, ref-name
strings, negative snapshot ids, multi-relation joins, comment and string-literal immunity, quoted
name parts. The end-to-end (session) rows live in `../tests.rs`.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../time_travel.rs`.

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A clause was silently not rewritten | it cannot be silent — a RECOGNIZED clause with an unusable value returns `Err`; check `clause_kind_at` actually matched |
| `"main"` was read as a ref name | it must not be — a quoted token is an IDENTIFIER in this door and refuses, steering to `'main'` |

First checks: `cargo test -p repark-sql time_travel::`. Escalate to: [../map.md#debug](../map.md).
