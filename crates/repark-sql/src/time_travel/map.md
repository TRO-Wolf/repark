# map — repark-sql/src/time_travel

## Purpose

File-backed tests for `../time_travel.rs` (`FOR … AS OF` scanner).

The span pin set uses double-quote ANSI variants: span extraction, ref-name
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
| A `__repark_ansi_tt_*` name outlived its statement | `read_table_at` registers `__repark_tt_*` first; SQL records that name, then records its ANSI name before `register_table`. The router releases both after planning; the introspection pin checks both prefixes |
| A `__repark_tt_*` name (no `ansi`) outlived its statement | If core registration succeeds but `ctx.table` lookup fails, no frame returns and SQL cannot discover or record the core name. For a returned frame, `PinnedViews` releases both prefixes; reader-options registrations remain by design |

First checks: `cargo test -p repark-sql time_travel::`. Escalate to: [../map.md#debug](../map.md).
