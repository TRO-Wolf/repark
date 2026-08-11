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
| A `__repark_ansi_tt_*` name outlived its statement | `PinnedViews` records every registration and `router::execute` releases them after planning; the pin is `tests/introspection.rs::time_travel_pinned_views_do_not_leak_into_the_introspection_surface` |
| A `__repark_tt_*` name (no `ansi`) outlived its statement | The CORE half: `register_pinned_view` composes this door's view over `repark_core::read_table_at`, which registers a name of its own. Since H-1b both go into the same `PinnedViews`; the pin above asserts BOTH `LIKE` prefixes, which are disjoint (`__repark_tt%` never matches `__repark_ansi_tt_<n>`) |

First checks: `cargo test -p repark-sql time_travel::`. Escalate to: [../map.md#debug](../map.md).
