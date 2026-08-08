# map — repark-sql/src/create_table

## Purpose

File-backed tests for `../create_table.rs`. `CREATE TABLE` clause refusals: the clauses that must never be SILENTLY DROPPED. Each one,
if ignored, produces a table that exists but does not match what was asked for.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../create_table.rs`.

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A clause was accepted and ignored | add it to `refuse_unsupported_clauses` and to `silently_droppable_clauses_all_refuse` |

First checks: `cargo test -p repark-sql create_table::`. Escalate to: [../map.md#debug](../map.md).
