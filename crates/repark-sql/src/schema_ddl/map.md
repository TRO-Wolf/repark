# map — repark-sql/src/schema_ddl

## Purpose

File-backed tests for `../schema_ddl.rs`. They pin schema `WITH ( … )` vocabulary, name qualification, and
identifier hygiene that runs BEFORE anything reaches the catalog. The end-to-end effects live
in `../tests.rs` against a real catalog.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../schema_ddl.rs`.
- `location_guard_tests.rs` — ANSI `CREATE SCHEMA IF NOT EXISTS` four-shape twins (create-new / same /
  conflicting / no-location) against a memory catalog.

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A path escaped the warehouse root | `reject_path_escape_ident` runs before any path is composed; check the call sites in `../create_table.rs` |

First checks: `cargo test -p repark-sql schema_ddl::`. Escalate to: [../map.md#debug](../map.md).
