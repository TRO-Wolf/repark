# map — repark-sql/src/partitioning

## Purpose

File-backed tests for `../partitioning.rs`. They cover transform names, arity branches,
every bound, and the schema-resolution failure. A partition spec that is wrong is not
recoverable after the table exists.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../partitioning.rs`.
  **IPI-51 PR6 slice 3 (2026-09-21):** `unknown_partition_column_refuses_listing_columns`
  requires `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` / `42703` with backticked suggestions; the
  empty-transform, resolve-against-schema and unsupported-transform pins are untouched.
  pins: ice-error-conditions-1/C-011

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| Field names differ from a Spark-created table | the Java suffix rules live in `PartitionTransform::field_name` |

First checks: `cargo test -p repark-sql partitioning::`. Escalate to: [../map.md#debug](../map.md).
