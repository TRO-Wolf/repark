# map — repark-sql/src/partitioning

## Purpose

File-backed tests for `../partitioning.rs`. Partition-transform parsing and spec building: every transform name, every arity branch,
every bound, and the schema-resolution failure. A partition spec that is wrong is not
recoverable after the table exists.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../partitioning.rs`.

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| Field names differ from a Spark-created table | the Java suffix rules live in `PartitionTransform::field_name` |

First checks: `cargo test -p repark-sql partitioning::`. Escalate to: [../map.md#debug](../map.md).
