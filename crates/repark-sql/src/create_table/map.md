# map — repark-sql/src/create_table

## Purpose

File-backed tests for `../create_table.rs`. `CREATE TABLE` clause refusals: the clauses that must never be SILENTLY DROPPED. Each one,
if ignored, produces a table that exists but does not match what was asked for.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../create_table.rs`. Clause
  refusals plus A11: `nanosecond_timestamp_columns_refuse_with_column_and_precision`,
  `nanosecond_timestamptz_columns_refuse`, `microsecond_timestamp_columns_pass_the_ns_gate`.
  The SE-1 PR-D1 tightened-CTAS refuse is pinned in
  [`../../tests/declared_sorted_tighten.rs`](../../tests/declared_sorted_tighten.rs).

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A clause was accepted and ignored | add it to `refuse_unsupported_clauses` and to `silently_droppable_clauses_all_refuse` |
| `CREATE TABLE (ts TIMESTAMP)` hit Iceberg `timestamp_ns` / v3 | A11: `refuse_nanosecond_timestamp_columns` must fire before `arrow_schema_to_schema_auto_assign_ids`. Declare `TIMESTAMP(6)`. |

First checks: `cargo test -p repark-sql create_table::`. Escalate to: [../map.md#debug](../map.md).
