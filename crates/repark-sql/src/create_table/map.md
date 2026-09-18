# map — repark-sql/src/create_table

## Purpose

File-backed tests for `../create_table.rs`. Clause refusals prevent silent loss of requested semantics. Each one,
if ignored, produces a table that exists but does not match what was asked for.
**V3-2:** `format_version = 3` is stored at parse and resolved at execute against the session
opt-in; end-to-end pins live in [`../v3/create.rs`](../v3/create.rs).

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../create_table.rs`. Clause
  refusals plus A11: `nanosecond_timestamp_columns_refuse_with_column_and_precision`,
  `nanosecond_timestamptz_columns_refuse`, `microsecond_timestamp_columns_pass_the_ns_gate`.
  **V3-6 C-003** (2026-09-01): the A11 gate admits declared v3 `timestamp_ns` /
  `timestamptz_ns` columns — the end-to-end pins live in
  [`../v3/types.rs`](../v3/types.rs) (pins: v3-6-v3-types/C-003). The tightened-CTAS refusal is pinned in
  [`../../tests/declared_sorted_tighten.rs`](../../tests/declared_sorted_tighten.rs).
  **ICE-COMMIT-UNKNOWN-1 (2026-09-14):**
  `service_managed_ctas_commit_state_unknown_keeps_table_and_surfaces_class` — the
  native-door twin of the repark-spark pin: a delegating catalog answers
  `CommitStateUnknown` from `update_table` after capturing the stamped
  `engine.operation-id`; `drop_table` is never called, the table stays, and `engine_err`
  classifies to `Error::CommitStateUnknown` with the minted id.
  pins: ice-commit-unknown-1/C-001, C-003, C-004

- `rtas_ops_tests.rs` — **ICE-RTAS-OPS-2 round 2 (2026-09-18):** the native-door
  snapshot-operation pins for fixture `rtas_ops`
  (`python/repark/tests/ice_rtas_byname_1_spark_oracle.json`), declared as
  `#[cfg(test)] mod rtas_ops_tests;` in `../create_table.rs`. A memory catalog under
  `RequireExplicitLocation` (staged) or `ServiceManagedLocation` (create-first).
  RTAS cells: `native_ctas_then_rtas_records_append_then_overwrite`,
  `native_rtas_creating_the_table_records_overwrite`,
  `native_empty_rtas_on_new_table_records_delete`,
  `native_empty_rtas_twice_records_two_deletes`. Controls:
  `native_plain_ctas_records_append` (`[append]`) and
  `native_coldef_replace_commits_no_snapshot` (the 2026-09-17 Spark cell: ops stay
  `[append]`, zero rows; red if the column-def form takes the opt-in).
  Service-managed: `native_service_managed_rtas_creating_the_table_records_overwrite`,
  `native_service_managed_empty_rtas_records_delete_then_delete`,
  `native_service_managed_plain_ctas_records_append`.
  pins: ice-rtas-ops-2/C-015, C-016, C-017, C-020

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A clause was accepted and ignored | add it to `refuse_unsupported_clauses` and to `silently_droppable_clauses_all_refuse` |
| `CREATE TABLE (ts TIMESTAMP)` hit Iceberg `timestamp_ns` / v3 | A11: `refuse_nanosecond_timestamp_columns` must fire before `arrow_schema_to_schema_auto_assign_ids`. Declare `TIMESTAMP(6)`. |

First checks: `cargo test -p repark-sql create_table::`. Escalate to: [../map.md#debug](../map.md).
