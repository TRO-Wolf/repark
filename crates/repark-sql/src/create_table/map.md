# map — repark-sql/src/create_table

## Purpose

File-backed tests for `../create_table.rs`. Clause refusals prevent silent loss of requested semantics. Each one,
if ignored, produces a table that exists but does not match what was asked for.
**V3-2:** `format_version = 3` is stored at parse and resolved at execute against the session
opt-in; end-to-end pins live in [`../v3/create.rs`](../v3/create.rs).

## Contents

- `nanosecond_ask.rs` — **ICE-TSNS-CREATE-DOOR-1 (2026-10-13):** the native door's adapter
  between the session and `repark_iceberg::write::nanosecond_door`, the same body as the
  Spark door's: `NanosecondAsk::every()` when `repark.sql.allowImplicitTimestampNanos` is
  on, else the flags of `asked_nanosecond_columns` over the eagerly analyzed plan. `ask_of`
  answers `every()` for column DDL, which has no query. This door's remedies say the
  nanosecond cast cannot be written here (`CAST(x AS timestamp_ns)` is `Unsupported SQL
  type`) and the microsecond spelling is `TIMESTAMP(6)` for both kinds
  (`TIMESTAMP(6) WITH TIME ZONE` parses but stores a naive `timestamp`, measured), so the
  refusal raised here names the session setting and `CAST(<expression> AS TIMESTAMP(6))`
  only. `repark-sql` does not name
  `repark-spark`, so the body is repeated here.
  pins: ice-tsns-create-door-1/C-005
- `nested_type.rs` — **ICE-NESTED-EVO-1 round 2 (2026-09-18, run 22b):** the ANSI door's
  structural nested-type mapping, a child module of `../create_table.rs`:
  `needs_structural_mapping` (a `MAP` anywhere, or a struct-field option),
  `structural_type_to_iceberg` (struct / `ARRAY<…>` / map recursion, leaves through the
  parent's `CAST` path, a required struct child from `struct_field_required`), and
  `rewrite_nested_create_types` (the router's CREATE-only token rewrite). Pinned by
  `../../tests/ansi_nested_ddl_oracle.rs`.
  pins: ice-nested-evo-1/C-016, C-017
  **Round 3 (2026-09-18, run 22b, V-002):** the gate and the rewrite cover only the
  column-definition list (`create_column_list_has_nested_type_opener`,
  `rewrite_create_column_types`), so `CREATE TABLE t AS SELECT * FROM src WHERE map < 5 AND
  id > 0` is no longer rewritten to `map ( 5 AND id ) 0`.
  pins: ice-nested-evo-1/C-022
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

- `rtas_ops_tests.rs` — **U7 PR2 slice-1 round 3 (2026-09-25):**
  `native_column_def_replace_keeps_field_ids_by_name` and `native_rtas_keeps_field_ids_by_name`
  pin the replace's ids by name and `last-column-id`. Critic r3 [S2]:
  `native_partitioned_rtas_keys_the_spec_by_name_and_keeps_the_old_branch` and
  `native_partitioned_column_def_replace_keys_the_spec_by_name_and_keeps_the_old_branch` put the
  added column first, so an un-re-keyed schema would key it 1 while the pins hold the default
  spec's source id at the re-keyed 4 (`WITH (partitioning = ARRAY['extra' | 'payload'])`), and read
  branch `b1` (created before the replace) after it: its pre-replace row comes back through the
  new schema as `[NULL, 1, x]`, as measured against Spark. Helpers `rows` (cells as text, nulls
  as `NULL`) and `spec_source_ids`. pins: u7-write-df-2/C-011
  **ICE-RTAS-OPS-2 round 2 (2026-09-18):** the native-door
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

**ICE-TT-RESOLVE-1 round 2 (2026-09-19):** call sites use the 3-arg `EngineContext::new` again; the zone flows only through `new_with_time_zone`. pins: ice-tt-resolve-1/C-003
