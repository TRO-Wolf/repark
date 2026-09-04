# map — repark-sql/src/router

## Purpose

File-backed tests for `../router.rs`. They pin which statements are intercepted, delegated, and in what
order the guards run — as distinct from what each handler then does.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../router.rs`.
  FNP-15/16: `execute_refuses_every_armed_declared_name` walks `declared_refuse::armed_names()`
  through `execute` and asserts `NotImplemented` plus the registry-section reason.
  pins: fnp-15-16/C-001, C-008, C-009, C-010, C-011

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A routing regression | the end-to-end battery (`cargo test -p repark-sql tests::`) pins each intercepted form against a real catalog |
| A `COLLATE` spelling reached DataFusion | G15 valve runs immediately after the stock parse (`refuse_collation_in_statement`) — check `../guards.rs`. A string literal containing the word COLLATE must still run |
| A `DELETE`/`UPDATE` reached delegation without its valves | Both DML data-loss valves live in the shared `Statement::Delete \| Statement::Update` arm (G3-E8 first, then BUG-001) — the BUG-001 valve no longer runs at the router head. A statement that does not PARSE to `Delete`/`Update` bypasses the arm entirely; that is the fail-open attachment class, and `guards::tests::router_parse_dialect_matches_the_session_default` is the net that keeps this door's routing parse equal to the parse `delegate` plans. **RP-9 r2:** a three-part Iceberg `DELETE … WHERE <scalar comparison>` is intercepted by `plain::try_allowed_plain_identity` on this arm before G3-E8; UPDATE, literal `IN`, and `table.branch_*` stay delegated |
| A DDL statement reached DataFusion's own CTAS/DROP | `metadata_reference_does_not_bypass_the_create_handler` pins the invariant that `$` metadata references do not bypass the statement match |

First checks: `cargo test -p repark-sql router::`. Escalate to: [../map.md#debug](../map.md).
