# map — repark-sql/src/router

## Purpose

File-backed tests for `../router.rs`. Routing DECISIONS: which statements are intercepted, which are delegated, and in what
order the guards run — as distinct from what each handler then does.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../router.rs`.

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A routing regression | the end-to-end battery (`cargo test -p repark-sql tests::`) pins each intercepted form against a real catalog |
| A `DELETE`/`UPDATE` reached delegation without its valves | Both DML data-loss valves live in the shared `Statement::Delete \| Statement::Update` arm (G3-E8 first, then BUG-001) — the BUG-001 valve no longer runs at the router head. A statement that does not PARSE to `Delete`/`Update` bypasses the arm entirely; that is the fail-open attachment class, and `guards::tests::router_parse_dialect_matches_the_session_default` is the net that keeps this door's routing parse equal to the parse `delegate` plans |
| A DDL statement reached DataFusion's own CTAS/DROP | something short-circuited before the statement match — `metadata_reference_does_not_bypass_the_create_handler` pins the `$` case that once did |

First checks: `cargo test -p repark-sql router::`. Escalate to: [../map.md#debug](../map.md).
