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
| A DDL statement reached DataFusion's own CTAS/DROP | something short-circuited before the statement match — `metadata_reference_does_not_bypass_the_create_handler` pins the `$` case that once did |

First checks: `cargo test -p repark-sql router::`. Escalate to: [../map.md#debug](../map.md).
