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

First checks: `cargo test -p repark-sql router::`. Escalate to: [../map.md#debug](../map.md).
