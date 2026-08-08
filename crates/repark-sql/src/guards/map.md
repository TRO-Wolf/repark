# map — repark-sql/src/guards

## Purpose

File-backed tests for `../guards.rs`. The guard set. Every guard is a behavior and every REFUSAL is a behavior, so each refusal
message class has its own test alongside an acceptance case proving the guard is not simply
refusing everything.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../guards.rs`.

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A guard fires on a string literal | it cannot — the guards read scrubbed text; check `../scan.rs`'s tests |

First checks: `cargo test -p repark-sql guards::`. Escalate to: [../map.md#debug](../map.md).
