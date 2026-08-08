# map — repark-sql/src/merge

## Purpose

File-backed tests for `../merge.rs` — the `MERGE INTO` lowering.

The executor is tier-1 (`repark_iceberg::write::merge`) and carries its own battery. What is
pinned here is the AST → `MergeSpec` mapping — the half that could drift from the Spark door's
mapping of the SAME target type (design §6 R3).

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../merge.rs`.

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A lowering test fails after a sqlparser bump | the clause/action pairings sqlparser itself enforces are pinned as parser properties; a bump can move that line |
| A MERGE expression did not resolve at execution | aliases: an unaliased relation is referenced by its bare name, and the alias is rendered WITH its quoting |

First checks: `cargo test -p repark-sql merge::`. Escalate to: [../map.md#debug](../map.md).
