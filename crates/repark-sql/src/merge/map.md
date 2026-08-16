# map — repark-sql/src/merge

## Purpose

File-backed tests for `../merge.rs` — the `MERGE INTO` lowering.

The executor is tier-1 (`repark_iceberg::write::merge`) and carries its own battery. What is
pinned here is the AST → `MergeSpec` mapping — the half that could drift from the Spark door's
mapping of the SAME target type (design §6 R3).

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../merge.rs`.
  **MG-2 (2026-08-15):** M2 Oracle `UPDATE SET … WHERE` / `DELETE WHERE` /
  `INSERT … WHERE` refusals; M3 SET/INSERT target qualification + nested-field
  refuse + target-qualified/bare positives + quoted-alias SET; M10 non-last unconditional MATCHED
  and NOT MATCHED refusals + last-unconditional positive. M8 column-list
  refuse was already pinned (`degenerate_update_and_insert_shapes_refuse`).
- `cardinality_tests.rs` — native-door execute pins for the M11 lone-unconditional-DELETE
  cardinality exemption (refuse-gone + still-raises on UPDATE and conditional DELETE).

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A lowering test fails after a sqlparser bump | the clause/action pairings sqlparser itself enforces are pinned as parser properties; a bump can move that line |
| A MERGE expression did not resolve at execution | aliases: an unaliased relation is referenced by its bare name, and the alias is rendered WITH its quoting |

First checks: `cargo test -p repark-sql merge::`. Escalate to: [../map.md#debug](../map.md).
