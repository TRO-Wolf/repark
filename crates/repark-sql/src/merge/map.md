# map — repark-sql/src/merge

## Purpose

File-backed tests for `../merge.rs` (`MERGE INTO` lowering).

The executor is tier-1 (`repark_iceberg::write::merge`) and carries its own battery. What is
pinned here is the AST → `MergeSpec` mapping — the half that could drift from the Spark door's
mapping of the SAME target type (design §6 R3).

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in `../merge.rs`.
  Oracle `UPDATE SET … WHERE` / `DELETE WHERE` / `INSERT … WHERE` refusals; SET/INSERT target
  qualification and nested-field refusal; target-qualified, bare, and quoted-alias positives;
  non-last unconditional MATCHED and NOT MATCHED refusals; and column-list refusal.
- `cardinality_tests.rs` — native-door execute pins for the lone-unconditional-DELETE
  cardinality exemption (refuse-gone + still-raises on UPDATE and conditional DELETE).
- `nmbs_tests.rs` — DML-A ANSI-door `WHEN NOT MATCHED BY SOURCE` execute pins (COW+MOR
  DELETE, UPDATE, three arms, source-empty wipe, cardinality).
  pins: dml-a-merge-not-matched-by-source/C-002, C-003, C-004, C-005, C-006

## Pointers

- Up: [../map.md](../map.md). Design: `../../../../docs/design/sql-doors.md`.

## Debug

| Symptom | First check |
|---|---|
| A lowering test fails after a sqlparser bump | the clause/action pairings sqlparser itself enforces are pinned as parser properties; a bump can move that line |
| A MERGE expression did not resolve at execution | aliases: an unaliased relation is referenced by its bare name, and the alias is rendered WITH its quoting |

First checks: `cargo test -p repark-sql merge::`. Escalate to: [../map.md#debug](../map.md).
