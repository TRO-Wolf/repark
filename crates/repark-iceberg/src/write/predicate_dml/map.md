# map — repark-iceberg/src/write/predicate_dml

## Purpose

Test modules for [`predicate_dml.rs`](../predicate_dml.rs) (`execute_predicate_dml` — identity
DELETE / UPDATE with a subquery `WHERE`). They are `#[cfg(test)]` children of that module, split
into files because the parent sits near its size ceiling.

Created by **LRS-5 (2026-08-20)**: both had been included from `write/` with `#[path = "…"]`.
Source comments retain predicate and cleanup contracts; implementation narration is omitted.
AGENTS.md allows a test-fixture exception only where the canonical layout cannot work — here it
works, so the attribute is gone rather than documented.

## Contents

- `predicate_dml_tests.rs` — DELETE: `IN` / `NOT IN (SELECT …)` including the NULL 3VL trap,
  `[NOT] EXISTS` with and without correlation, correlated `IN`. Also the isolation-level property
  pins (M19 / A10).
- `predicate_dml_update_tests.rs` — the identity `UPDATE … SET <scalar> WHERE col IN` arm.
  **MW-9:** unknown `write.delete.granularity` refuses before any parquet write.

## Pointers

- Up: [`../map.md`](../map.md)
