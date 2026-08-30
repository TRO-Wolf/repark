# map — repark-iceberg/src/write/predicate_dml/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

Identity DELETE/UPDATE tests. `predicate_dml.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index.
- `predicate_dml.rs` — DELETE: `IN` / `NOT IN (SELECT …)` including the NULL 3VL trap,
  `[NOT] EXISTS` with and without correlation, correlated `IN`, isolation-level pins
  (M19 / A10).
- `update.rs` — identity `UPDATE … SET <scalar> WHERE col IN`. Unknown
  `write.delete.granularity` refuses before any parquet write (MW-9).

## Pointers

- Up: [../map.md](../map.md)
