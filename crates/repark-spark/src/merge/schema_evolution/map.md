# map — repark-spark/src/merge/schema_evolution

## Purpose

File-backed tests for `../schema_evolution.rs` (IPI-19, 2026-09-20): the
`MERGE WITH SCHEMA EVOLUTION` clause strip.

## Contents

- `tests.rs` — the `#[cfg(test)] mod tests;` declared in
  `../schema_evolution.rs`. The clause strips and the rest parses as an
  ordinary MERGE; case, whitespace, and comments between the keywords are
  ignored; a plain MERGE, a partial clause, a CTE-leading `WITH`, a non-MERGE
  statement, and a column or value named `evolution` are never touched.
  pins: ipi-19-56-37-schema-evolution-write/C-005, C-007
