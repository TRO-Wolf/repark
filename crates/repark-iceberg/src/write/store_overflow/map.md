# map — repark-iceberg/src/write/store_overflow

## Purpose

The test half of [`../store_overflow.rs`](../map.md), moved out by ICE-STRUCT-PAIRING-1
(2026-10-14) so the module stays under the file-size ceiling.

## Contents

- `tests.rs` — the CAST-OVERFLOW-INSERT-1 tests of `StoreOverflowCast`, unchanged: constant,
  `VALUES`, column and subquery overflows, truncated stores, the lazy UPDATE and the divisor
  guard swap. **ICE-STRUCT-PAIRING-1 build round 2 (2026-10-14), parity row R-016:**
  `an_insert_pairs_struct_fields_by_position_unless_its_statement_says_by_name` runs the rule
  on a plain context: an `INSERT` whose state carries no `StatementPairing` stores a reordered
  struct by position (Spark's `INSERT` rule), and one carrying `source_by_name` stores it by
  name. It is the cell that kills mutant 8 of the sketch's §9 (the default of
  `WriteOp::Insert` turned by name), which no other test reached.

Up: [../map.md](../map.md).
