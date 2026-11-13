# map — repark-sql/tests/setop_reorder_common

## Purpose

The shared harness of the struct set-operation suites (`../struct_setop_reorder_refusal.rs`,
`../struct_setop_reorder_refusal_delta2.rs`, `../struct_setop_type_family_1.rs`,
`../struct_setop_recursive_kind.rs`): the three doors, the shapes, the cell runner and the
outcome checks.

## Contents

- `mod.rs` — STRUCT-SETOP-REORDER-REFUSAL-1 (2026-11-08): the domain table on the ANSI, DataFusion
  and Spark doors over v2 copy-on-write tables; reordered operands refuse, controls answer,
  refusals persist nothing. **SQL-SETOP-ALL-REFUSE-1 (2026-11-12):** the `INTERSECT ALL` and
  `EXCEPT ALL` rows expect that unit's statement refusal on every door (`is_multiset`,
  `check_multiset_refusal`: the error carries `(SQL-SETOP-ALL-MULTISET-1)`, and a refused
  `INSERT` leaves the rows and the snapshot as they were), because the session's statement seat
  refuses before the struct rule runs.

## Pointers

- Up: [../map.md](../map.md)
