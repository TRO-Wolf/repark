# map — repark-sql/tests/setop_reorder_common

## Purpose

The shared harness of the struct set-operation suites (`../struct_setop_reorder_refusal.rs`,
`../struct_setop_reorder_refusal_delta2.rs`, `../struct_setop_type_family_1.rs`,
`../struct_setop_recursive_kind.rs`): the three doors, the shapes, the cell runner and the
outcome checks.

## Contents

- `mod.rs` — STRUCT-SETOP-REORDER-REFUSAL-1 (2026-11-08): the domain table on the ANSI, DataFusion
  and Spark doors over v2 copy-on-write tables; reordered operands refuse, controls answer,
  refusals persist nothing. **SQL-SETOP-ALL-MULTISET-1 (2026-11-23):** the `INTERSECT ALL` and
  `EXCEPT ALL` rows are checked like the distinct ones again: the struct rule runs before the
  multiset rewrite, so a reordered operand refuses with `[INCOMPATIBLE_COLUMN_TYPE]`.

## Pointers

- Up: [../map.md](../map.md)
