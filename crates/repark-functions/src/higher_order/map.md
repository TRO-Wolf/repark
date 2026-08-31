# map — repark-functions/src/higher_order

## Purpose

Spark higher-order kernels registered on the FNP-4a shared table (`functions` /
`by_name` / `register`). One RePark kernel per Spark name except `reduce` (alias
of `aggregate`) and `exists` (alias of DataFusion `array_any_match`).
pins: fnp-4c-higher-order-kernels/C-001, C-002, C-003, C-004, C-005, C-006
pins: fnp-4c-higher-order-kernels/C-007, C-008, C-009, C-010, C-011, C-013, C-014

## Contents

- `mod.rs` — registry both doors read.
- `lambda_utils.rs` — list coerce, `[element, index]` params, extract/assemble.
- `transform.rs` — Spark `transform`.
- `filter.rs` — Spark `filter` (null predicate drops).
- `forall.rs` — all-match rewrite of `exists`.
- `aggregate.rs` — sequential fold; alias `reduce`.
- `zip_with.rs` — null-pad the shorter array.
- `map_common.rs` — flatten/rebuild, `NULL_MAP_KEY`, `DUPLICATED_MAP_KEY`.
- `transform_keys.rs` / `transform_values.rs` / `map_filter.rs` / `map_zip_with.rs`.

## Pointers

- Up: [../map.md](../map.md)
- Seam: FNP-4a ledger (archived).
