# map — repark-functions/src/higher_order

## Purpose

Spark higher-order kernels registered on the FNP-4a shared table (`functions` /
`by_name` / `register`). One RePark kernel per Spark name except `reduce` (alias
of `aggregate`).
pins: fnp-4c-higher-order-kernels/C-001, C-002, C-003, C-004, C-005, C-006
pins: fnp-4c-higher-order-kernels/C-007, C-008, C-009, C-010, C-011, C-013, C-014

## Contents

- `mod.rs` — registry both doors read.
- `kernel_eval.rs` — Databricks-dialect SQL value pins (test-only).
- `lambda_utils.rs` — list coerce, `[element, index]` params, extract/assemble.
  Unary lambdas do not materialize the index array (`LambdaArgument::evaluate`
  only invokes `variables[..params.len()]`).
- `transform.rs` — Spark `transform`.
- `filter.rs` — Spark `filter` (null predicate drops).
- `exists.rs` — native Spark `exists`; three-valued nulls and output nullability follow the
  input array and predicate.
- `forall.rs` — all-match rewrite of `exists`.
- `hof_keep.rs` — `__hof_keep` marker UDF; keeps every minted lambda parameter in the body tree
  so DataFusion cannot drop it: a two-parameter lambda that only mentions `i` still occupies both
  slots the kernel declared as `[element, index]` (the reason the facade's former
  `_keep_lambda_params` docstring carried; FNP-8 moved the packing here). `exists.rs` carries no
  module comment (FNP-8-REVIEW, 2026-09-07: the one comment line #412 added, removed).
- `aggregate.rs` — sequential fold; alias `reduce`. The merge-output type must
  equal the init type; `LambdaRebind` refuses a mismatch with Spark's
  `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` naming `aggregate`.
  pins: fnp-4c-higher-order-kernels/C-003
- `zip_with.rs` — null-pad the shorter array.
- `map_common.rs` — flatten/rebuild, `NULL_MAP_KEY`, `DUPLICATED_MAP_KEY`.
- `transform_keys.rs` / `transform_values.rs` / `map_filter.rs` / `map_zip_with.rs`.

## Pointers

- Up: [../map.md](../map.md)
- Seam: FNP-4a ledger (archived).
