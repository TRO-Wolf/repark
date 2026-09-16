# map — repark-functions/src/generator

## Purpose

FNP-GEN-1 generator support. The parent `generator.rs` holds the placeholder
`ScalarUDF`s, the internal `__repark_gen_alias` / `__repark_gen_ordinality` /
`__repark_gen_field` UDFs, and the `GeneratorRewrite` analyzer rule that expands a
single generator call in a `Projection` into list `Unnest` plans on both doors.

## Contents

- `tests.rs` — the module's `#[cfg(test)]` suite (moved out of `generator.rs` in
  FNP-GEN-1 remediation round 1 so the parent keeps the file-size ceiling): the
  array/map/struct answer pins, the select-position pin, the one-generator /
  alias-arity / aggregate-input / `stack` / `__repark_arr_*` sibling refusals, the
  name-restored `Expr::Alias` pin, and the NULL-struct-element regression pin over a
  non-nullable-field `StructArray` with a NULL parent slot.
  pins: fnp-gen-1/C-002, C-003, C-005

## Pointers

- Up: [../map.md](../map.md)
- **Verification-critic fix-up (2026-09-16, run 17a):** `tests.rs` gains
  `ordinality_packs_positions_for_a_sliced_list`, the regression pin for V-002 — `ordinality` used
  to clone the input's offset buffer while packing positions from 0, so a sliced `ListArray` whose
  first offset is not 0 panicked in `ListArray::new`. pins: fnp-gen-1/C-002
