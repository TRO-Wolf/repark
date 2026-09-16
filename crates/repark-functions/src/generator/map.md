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
