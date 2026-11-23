# map — repark-functions/src/generator

## Purpose

FNP-GEN-1 generator support. The parent `generator.rs` holds the placeholder
`ScalarUDF`s (naming unaliased `explode` / `explode_outer` outputs `col` in the
plan schema so outer references, `ORDER BY col`, and unaliased `INSERT` resolve
before `GeneratorRewrite` runs) and the `GeneratorRewrite` analyzer rule that
expands a single generator call in a `Projection` into list `Unnest` plans on
both doors. Outer guards widen non-nullable list element fields to nullable and
build the one-NULL-element list with `ListArray::try_new`. `GeneratorAliasMark`
(first Spark-door rule; `mark_generator_aliases` before attribute-id stamping) turns
explicit aliases into `__repark_gen_alias`, so the rewrite names bare outputs by
type; bare frame calls are held in `__repark_gen_bare`; `is_bare_generator_expr` feeds the facade.

## Contents

- `udfs.rs` — the internal `__repark_gen_ordinality` (fallible `ListArray::try_new`
  position lists) and `__repark_gen_field` (parent-null-masking struct reader)
  UDFs, split out of `generator.rs` to keep the parent under the file-size ceiling.
- `tests.rs` — the module's `#[cfg(test)]` suite (moved out of `generator.rs` in
  FNP-GEN-1 remediation round 1 so the parent keeps the file-size ceiling): the
  array/map/struct answer pins, the select-position pin, the one-generator /
  alias-arity / aggregate-input / `stack` / `__repark_arr_*` sibling refusals, the
  call-spelled alias refusal pin, and the NULL-struct-element regression pin over a
  non-nullable-field `StructArray` with a NULL parent slot. **SQL-EXPLODE-1 (2026-11-23):**
  the `explode` / `explode_outer` pins (array `col`, alias, NULL and empty rows, map
  `key` / `value`, scalar and nested refusals, non-nullable-element outer lists,
  unaliased subquery / `ORDER BY col` resolution, explicit `explode(...)` aliases; delta 2:
  coerced arguments, carried stamps, the zero-name marker, the `array_repeat` seat).
- `json_tuple.rs` — **FNP-GEN-1 step 2 (2026-09-16, run 18a):** the `json_tuple`
  expansion (moved out of `generator.rs` so the parent keeps the file-size ceiling):
  string-type checks with `[DATATYPE_MISMATCH.NON_STRING_TYPE]`, the
  `[WRONG_NUM_ARGS]` arity refusal, the lone-alias ignore and
  `[UDTF_ALIAS_NUMBER_MISMATCH]` arity refusal, and the unnest-free expansion that
  computes one struct per row through `__repark_json_tuple` and reads the fields
  back through `__repark_gen_field`.

## Pointers

- Up: [../map.md](../map.md)
- **Verification-critic fix-up (2026-09-16, run 17a):** `tests.rs` gains
  `ordinality_packs_positions_for_a_sliced_list`, the regression pin for V-002 — `ordinality` used
  to clone the input's offset buffer while packing positions from 0, so a sliced `ListArray` whose
  first offset is not 0 panicked in `ListArray::new`.
