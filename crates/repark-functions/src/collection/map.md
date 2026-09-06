# map — repark-functions/src/collection

CC-4 (2026-08-30): remaining banner files condensed to the one-line rule
(pins: cc-3-comment-condensation/C-009).

## Purpose

The child modules of [`collection.rs`](../collection.rs). Each holds one Spark collection shim whose
implementation is large enough that keeping it in the parent would push a ceiling; the parent
declares them (`mod str_to_map;` …) and registers their UDFs.

Rust's default module layout keeps these children beside `collection.rs`; no `mod.rs` rename is
needed.

## Contents

- `str_to_map.rs` — regex `str_to_map` (Spark treats both delimiters as regular
  expressions, where the DataFusion kernel splits on literals). Exports
  `bind_ascii_perl_classes`. Depends on workspace `regex`.
- `shuffle.rs` — **X1:** NULL-guarded `shuffle`; the upstream kernel panics on an all-NULL list.
- `map_from_entries.rs` — **X7:** `map_from_entries` under Spark's `EXCEPTION` map-key dedup
  policy (duplicate keys raise rather than last-wins).
- `array_position.rs` — **FN-FIX-1:** not-found → `0`; NULL only for NULL array/needle.
  pins: fn-fix-1-registry-rows/C-002
- `array_insert.rs` — **FNP-9 (2026-09-05):** Spark `array_insert(array, position, value)`. A
  positive position is 1-based; a negative one counts back from the end so `-1` appends. A
  position past either end pads with NULLs. Position `0` raises `INVALID_INDEX_OF_ZERO`.
  **Round 2 (2026-09-06, finding F11):** the element and value types widen through the TIGHTEST
  common type — numeric with numeric, text with text — so a DOUBLE inserted into an INT array
  widens the array instead of truncating the value, and a BOOLEAN or STRING against an INT array
  raises `ARRAY_FUNCTION_DIFF_TYPES` the way Spark raises it. DataFusion's `comparison_coercion`
  alone is too loose here: it accepts string-with-numeric, which Spark refuses.
  pins: fnp-9-collections-json/C-006
- `arrays_zip.rs` — **FNP-9 (2026-09-05):** Spark `arrays_zip`. Zips to the LONGEST array and
  NULL-fills the rest; the struct field takes its 0-based position — NOT the child column name
  Spark uses for an attribute child. A UDF's return field must be a pure function of the
  argument TYPES: naming from the argument field names made `optimize_projections` fail its own
  schema-stability invariant once the optimizer inlined a subquery or folded a literal, and
  pinning the names in `simplify` only moved the same failure onto the analyzer's schema.
  Divergence `FNP9-ARRAYS-ZIP-NAMES-1`. The NULL-fill test exists because a mutation knob
  found its absence: with the field-name tests alone, zipping to the SHORTEST array was
  0 red of 42. pins: fnp-9-collections-json/C-006, C-008, C-009
- `map_concat.rs` — **FNP-9 (2026-09-05):** Spark `map_concat`. A NULL map argument nulls the
  row and an untyped NULL raises `MAP_CONCAT_DIFF_TYPES`, both the way Spark answers them;
  a key repeated across the concatenated maps raises `DUPLICATED_MAP_KEY` with the text
  `map_from_entries` and `str_to_map` already use; no arguments answer an empty
  `MAP<STRING,STRING>`. pins: fnp-9-collections-json/C-006
- `create_map.rs` — **FNP-9 (2026-09-05):** the PySpark-only `create_map(k1, v1, …)` name. It is
  NOT in `functions()`: the Spark door spells this `map(...)` and already has it, so the kernel
  reaches only the facade through `expr_fn::create_map`. Its own kernel rather than DataFusion's
  `map(make_array, make_array)` lowering, which cannot mix a scalar key with a column value.
  Non-nullable result, `NULL_MAP_KEY` on a null key, `DUPLICATED_MAP_KEY` on a repeat.
  pins: fnp-9-collections-json/C-006
- `array_sort.rs` — **FN-FIX-1:** `array_sort` NULLs LAST; `sort_array` Spark order
  (asc NULLS FIRST, desc NULLS LAST).
  pins: fn-fix-1-registry-rows/C-002
- `arrays_overlap.rs` — **FN-FIX-1:** three-valued overlap. HashSet of owned
  `ScalarValue` per row; a borrowed-key set is not a one-line change.
  pins: fn-fix-1-registry-rows/C-002
- `flatten.rs` — **FN-FIX-1:** a NULL sub-array makes the row NULL.
  Output `ListArray` from inner values + mapped offsets (no per-row concat).
  `#[ignore = "1e6-row release bench"]` `one_million_rows_within_three_times_datafusion` (≤ 3× DataFusion).
  pins: fn-fix-1-registry-rows/C-002

## I want to...

| ...do this | go to |
|---|---|
| add a collection shim | a new file here, declared and registered in [`../collection.rs`](../collection.rs) |
| find where these are registered | `collection::functions()` in [`../collection.rs`](../collection.rs) |
| see why the parent is split at all | crate-root and file-size ceilings — [`../../map.md`](../../map.md) |

## Pointers

- Up: [`../../map.md`](../../map.md)
