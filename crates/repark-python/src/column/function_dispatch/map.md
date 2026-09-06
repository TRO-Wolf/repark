# map — repark-python/src/column/function_dispatch

## Purpose

Child modules of [`function_dispatch.rs`](../function_dispatch.rs). The parent's arm table sat at
992 of its 1000-line ceiling when FNP-9/10 arrived, and the cohesive `column/dispatch/` split the
campaign charter names belongs to FNP-Z — the slate forbids doing it piecemeal inside a feature
unit — so a new family gets a child module and the parent's default arm falls through to it.

## Contents

- `dispatch_json.rs` — **FNP-9/10 (2026-09-05):** the collections and JSON arms —
  `get_json_object`, `json_array_length`, `json_object_keys`, `schema_of_json`, `to_json`,
  `from_json`, `array_insert`, `arrays_zip`, `map_concat`, and `create_map`. `create_map` calls
  the Spark-named `create_map` kernel, because DataFusion's `map(make_array, make_array)`
  lowering cannot mix a scalar key with a column value.
  pins: fnp-9-collections-json/C-006, C-007

## Pointers

- Up: [../map.md](../map.md)
