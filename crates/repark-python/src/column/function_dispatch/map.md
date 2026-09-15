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
  **ARRAY-NULL-1 (2026-09-14):** `array_append`/`array_prepend` build one
  `ScalarFunction::new_udf` over the null-preserving `spark_array_*_udf` shims — one
  native call per facade level, Spark `(array, element)` order.
  pins: fnp-9-collections-json/C-006, C-007, array-null-1/C-003
  pins: fnp-9-collections-json/C-006, C-007
  **FNP-WIN-1 step 3 (2026-09-15):** the `window_time` arm builds one
  `ScalarFunction::new_udf` over `spark_window_time::window_time_udf`.
  pins: fnp-win-1/C-003
- `dispatch_spark.rs` — **DOOR-CONVERGE-1 (2026-09-15):** the converged scalar arms —
  `abs`, `hypot`, `bin`, `rint`, `base64`, `unbase64`, `size`, `cardinality`,
  `array_contains` / `array_has`, `ascii`, `length` / `character_length` /
  `char_length`. Each enforces arity and calls the `repark_functions::expr_fn` builder
  for the same kernel `register_all` installs on the SQL door — one kernel per name on
  both doors.
  pins: door-converge-1/C-001..C-008
  **DOOR-CONVERGE-2 (2026-09-15):** `reverse` joins the converged arms (the facade's old
  DataFusion-core lowering answered strings only).
  pins: door-converge-2/C-002
  **DOOR-CONVERGE-2 (2026-09-15):** `sequence` / `generate_series` / `gen_series` join the
  converged arms (replacing the `nested_fn::gen_series` lowering) with the facade literal
  expansion ceiling kept.
  pins: door-converge-2/C-003
  **DOOR-CONVERGE-2 (2026-09-15):** `split` joins the converged arms (2–3 args, `-1`
  default limit); the Python `F.split` refusal sits above it, owned by run 16a.
  pins: door-converge-2/C-004
  **DOOR-CONVERGE-2 G-2 (2026-09-15):** the three single-name converged arms merge into
  the converge-1 arm (one pattern list, identical bodies), and the `sequence` arm body
  moves to `sequence_expr` (the `call_scalar_expr` 100-line ceiling holds).
  pins: door-converge-2/C-005

## Pointers

- Up: [../map.md](../map.md)

- `dispatch_json.rs` also hosts the FNP-11A temporal `call_scalar` arms
  (`make_timestamp` family, `make_ym_interval`, `try_make_interval`, `months_between`,
  `convert_timezone`, `localtimestamp`, `timestampadd`, `timestampdiff`, `datediff`): the parent
  `function_dispatch.rs` is at its 1000-line ceiling, so no arm lands there.
  pins: fnp-11a/C-002, C-003, C-019
