# map — repark-python/src/column

## Purpose

`PyColumn` binding module. `mod.rs` holds the `#[pyclass]` and the single `#[pymethods]` impl;
non-pymethods helpers live in sibling files so the file-size EXCEPTIONS row can ratchet DOWN
without enabling PyO3 `multiple-pymethods`.

## Contents

- `mod.rs` — `#[pyclass] PyColumn` + the one `#[pymethods]` impl (constructors, operators,
  **G6-3 rider (2026-08-15):** `call_scalar` grew a `"unix_date"` arm (`repark_functions::expr_fn::unix_date`) so the facade's `F.unix_date` builds the engine's function instead of the `CAST(x AS DATE) AS INT` chain the cast-legality gate now refuses.
  `call_scalar`, date / window / aggregate arms) and `expr_tests` (sql / `call_scalar` handoff
  pins). `multiple-pymethods` stays off.
- `window.rs` — `window_udwf` / `window_udwf_i32` inherent helpers (`pub(super)`) and Spark
  `rowsBetween` / `rangeBetween` frame translation (`spark_window_frame`, offset/bound scalars).
- `expr_build.rs` — expression-construction helpers (`parse_data_type` / `parse_decimal_type`,
  alias collapse, projection extract, reciprocal-trig Inf CASE, `collect_aggregate` /
  `count_distinct_argument`) plus the unit tests that pin those helpers.

## Pointers

- Up: [../map.md](../map.md)

## Debug

| Symptom | First check |
|---|---|
| `cast` / `try_cast` rejects a type string | vocabulary lives in `expr_build.rs` `parse_data_type` |
| window frame bound wrong | `window.rs` `spark_window_frame` / `spark_offset_to_bound` |
| `sec`/`csc` at zero is NULL not Inf | `expr_build.rs` `reciprocal_trig_or_inf` |
| `… AS x AS x` in a plan | `expr_build.rs` `collapse_identity_alias_chain` |

First checks: `cargo test -p repark-python column`. Escalate to: [../map.md#debug](../map.md).
