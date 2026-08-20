# map — repark-python/src/column

## Purpose

`PyColumn` binding module. `mod.rs` holds the `#[pyclass]` and the single `#[pymethods]` impl;
non-pymethods helpers live in sibling files so the file-size EXCEPTIONS row can ratchet DOWN
without enabling PyO3 `multiple-pymethods`.

## Contents

- `mod.rs` — `#[pyclass] PyColumn` + the one `#[pymethods]` impl (constructors, operators,
  thin `call_scalar` / `aggregate` / `aggregate_binary` wrappers, date / window / aggregate
  arms) and `expr_tests` (sql / `call_scalar` handoff pins). `multiple-pymethods` stays off.
- `function_dispatch.rs` — **FN-GX (2026-08-16):** move-only extract of the `call_scalar`
  name table plus the `aggregate` / `aggregate_binary` UDAF match tables. New function
  names land as arms here. **G6-3 rider:** `"unix_date"` arm
  (`repark_functions::expr_fn::unix_date`) so `F.unix_date` builds the engine function
  instead of the `CAST(x AS DATE) AS INT` chain the cast-legality gate refuses.
  **FN-GT1 (2026-08-17):** leftover math/string/bitwise/utf8 arms (`bin`/`hex`/`unhex`/
  `factorial`/`rint`/`width_bucket`/`bit_count`/`bit_get`/`getbit`/shifts/`split_part`/
  `regexp_count`/`regexp_instr`/`bit_length`/`octet_length`/`is_valid_utf8`/
  `make_valid_utf8`).
  **GT1-FIX (2026-08-18):** `regexp_instr` 3-arg arm is Spark `idx` (NULL-propagate,
  ignore value) — never DataFusion start-position. `getbit` stays a reachable
  name on the wire. `bit_length`/`octet_length` embed the repark stringify
  shim. Ledger: `task/fn-gt1-ledger.md`.
  **GT1-FIX round-2 (2026-08-19):** `regexp_count` / `regexp_instr` / `split_part`
  embed the `repark-functions` overwrites (one semantics source for both doors).
  **FN-GT2 (2026-08-17):** datetime/collections/url/bitmap leftover arms
  (`make_date`/`make_interval`/`make_dt_interval`/`unix_micros`/`date_diff`/
  `element_at`/`array_compact`/`shuffle`/`map_from_entries`/`str_to_map`/
  `parse_url`/`try_parse_url`/`url_encode`/`url_decode`/`try_url_decode`/
  `bitmap_bit_position`/`bitmap_bucket_number`/`bitmap_count`).
  Rework: `str_to_map` now embeds the regex UDF from `repark-functions`.
  **X-round (2026-08-18):** the `shuffle` arm takes 1 OR 2 args (the Spark 4.0
  seed the facade used to drop — X2), and `shuffle` / `map_from_entries` /
  `parse_url` / `try_parse_url` now embed repark shims rather than
  `datafusion-spark`'s kernels, so a facade Column and `spark.sql()` resolve the
  same UDF. Ledger: `task/fn-gt2-ledger.md`.
- `window.rs` — `window_udwf` / `window_udwf_i32` inherent helpers (`pub(super)`) and Spark
  `rowsBetween` / `rangeBetween` frame translation (`spark_window_frame`, offset/bound scalars).
- `function_dispatch.rs` gained the **FNP-3 (2026-08-20)** arms — `crc32`, `sha1`/`sha`,
  `xxhash64`, `soundex`, `format_string`, `from_utc_timestamp`, `to_utc_timestamp`,
  `map_from_arrays`, and `datediff` sharing `date_diff`'s arm. Each of these names already
  evaluated through `spark.sql(...)`; the missing arm was the whole refusal. A kernel registered
  by `register_all` but absent from this table is a facade-only `UnsupportedOperationException`.
- `mod.rs` also carries the **FNP-4a (2026-08-20)** lambda constructors: `lambda_variable`
  (a placeholder for one lambda parameter) and `call_higher_order` (value arguments, then one
  lambda per `(params, body)`). Resolution of those placeholders is `PyDataFrame::bound`, not here
  — a `PyColumn` has no schema.
- `function_dispatch.rs` also carries the **FNP-5 (2026-08-20)** aggregate arms: the nine
  `regr_*` and `string_agg`/`listagg` in `binary_aggregate_udaf`, `grouping` and
  `approx_count_distinct`/`approx_distinct` in `unary_aggregate_udaf`. Same story as the scalar
  arms — every one of these was already registered by `register_all` and resolvable through
  `spark.sql(...)`; the facade had no arm.
- `door_parity_tests.rs` — **FNP-1 (2026-08-20):** the charter clause C-012 guard. Compares the
  UDF this crate's dispatch table embeds against the one `repark_functions::register_all` installs
  on a session, so the facade and the SQL door cannot silently resolve different kernels for the
  same spelling. Carries `EXPECTED_DIVERGENCES`, a sanctioned-out table that **ratchets DOWN
  only** — a listed name that has quietly been fixed fails the build. Ledger:
  [../../../../../task/fnp-1-two-door-asymmetry-ledger.md](../../../../../task/fnp-1-two-door-asymmetry-ledger.md).
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
| `sec`/`csc` at zero is NULL not Inf | `function_dispatch.rs` `sec`/`csc` arms + `expr_build.rs` `reciprocal_trig_or_inf` |
| `call_scalar` unknown name / arity | `function_dispatch.rs` `call_scalar_expr` |
| `F.f(x)` and `spark.sql("SELECT f(x)")` disagree | `door_parity_tests.rs` — the name resolves a different kernel per door |
| a name works in SQL but raises through `F.` | `function_dispatch.rs` has no arm for it — the kernel is registered, the facade cannot reach it |
| unknown `aggregate` / `aggregate_binary` kind | `function_dispatch.rs` `unary_aggregate_udaf` / `binary_aggregate_udaf` |
| `… AS x AS x` in a plan | `expr_build.rs` `collapse_identity_alias_chain` |

First checks: `cargo test -p repark-python column`. Escalate to: [../map.md#debug](../map.md).
