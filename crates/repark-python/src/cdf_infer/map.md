# map — repark-python/src/cdf_infer

FACADE-3 step 2 (2026-09-13): the `createDataFrame` rows/tuple/dict → Arrow
implementation split out of `cdf_infer.rs`. See the parent
[src/map.md](../map.md) `cdf_infer` row for the dispatch contract; this map
lists the module files.

## Files

| Path | Contract |
|---|---|
| [`cells.rs`](cells.rs) | `Cell`/`CellKind` extraction: one classified, typed value per Python object (ints, floats, bools, strings, binary, dates, datetimes with `tzinfo.utcoffset`, decimals under the `_validate_decimal_envelope` rules, lists, tuples, `Row`, dicts). `CellKind::Fill` is never extracted — `build_struct` mints it for null-parent child slots so children carry the `pa.array` type default instead of a child null. `Cdf::Fallback` marks every object the port does not cover; `Ctx` carries the session flags the Python dispatch reads (UTC tz, NTZ, dict-as-struct, legacy-first-element, decimal precision). pins: facade-3/C-010 |
| [`infer.rs`](infer.rs) | Arrow type inference for a cell column under Spark's merge rules: scalar merge-kind ordering with the incompatible-kind refusal, list/struct/map/dense/sparse shapes, dict-key union order, null/empty-column `Utf8`, timestamp defaults from the NTZ flag. Legacy-first-element mode scans only the first relevant nested element. pins: facade-3/C-010 |
| [`build.rs`](build.rs) | Typed Arrow array builders per column kind plus the recursive list/fixed-list/map/struct assemblers. Null struct parents write each child's type default (`CellKind::Fill`) rather than a child null — `pa.array` parity, required for pandas NaN-coercion identity. Timestamp slots apply the session-UTC wall/`utcoffset` subtraction; decimal slots carry the scale-18 unscaled integer. pins: facade-3/C-010 |
