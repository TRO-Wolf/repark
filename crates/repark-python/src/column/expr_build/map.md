# map — repark-python/src/column/expr_build

Submodules of [expr_build.rs](../expr_build.rs) (FEXPR-TIMESTAMP-NTZ-1, 2026-11-24).

## Files

- [fragment_canon.rs](fragment_canon.rs) — `canonicalize_fragment` and `canonicalize_predicate`,
  rewriting Spark-door fragment `TIMESTAMP_LTZ '...'` literals before calling
  `repark_spark::spark_literals::canonicalize_verbatim` so `repark-spark`'s single front-door
  caller invariant holds.

## Pointers

- Up: [../map.md](../map.md)
