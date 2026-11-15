# map — repark-functions/src/spark_math

## Purpose

`bround` submodule beside the `spark_math.rs` kernels that own it.

## Files

- [`bround.rs`](bround.rs) — Spark `bround` HALF_EVEN kernel (double, decimal,
  integral; negative scale; Spark result types and display names) with its Rust
  tests. The legacy-wrap `as` cast in `impl_integral_bround` is intentional
  (Spark wraps on overflow; ANSI raises first). ROUND-DOUBLE-EXACT-1 adds `round_udf` and `call_round` (`SparkExactRound` over datafusion-spark's `SparkRound`): the FLOAT and DOUBLE `round` arms call the kernel, and a column scale refuses.
- [`conv.rs`](conv.rs) — Spark `NumberConverter` kernel (signed output on negative
  `toBase`, stop-at-invalid-digit parsing, out-of-range bases answer NULL, u64
  overflow saturates or raises `[ARITHMETIC_OVERFLOW]` from the ANSI carrier)
  with its Rust tests.
- [`round_tests.rs`](round_tests.rs) — the `round` UDF through `register_all`: DOUBLE half up, `bround` half even, FLOAT stays FLOAT, NULL and column scales, INT and DECIMAL arms unchanged.

## Pointers

- Up: [src map](../map.md)
- Kernels: [`spark_math.rs`](../spark_math.rs)
