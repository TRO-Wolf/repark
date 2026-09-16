# map — repark-functions/src/spark_math

## Purpose

`bround` submodule beside the `spark_math.rs` kernels that own it.

## Files

- [`bround.rs`](bround.rs) — Spark `bround` HALF_EVEN kernel (double, decimal,
  integral; negative scale; Spark result types and display names) with its Rust
  tests. pins: fnp-math-1/C-002, C-003
- [`conv.rs`](conv.rs) — Spark `NumberConverter` kernel (signed output on negative
  `toBase`, stop-at-invalid-digit parsing, out-of-range bases answer NULL, u64
  overflow saturates or raises `[ARITHMETIC_OVERFLOW]` from the ANSI carrier)
  with its Rust tests. pins: fnp-math-1/C-002, C-003, C-004

## Pointers

- Up: [src map](../map.md)
- Kernels: [`spark_math.rs`](../spark_math.rs)
