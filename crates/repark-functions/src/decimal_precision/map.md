# map — repark-functions/src/decimal_precision

## Purpose

Unit tests for `SparkNegateNullDecimal`: negated null decimals fold to a typed null,
everything else passes through.

## Files

- [`negate_null_tests.rs`](negate_null_tests.rs) pins the fold shapes (null cast /
  literal / try-cast / nested negative / wide target keep their type) and the untouched
  shapes (valued decimal, null int, null string, non-decimal cast target).

## Contracts pinned

- `Negative` over a null decimal answers a null literal of the child type (Spark
  `UnaryMinus` null-propagation); the DataFusion scalar kernel would `Internal error`.
- Nothing else rewrites: valued decimals keep kernel evaluation, non-decimals keep
  their kernel agree-or-refuse behavior.
  pins: decimal-cache-1/C-012

## Pointers

- Up: [src map](../map.md)
- Rule: [`decimal_precision.rs`](../decimal_precision.rs)
