# map — repark-functions/src/string

## Purpose

Unit tests for [`string.rs`](../string.rs), beside the kernels they pin.

## Files

- [`tests.rs`](tests.rs) — `substring` edges/multibyte, `concat` null-propagation,
  physical-type pins, the all-`Binary` and `register_all`-overwrite legs, plus the
  opt-in `REPARK_PERF_MEASURE=1` substring measurement. Moved verbatim 2026-09-15
  as a file-size split (move-only). pins: door-converge-2/C-001, C-008
- [`format_number.rs`](format_number.rs) — Spark `format_number` grouping renderer
  (HALF_EVEN digit rounding, grouping commas, `d` decimals; double/decimal/integral
  inputs, NULL stays NULL) with its Rust tests. pins: fnp-math-1/C-002, C-003
- [`mask.rs`](mask.rs) — Spark `mask` character-class masker (upper/lower/digit/other
  replacements with `X`/`x`/`n`/keep defaults; NULL replacement keeps the class;
  NULL in → NULL out) with its Rust tests. pins: fnp-math-1/C-002, C-003

## Pointers

- Up: [src map](../map.md)
- Kernels: [`string.rs`](../string.rs)
