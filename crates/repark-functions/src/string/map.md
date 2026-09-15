# map — repark-functions/src/string

## Purpose

Unit tests for [`string.rs`](../string.rs), beside the kernels they pin.

## Files

- [`tests.rs`](tests.rs) — `substring` edges/multibyte, `concat` null-propagation,
  physical-type pins, the all-`Binary` and `register_all`-overwrite legs, plus the
  opt-in `REPARK_PERF_MEASURE=1` substring measurement. Moved verbatim 2026-09-15
  as a file-size split (move-only). pins: door-converge-2/C-001, C-008

## Pointers

- Up: [src map](../map.md)
- Kernels: [`string.rs`](../string.rs)
