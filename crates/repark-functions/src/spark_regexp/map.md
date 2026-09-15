# map — repark-functions/src/spark_regexp

## Purpose

Unit tests for [`spark_regexp.rs`](../spark_regexp.rs), beside the kernels they pin.

## Files

- [`tests.rs`](tests.rs) — `regexp_count` / `regexp_instr` find-loop semantics,
  `regexp_extract` groups/defaults/refusals, dictionary input, mid-surrogate
  probes. Moved verbatim 2026-09-15 as a file-size split (move-only); existing
  pins travel with the file, and the suite guards the shared compiler round 3
  extended. pins: door-converge-2/C-008 (move)

## Pointers

- Up: [src map](../map.md)
- Kernels: [`spark_regexp.rs`](../spark_regexp.rs)
