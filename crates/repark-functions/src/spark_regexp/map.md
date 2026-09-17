# map — repark-functions/src/spark_regexp

## Purpose

Unit tests for [`spark_regexp.rs`](../spark_regexp.rs), beside the kernels they pin.

## Files

- [`tests.rs`](tests.rs) — `regexp_count` / `regexp_instr` find-loop semantics,
  `regexp_extract` groups/defaults/refusals, dictionary input, mid-surrogate
  probes. Moved verbatim 2026-09-15 as a file-size split (move-only); existing
  pins travel with the file, and the suite guards the shared compiler round 3
  extended. pins: door-converge-2/C-008 (move)
  **JAVA-REGEX-FEATURES-1 (2026-09-16):** call sites follow the compiler move
  (`compile_spark_regex` takes the caller name; count/probe run as `SparkRegex`
  methods). pins: java-regex-features-1/C-001

## Pointers

- Up: [src map](../map.md)
- Kernels: [`spark_regexp.rs`](../spark_regexp.rs)
