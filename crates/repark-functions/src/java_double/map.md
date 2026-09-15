# `java_double/` — Java float spellings and the Spark-door float/string rewrites

Parent `../java_double.rs` keeps the crate-visible surface (`with_java_double_text`,
`java_double_text`, the `__repark_float_to_string__` UDF, the `SparkFloatStringify`
analyzer rule) and re-exports the digit engine below, so `json/`, `string.rs`,
`spark_length.rs` and `bitmap_agg.rs` keep importing from `crate::java_double`.

- `dtoa.rs` — **JAVA-DOUBLE-FD-1 (2026-09-15):** the digit engine. Independent Rust
  implementation written from the published JDK 17 `FloatingDecimal` algorithm
  structure (fast long-arithmetic path, `estimateDecExp`, `FDBigInteger` slow path,
  `insignificantDigitsForPow2`, `roundup`) with the `toJavaFormatString` layout, for
  `f64` and `f32`. No `unsafe`, no new dependency; the reference files never enter
  the repo. Corroborated by the corpus test in `tests_corpus.rs` over the recorded
  JDK 17.0.15 fixture (env `REPARK_JDK17_TOSTRING_CORPUS`, skipped when unset) and
  the in-tree tables beside it.
- `bigint.rs` — the ported `FDBigInteger` magnitude arithmetic (`5^p5·2^p2`
  construction, normalizing shift, quotient-remainder digit iteration, compare and
  compare-against-sum), little-endian `u32` limbs, no `unsafe`. The quotient
  estimate is top-one-over-top-one with a decrement correction; the disparate-size
  throw is a correct general division instead.
- `tables_doubles.rs`, `tables_floats.rs` — in-tree corpus tables (82 non-shortest
  doubles + 100 seeded random rows; 579 non-shortest floats + 100 seeded random
  rows) so CI holds the claim without the fixture file.
- `tests_corpus.rs` — the corpus byte-equality test and the in-tree table test.
