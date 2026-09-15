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
  the repo. Corroborated by the corpus test in `tests.rs` over the recorded JDK
  17.0.15 fixture (env `REPARK_JDK17_TOSTRING_CORPUS`, skipped when unset) and the
  in-tree tables beside it.
- `bigint.rs` — the ported `FDBigInteger` magnitude arithmetic (`5^p5·2^p2`
  construction, normalizing shift, quotient-remainder digit iteration, compare and
  compare-against-sum), little-endian `u32` limbs, no `unsafe`.
- `tables_doubles.rs`, `tables_floats.rs` — generated in-tree corpus tables (unique
  non-shortest doubles / floats plus seeded random rows) so CI holds the claim
  without the fixture file. Regenerate from the corpus with the seed in the ledger.
- `tests.rs` — unit pins for this directory, including the corpus test.
- `format_fixed.rs` — **JAVA-DOUBLE-FD-1 (2026-09-15):** Java `Formatter` HALF_UP
  fixed-point/scientific rendering for bare single-verb `%[.N]f` / `%[.N]e`
  `format_string` / `printf` literals over float args, plus the mixed-format
  pre-round UDF. `%g` / `%a` / flags / width stay on the upstream kernel.
