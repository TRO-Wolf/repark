# `java_double/` — Java float spellings and the Spark-door float/string rewrites

Parent `../java_double.rs` keeps the crate-visible surface (`with_java_double_text`,
`java_double_text`, the `__repark_float_to_string__`, `__repark_format_float__`
and `__repark_parse_java_double__` / `__repark_parse_java_float__` UDFs, the
`SparkFloatStringify` analyzer rule) and re-exports the digit engine below, so
`json/`, `string.rs`, `spark_length.rs` and `bitmap_agg.rs` keep importing from
`crate::java_double`. The rule holds five float seats: longhand `CAST`/`TRY_CAST`
folds (with the Java-suffix strip), one-level literal propagation through
projections, `%s`-verb float wrapping, single-verb `%f`/`%F` routing to the
HALF_UP shim, and non-literal STRING to FLOAT/DOUBLE casts routing to the
column parse kernel. Round 2 (2026-09-15, L-004/L-001): `%F` parses only to refuse with
`Conversion = 'F'` (Java has no upper-float conversion); NaN renders bare `NaN`
under every sign/space/paren flag while infinity keeps sign handling. L-002: `#`
sets an ALT flag that appends `.` when precision is 0.

- `parse_float.rs` — Round 2 (2026-09-15, L-003): the `__repark_parse_java_double__`
  / `__repark_parse_java_float__` UDFs over one shared Spark-grammar text parser
  (fast parse, trim plus one trailing-suffix strip, Arrow fallback, blank to NULL,
  session ANSI at invoke). Non-literal STRING to FLOAT/DOUBLE casts route there;
  literals keep the plan-time fold; TryCast stays on Arrow.

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
  throw is a correct general division instead. The 346-row `5^p` table is a
  heap-built `LazyLock` static (the stack-array gate); low-word takes share one
  narrowing helper.
- `format_float.rs` — **JAVA-DOUBLE-FD-1 C-003 (2026-09-15):** the
  `__repark_format_float__` UDF. Whole-format single-verb `%[flags][width][.precision]f|F`
  only; exact binary-to-decimal digit generation (limb shift/mask loop) with
  HALF_UP rounding, then upstream-shaped sign/width/grouping/padding. Null and
  non-finite render as upstream does (`null`, `Infinity`/`NaN`, uppercased for
  `%F`). Multi-verb, non-float-arg and `%e`/`%g` calls are never rewritten.
- `tables_doubles.rs`, `tables_floats.rs` — in-tree corpus tables (82 non-shortest
  doubles + 100 seeded random rows; 579 non-shortest floats + 100 seeded random
  rows) so CI holds the claim without the fixture file.
- `tests_corpus.rs` — the corpus byte-equality test and the in-tree table test.
  pins: java-double-fd-1/C-001, C-002

Port-lint posture (2026-09-15): `dtoa.rs`/`bigint.rs` mirror Java `int`/`long`
wraparound arithmetic, so their `as` casts carry per-function `allow` attributes
for the exact pedantic cast lints each trips; lossless widenings use `From`
instead. The corpus byte-equality test (29,451 + 9,976 rows) guards every edit
there. The eight-argument `dtoa_int`/`dtoa_long`/`dtoa_big` signatures and the
`lvalue`/`ivalue` port names are kept 1:1 with the JDK structure under the same
treatment rather than renamed.
- **R-16c-17 (2026-09-15, orchestrator):** the nine `///` lines added in round 1 on private and `pub(crate)` items of `format_float.rs` are removed (owner comment ban; no lint requires them). The per-item rationale stays in this map and the ledger.
