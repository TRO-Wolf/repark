# map — repark-common/src/java_double

## Purpose

Java `Double.toString` / `Float.toString` text for scalar values: the digit
engine behind the engine's `CAST(float AS STRING)`. Moved here from
`repark-functions`' `java_double/` in **C-4 fold 3 item 2 (2026-10-11)** so the
Postgres write chokepoint (`repark-core`, which cannot take an edge on the
tier-3 capability crate) calls the same function the Spark string cast uses
instead of a second implementation. `repark-functions` keeps its array wrappers
and UDFs and re-exports these scalars, so every existing importer is untouched.

## Contents

- `mod.rs` — the ported JDK 17 `FloatingDecimal` digit engine (fast
  long-arithmetic path, `estimateDecExp`, `FDBigInteger` slow path,
  `insignificantDigitsForPow2`, `roundup`) with the `toJavaFormatString`
  layout, for `f64` and `f32`: `with_java_double_text` / `with_java_float_text`
  plus the `java_double_text`, `java_float_text` and length helpers. No
  `unsafe`, no new dependency. **DOUBLE-DECIMAL-STORE-ROUND-1 (2026-11-13):**
  `java_double_unscaled` is Spark's FLOAT/DOUBLE-to-DECIMAL value: the JDK 17 text rounded half
  up at the target scale, with a binary fast path away from a half. Behaviour pins stay beside the callers in
  `repark-functions` (the corpus byte-equality test over the recorded JDK
  17.0.15 fixture and the in-tree tables). **ROUND-DOUBLE-EXACT-1 (2026-11-14):** `java_double_round` and `java_float_round` (`JavaRounding`, half up or half even) round the JDK 17 text at an `i32` scale; a FLOAT rounds its widened double and converts once, straight to `f32`.
- `bigint.rs` — the ported `FDBigInteger` magnitude arithmetic, little-endian
  `u32` limbs, no `unsafe`.
- `round_tests.rs` — ROUND-DOUBLE-EXACT-1 pins: Spark-measured cells, zero sign, non-finite values, FLOAT cells, and the fast path against the digit path over random values and half points.
- `decimal_double.rs` (declared and re-exported by `mod.rs`) — **DECIMAL-DOUBLE-UNION-COERCION-1 part 1 (2026-11-15), K2:** `java_decimal_to_double(unscaled, scale)` is JDK 17 `BigDecimal.doubleValue()`, the correctly rounded double. Below 2^53 at scale 1 to 22 it is one division; otherwise Arrow's quotient is refined against the exact halfway point in 256-bit integers, half even. A negative scale or one above 38 uses the text parse. **DECIMAL-DOUBLE-UNION-COERCION-1 part 1 delta (2026-11-15):** `decimal_preimage_of_double(constant, precision, scale)`, re-exported by `mod.rs`, returns `(lower, upper)`, the least unscaled values in `[-(10^p - 1), 10^p - 1]` whose K2 double is at or above, and strictly above, a finite `f64` (`f64::total_cmp`, binary search over K2); `None` for NaN, ±Infinity, a precision outside 1 to 38, or a constant at or above the largest value's double. `decimal_double/tests.rs` (11 tests: 6 before, 5 added) pins it against the Spark cells, the digit path, the halfway points, the per-row form and fuzzed constants.
