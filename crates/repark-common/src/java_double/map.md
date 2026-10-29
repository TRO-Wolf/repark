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
  `unsafe`, no new dependency. Behaviour pins stay beside the callers in
  `repark-functions` (the corpus byte-equality test over the recorded JDK
  17.0.15 fixture and the in-tree tables).
- `bigint.rs` — the ported `FDBigInteger` magnitude arithmetic, little-endian
  `u32` limbs, no `unsafe`.
