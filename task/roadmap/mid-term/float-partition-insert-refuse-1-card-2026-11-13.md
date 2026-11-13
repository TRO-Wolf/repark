# Card FLOAT-PARTITION-INSERT-REFUSE-1: an `INSERT` of a FLOAT partition value that is not exactly representable refuses

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** NAN-PARTITION-VALUE-1 (PR #1319,
fork PR #383) verifier 1, 2026-11-13, finding c1.

**Status:** filed 2026-11-13, not dispatched. Severity **S2, loud**: a refusal, not a silent wrong write.

## Measured

- `INSERT … CAST('100.1' AS FLOAT)` into a FLOAT-partitioned table refuses with
  `DataInvalid … fail convert to type float for: type mismatch`. `3.4e38` refuses the same way. `1.5` stores.
- CTAS stores all three.
- The fork's byte test shows the same as `RT … ERR` for an `f32::MAX` row.

**Builds:** #1319's head `6ed266ce` (fork `46d5db39`) and its base, main `a3f8c7d9` (fork `66901f0`), identical.

## Seat (from the source)

The fork's `crates/iceberg/src/spec/values/serde.rs`, `RawLiteralEnum::Double → Float` (about lines 466-476), on the
same JSON transport #1319 changed. An `f32` travels as the shortest JSON decimal and is parsed back as an `f64`. The
check `(f64::from(v as f32) - v).abs() > f32::EPSILON` is absolute, so it fails for any value above about 1 in
magnitude that carries a rounding residue.

## Fix direction

The source names the seat and the absolute epsilon check as the cause; it gives no fix shape.

## Close

An `INSERT` of any finite FLOAT partition value stores Spark's value, pinned with `100.1`, `3.4e38` and `f32::MAX`,
with CTAS as the control.
