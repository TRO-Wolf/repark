# Card POW-NEGATIVE-FRACTIONAL-1: `pow(-8.0, 0.5)` raises "not finite" where Spark answers NaN

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`), under owner ruling Q8 of 2026-11-12 ("Go with
the defaults"). **Source:** Fable's review of 2026-11-12, §3 (measured on PyPI 1.5.6), and SIGNED-ZERO-NORMALIZE-1 part
B evidence (`task/wo/signed-zero-normalize-1/evidence/part-b/`, filed with that unit): item 1's addendum on main
`276dc503`, cell LG-pow.

**Status:** filed 2026-11-12, not dispatched. Severity **S3, over-refusal** (Fable). The refusal is measured only on
1.5.6, and only for the decimal-literal form; the DOUBLE form answers on main.

**Measured:**

- **1.5.6 only (Fable), Spark door:** `SELECT pow(-8.0, 0.5)`, whose bare literals the Spark door types as decimals,
  raises `Arrow error: Arithmetic overflow: Result of -8^0.5 is not finite`. Spark 4.1.2 answers NaN (Fable's Spark
  leg used `pow(-8.0D, 0.5D)`; Spark's `pow` takes doubles).
- **Main `276dc503` (item 1, LG-pow):** `pow(CAST(-8.0 AS DOUBLE), CAST(0.5 AS DOUBLE))` answers NaN on the Spark door
  and on native, EQUAL to Spark by value. Its bits are `fff8…` where Spark's are `7ff8…`; that part is
  [NAN-PAYLOAD-1](nan-payload-1-card-2026-11-12.md).
- The decimal-literal form was not re-measured on main.

**Fix direction:** first re-measure `pow(-8.0, 0.5)` with bare literals on main, on both doors. If it still raises,
cast decimal arguments of `pow` to DOUBLE as Spark does, so that a non-finite result is NaN rather than an overflow
error. If it answers NaN, close the card as fixed upstream of 1.5.6 with that cell as the pin.

**Close:** fixed or found fixed, with the bare-literal pin; or declined by a recorded ruling.
