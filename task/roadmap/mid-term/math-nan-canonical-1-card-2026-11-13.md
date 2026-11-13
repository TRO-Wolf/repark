# Card MATH-NAN-CANONICAL-1: where a sign-set NaN is made, and the three seats the owner chooses between

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** MATH-NAN-CANONICAL-1 builder,
2026-11-13: the step-0 hand-back, halted at step 2. Its evidence (`task/wo/math-nan-canonical-1/evidence/step0/`) is on
branch `fix/math-nan-canonical-1` at `a3f3f683`, not merged at filing.

**Status:** closed 2026-11-13: the owner chose (C), carried by [NAN-WRITE-CANONICAL-1](nan-write-canonical-1-card-2026-11-13.md); (B) is the first unit after the label. This is the follow-up unit the owner queued on
[NAN-PAYLOAD-1](nan-payload-1-card-2026-11-12.md); it halted before any product code.

**Measured on:** main `168dd934`, release build, against Spark 4.1.2 at default ANSI.

## What step 0 found

- **Spark also keeps a sign-set NaN in memory.** Over a column (`VALUES` input), Spark's `sqrt(-1.0)`, trig of
  infinity, `atanh(2.0)`, `pow(-8.0, 0.5)`, the arithmetic operators and the aggregates all return `fff8…`;
  `java_method('java.lang.Double', 'doubleToRawLongBits', …)` confirms it inside the JVM. Only the folded literal form
  gives `7ff8…`, and NAN-PAYLOAD-1's NS-* cells used that form. `acosh(0.5)` is the one producer where Spark gives
  `7ff8…` over a column.
- **The difference is comparison semantics, plus canonicalisation on write.** Every Spark comparison treats any NaN
  as one value, the largest: `max` is NaN, `min` is 1.5, `greatest` is NaN, `least` is -100, the NaN row sorts last,
  and `= NaN` finds it. RePark compares by bits (Arrow's total order), so `fff8…` is the smallest value and unequal to
  `CAST('NaN' …)`. Spark's Parquet write stores `7ff8…`/`7fc00000` for every NaN row (including `sqrt(x)`, `x - x`
  and `-x` over a column); RePark stores `fff8…`/`ffc00000`.
- **The producers include the arithmetic operators,** on every door and on DOUBLE and FLOAT: unary `-` of NaN,
  `inf - inf`, `inf + -inf`, `0 * inf`, `inf / inf` and `inf % 1`. On the native door `0.0 / 0.0` and `x % 0.0` do too;
  the Spark door raises `DIVIDE_BY_ZERO` there, as Spark does under ANSI.
- **The other producers:** about 15 functions on three doors (trig, `acosh`, `atanh`, `ln`, `log2` and native `log`,
  `pow`/`power` with a negative base and a fractional exponent, `inf % 1`, `pmod`, and `nanvl` passing its argument's
  sign), and nine aggregates (`stddev`, `stddev_pop`, `var_pop`, `variance` on the frame door, `covar_pop`,
  `covar_samp`, `avg`, `sum`, `median`). Spark gives `fff8…` for the aggregates too.

## The options (from the hand-back; not chosen)

- **(A) Canonicalise at every producer:** every float operator, about 15 functions on three doors, and nine
  aggregates. A hot-path decision the order reserves, and it does not cover NaNs already in stored data.
- **(B) Spark's model:** NaN-sign-blind comparison in `min`/`max` (and the Parquet-statistics shortcut),
  `greatest`/`least`, sort (`ORDER BY` and windows) and `=`, plus a canonicalising write. It matches Spark, including
  data from another engine; part B's key UDF already covers group, join and distinct keys. Several seats, each with
  its own cost row.
- **(C) A narrow first step:** one seat at RePark's Iceberg/Parquet write batch that canonicalises NaN as Spark's
  writer does. It fixes NAN-PAYLOAD-1's wrong write (stored `fff8…`), so reads of RePark-written tables agree;
  in-query answers over computed NaNs still differ. Its kernel is part B's `canonical_f64`/`canonical_f32`
  (`float_canon.rs` in `repark-common`) once part B merges.

## Side observations (recorded in the step-0 evidence, not acted on)

- The Spark door does not resolve `variance` or `percentile`, and `F.percentile` is missing.
- `corr` with an infinite input returns NULL where Spark returns NaN.
- `INSERT INTO t SELECT … FROM (VALUES (…)) AS s(x)` fails planning on the Spark door ("Inconsistent data length
  across values list").

## Close

The owner chooses (A), (B) or (C); the unit resumes at step 2 on that seat, or the card records a decline.

**Closed (2026-11-13):** the owner ruled "Defaults" on Q11: (C) before the label, as [NAN-WRITE-CANONICAL-1](nan-write-canonical-1-card-2026-11-13.md) (a new unit, not this
one resumed); a label exclusion for in-query answers over a computed NaN; (B) first after the label. That unit's
seat is the fork's file writer and partition calculator, not part B's `canonical_f64`, which also rewrites
`-0.0`.
