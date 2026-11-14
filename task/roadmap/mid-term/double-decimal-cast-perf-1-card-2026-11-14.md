# Card DOUBLE-DECIMAL-CAST-PERF-1: a faster exact path for FLOAT/DOUBLE into DECIMAL

**Date:** 2026-11-14. **Filed by:** the DOUBLE-DECIMAL-STORE-ROUND-1 finishing builder (`claude-opus-5-5`).
**Sources:** that unit's cost tables ([card](double-decimal-store-round-1-card-2026-11-13.md#cost),
[evidence](../../wo/double-decimal-store-round-1/evidence/fix/cost-finish.md)); the owner question of 2026-11-14,
about 01:06 UTC, option C.

**Status:** **PROPOSED**, not dispatched; **re-scoped 2026-11-14 to stores.** The owner's "Merge and go with the
split plan" (2026-11-14, about 13:38 UTC) puts DOUBLE-DECIMAL-STORE-ROUND-1's cost exception on stores only, with this
card as its performance card. Severity **S4** (performance; the answers are right). When step 4 of the split plan puts
the kernel on written casts, the cast rows come back here with that unit's own measurement.

## Measured (stores only)

Main `d843d37e` against the narrowed DOUBLE-DECIMAL-STORE-ROUND-1 head `d2b517c6`, 10M rows, five alternating pairs,
median seconds, cores 32-47 (16 cores) ([stores.md](../../wo/double-decimal-store-round-1/evidence/fix/stores.md#cost)):

| query | main | head | head/main |
|---|---|---|---|
| `INSERT … SELECT` into DECIMAL(10,2) | 0.1784 | 0.1617 | 0.91 |
| `INSERT … SELECT` into DECIMAL(38,18) | 0.2995 | 0.3944 | 1.32 |

`CAST` to DECIMAL(10,2) and (38,18) and the `decimal = double` filter equal main on that head (0.87x to 1.02x), so they
left this card; the plans without a store hold 1.05x. The earlier all-casts table (main `6f9f2aaa`, head `3eed6799`) is
in the unit card's [Cost](double-decimal-store-round-1-card-2026-11-13.md#cost).

## Where the time goes

`java_double_unscaled` (`crates/repark-common/src/java_double/mod.rs`) is Spark's value: JDK 17's `Double.toString`
digits of the double, rounded half up at the target scale. A binary fast path answers a row whose product `10^s·v` is
below `2^52` and more than four ulps from a half; every other row runs the ported `FloatingDecimal` digit generation. At scale 18 the product is past `2^52` for any
`|v| ≥ 0.005`, so nearly every row takes the digit path. At scale 2 the fast path covers most rows and the store row
holds the bar.

## Candidates (unmeasured)

- Profile the digit path per row (allocation, the big-integer steps of the port, the text-free digit hand-off) and
  remove what the cast does not need.
- A wider exact fast path: decide from the double's bits when JDK 17's digits rounded at scale `s` must equal the
  binary product's rounding, and run the digit path only near a half.
- The shortest-digits route stays option B, not this card: it differs from Spark on JDK 17 on non-shortest texts
  (`1e23`).

## Close

The store rows (`INSERT … SELECT` into DECIMAL(10,2) and (38,18)) hold 1.05x against main before
DOUBLE-DECIMAL-STORE-ROUND-1, with that unit's pins unchanged.

Up: [map.md](map.md).
