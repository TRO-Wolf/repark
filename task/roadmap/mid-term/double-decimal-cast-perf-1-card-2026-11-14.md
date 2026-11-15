# Card DOUBLE-DECIMAL-CAST-PERF-1: a faster exact path for the JDK 17 digit kernels (DECIMAL stores, `round`)

**Date:** 2026-11-14. **Filed by:** the DOUBLE-DECIMAL-STORE-ROUND-1 finishing builder (`claude-opus-5-5`).
**Sources:** that unit's cost tables ([card](double-decimal-store-round-1-card-2026-11-13.md#cost),
[evidence](../../wo/double-decimal-store-round-1/evidence/fix/cost-finish.md)); the owner question of 2026-11-14,
about 01:06 UTC, option C.

**Status:** **PROPOSED**, not dispatched; **re-scoped 2026-11-14 to stores.** The owner's "Merge and go with the
split plan" (2026-11-14, about 13:38 UTC) puts DOUBLE-DECIMAL-STORE-ROUND-1's cost exception on stores only, with this
card as its performance card. Severity **S4** (performance; the answers are right). When step 4 of the split plan puts
the kernel on written casts, the cast rows come back here with that unit's own measurement.

**Widened 2026-11-15 to the shared kernel.** The owner's "Merge it. Defaults" (2026-11-15, about 09:30 UTC) merged
[ROUND-DOUBLE-EXACT-1](round-double-exact-1-card-2026-11-14.md) (#1337) with a cost exception for `round`/`bround` of a
FLOAT or DOUBLE, and adopted Fable's advice (`Fable reply — ROUND-DOUBLE-EXACT-1 cost 2026-11-14.md`, §2.3) to
fold that unit's speed work into this card rather than open a second one. The store kernel and the round kernel
share one shape: a binary fast path, and the digit path near a half. Step 3 of the split plan adds the exact
DECIMAL→DOUBLE cast, a third kernel of that shape. **Order:** one unit after steps 3 and 4 covers all three; it blocks
no label.

**Widened 2026-11-15 to DECIMAL-DOUBLE-UNION-COERCION-1 part 1's cost exception.** The owner's ruling (2026-11-15, about 13:59 UTC), verbatim: "Merge 1338, proceed with A and yes to 3". Here "3" means these three rows are that exception, and their speed work belongs to this card: `WHERE d38 < v` (K2 at `DECIMAL(38,18)`) at 1.66x, written `CAST(v AS DECIMAL(10,2))` (K1) at 1.34x and written `CAST(v AS DECIMAL(38,18))` (K1) at 3.36x, head/main at 100M rows on part 1's head before the delta, which the delta does not reach ([cost](../../wo/decimal-double-union-coercion-1/evidence/fix/cost.md); PR #1339). The Close section sets no bar for these three rows yet.

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

## Measured (`round` and `bround`, from ROUND-DOUBLE-EXACT-1)

Main `d1679e4f` against the #1337 head `d859b961`, 100M rows (20M for the stores), five alternating runs with a
main-against-main control, the two verifiers' medians of per-run ratios (verifier 1 on 16 cores, verifier 2 on 10;
[fix.md](../../wo/round-double-exact-1/evidence/fix/fix.md)):

| row | head/main |
|---|---|
| `sum(round(v, 2))` DOUBLE | 1.72 to 1.75 |
| `sum(round(f, 2))` FLOAT | 2.44 to 2.70 |
| `sum(round(p, 2))`, prices with an exact half every tenth value | 2.26 to 2.39 |
| `sum(round(v, 18))` | 1.15 to 1.17 |
| `sum(bround(v, 2))` | 1.05 to 1.06 |
| store `round(v, 2)` into DOUBLE | 1.18 to 1.19 |
| store `round(v, 2)` into DECIMAL(10,2) | 1.12 to 1.16 |

The added time is about +1.5 to +2.0 ns per DOUBLE value and +1.7 to +2.3 ns per FLOAT value, against
datafusion-spark's three-operation binary round. The C-012 set holds (0.997 and 1.004).

## Where the time goes

`java_double_unscaled` (`crates/repark-common/src/java_double/mod.rs`) is Spark's value: JDK 17's `Double.toString`
digits of the double, rounded half up at the target scale. A binary fast path answers a row whose product `10^s·v` is
below `2^52` and more than four ulps from a half; every other row runs the ported `FloatingDecimal` digit generation. At scale 18 the product is past `2^52` for any
`|v| ≥ 0.005`, so nearly every row takes the digit path. At scale 2 the fast path covers most rows and the store row
holds the bar.

## Candidates (unmeasured)

- **For `round` (the builder's design, which Fable judged plausible):** the scale is constant per batch, but the
  kernel re-reads the power table and re-branches on the scale's sign, the guard, the limit and the margin for every
  value. A batch kernel with a branch-free first pass that computes the binary answer and flags the values near a
  half, then an exact second pass over the flagged ones.
- **The exact halves are a different problem:** every exact half needs JDK 17's text, which is not always the
  shortest, so the tie cannot be decided from the bits without a proof against the JDK.

- Profile the digit path per row (allocation, the big-integer steps of the port, the text-free digit hand-off) and
  remove what the cast does not need.
- A wider exact fast path: decide from the double's bits when JDK 17's digits rounded at scale `s` must equal the
  binary product's rounding, and run the digit path only near a half.
- The shortest-digits route stays option B, not this card: it differs from Spark on JDK 17 on non-shortest texts
  (`1e23`).

## Close

- The store rows (`INSERT … SELECT` into DECIMAL(10,2) and (38,18)) hold 1.05x against main before
  DOUBLE-DECIMAL-STORE-ROUND-1, with that unit's pins unchanged.
- The `round`/`bround` rows above, except the exact-halves row, hold 1.05x against main before ROUND-DOUBLE-EXACT-1
  at 100M rows, with that unit's pins unchanged.
- The exact-halves row gets its own close bar, which the owner sets when the unit is dispatched, because 1.05x may
  not be reachable there.

Up: [map.md](map.md).
