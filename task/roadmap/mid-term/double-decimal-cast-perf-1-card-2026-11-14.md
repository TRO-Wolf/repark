# Card DOUBLE-DECIMAL-CAST-PERF-1: a faster exact path for FLOAT/DOUBLE into DECIMAL

**Date:** 2026-11-14. **Filed by:** the DOUBLE-DECIMAL-STORE-ROUND-1 finishing builder (`claude-opus-5-5`).
**Sources:** that unit's cost tables ([card](double-decimal-store-round-1-card-2026-11-13.md#cost),
[evidence](../../wo/double-decimal-store-round-1/evidence/fix/cost-finish.md)); the owner question of 2026-11-14,
about 01:06 UTC, option C.

**Status:** **PROPOSED**, not dispatched. **Owner word owed (asked 2026-11-14):** the question offers A (accept a
cost exception for plans that cast a FLOAT/DOUBLE to a DECIMAL, with this card as the performance card), B (shortest
digits, reversing Q6) or C (this card first, before the unit merges). Severity **S4** (performance; the answers are
right).

## Measured

Main `6f9f2aaa` against the merged DOUBLE-DECIMAL-STORE-ROUND-1 head `3eed6799`, 10M rows, five rotated runs, median
seconds (the unit card's [Cost](double-decimal-store-round-1-card-2026-11-13.md#cost) has both runs):

| query | main | head | head/main |
|---|---|---|---|
| `SELECT max(CAST(v AS DECIMAL(10,2)))` | 0.0615 | 0.0692 | 1.13 |
| `SELECT max(CAST(v AS DECIMAL(38,18)))` | 0.0625 | 0.1556 | 2.49 |
| `count(*) WHERE d = v` (DECIMAL(16,4) = DOUBLE) | 0.0990 | 0.1771 | 1.79 |
| `INSERT … SELECT` into DECIMAL(10,2) | 0.1508 | 0.1657 | 1.10 |
| `INSERT … SELECT` into DECIMAL(38,18) | 0.3121 | 0.4024 | 1.29 |

Plans without that cast and the C-012 like-for-like set hold 1.05x on the same head. The `decimal = double` row goes
away when DECIMAL-DOUBLE-UNION-COERCION-1 compares as DOUBLE (no float-to-DECIMAL cast is left in that plan).

## Where the time goes

`java_double_unscaled` (`crates/repark-common/src/java_double/mod.rs`) is Spark's value: JDK 17's `Double.toString`
digits of the double, rounded half up at the target scale. A binary fast path answers a row whose product `10^s·v` is
below `2^52` and more than four ulps from a half; every other row runs the ported `FloatingDecimal` digit generation. At scale 18,
and at the `decimal(30,15)` a `decimal = double` comparison coerces to, the product is past `2^52` for any
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

The cast rows of the cost table (`CAST` to DECIMAL(10,2) and (38,18), `decimal(16,4) = double`, the stores into
DECIMAL(10,2) and (38,18)) hold 1.05x against main before DOUBLE-DECIMAL-STORE-ROUND-1, with that unit's pins
unchanged.

Up: [map.md](map.md).
