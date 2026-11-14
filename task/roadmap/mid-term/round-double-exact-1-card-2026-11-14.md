# Card ROUND-DOUBLE-EXACT-1: `ROUND(1.005, 2)` answers `1.0`; Spark answers `1.01`

**Date:** 2026-11-14. **Filed by:** the DOUBLE-DECIMAL-STORE-ROUND-1 stores builder (`claude-opus-5-5`).
**Sources:** Fable's guidance, `Fable reply — DECIMAL-DOUBLE stack 2026-11-14.md` (finding 2 and §4), adopted by the
owner on 2026-11-14, about 13:38 UTC, verbatim "Merge and go with the split plan"; this builder's re-measure on main
`d843d37e` ([small-cells.md](../../wo/double-decimal-store-round-1/evidence/fix/small-cells.md)).

**Status:** filed 2026-11-14, not dispatched. **Step 2 of the split plan**: it starts after
[DOUBLE-DECIMAL-STORE-ROUND-1](double-decimal-store-round-1-card-2026-11-13.md) merges. Severity **S1**: a wrong value
for ordinary inputs, and a wrong write wherever the result is stored.

## Measured (Spark 4.1.2 on JDK 17, both ANSI modes)

| cell | main | Spark |
|---|---|---|
| `ROUND(1.005, 2)` | 1.0 | 1.01 |
| `ROUND(0.285, 2)` | 0.28 | 0.29 |
| `ROUND(1.255, 2)` | 1.25 | 1.26 |
| `ROUND(-22.129547339983247, 18)` | -22.12954733998325 | -22.129547339983247 |
| `ROUND(CAST('1.255' AS FLOAT), 2)` | 1.2599999904632568 | 1.25 |

The first three rows are Fable's (main `6f9f2aaa`); every row is re-measured on main `d843d37e`, and the scale-18 row
is verifier 2's c3 on #1335. It is not a scale-18 corner of the fuzz. The FLOAT row also answers a DOUBLE's digits
where Spark answers a FLOAT.

## Where it is decided (read, not built)

- RePark has no `round` of its own. The DataFrame door passes it to DataFusion's
  (`crates/repark-python/src/column/function_dispatch.rs:250-257`), and `crates/repark-functions/src/spark_math`
  carries only `bround`. DataFusion's `round` multiplies, rounds and divides in binary.
- Spark's rule, from memory and to be checked against the source at step 0, is `BigDecimal(d).setScale(s,
  HALF_UP).toDouble`: the kernel of DOUBLE-DECIMAL-STORE-ROUND-1 (`java_double_unscaled`, JDK 17's text half up) at
  scale `s`, converted back with one correct rounding. When the double's shortest text has at most `s` fractional
  digits, the answer is the input.

## Step 0 should measure

FLOAT, negative scales, NaN and ±Infinity, a non-literal scale, the DataFrame door, whether the native door shares the
function name, and one cost row.

## Relation

- The coercion unit (step 4 of the split plan) holds 1,200 fuzz cells at DECIMAL(38,18) that this card fixes
  (`ROUND(v, 18)` used to cancel against Arrow's binary cast); its bar of 0 held cells assumes this card has merged.
- Fable did not check whether `ROUND` is inside the scoped label's named surface; if it is, this card blocks the label
  on its own.

## Close

`ROUND` of a FLOAT or DOUBLE answers Spark's value on the step 0 set, on both doors where `ROUND` reaches, in both
ANSI modes, with the cost row recorded.

Up: [map.md](map.md).
