# Card ROUND-DOUBLE-EXACT-1: `ROUND(1.005, 2)` answers `1.0`; Spark answers `1.01`

**Date:** 2026-11-14. **Filed by:** the DOUBLE-DECIMAL-STORE-ROUND-1 stores builder (`claude-opus-5-5`).
**Sources:** Fable's guidance, `Fable reply — DECIMAL-DOUBLE stack 2026-11-14.md` (finding 2 and §4), adopted by the
owner on 2026-11-14, about 13:38 UTC, verbatim "Merge and go with the split plan"; this builder's re-measure on main
`d843d37e` ([small-cells.md](../../wo/double-decimal-store-round-1/evidence/fix/small-cells.md)).

**Status:** **FIXED 2026-11-14 on the Spark doors** (branch `fix/round-double-exact-1`, not merged; builder
`claude-opus-5-5`). The native door is a recorded gap (below). **Step 2 of the split plan**, after
[DOUBLE-DECIMAL-STORE-ROUND-1](double-decimal-store-round-1-card-2026-11-13.md). Severity **S1**: a wrong value
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

## Fixed (2026-11-14)

- **The seat.** One kernel beside step 1's K1 (`crates/repark-common/src/java_double/mod.rs`:
  `java_double_round`, `java_float_round`): JDK 17's text of the value (of the widened double for a FLOAT),
  half up or half even at the scale, converted back once (straight to `f32` for a FLOAT), with a zero result
  `+0.0`. `round` is a RePark UDF over datafusion-spark's (`spark_math/bround.rs`): its FLOAT and DOUBLE arms
  call the kernel, and every other type keeps datafusion-spark's arm. `register_all` installs it (SQL,
  `F.expr`, `selectExpr`, `filter(str)`), and `F.round` builds it (`function_dispatch.rs`). `bround`'s float
  arms call the same kernel with half even, and a FLOAT stays a FLOAT. A column scale refuses with Spark's
  `DATATYPE_MISMATCH.NON_FOLDABLE_INPUT` on every Spark door.
- **Measured.** The step 0 table re-run on the head: on the Spark doors every cell answers Spark's value, in
  both ANSI modes. 94,712 cells per mode turn EQUAL, and no cell that was EQUAL on main moves.
  The evidence is in [the unit's fix/ directory](../../wo/round-double-exact-1/evidence/fix/fix.md).
- **Cost (the brief's 1.05x halt fires).** The C-012 set holds at 0.997 and 1.004, as do `round(v, 18)` at
  1.021 and `bround` at 0.974. A 10M-row `round(v, 2)` projection is 1.49x for DOUBLE, 1.72x for FLOAT and
  1.91x for prices with exact halves, and a `DECIMAL(10,2)` store of `round(v, 2)` is 1.09x. The cost is in
  the kernel, against a 3-flop binary round; five alternating runs, in [fix.md](../../wo/round-double-exact-1/evidence/fix/fix.md).
  The owner rules on the cost before this merges.
- **The native gap** (orchestrator ruling, 2026-11-14, after the owner's Q2 and native-gap rulings).
  `repark.sql` keeps DataFusion's own `round`: 11,158 `round` cells per mode still differ from Spark,
  `bround` is missing there (71,763 cells), and `ROUND(v, 309)` answers NaN.
- **Left as they are (controls).** The INT and DECIMAL arms keep datafusion-spark's answers. The INT
  overflow with ANSI on is card [ROUND-INT-ANSI-OVERFLOW-1](round-int-ansi-overflow-1-card-2026-11-14.md).
- **Side findings, not fixed.** `ROUND('1.005', 2)` (a string argument) is refused on both RePark doors,
  where Spark with ANSI on answers `1.01`. On the Spark door, `SELECT v, CAST(v AS STRING)` and
  `SELECT round(1.005D, 2), round(1.005E0, 2)` fail "Projections require unique expression names", which
  Spark allows.

Up: [map.md](map.md).
