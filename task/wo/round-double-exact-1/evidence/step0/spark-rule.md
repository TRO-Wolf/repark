# ROUND-DOUBLE-EXACT-1 step 0 — Spark's rule and the kernel

**Read from source, then measured.** Spark `v4.1.2`
`sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala`; Scala
`v2.13.17` `src/library/scala/math/BigDecimal.scala` (the oracle ships `scala-library-2.13.17.jar`); JDK
17.0.15 `java.base/java/math/BigDecimal.java` (the oracle JVM's own `lib/src.zip`). Lines below are those
files' lines.

## The rule

| # | Spark | where |
|---|---|---|
| R1 | `round` is `RoundBase` with `HALF_UP`; `bround` is `RoundBase` with `HALF_EVEN`; `round(x)` and `bround(x)` are scale `Literal(0)` | `mathExpressions.scala:1739-1746`, `:1772-1779` |
| R2 | the result type of a FLOAT or DOUBLE input is the input type (`case t => t`); only DECIMAL narrows | `:1529-1546` |
| R3 | the scale is `IntegerType` and must be foldable, else `DATATYPE_MISMATCH.NON_FOLDABLE_INPUT` at analysis | `:1548`, `:1550-1564` |
| R4 | the scale is evaluated once; a NULL scale answers NULL without evaluating the child | `:1570-1571`, `:1581-1584`, codegen `:1708` |
| R5 | DOUBLE: NaN and ±Infinity answer themselves; otherwise `BigDecimal(d).setScale(_scale, mode).toDouble` | `:1636-1642`; codegen `java.math.BigDecimal.valueOf(d).setScale(...).doubleValue()` `:1697-1704` |
| R6 | FLOAT: `BigDecimal(f.toDouble).setScale(_scale, mode).toFloat`; codegen `BigDecimal.valueOf(f)`, which widens the float to a double, then `.floatValue()` | `:1629-1635`, `:1689-1696` |
| R7 | Scala `BigDecimal(d)` is `new BigDec(java.lang.Double.toString(d), mc)`; JDK `BigDecimal.valueOf(double)` is `new BigDecimal(Double.toString(val))` | `BigDecimal.scala:219`, `:50-51`; `BigDecimal.java:1363-1368` |
| R8 | `doubleValue()`: one IEEE divide (or multiply) of the exact unscaled value by an exact power of ten when the unscaled value is under 2^52 and the scale is small, else `Double.parseDouble(toString())`; both are one correct rounding | `BigDecimal.java:3805` (`doubleValue`) |
| R9 | `floatValue()`: the same with 2^22 and float powers of ten, else `Float.parseFloat(toString())`: the decimal rounds to a float once, never through a double | `BigDecimal.java:3760` (`floatValue`) |
| R10 | a Java `BigDecimal` has no negative zero, so `-0.0`, and any value that rounds to zero, answers `+0.0` | R7, R8 |

**In one line:** take JDK 17's `Double.toString` of the value (of the value widened to a double, for a
FLOAT), round that decimal text at scale `s` half up (`round`) or half even (`bround`), and convert the
result to the input type with one correct rounding. A zero result is `+0.0`. NaN and ±Infinity pass
through, and a NULL scale answers NULL.

Two consequences:
- **The text has at most 17 significant digits.** When the text has no more than `s` fractional digits,
  the answer is the input, at any magnitude: `-22.129547339983247` at scale 18 stays itself.
- **The text is JDK 17's, not the shortest.** The scan in [raw/jdk-scan-hits.json](raw/jdk-scan-hits.json)
  covered 280,151 doubles and found 6,189 whose JDK 17 text is not the shortest (powers of two, values
  above 2^54). For 383 of them a half-up or half-even rounding at some scale from -3 to 30 gives a
  different double from the one the shortest text gives (461 value, scale and mode combinations). One example is `1.949133209895265e16` at scale -2: JDK 17 prints
  `1.9491332098952648E16`, so Spark answers `1.94913320989526e16`. Twelve of these values are in the table.

## Measured check of the rule

[probes/model.py](probes/model.py) implements the line above with Python's `decimal` module, starting
from Spark's own `CAST(v AS STRING)`. A FLOAT uses `CAST(CAST(v AS DOUBLE) AS STRING)`.
[probes/analyze.py](probes/analyze.py) compares the model with Spark's measured SELECT answer in every
cell:

- **0 differences in 82,720 cells.** That is 940 values × 22 scales × `round` and `bround`, in both ANSI
  modes.
- **ANSI has no effect** on a FLOAT or DOUBLE `round` or `bround` (R5, R6). Only the integral arms read
  `ansiEnabled`.

## Does step 1's kernel yield it? (`crates/repark-common/src/java_double/mod.rs`)

**Yes, for the digits; the back conversion and four edges are new.**
`java_double_unscaled(value, scale: i8) -> Option<i128>` (`mod.rs:670-681`) is K1. It rounds the JDK 17
digits (`dtoa_double_bits`, `:551`) half up at `scale` (`digits_unscaled`, `:710-744`), with a binary fast
path away from a half (`product_unscaled`, `:684-700`, margin of 4 ulps).

**Measured:** [probes/kcheck.py](probes/kcheck.py) stores every finite table value into `DECIMAL(38,s)`
on main, for `s` from 0 to 18, under `|v| < 10^(37-s)`. That store is K1's implicit-store seat. It reads
the decimal back and converts it to a double, or directly to a float for a FLOAT source, with one correct
rounding. Every one of the 17,540 cells equals Spark's `ROUND(v, s)`.

So `ROUND(v, s)` is K1's unscaled value divided by `10^s`, rounded once. What the kernel does not cover yet:

| edge | K1 today | what `round` needs |
|---|---|---|
| E1 scale range | `i8`; `product_unscaled` takes only `s` from 0 to 22, and a negative scale goes the digit path | Spark's scale is any `Int`. Measured cells run from -400 to 400, so the scale must widen to `i32` |
| E2 magnitude | `i128` overflows, and `None` comes back, from about `|v|·10^s ≥ 1.7·10^38` | when the text has no more than `s` fractional digits (`shift ≥ 0` in `digits_unscaled`), answer the input without forming the integer |
| E3 `bround` | half up only (`(digits % divisor) * 2 >= divisor`) | a half-even arm. The fast path is unchanged, because away from a half `round()` and `round_ties_even()` agree |
| E4 back conversion | none (K1 ends at the decimal) | unscaled under 2^53 and `|s| ≤ 22`: one divide or multiply by an exact power of ten, as JDK R8 does. Otherwise parse the decimal digits, which `f64::from_str` / `f32::from_str` round correctly and, for a float, directly (R9) |

**FLOAT:** the digits are the widened double's (`dtoa_double_bits((f as f64).to_bits())`), as R6 says. The
conversion back goes straight to `f32` (R9). An `f64` first and then an `f32` would round twice.

**Zero sign (R10):** K1 already returns `Some(0)` for a zero result, so the back conversion yields `+0.0`.
On main, 1,709 DOUBLE cells per door answer `-0.0` where Spark answers `0.0`; this fixes them too.

Up: [map.md](map.md).
