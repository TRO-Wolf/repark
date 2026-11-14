# DOUBLE-DECIMAL-STORE-ROUND-1 step 0 — Spark's rule, from the source

Read from the `v4.1.2` tag of `apache/spark` (raw files fetched 2026-11-13; line numbers by `grep -n` on the
fetched text) and Scala `v2.13.16`. The oracle runs Spark 4.1.2 on Zulu JDK 17.

## The rule

**A FLOAT or DOUBLE becomes a DECIMAL through the text `java.lang.Double.toString(d)` of the double (a FLOAT
widened to double first), parsed exactly as a `BigDecimal`, then rescaled to the target scale `HALF_UP`;
a result wider than the precision overflows; a non-finite value is NULL.**

| step | source | what it does |
|---|---|---|
| widen | `sql/catalyst/.../expressions/Cast.scala:1113-1116` (interpreted: `case x: FractionalType => ... Decimal(fractional.toDouble(b))`); `:1602-1607` (codegen: `Decimal.apply(scala.math.BigDecimal.valueOf((double) $c))`) | one arm for FLOAT and DOUBLE; a FLOAT is widened to double before anything else. |
| text | `sql/api/.../types/Decimal.scala:590` (`def apply(value: Double): Decimal = new Decimal().set(value)`); no `set(Double)` exists, so the double converts by Scala's implicit `double2bigDecimal` (`scala/math/BigDecimal.scala:304`) → `decimal(d)` → `:50-51` `new BigDecimal(new BigDec(java.lang.Double.toString(d), mc), mc)`; codegen's `BigDecimal.valueOf(d)` (`BigDecimal.scala:138`) wraps `java.math.BigDecimal.valueOf(double)`, which the JDK documents as `new BigDecimal(Double.toString(d))` | the decimal is the **text** of the double, not its binary value. DECIMAL128 holds 34 digits and the text at most 17 significant digits plus JDK 17's occasional 18th (below), so the `MathContext` never rounds. |
| rescale | `Decimal.scala:141-161` (`set(decimal: BigDecimal)` keeps the text's precision and scale); `Cast.scala:1053-1076` (`changePrecision(value, decimalType)` → `value.changePrecision(precision, scale)`); `Decimal.scala:352-353` (the two-argument `changePrecision` is `ROUND_HALF_UP`); the BigDecimal arm `dv.setScale(scale, roundMode)` | half-up on the text's digits: `1.255` → `1.26`, `1.005` → `1.01`, `9.995` → `10.00`, `0.125` → `0.13`. |
| overflow | `Cast.scala:1057-1076` (`nullOnOverflow = !ansiEnabled`: NULL, else `cannotChangeDecimalPrecisionError`, `NUMERIC_VALUE_OUT_OF_RANGE`); `canNullSafeCastToDecimal` `Cast.scala:407-414` is false for a float source (`DecimalType.isWiderThan` `DecimalType.scala:69-79` has no float arm), so the check is always made | a SELECT `CAST` follows `spark.sql.ansi.enabled`. |
| store | `sql/catalyst/.../analysis/TableOutputResolver.scala:700-716` (under the default `storeAssignmentPolicy=ANSI` the store cast is `Cast(..., ansiEnabled = true)` tagged `BY_TABLE_INSERTION`); `:588` and `:619-622` (`CheckOverflowInTableInsert` when the cast can overflow) | **a store into a DECIMAL column raises on overflow in both ANSI modes**, with the same text-then-half-up rounding. |
| non-finite | `Cast.scala:1117-1118` (`case _: NumberFormatException => null`, unconditional); codegen `:1609-1610` | `Double.toString` gives `NaN` / `Infinity`, `new BigDecimal` throws, and the value is NULL in both modes, on the store too. |

## The text is JDK 17's, not the shortest

`Double.toString` on JDK 17 is `FloatingDecimal`, which is not always the shortest round-trip text (fixed in JDK
19, JDK-4511638). Measured on the oracle's JVM (`J17.java`, scratchpad): `2.82879384806159E17` prints
`2.82879384806159008E17`, `1e23` prints `9.999999999999999E22`, `4.030184897929827e17` prints
`4.0301848979298272E17`. The table shows Spark using that text: `1e23` into `(38,0)` stores
`99999999999999990000000` (shortest text would give `10^23`, the binary value `99999999999999991611392`).
RePark already ports JDK 17's `FloatingDecimal` digit engine in `crates/repark-common/src/java_double/mod.rs`
(`with_java_double_text` `:602`, used by `CAST(double AS STRING)` and the Postgres chokepoint), so the rule is
reproducible bit for bit without a new dependency.

## Agreement with the table

The domain table ([domain-table.md](domain-table.md)) shows exactly this rule on every door and both modes:
the DOUBLE and widened-FLOAT texts round half-up (`1.255` → `1.26`; FLOAT `1.255` is
`1.2549999952316284` → `1.25`, and `(38,18)` stores `1.254999995231628400`), non-finite values are NULL, and
overflow raises on every store door in both modes. The rule is not contradicted by any cell, so the
"Spark's rule is not what the table shows" halt does not fire.

Up: [map.md](map.md).
