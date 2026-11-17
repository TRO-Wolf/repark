# AVG-DECIMAL-ROUND-1 — step 0 sketch (measured; with DECIMAL-DIVIDE-LITERAL-1)

**Date:** 2026-11-17. **Builder:** Claude Opus 5.5 (`claude-opus-5-5`), step 0 only: documents and probes, no product
code. **Rulings carried:** B7 (owner, 2026-11-16, "Merge those and defaults."): AVG-DECIMAL-ROUND-1 is S1 and
label-blocking; owner "Defaults" (2026-11-17): this unit runs next, before NAN-MINMAX-1 part 2, and its step 0
measures DECIMAL-DIVIDE-LITERAL-1 (S1, label-blocking), folding the two only if they share a seat; owner "Merge them.
Dispatch the avg unit" (2026-11-17). Card:
[avg-decimal-round-1-card-2026-11-16.md](../../roadmap/mid-term/decimal-common-type-1-side/avg-decimal-round-1-card-2026-11-16.md).
Measured: [domain-table.md](domain-table.md); seats: [inventory.md](inventory.md); raw: [evidence/](evidence/map.md).

## 1. The faults

1. **`avg` of a decimal truncates** where Spark rounds half up, at every precision (10 to 38), on every door
   (SQL, `mean`, `DISTINCT`, `try_avg`, every window frame, the partial-state merge, every DataFrame spelling,
   `describe` and `summary`, the native door), in both ANSI modes. The type is Spark's. 413 of 616 Spark-door `avg`
   cells per mode are one-unit truncations; `HAVING` keeps the wrong groups; 10 Iceberg statements per mode store
   truncated values.
2. **`avg` near the 38-digit limit raises where Spark answers**: `sum × 10^(target scale − sum scale)` is formed in
   `i128` before the division, so a sum that Spark holds (p = 34, or p = 38 below 10^34) fails with a
   condition-less `Arithmetic Overflow in AvgAccumulator`; and where the sum itself passes 38 digits, Spark answers
   NULL with ANSI off (RePark raises) and `[ARITHMETIC_OVERFLOW] Overflow in sum of decimal` with ANSI on (RePark
   raises its own text).
3. **A decimal divided by an integer literal (DECIMAL-DIVIDE-LITERAL-1)** takes the literal's integer-type width
   (`decimal(10,0)` for `3`) where Spark takes its digits (`decimal(1,0)`), on either side of `/` and in `%`: the type
   always differs; the digits differ whenever the quotient does not terminate (more digits at narrow precisions,
   fewer at p ≥ 28 when the literal is on the left). Suffixed literals (`3S`, `3L`) take the type width in `*` too,
   so DEC-5 is partial.
4. **The DataFrame operator `/` answers `double`** for any decimal operand (`col("d") / 3`, `/ F.lit(3)`, `/ col("i")`,
   `3 / col("d")`): `0.30 / 3` is `0.09999999999999999`. Not a literal fault; a second seat in the same family.
5. **`try_divide`, `try_add`, `try_multiply` of a decimal and an integer cast the decimal to `INT`**:
   `try_add(2.51, 1)` is `3`. A third seat.

## 2. Spark's rules, read from the 4.1.2 jars and measured

Read with `javap -c -p` from `spark-catalyst_2.13-4.1.2.jar` and `spark-sql-api_2.13-4.1.2.jar` (excerpts in
[evidence/jar/javap-excerpts.txt](evidence/jar/javap-excerpts.txt)); each point is confirmed by measured cells.

- **`Average`** (`org.apache.spark.sql.catalyst.expressions.aggregate.Average`):
  - `resultType` = `DecimalType.bounded(p + 4, s + 4)`; `sumDataType` = `DecimalType.bounded(p + 10, s)`.
  - `updateExpressions`: `sum = add(sum, coalesce(child.cast(sumDataType), 0))`, `count = If(child IS NULL, count,
    count + 1)`; `add` is `DecimalAddNoOverflowCheck` for decimals. `mergeExpressions` adds both buffers.
  - `evaluateExpression` (decimal arm): `If(count = 0, NULL, DecimalDivideWithOverflowCheck(sum,
    count.cast(DecimalType.LongDecimal), resultType, context, nullOnOverflow = evalMode != ANSI))`.
    **`avg` does not use `Divide`.**
- **`DecimalDivideWithOverflowCheck.eval`**: a NULL sum (the sum left `sumDataType`) answers NULL when
  `nullOnOverflow`, else raises `QueryExecutionErrors.overflowInSumOfDecimalError(context, "try_avg")` (measured:
  `[ARITHMETIC_OVERFLOW] Overflow in sum of decimal`); otherwise `Decimal.$div(sum, count)` then
  `toPrecision(p, s, ROUND_HALF_UP, nullOnOverflow, context)`.
- **`Decimal.$div`** = `BigDecimal.divide(that, DecimalType.MAX_SCALE + 1, MATH_CONTEXT.getRoundingMode())`, and
  `Decimal$.<clinit>` builds `MATH_CONTEXT = new MathContext(MAX_PRECISION + 1, RoundingMode.DOWN)`. So the quotient
  is truncated at scale 39, then rounded half up to `s + 4` (≤ 38). The digit that decides the rounding is kept, so
  this equals one half-up rounding of the exact quotient.
- **So Fable's memory does not hold for `avg` in 4.1.2** (Fable marked it unmeasured): there is no `Divide` and no
  scale cut below `s + 4`; `avg` rounds half up at `s + 4` at every precision. Measured: `(38,18)` and `(34,30)`
  round exactly as `(10,2)` does (domain table, the group grid).
- **`Divide.resultDecimalType(p1, s1, p2, s2)`**: with `allowPrecisionLoss`: `scale = max(6, s1 + p2 + 1)`,
  `precision = p1 − s1 + s2 + scale`, then `DecimalType.adjustPrecisionScale`; without it: `intDig = min(38, p1 − s1
  + s2)`, `decDig = min(38, max(6, s1 + p2 + 1))`, and when `intDig + decDig > 38` `decDig` drops by
  `diff / 2 + 1`, then `bounded`. The value: `Decimal.$div` then `BinaryArithmetic.checkDecimalOverflow` =
  `toPrecision(p, s, ROUND_HALF_UP, !failOnError)`: one half-up rounding at the result scale.
- **The integer literal**: `DecimalPrecisionTypeCoercion$$anonfun$nondecimalAndDecimal$1` (taking the conf
  `literalPickMinimumPrecision`) types an integral `Literal` beside a decimal with `DataTypeUtils.fromLiteral`, and
  any other integral operand with `DecimalType.forType`. `fromLiteral` has `Short`, `Integer` and `Long` arms
  (digits: `decimal(1,0)` for 3) and falls back to `forType` otherwise, so a `TINYINT` literal stays `decimal(3,0)`
  (measured: `3Y / d10_2` is `decimal(16,11)` on Spark). It applies to every `BinaryOperator`, so `*`, `+`, `-`, `/`
  and `%` all take the digits.

## 3. Headline counts (Spark door; details in the domain table)

| | cells per mode | wrong, ANSI on | wrong, ANSI off |
|---|---|---|---|
| `avg` (13 columns, 19 groups, 26 forms, controls) | 616 | 465 (413 `t`, 39 `V`, 13 `R`) + 8 both raise | 473 (413 `t`, 39 `V`, 21 `R`) |
| division and controls (10 columns) | 456 | 235 (46 `T`, 169 `V`, 12 `R`, 8 `S`) + 45 both raise | 241 (46 `T`, 177 `V`, 18 `R`) |
| Iceberg wrong writes (both families) | 28 statements | 22 | 22 |

Under `pickMinimumPrecision=false` Spark's own division agrees with RePark (281 of 293 SQL division cells), which
pins the division fault to the literal rule alone.

## 4. Seat proposals

### Seat A (recommended): fix each fault where its value or type is decided

**A1 — the averager (AVG-DECIMAL-ROUND-1).** Replace `DecimalAverager::avg` (`aggregate.rs:303-327`) with
Spark's `DecimalDivideWithOverflowCheck`: widen the sum to `i256`, scale it to the target scale, divide by the
count with half-up rounding (the crate already has an `i256` half-up kernel, `decimal_spark::div_half_up`,
`decimal_spark.rs:527`; make it `pub(crate)` and call it), then check the target precision. Every `avg` path calls
this one function (`DecimalAvgAccumulator::evaluate`, `SparkAvgGroupsAccumulator` through `decimal_average`,
the window operator through `evaluate`, `try_avg`, the `DISTINCT` rewrite), so one edit covers every position in
the table. Alongside it:
- **The sum's overflow**: today `add_wrapping` on `i128` wraps silently past 1.7·10^38. Use a checked add (or an
  `i256` sum) and treat a sum whose magnitude reaches 10^38 (Spark's `sumDataType` at p ≥ 28 is `decimal(38, s)`)
  as Spark's NULL sum. State fields keep their declared types; the merge path checks the same way.
- **The ANSI flag**: the accumulator cannot read it (`AccumulatorArgs` has no config). An analyzer arm swaps the
  `null_on_overflow` struct in under the name `avg` when ANSI is off (display name stays `avg(x)`), and the ANSI-on
  overflow raises Spark's condition (`ARITHMETIC_OVERFLOW`, "Overflow in sum of decimal", the `try_avg` hint)
  instead of `Arithmetic Overflow in AvgAccumulator`.
- Covers: every `t` cell (128 ungrouped, 26 forms, DataFrame, `describe`, Iceberg), the `R` cells g20 and g22 at
  p ≥ 34, the ANSI-off `R` cells at p = 38, the error class of the ANSI-on `B` cells. Not covered: `avg(DISTINCT …)`
  beside another aggregate (the `DistinctAvgAccumulator` refusal) — owner question 4.
- Confs: neither changes `avg` on Spark or RePark (measured), so A1 reads none.

**A2 — the literal (DECIMAL-DIVIDE-LITERAL-1).** In `SparkDecimalPrecision` (`decimal_precision.rs:65-70`), let
`Divide` and `Modulo` take the U3 literal step (`min_precision_integer_literal`) on both sides, before
`type_coercion`, **without** U4a's cast-after for `/` (the `__repark_spark_decimal_div__` UDF already computes
Spark's type from its operands and rounds half up: `sum(x) / count(x)` and the typed operands agree). Then:
- make suffixed literals visible to U3 (`3S`, `3L`: today not seen in `*` either) and drop the `Int8`/`UInt8` arms
  of `integer_literal_value` (Spark's `fromLiteral` has no `Byte` arm; `3Y` must stay `decimal(3,0)`);
- `PyColumn::div` (`column/mod.rs:697`): stop casting decimal operands to `Float64`; emit the plain `/` and let the
  analyzer type it as SQL text is typed (a reading: integer by integer stays `double` through the existing
  `SparkFractionalDivision` rule, `lambda_rebind.rs:44`; a float operand stays `double`). This is the DataFrame `/` fault (§1.4), on the same unit because it is
  the same operator and its fix needs A2 to be right (`col(d) / 3` then reaches U3);
- covers every literal cell on both sides, `%`, the comparisons and `CASE`, `F.expr`, `selectExpr`, the `h / 2`
  scopes, the Iceberg CTAS types and the `INSERT`/`UPDATE`/`MERGE` digits, and the DataFrame `/` cells;
- confs: U3 reads `spark.sql.legacy.literal.pickMinimumPrecision` (default `true`) for every operator, so the
  `false` run then agrees for `*` as it already does for `/`. `allowPrecisionLoss=false` changes every operator's
  formula and is not this unit's (owner question 5).

**A3 — `try_*` coercion (proposed card TRY-ARITH-DECIMAL-INT-1).** `coerce_pair`
(`try_invert/arith.rs:263-315`) gains a decimal-with-integer arm (the integer becomes its decimal form, a literal by
its digits) ahead of `integer_width`.

### Seat B (alternative): one decimal-division rewrite for both

**B1 — `avg` as Spark writes it**: an analyzer rule rewrites a decimal `avg(x)` to Spark's `evaluateExpression`
shape, `__repark_spark_decimal_div__(sum(x) as decimal(p+10, s), count(x) as decimal(20,0))` cast to `(p+4, s+4)`,
so `avg` and `/` share the division UDF. Trade-offs: it changes the plan of every decimal `avg` (two aggregates
for one; window frames need `sum` and `count` frames that retract together; the display name and the
`HAVING`/`ORDER BY` references to `avg(x)` must survive the rewrite; the UDF rounds at `s + 4` directly, so the
result equals A1's); it does not remove `DecimalAverager`, which stays for the native door and `try_avg`; and the
`DISTINCT`, merge and `t4` paths change shape. More plan surface than A1 for the same values.

**B2 — the literal after coercion**: `rewrite_decimal_div` (`decimal_spark.rs:135`) narrows a
`CAST(<integer literal> AS decimal(10,0))` operand before building the UDF. Trade-offs: after `type_coercion` it
cannot tell DataFusion's cast from a user's `CAST(3 AS DECIMAL(10,0))`, which Spark keeps at `decimal(10,0)`
(that is why `is_default_integer_to_decimal` excludes the `Int32`/`(10,0)` pair today); `%` and the suffixed `*`
need the same arm elsewhere; and the DataFrame `/` still answers `double`.

### One seat for both?

**No.** `avg`'s value is decided in the accumulator; the division's type in the analyzer's literal step and in
`PyColumn::div`. They share a kernel (the `i256` half-up division), not a seat. B1 would make them share the
division UDF, at the cost above.

### Cost cases a build must run (at most 1.05x on held rows)

- a grouped decimal `avg` over many groups (`SparkAvgGroupsAccumulator`: `i256` work once per group at `evaluate`,
  plus the checked add per row);
- a window `avg` with a sliding `ROWS` frame (`evaluate` per row: the `i256` division per output row);
- a division-heavy projection (`d / 3` over many rows: the UDF's type gets narrower, so the scale-up shrinks);
- a non-decimal control (`avg` of `DOUBLE` and `BIGINT`, `/` of two integers on the DataFrame door after the
  `PyColumn::div` change);
- the C-012 set.

## 5. The unit split

**Two units, because no seat is shared** (the owner's rule):

1. **AVG-DECIMAL-ROUND-1** (A1): `aggregate.rs`, `avg_groups.rs`, one ANSI arm. Fixes the 413 truncation cells and
   the 13–21 overflow refusals per mode, and these wrong writes: avg CTAS (`c10_0`, `c10_2`, `c28_2`, `c38_18`),
   avg `INSERT` (`c10_2`, `c10_0`), avg `MERGE` (`c10_2`, `c10_0`, copy-on-write and merge-on-read).
2. **DECIMAL-DIVIDE-LITERAL-1** (A2): `decimal_precision.rs`, the suffixed-literal spelling, `PyColumn::div`. Fixes
   the literal-width cells (`/`, `%`, suffixed `*`), the DataFrame `/` cells, R-INTDIV-2's `h / 2`, and these wrong
   writes: division CTAS (`d10_2`, `d28_2`: wrong stored type), division `INSERT` (`d10_2`, `d28_2`), `UPDATE` and
   `MERGE` (`d10_2`, `d28_2`, copy-on-write and merge-on-read).
3. **TRY-ARITH-DECIMAL-INT-1** (A3), proposed card: the `try_*` coercion.

Order: 1 then 2 (the owner's order puts `avg` next); 2 could run beside 1 on a second lane since the files do not
overlap, but the lane cap rules that.

## 6. Owner questions (each with a default)

1. **Split into two units, or fold?** Default: **two units** (§5); no shared seat. Rests on the inventory rows
   `DecimalAverager::avg` and `SparkDecimalPrecision` U3, and the `sum(x) / count(x)` control (`=`: the division
   kernel already rounds as Spark).
2. **Does DECIMAL-DIVIDE-LITERAL-1 take the DataFrame `/` (`double`) fault?** Default: **yes**, as its second seat
   (same operator; cells `divdf/col_div_int/*`, `col_div_lit`, `col_div_intcol`, `int_rdiv_col`, `where_round`: all 50
   of those DataFrame cells ANSI off).
   Severity: S1 by the label's classes (silent wrong value on the Spark door, ANSI on).
3. **Does DECIMAL-DIVIDE-LITERAL-1 take the suffixed-literal `*` cells and `%`?** Default: **yes** (same U3 step;
   `extra/mul_smallint_literal`, `extra/mul_bigint_literal`, `divctl/mod/*/i3`: type only, values equal).
4. **`avg(DISTINCT x)` beside another aggregate raises `DistinctAvgAccumulator` not implemented** (Spark answers).
   Default: **card it (AVG-DISTINCT-MIXED-1), S3, not label-blocking**; a refusal, not a wrong value
   (`extra/avg_distinct_with_count`).
5. **`allowPrecisionLoss=false` is accepted and ignored** for every decimal operator. Default: **card it
   (DECIMAL-PRECISION-LOSS-CONF-1), S2, not label-blocking**: a non-default conf; the default answers are right after
   the two units (`apl_false` runs: 53 `S`, 96 `V` in division SQL, ANSI on).
6. **TRY-ARITH-DECIMAL-INT-1 severity.** Default: **S1, label-blocking**, as a silent wrong value on the Spark door
   with ANSI on (`try_add(2.51, 1)` = `3`; `div/try_divide/*`, `extra/try_add_dec_int`, `extra/try_multiply_dec_int`).
7. **The ANSI-off overflow of a decimal `avg`** (Spark NULL, RePark raises; g20 and g24 at p = 38). Default: **in
   AVG-DECIMAL-ROUND-1** (the ANSI arm, §4 A1), though ANSI off is outside the label's scope, because the same
   averager edit carries it.
8. **`d38_10 * 3` raises `Arrow error: Arithmetic overflow`** where Spark answers `decimal(38,8)`. Default: **card
   it, S2**, outside both units (a multiplication at 38 digits; `divctl/mul/d38_10/*`).

## 7. What this step 0 did not settle

- Whether Spark's sum overflow is sticky per buffer (a sum that passes 38 digits and comes back): not measured.
- How `3S` and `3L` are spelled when U3 runs (suffix UDF or cast); the build reads it before choosing the arm.
- Which `avg` answers on the native door (RePark's or DataFusion's): both truncate; the native door is not the
  label's.
- Whether RePark's conf store can reach an analyzer rule for `pickMinimumPrecision` the way `SparkAnsiConfig`
  does: not read.

Up: [map.md](map.md).
