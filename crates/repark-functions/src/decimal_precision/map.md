# map — repark-functions/src/decimal_precision

## Purpose

The U3 literal step of `SparkDecimalPrecision` for every operator (DECIMAL-DIVIDE-LITERAL-1),
and unit tests for `SparkNegateNullDecimal`: negated null decimals fold to a typed null,
everything else passes through.

## Files

- [`literal_step.rs`](literal_step.rs) — **DECIMAL-DIVIDE-LITERAL-1 (2026-11-17):** Spark's
  `DecimalPrecisionTypeCoercion.nondecimalAndDecimal` for every `BinaryOperator`. An integral
  literal beside a decimal is `DataTypeUtils.fromLiteral` (`DECIMAL(digits, 0)`: `Short`,
  `Integer`, `Long` arms; no `Byte` arm, so `3Y` stays `forType`'s `(3,0)`); any other integral
  operand keeps `forType`. Its parts:
  - the U3 functions moved here from `decimal_precision.rs` (`min_precision_integer_literal`,
    the default-cast check, `min_precision_digits`); `Int8`/`UInt8` literals are no longer
    narrowed;
  - a bottom-up pass ahead of the `+ − *` pass: `/` and `%` narrow a bare or suffixed integer
    literal on either side, with no cast after (the `__repark_spark_decimal_div__` UDF types
    `/` from its operands; DataFusion's `%` type is Spark's `Remainder` formula). It skips
    the user-cast arm, so `CAST(3 AS DECIMAL(20,0))` keeps its width; it runs before
    `type_coercion` (`analyzer_rules_with_higher_order_preparation`), so `d / (1 + 2)` keeps
    `(21,13)` as on Spark, which folds after its coercion;
  - the `mod()` / `pmod()` call arm (datafusion-spark `SparkMod`, `SparkPmod`, which type the
    first argument): it narrows an integer literal argument. **Delta 1 (2026-11-18):** a decimal
    `mod()` call is rewritten onto the `%` operator (Spark's `mod` is `Remainder`, the same
    expression as `%`), with an integer argument cast to its `forType` width (`(10,0)` for an
    `INT`), so it takes `%`'s `Remainder` type and `SparkExprSemantics`' zero-divisor guard:
    `mod(d, 0)` raises with ANSI on, as on Spark (`[DIVIDE_BY_ZERO]` for Spark's
    `[REMAINDER_BY_ZERO]`), and is NULL with ANSI off. `pmod()` keeps `SparkPmod` and a cast to
    `Pmod.resultDecimalType` (`max(s1, s2)`, `min(p1 − s1, p2 − s2) + scale`,
    `adjustPrecisionScale`). An integer `mod()` keeps `SparkMod`. A cast over a call is left
    alone, so a second analyze changes nothing;
  - the DataFrame `/` marker: `PyColumn::div` builds `CAST(CAST(x AS DOUBLE) AS DOUBLE)` on
    each side (`dataframe_divide_operand`). The bottom-up pass strips it when one side is a
    decimal and the other a decimal, an integer or NULL (the plain `/` is then typed as SQL
    text is), and otherwise collapses it to main's single `DOUBLE` casts (integer by integer,
    float, string). On the native door no rule reads it; the double cast evaluates as one;
  - `spell_integer_suffix`, called by the SQL canonicaliser (`repark-spark`
    `spark_rewrites`): a `3S` or `3L` that is an operand of `* / % + -` or an argument of
    `mod(` / `pmod(` is spelled `__repark_suffix_literal__(CAST(3 AS BIGINT))`, so U3 tells
    it from a written `CAST(3 AS BIGINT)` (`forType` on Spark); elsewhere (`LIMIT`, frames,
    `VALUES`, partition transforms) the plain cast stays;
  - the conf `spark.sql.legacy.literal.pickMinimumPrecision` (default `true`): a builder conf
    installs `SparkPickMinimumPrecision`; `false` turns the literal step off for every
    operator, as Spark's `literalPickMinimumPrecision` does. A runtime `SET` is accepted and
    not read (as on main).
- [`literal_step_tests.rs`](literal_step_tests.rs) pins the literal widths of `/`, `%`,
  `mod`, `pmod`, a decimal `mod()` answering as `%` (`mod(d, 0)` raising with ANSI on), the
  folded and cast divisors, the `false` conf, the second analyze, the DataFrame marker and the
  suffix spelling.
- [`negate_null_tests.rs`](negate_null_tests.rs) pins the fold shapes (null cast /
  literal / try-cast / nested negative / wide target keep their type) and the untouched
  shapes (valued decimal, null int, null string, non-decimal cast target).

## Contracts pinned

- `Negative` over a null decimal answers a null literal of the child type (Spark
  `UnaryMinus` null-propagation); the DataFusion scalar kernel would `Internal error`.
- Nothing else rewrites: valued decimals keep kernel evaluation, non-decimals keep
  their kernel agree-or-refuse behavior.
- The literal step: Spark's types for `d / 3`, `3 / d`, `d % 3`, `mod(d, 3)`, `pmod(d, 3)`;
  `d / (1 + 2)` and `d / CAST(3 AS DECIMAL(20,0))` keep their integer widths.

## Pointers

- Up: [src map](../map.md)
- Rule: [`decimal_precision.rs`](../decimal_precision.rs)
- Card: [DECIMAL-DIVIDE-LITERAL-1](../../../../task/roadmap/mid-term/consult-2026-11-17/decimal-divide-literal-1-card-2026-11-17.md)
