# Card DECIMAL-FLOAT-ARITH-1: `decimal + FLOAT` answers `float`; Spark answers `double`

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** the DECIMAL-DOUBLE-UNION-COERCION-1
design sketch (#1324, Q6; [sketch](../../wo/decimal-double-union-coercion-1/sketch.md),
[domain table](../../wo/decimal-double-union-coercion-1/domain-table.md), the `ctl_add` FLOAT rows; inventory row A6);
owner, 2026-11-13, about 14:52 UTC, verbatim "Defaults, and merge #1323 when green" (Q6: a card).

**Status:** filed 2026-11-13, not dispatched. Severity **S3**. Outside the decimal unit's seats.

## Measured (main `04d84ab9`, Spark 4.1.2)

- The `ctl_add` row (`<decimal> + <float>`), decimal operands `0.1`, `1.5`, `123456789.123456789`, `0.001`,
  `CAST(0.5 AS DECIMAL(38,18))` and a DECIMAL(10,2) column: against FLOAT `1.5` and FLOAT `1e-20`, the Spark door
  gives `WTWWTT` in both ANSI modes (`W` a wrong value, `T` the type only); the native door gives `====TT`.
  Against DOUBLE operands every cell is EQUAL.
- **Seat.** DataFusion's `mathematics_numerical_coercion` (`binary.rs:1426`, `numerical_coercion` `:1464`):
  `(Float64, _) => Float64`, then `(_, Float32) => Float32`. Arithmetic never reaches the decimal coercion, so
  decimal + DOUBLE is already `double` as in Spark; decimal + FLOAT is `float`.

## Close

`decimal + FLOAT` (and the other arithmetic operators) types `double` and answers Spark's value, pinned on the `ctl_add`
FLOAT cells in both ANSI modes.
