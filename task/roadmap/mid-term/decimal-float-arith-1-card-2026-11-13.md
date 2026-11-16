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

## Append (2026-11-16): an integer beside a FLOAT, from Fable's sweep

Fable's type-pair sweep of 2026-11-16 (family 3) measured an integer beside a FLOAT, which this card's Close covers as "the other arithmetic operators". With ANSI on, `INT + FLOAT` answers `double` on Spark and `float` on main, with different values: `1 + 0.1` answers `1.1000000014901161` on Spark and `1.100000023841858` on main (cell `t:add:INT:FLOAT`, class T). With ANSI off both answer `float` with the same values (cell `f:add:INT:FLOAT`, class E). So for an integer beside a FLOAT the Spark type depends on the mode, which the decimal cells above do not show. Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. Reproduction: `python3 <scratchpad>/sweep/v.py t:add:INT:FLOAT f:add:INT:FLOAT`, with `<scratchpad>` the Fable sweep scratch folder (not committed).
