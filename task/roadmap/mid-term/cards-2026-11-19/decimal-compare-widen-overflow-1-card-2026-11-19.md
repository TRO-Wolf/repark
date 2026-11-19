# Card DECIMAL-COMPARE-WIDEN-OVERFLOW-1: SQL compares a `decimal(38,10)` quotient widened to `(38,14)`, so `d38_10 / 3 > 2` raises past 24 integer digits

**Date:** 2026-11-19. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's rulings of the 2026-11-18 window (item 7; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-18.md)). **Source:** `S/verify-ddl/verdict.md`, "Pre-existing notes" N3, and F1 (its consequence, the DataFrame `/`) in "Findings".

**Status:** filed 2026-11-19, not dispatched.

**Severity: S2, not label-blocking** (owner, 2026-11-18, about 19:23 UTC, verbatim "Go with a, merge 1361": decision H (a), by which the DataFrame regression F1 is accepted as part of ruling B's cost; the card is filed not label-blocking under ruling 7).

## Finding

- **N3 (class (c), the root):** SQL `d38_10 / 3 > 2` (and `>=`, `=`, `> CAST(2 AS BIGINT)`, `> i`) raises a `Decimal128(38, 14)` cast overflow when the quotient has more than 24 integer digits. Spark answers. The plan is `CAST(__repark_spark_decimal_div__(d, 3) AS Decimal128(38, 14)) > 2` (`S/verify-ddl/probes/explain2.py`): the comparison widens the `decimal(38,10)` quotient to `(38,14)`, which overflows past 24 integer digits.
- **F1 (class (a), the consequence, S2 by the verdict's own rating of the same shape as DECIMAL-MULTIPLY-OVERFLOW-1):** the DataFrame `/` of a 38-digit decimal, compared or filtered, raises `Arrow error: Cast error: Cannot cast to Decimal128(38, 14)` on the head, in both ANSI modes, 14 cells over both modes. Main (both packages) answers Spark's rows (`[1, 6]`; `[1, 2, 3, 4]`). The DataFrame `/` now plans as SQL text does, so this is inherited from N3.
- Not affected: `DECIMAL(38,2)` and `DECIMAL(30,4)` at the same values, a divisor of `100`, a column divisor, and every `d10_2` and `d28_2` cell.

**Main:** SQL raises the same cast overflow on main and head (N3). For the DataFrame `/` (F1), main answers Spark's rows.

Up: [map.md](map.md).
