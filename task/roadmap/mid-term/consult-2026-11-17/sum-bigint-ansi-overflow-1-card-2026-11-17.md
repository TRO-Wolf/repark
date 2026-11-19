# Card SUM-BIGINT-ANSI-OVERFLOW-1: with ANSI on, `sum` over `BIGINT` wraps where Spark raises `long overflow`

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), from the SUM-DEC-I128WRAP-1 step 0 and its revision (PR #1357, head `2f63e2c7`), under the owner's ruling "defaults on both" (about 23:05, later the same evening; [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md), later section).

**Status:** filed 2026-11-17, not dispatched.

**Severity: S1, label-blocking** (owner, 2026-11-17, "defaults on both", question 4: "S1 by the same class rule").

## Finding

`sum` over a `BIGINT` column with ANSI on adds with a wrapping accumulator. Spark's `Add` per row raises `long overflow` at the first overflowing row; RePark answers the wrapped value. With ANSI off both answer the wrapped value.

## Cells

Sum step 0 (PR #1357, `3fecb0ff`): controls `INT`, `BIGINT` and `DOUBLE`, 150 cells per mode. 17 `BIGINT` cells are silent with ANSI on and none with ANSI off. Native door, 589-cell sample per mode: 15 `BIGINT` cells silent with ANSI on. The critic re-measured 8 `BIGINT` cells: 7 silent with ANSI on (`int/ungrouped/g42`, `g44`, `int/grouped/g42`, `g43`, `g44`, `int/win_running/g42`, `intdf/F.sum/g42`), all equal with ANSI off, and `try_sum` equal.

## Seat

Not diagnosed beyond the unit's shape. Sum sketch §5 puts it as the integer arm of the same aggregate in ANSI mode: it shares only the registration with SUM-DEC-I128WRAP-1's seat A and needs its own cost case.

## Relation

SUM-DEC-I128WRAP-1 (the same registration; a second unit by ruling). The cause is not the decimal wrap.

## Source

SUM-DEC-I128WRAP-1 step 0 sketch, §5 and §6 question 4; step 0 hand-back, "Spark's `Sum`" and "Unit split"; step 0 revision hand-back, items 1 to 9 (the controls); the sum critic's verdict (GAPS), step 4, the `BIGINT` row. PR #1357 (head `2f63e2c7`).

**Append (2026-11-19, ruling 8):** its step 0 starts from decision I (option (1), exact in every phase with the state chosen by input precision; adopted 2026-11-18 about 20:05 UTC with Fable's seven recommendations, [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-18.md), item 8) and measures the same determinism table, since Spark's integer `sum` under ANSI may also check per row.

Up: [map.md](map.md).
