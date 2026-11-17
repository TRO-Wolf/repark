# Card AND-GUARD-ANSI-1: with ANSI on, AND does not protect its right side, so a guard before a division raises DIVIDE_BY_ZERO where Spark answers

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's simplifier audit of 2026-11-16 (difference 7; not a rewrite). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. The sweep re-run on main `e50ed17f` does not cover this audit cell; it was not re-measured on that main.

**Status:** ruled 2026-11-16, not dispatched; not label-blocking. Queued first once the label blockers are done.

**Severity:** S2, ruled 2026-11-16, not label-blocking. A raise where Spark answers, on a common guard shape.

Ruled 2026-11-16 ("Merge those and defaults."): S2, not label-blocking; the label text lists this loud card as an exclusion. Queued first once the label blockers are done, with CASE-BOOL-GUARD-ANSI-1.

## Finding

`WHERE b <> 0 AND a / b > 1`, with ANSI on: Spark keeps the rows where `b` is non-zero and the quotient is above 1. Main raises `DIVIDE_BY_ZERO` on the row where `b = 0`, which the guard was written to exclude. Fable's audit states that no DataFusion rewrite fires here: the difference is in the evaluation of `AND`.

## Measured

ANSI on, from the audit's recorded outputs.

| cell | Spark 4.1.2 | main |
|---|---|---|
| `WHERE b <> 0 AND a / b > 1` | rows 1 and 2 | raises `DIVIDE_BY_ZERO` |
| `CASE WHEN b = 0 THEN NULL ELSE a / b END`, as a value | equal | equal |

Reproduction, with `<scratchpad>` the Fable audit scratch folder (not committed):
`python3 <scratchpad>/audit/check/verify.py repark true`, and the same with `spark`; the case `guard_and`.

## Not yet known

The seat. ANSI-REMAINDER-CLASS-1 names the division guard on the analyzer seam, which raises on a zero divisor for the rows it evaluates. Step 0 finds where `AND` plans and evaluates its right side, and whether the guard covers this shape. The sibling with a rewrite is CASE-BOOL-GUARD-ANSI-1.

Up: [map.md](map.md).

## Append (2026-11-17): the coalesce shape is cross-listed

Ruling 4 of the [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md) lists the `coalesce_overflow_lit` shape here as well. It is a planning fold that raises in a branch Spark never evaluates: with ANSI on, Spark answers and RePark's fold raises (Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 3, Q3). The card for that literal is [DECIMAL-LITERAL-OVERFLOW-1](../decimal-common-type-1-side/decimal-literal-overflow-1-card-2026-11-17.md) (S2, not label-blocking). This card's severity and label status are unchanged.

## Append (2026-11-17, later): two cells through a temp view

The view step 0's round 2 re-critic (TEMP-VIEW-SESSION-SETTINGS-1, PR #1356, head `a2e34b9e`) measured the same shape through a temp view: `NOT (b = 0) AND m > 1`, created with ANSI on and read with it off. Spark answers 5.0 on both doors. RePark answers 5.0 on the SQL door and raises on the DataFrame door. The view unit's ruled filter rule leaves both cells raising under the reader's filter, and the wrapper follow-up ([VIEW-FILTER-SETTINGS-WRAPPER-1](../consult-2026-11-17/view-filter-settings-wrapper-1-card-2026-11-17.md)) does not close them either. The control, the same shape with ANSI on at creation and read, raises on both RePark doors, and so does a direct query. This append records the two cells; the severity and label status are unchanged (S2, not label-blocking).
