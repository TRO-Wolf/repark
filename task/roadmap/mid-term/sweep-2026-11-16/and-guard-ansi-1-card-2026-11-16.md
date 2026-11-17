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
