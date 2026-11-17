# Card CASE-BOOL-GUARD-ANSI-1: with ANSI on, a boolean CASE that guards a division raises DIVIDE_BY_ZERO, where Spark answers

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's simplifier audit of 2026-11-16 (rewrite 6). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. The sweep re-run on main `e50ed17f` does not cover this audit cell; it was not re-measured on that main.

**Status:** ruled 2026-11-16, not dispatched; not label-blocking. Queued first once the label blockers are done.

**Severity:** S2, ruled 2026-11-16, not label-blocking. A raise where Spark answers, on a common guard shape.

Ruled 2026-11-16 ("Merge those and defaults."): S2, not label-blocking; the label text lists this loud card as an exclusion. Queued first once the label blockers are done, with AND-GUARD-ANSI-1.

## Finding

DataFusion's rule SIM-CASE-TO-BOOL (`ES:1518-1561`, per the audit's inventory) rewrites a boolean `CASE` into `AND` and `OR` terms. Those terms evaluate both sides, so the division the `CASE` guards runs on the row the guard was meant to exclude. The same guard written as a value `CASE` or as `if` answers on both engines.

## Measured

ANSI on, from the audit's recorded outputs.

| cell | Spark 4.1.2 | main |
|---|---|---|
| `WHERE CASE WHEN b = 0 THEN false ELSE a / b > 1 END` | rows 1 and 2 | raises `DIVIDE_BY_ZERO` |
| `CASE WHEN b = 0 THEN NULL ELSE a / b END`, as a value | equal | equal |
| `if(b = 0, 0, a / b)`, as a value | equal | equal |
| `WHERE CASE WHEN b = 0 THEN 0 ELSE a / b END > 1` | rows 1 and 2 | rows 1 and 2 |

Reproduction, with `<scratchpad>` the Fable audit scratch folder (not committed):
`python3 <scratchpad>/audit/check/verify.py repark true`, and the same with `spark`; the case `guard_bool_case`.

## Not yet known

The seat. AND-GUARD-ANSI-1 is the same effect without a rewrite. ANSI-REMAINDER-CLASS-1 names the division guard on the analyzer seam; step 0 checks whether this case reaches that guard.

Up: [map.md](map.md).

## Append (2026-11-17): the coalesce shape is cross-listed

Ruling 4 of the [rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md) lists the `coalesce_overflow_lit` shape here as well. It is a planning fold that raises in a branch Spark never evaluates: with ANSI on, Spark answers and RePark's fold raises (Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 3, Q3). Fable adds: "Yesterday's audit agent reported the same class for a dead `CASE` branch; I did not re-measure that one." The card for the literal is [DECIMAL-LITERAL-OVERFLOW-1](../decimal-common-type-1-side/decimal-literal-overflow-1-card-2026-11-17.md) (S2, not label-blocking). This card's severity and label status are unchanged.
