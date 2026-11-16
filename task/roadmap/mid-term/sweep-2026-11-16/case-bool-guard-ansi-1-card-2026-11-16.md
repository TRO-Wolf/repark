# Card CASE-BOOL-GUARD-ANSI-1: with ANSI on, a boolean CASE that guards a division raises DIVIDE_BY_ZERO, where Spark answers

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's simplifier audit of 2026-11-16 (rewrite 6). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. The sweep re-run on main `e50ed17f` does not cover this audit cell; it was not re-measured on that main.

**Status:** PROPOSED, not dispatched; severity and label-blocking: owner to rule.

**Severity:** Proposed: S2 (owner to rule). A raise where Spark answers, on a common guard shape.

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
