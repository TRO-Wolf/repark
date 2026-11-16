# Card DATAFUSION-SIMPLIFIER-UPSTREAM-1: five DataFusion 54.1.0 simplifier rewrites give answers Spark does not give, and each is wrong by DataFusion's own semantics

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's simplifier audit of 2026-11-16 (rewrites 1 to 5; Fable re-measured them with its own probe). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2.

**Status:** PROPOSED, not dispatched; severity and label-blocking: owner to rule. Upstream reports and any patched crate are not decided here.

**Severity:** Proposed: S1 (owner to rule) for the group. Each rewrite can be ruled on its own; rewrite 1 needs a non-nullable input.

## Finding

Five rewrites in DataFusion 54.1.0's expression simplifier change an answer on a plain shape. The audit's inventory gives each rule an id and a file:line. Fable's audit states they are wrong under DataFusion's own semantics, so they are upstream bugs.

## Measured

| # | rule (inventory id, file:line) | cell | Spark 4.1.2 | main |
|---|---|---|---|---|
| 1 | SIM-BIT-NEG (`ES:1193-1215`, `1267-1289`, `1341-1363`): `(-A) & A → 0`, `(-A) \| A → -1`, `(-A) ^ A → -1` on a non-nullable column | `(-id) & id` for id 1, 2, 3 | 1, 2, 1 | 0, 0, 0 |
| 2 | SIM-BIT-XOR-SELF (`ES:1365-1397`): `A ^ A → 0` | `a ^ a` with a NULL `a` (id 3) | NULL | 0 |
| 3 | SIM-NEG-DISTRIBUTE (`ES:1427-1429`): `-(A & B) → (-A) \| (-B)` | `-(a & b)` for (1,0), (2,1), (3,2) | 0, 0, -2 | -1, -1, -1 |
| 4 | SIM-INLIST-ALGEBRA (`ES:1812-1926`): `a IN L1 AND a NOT IN L2` folded into one list, a NULL item dropped | `WHERE a IN (1, 2, 3) AND a NOT IN (2, NULL)` | no rows | the rows with id 0 and id 2 |
| 5 | FN-POWER (`fn/math/power.rs:568-615`): `power(a, 0) → 1` | `power(NULL, 0)` (id 1) | NULL | 1.0 |

Rewrite 1 was measured in the audit's v2 case `bit_neg_nonnull`, whose output is not saved in the audit's check folder; the figures are the audit report's. Rewrites 2 to 5 are from the recorded ANSI-on outputs. The audit reports rewrites 1 to 5 as identical in both ANSI modes.

Reproduction, with `<scratchpad>` the Fable audit scratch folder (not committed):
`python3 <scratchpad>/audit/check/verify.py repark true`, and the same with `spark` in place of `repark`; the cases `xor_self`, `neg_and`, `in_notin_null`, `power_zero`.

## Not yet known

The seat, and the choice between one small patch to the simplifier with upstream reports and six guards in RePark, which the audit's section 6 raises. Not decided here. CAST-LITERAL-UNWRAP-1 sets aside its own unwrap rule (inventory id SIM-UNWRAP-CAST), a different rule.

Up: [map.md](map.md).
