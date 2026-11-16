# Card STRING-IN-COERCE-1: a STRING beside a DOUBLE or DECIMAL list in IN answers a different boolean, or refuses, where Spark answers

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's type-pair sweep of 2026-11-16 (family 7). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. Re-run on main `e50ed17f` (RePark side): unchanged; only family 10's 32 `VALUES` cells moved to equal (#1341).

**Status:** PROPOSED, not dispatched; severity and label-blocking: owner to rule.

**Severity:** Proposed: S1 (owner to rule) for the silent ANSI-off cells. The DECIMAL refusals are proposed S2 (owner to rule).

## Finding

With a STRING left operand and numeric constants in an `IN` list, the two engines pick different comparison types. With ANSI off, `'1' IN (1.0)` answers `false` on Spark and `true` on main: a silent wrong answer. With a DECIMAL(38,0) or DECIMAL(38,18) constant on the right, main refuses where Spark answers. The FLOAT and DOUBLE cells agree with ANSI on.

## Measured

| cell | ANSI | Spark 4.1.2 | main | class |
|---|---|---|---|---|
| STRING `'1'` `IN (1.0)`, DOUBLE constant | off | `false` | `true` | V |
| STRING beside DOUBLE, `IN` | on | `true` | `true` | E |
| STRING beside FLOAT, `IN` | off | answers | answers, different values | V |
| STRING beside DECIMAL(38,0) or DECIMAL(38,18), `IN` | both | answers | refuses | R |

Reproduction, with `<scratchpad>` the Fable sweep scratch folder (not committed):
`python3 <scratchpad>/sweep/v.py f:in:STR_NUM:DOUBLE t:in:STR_NUM:DOUBLE f:in:STR_NUM:DEC38_0`

## Not yet known

The seat. The comparison type for an `IN` list with a STRING left operand is the question; step 0 finds where main takes the numeric type and where Spark takes the string type. Related: STRING-CAST-LENIENT-1 (text refusals in CAST and writes), and DECIMAL-COMMON-TYPE-1 (the decimal common type, which covers `IN` as a position).

Up: [map.md](map.md).
