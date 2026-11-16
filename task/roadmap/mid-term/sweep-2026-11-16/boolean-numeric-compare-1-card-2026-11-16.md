# Card BOOLEAN-NUMERIC-COMPARE-1: a BOOLEAN compared with an integer refuses at planning with ANSI off, where Spark answers

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's type-pair sweep of 2026-11-16 (family 13). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. Re-run on main `e50ed17f` (RePark side): unchanged; only family 10's 32 `VALUES` cells moved to equal (#1341).

**Status:** PROPOSED, not dispatched; severity and label-blocking: owner to rule.

**Severity:** Proposed: S3 (owner to rule). A refusal on an unusual shape: a comparison of a boolean column with an integer column.

## Finding

With ANSI off, `BOOLEAN = INT` refuses at planning on main with `Cannot infer common argument type for comparison operation Boolean = Int32`. Spark answers, comparing the boolean with the integer, and so does the reverse order. With ANSI on both engines refuse.

## Measured

| cell | ANSI | Spark 4.1.2 | main | class |
|---|---|---|---|---|
| BOOLEAN `=` INT (`bool_col = int_col`) | off | answers, with NULL rows | refuses at planning | R |
| INT `=` BOOLEAN | off | answers | refuses at planning: `Int32 = Boolean` | R |
| either order | on | raises | refuses | B |

Reproduction, with `<scratchpad>` the Fable sweep scratch folder (not committed):
`python3 <scratchpad>/sweep/v.py f:eq:BOOLEAN:INT f:eq:INT:BOOLEAN`

## Not yet known

The seat. No card covers a boolean in a comparison, found by content search of the mid-term cards. Step 0 finds where main's comparison coercion gives up on the pair.

Up: [map.md](map.md).
