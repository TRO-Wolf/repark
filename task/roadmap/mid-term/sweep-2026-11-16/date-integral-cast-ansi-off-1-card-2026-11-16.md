# Card DATE-INTEGRAL-CAST-ANSI-OFF-1: with ANSI off, CAST of a DATE to an integer refuses at planning, where Spark answers NULL

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's type-pair sweep of 2026-11-16 (family 14). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2.

**Status:** PROPOSED, not dispatched; severity and label-blocking: owner to rule.

**Severity:** Proposed: S3 (owner to rule). A refusal where Spark answers NULL, on an unusual shape.

## Finding

With ANSI off, Spark answers NULL for every row of `CAST(date AS INT)` and `CAST(date AS TINYINT)`. Main refuses at analysis with `DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION` (`cannot cast "DATE"`). With ANSI on both engines raise.

## Measured

| cell | ANSI | Spark 4.1.2 | main | class |
|---|---|---|---|---|
| `CAST(d AS INT)` | off | NULL on every row | refuses at analysis | R |
| `CAST(d AS TINYINT)` | off | NULL on every row | refuses at analysis | R |
| `CAST(d AS INT)` | on | raises | raises | B |

Reproduction, with `<scratchpad>` the Fable sweep scratch folder (not committed):
`python3 <scratchpad>/sweep/v.py f:cast:DATE:INT f:cast:DATE:TINYINT t:cast:DATE:INT`

## Not yet known

The seat. ANSI-OFF-LEGACY-CAST-1 covers the ANSI-off cast of array elements in a map key and does not cover this scalar cast. ANSI-OFF-NARROWING-OVERFLOW-1 covers the numeric narrowing refusals, not a DATE source.

Up: [map.md](map.md).
