# Card BINARY-STRING-COMPARE-1: with ANSI off, a BINARY beside a STRING answers in IN and UNION ALL, where Spark refuses

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's type-pair sweep of 2026-11-16 (family 17). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. Re-run on main `e50ed17f` (RePark side): unchanged; only family 10's 32 `VALUES` cells moved to equal (#1341).

**Status:** PROPOSED, not dispatched; severity and label-blocking: owner to rule.

**Severity:** Proposed: S3 (owner to rule). A lenient answer where Spark refuses, on an unusual shape.

## Finding

With ANSI off, Spark refuses `binary IN (string)` with `DATATYPE_MISMATCH.DATA_DIFF_TYPES` and refuses `UNION ALL` of a binary and a string column with `INCOMPATIBLE_COLUMN_TYPE`. Main answers both: a boolean column for the `IN`, and a binary column for the union. With ANSI on the `IN` cell agrees.

## Measured

| cell | ANSI | Spark 4.1.2 | main | class |
|---|---|---|---|---|
| BINARY `IN` (STRING constant) | off | refuses: `DATA_DIFF_TYPES` | boolean rows | A |
| BINARY `IN` (STRING constant) | on | boolean rows | boolean rows | E |
| BINARY `UNION ALL` STRING | off | refuses: `INCOMPATIBLE_COLUMN_TYPE` | answers, binary column | A |

The `IN` cell also holds for a STRING column of numeric text in ANSI off (class A).

Reproduction, with `<scratchpad>` the Fable sweep scratch folder (not committed):
`python3 <scratchpad>/sweep/v.py f:in:BINARY:STR_OTHER f:union_all:BINARY:STR_OTHER t:in:BINARY:STR_OTHER`

## Not yet known

The seat. COALESCE-ANSI-WIDEN-1 covers the opposite direction for BINARY beside STRING: bare `coalesce` and `array` refuse or mistype where Spark widens. This card is the lenient direction, where main answers and Spark refuses.

Up: [map.md](map.md).
