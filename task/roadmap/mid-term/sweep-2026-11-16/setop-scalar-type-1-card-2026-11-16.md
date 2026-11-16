# Card SETOP-SCALAR-TYPE-1: EXCEPT and INTERSECT keep the left operand's integer type where Spark widens

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's type-pair sweep of 2026-11-16 (family 5). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. Re-run on main `e50ed17f` (RePark side): unchanged; only family 10's 32 `VALUES` cells moved to equal (#1341).

**Status:** PROPOSED, not dispatched; severity and label-blocking: owner to rule.

**Severity:** Proposed: S3 (owner to rule). In the measured rows the values agree and only the result type differs; a later expression on the result is not measured.

## Finding

For `EXCEPT` and `INTERSECT` over two integer columns of different widths, main types the result as the left operand's type. Spark types it as the wider common type. The rows the sweep compared hold the same values on both engines, so the difference is the type alone.

## Measured

Both ANSI modes give the same result (class T: different result type, no value difference in the measured rows).

| cell | Spark 4.1.2 | main |
|---|---|---|
| TINYINT `EXCEPT` BIGINT | `bigint` | `tinyint` |
| TINYINT `INTERSECT` BIGINT | `bigint` | `tinyint` |
| INT `EXCEPT` BIGINT | `bigint` | `int` |
| INT `INTERSECT` BIGINT | `bigint` | `int` |

Across the whole matrix, `EXCEPT` has 58 class-T cells in ANSI off and 54 in ANSI on, and `INTERSECT` the same counts (sweep summary).

Reproduction, with `<scratchpad>` the Fable sweep scratch folder (not committed):
`python3 <scratchpad>/sweep/v.py f:except:TINYINT:BIGINT f:intersect:INT:BIGINT`

## Not yet known

The seat. Set operations are one of the positions the common-type rule of DECIMAL-COMMON-TYPE-1 names, so step 0 checks whether that seat also types integer set operations. Related and not the same: DECIMAL-DOUBLE-UNION-COERCION-1 (DECIMAL beside DOUBLE in set operations), STRUCT-SETOP-TYPE-FAMILY-1 (struct kinds in set operations), and SQL-SETOP-ALL-MULTISET-1 (the multiset semantics of `ALL`, not the types).

Up: [map.md](map.md).
