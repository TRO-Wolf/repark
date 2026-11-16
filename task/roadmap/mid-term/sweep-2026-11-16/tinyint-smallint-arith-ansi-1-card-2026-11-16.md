# Card TINYINT-SMALLINT-ARITH-ANSI-1: with ANSI on, TINYINT and SMALLINT arithmetic wraps where Spark raises

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), from Fable's type-pair sweep of 2026-11-16 (family 1). Measured before #1341 and #1345, on main's package `pkg-ddu1-7b21bde3` against Spark 4.1.2. Re-run on main `e50ed17f` (RePark side): unchanged; only family 10's 32 `VALUES` cells moved to equal (#1341).

**Status:** PROPOSED, not dispatched; severity and label-blocking: owner to rule.

**Severity:** Proposed: S1 (owner to rule). The wrong value appears only when the result overflows, which Spark raises on.

## Finding

With `spark.sql.ansi.enabled = true`, Spark raises `BINARY_ARITHMETIC_OVERFLOW` when a TINYINT or SMALLINT sum or product leaves its own type. Main answers the wrapped value. INT and BIGINT products already raise on both engines, so the defect is specific to the two small types. With ANSI off every cell agrees.

## Measured

| cell, ANSI on | Spark 4.1.2 | main | class |
|---|---|---|---|
| `add` TINYINT + TINYINT, `127 + 127` | raises: `1S + 127S caused overflow` | `-2` | A |
| `mul` TINYINT × TINYINT | raises | answers, wrapped | A |
| `mul` TINYINT × SMALLINT and SMALLINT × TINYINT | raises | answers, wrapped | A |
| `mul` SMALLINT × SMALLINT, `300 * 300` | raises: `300S * 300S caused overflow` | `24464` | A |
| control: `mul` INT × INT, `100000 * 100000` | raises `ARITHMETIC_OVERFLOW` | raises the same | B |
| control: `add` SMALLINT + SMALLINT, `300 + 300` | `600` | `600` | E |

Class A is RePark answers and Spark refuses; B is both refuse; E is equal. Every TINYINT and SMALLINT arithmetic cell in ANSI off is class E.

Reproduction, with `<scratchpad>` the Fable sweep scratch folder (not committed):
`python3 <scratchpad>/sweep/v.py t:add:TINYINT:TINYINT t:mul:SMALLINT:SMALLINT t:mul:INT:INT`

## Seat (not diagnosed)

ANSI-DOOR-1 says the SQL door's datafusion-spark kernels read `execution.enable_ansi_mode`, which repark never sets. That explains the wrapped `abs` and ROUND-INT-ANSI-OVERFLOW-1, but it does not explain this card alone: INT and BIGINT products raise on the same door, so some ANSI check already runs for them. Step 0 finds which operator path TINYINT and SMALLINT take, and whether the INT check covers them.

Up: [map.md](map.md).
