# Card DECIMAL-DIVIDE-LITERAL-1: a DECIMAL divided by an integer literal takes the wrong type, and the wrong digits when the quotient does not terminate

**Date:** 2026-11-17. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Defaults." (2026-11-17, about 10:40 UTC), ruling 2 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-17.md)). **Source:** Fable (`claude-fable-5-2`), consult reply of 2026-11-17, section 1, with the probe outputs Fable named (`p2-spark`, `p2-pkg-cu-88999454` and `p2-pkg-dct-7bf32859`; kept outside the repository).

**Status:** filed 2026-11-17, not dispatched.

**Severity: S1, label-blocking** (ruled 2026-11-17, ruling 2).

## Finding

Let `d` be a `DECIMAL(10,2)` column. In a division, RePark types an integer literal as `decimal(10,0)`. Spark types it by its digits, `decimal(1,0)` for `3`. So the result type differs on every such division, and the digits differ whenever the quotient does not terminate.

Fable's reading (a reading, not checked against Spark's source): multiplication and addition already follow Spark, and only the division differs. The registry row DEC-5 in [the parity registry](../../../../docs/spark-sql-iceberg-parity.md) states the literal rule for multiplication: "the integer literal `5` is `DECIMAL(1,0)`".

Fable's reason for severity (section 1): "by the rulings' classes it is a silent difference on the Spark door under ANSI on." The card is S1 by ruling 2.

## Cells

Measured on main and on #1350's head, ANSI on, Spark 4.1.2. `d` is `DECIMAL(10,2)` (Fable's §1 table; the probe outputs agree):

| cell | Spark | main `pkg-cu-88999454` | #1350's head `pkg-dct-7bf32859` |
|---|---|---|---|
| `d / 3` (d = 0.30) | `decimal(14,6)`: 0.100000 | `decimal(21,13)`: 0.1000000000000 | the same |
| `d / 100` | `decimal(14,6)`: 0.003000 | `decimal(21,13)`: 0.0030000000000 | the same |
| `1.0 / 3` | `decimal(7,6)`: 0.333333 | `decimal(13,12)`: 0.333333333333 | the same |
| `d * 2`, `d + 1`, `d * 1.1` | `decimal(12,2)`, `decimal(11,2)`, `decimal(13,3)` | the same | the same |
| `d / b`, `b` an `INT` column | `decimal(21,13)` | the same | the same |

Multiplication and addition agree with Spark. Division does not.

## Correction to the INTDIV-1 ledger

Residue R-INTDIV-2 of [intdiv-1-ledger.md](../../../ledgers/completed/intdiv-1-ledger.md), dated 2026-09-28, says "values equal". Fable's `1.0 / 3` cell (0.333333 against 0.333333333333) shows the values differ when the quotient does not terminate. This card records that correction. The ledger is not edited.

## Seat

Not diagnosed. The orchestrator's lead is "the division function from #99 that owns `/0`". The registry row DEC-7 names "the U4b division UDF that owns `/0` (#99)". This is a reading only.

## Neighbour

Fable: "It sits beside AVG-DECIMAL-ROUND-1, since both are Spark's decimal division." AVG-DECIMAL-ROUND-1's step 0 carries these cells as a neighbour. If they share a seat, they fold in; if not, this card stays its own unit (ruling 1).

Up: [map.md](map.md).
