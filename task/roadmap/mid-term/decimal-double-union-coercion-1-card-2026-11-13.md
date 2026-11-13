# Card DECIMAL-DOUBLE-UNION-COERCION-1: a DECIMAL literal unified with a DOUBLE types the column `decimal(30,15)`, so a `UNION` write stores 0.0 for 1e-20

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** orchestrator measurement,
2026-11-11, against Spark 4.1.2; Fable guidance, 2026-11-13, which flagged it as a label blocker missing from the
xhigh inquiry.

**Status:** filed 2026-11-13, not dispatched; ruled 2026-11-13 (see the append). First put to the owner on the 2026-11-12 morning board
([page](../../../docs/artifacts/morning-board-2026-11-12.html), item 2, "New S1") without a ruling. The board's
default was to open it as the next lane unit, after signed-zero part B or ahead of it. Severity **S1**, a wrong
write. **Label-blocking:** it is on scalar columns, so it is inside the scoped label ICE-V2-FLAT-AWS-LABEL-1 and the
full format-v2 label.

## Measured (2026-11-11; not re-measured since)

- **The typing.** RePark unifies a DECIMAL literal with a DOUBLE to `decimal(30,15)` in `VALUES` and in `UNION`;
  Spark types the column `double`.
- **Reads.** A `SELECT` over `VALUES (1, 0.0), (2, CAST(1e-20 AS DOUBLE))`, and over the `UNION ALL` form, reads 0
  where Spark reads 1e-20. `1e30` overflows on RePark where Spark answers. The sign of `-0.0` is lost.
- **Wrong write (S1).** `INSERT INTO t(d DOUBLE) SELECT 1, 0.1 UNION ALL SELECT 2, CAST(1e-20 AS DOUBLE)` stores
  0.0; Spark stores 1e-20. `INSERT … VALUES` alone writes correctly.

## Lead (likely seat, not a cause)

DataFusion's `decimal_coercion` (Float64 with Decimal128 to `Decimal128(30,15)`), which RePark's Spark-door
`widen_decimal_against_float` does not cover for set operations, `VALUES` or `coalesce`. Unmeasured as a cause.

## Relation

[COALESCE-NAN-DOUBLE-1](coalesce-nan-double-1-card-2026-10-03.md) (2026-10-03): the same root seen as an overflow
refusal, a DOUBLE NaN cast to `Decimal128(30, 15)`.

## Fix direction

None ruled. Fable's guidance (not a ruling) asks for a design sketch before any build.

## Close

Every `VALUES`, `UNION` and `coalesce` that mixes a DECIMAL literal with a DOUBLE types the column `double` as Spark
does, so the reads and the `UNION` write above store and answer Spark's values, pinned against the Spark oracle on
the read and write doors.

## Append (2026-11-13 afternoon): rulings, order and one more shape

**Sources:** the design sketch, merged as #1324 ([sketch](../../wo/decimal-double-union-coercion-1/sketch.md)); owner,
2026-11-13, about 14:52 UTC, verbatim "Defaults, and merge #1323 when green"; NAN-WRITE-CANONICAL-1 verifier 1,
2026-11-13 (#1325, finding C2).

- **Ruled.** Q1′ to Q8 are ruled to the sketch's defaults (its "Rulings" section): seat V plus seat E, the native door
  recorded, the `array` / `map` constructors folded in, a pre-fix stored view keeps its schema, both prerequisites
  first, and `MERGE … ON` follows Spark. The cards the rulings name:
  [DOUBLE-DECIMAL-STORE-ROUND-1](double-decimal-store-round-1-card-2026-11-13.md),
  [DECIMAL-FLOAT-ARITH-1](decimal-float-arith-1-card-2026-11-13.md),
  [DECIMAL-DOUBLE-NESTED-1](decimal-double-nested-1-card-2026-11-13.md) and
  [DECIMAL-PRUNING-CONJUNCT-1](decimal-pruning-conjunct-1-card-2026-11-13.md).
- **Order.** The build starts after SIGNED-ZERO-NORMALIZE-1 part B (#1326) and DOUBLE-DECIMAL-STORE-ROUND-1 merge.
- **One more shape (#1325 verifier 1, C2, pre-existing).** `UPDATE t SET d = 2.5 WHERE …` on a DOUBLE column that
  holds a NaN fails with "Cannot cast to Decimal128(30, 15). Overflowing on NaN"; Spark coerces the literal to
  DOUBLE. The same root as the `UNION` write, seen on an `UPDATE` assignment.
