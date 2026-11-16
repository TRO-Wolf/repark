# Card DECIMAL-DECIMAL-UNION-WIDEN-1: a `UNION ALL` of mixed DECIMAL types refuses where Spark narrows to fit

**Date:** 2026-11-16. **Filed by:** the records clerk, from the 2026-11-15 records backlog (item 2; #1339 verifier
note N5, also fails on main before #1339). Measured on main `a06c1088`'s package against Spark 4.1.2, both ANSI
modes, the Spark door and the native door.

**Status:** PROPOSED, not dispatched. Pre-existing: main refuses it too.

**Severity: S1.** Beside the refusals, neighbouring two-branch cells answer with the wrong type and wrong values
(`i2_p2`, `i2_q3`, `i2_q4` below): a `UNION ALL` of decimals is a common shape, and a mistyped union silently rounds
away fractional digits (1.25 becomes 1).

## Finding

A `UNION ALL` whose branches are mixed DECIMAL types refuses at planning with
`datafusion engine error: Optimizer rule 'simplify_expressions' failed` wherever the common type would need more
than 38 digits of precision. The smallest failing shape is two branches: `DECIMAL(38,0)` with `DECIMAL(38,38)`.
Spark answers every shape below by narrowing the result to fit `decimal(38, *)`, rounding half up.

## Measured

All RePark rows are identical with ANSI on and off, on the Spark door and the native door, except where noted.
`A` is `SELECT CAST(1 AS DECIMAL(38,0))`, `B` is `SELECT CAST(0.5 AS DECIMAL(38,38))`,
`C` is `SELECT CAST(1.25 AS DECIMAL(10,2))`.

| cell | branches | RePark | Spark 4.1.2 (both modes) |
|---|---|---|---|
| 3-way | A + B + C | refuses (`simplify_expressions`) | `decimal(38,0)`, rows 1, 1, 1 |
| `i2_p1` | A + B | refuses | `decimal(38,0)`, rows 1, 1 |
| `i2_p3` | B + C | refuses | `decimal(38,30)`, rows 0.500…0 (30dp), 1.250…0 (30dp) |
| `i2_p2` | A + C | `decimal(38,2)`, rows 1.00, 1.25 | `decimal(38,0)`, rows 1, 1 |
| `i2_q3` | `DECIMAL(38,0)` + `DECIMAL(38,1)` | `decimal(38,1)`, rows 1.0, 1.5 | `decimal(38,0)`, rows 1, 2 |
| `i2_q4` | `DECIMAL(20,0)` + `DECIMAL(20,20)` | `decimal(38,20)` | `decimal(38,18)`, rows 1.0…0, 0.5…0 (18dp) |
| `i2_p4` | `DECIMAL(10,0)` + `DECIMAL(10,2)` | `decimal(12,2)` | `decimal(12,2)` — agrees |
| `i2_q2` | `DECIMAL(10,10)` + `DECIMAL(10,2)` | `decimal(18,10)` | `decimal(18,10)` — agrees |
| same-type | `(38,38)` + `(38,38)`, `(10,10)` + `(10,10)`, `(6,6)` + `(6,6)`, `(38,0)` + `(38,0)` | answers | answers — agrees |

So two defects share one seat: wide mixed pairs refuse outright, and narrower mixed pairs answer a wider type than
Spark's (Spark trims scale to hold precision 38; RePark keeps the full scale). Same-type pairs agree on the Spark
door. An overflow literal (`CAST(1.5 AS DECIMAL(38,38))`, ANSI off) refuses inside the union on RePark where Spark
answers the row as NULL; with ANSI on Spark raises `NUMERIC_VALUE_OUT_OF_RANGE`.

Side note, native door only: the same-type `(38,38)` union answers `0.4999…` (binary noise,
`dec:0.49999999999999998874404911728017014784`) for 0.5, where the Spark door answers `dec:0.5…0` exactly. A wrong
value on one door of an agreeing cell.

## Not covered elsewhere

[DECIMAL-DOUBLE-UNION-COERCION-1](decimal-double-union-coercion-1-card-2026-11-13.md) is the DECIMAL-beside-DOUBLE
half (unify to `double`); this card is DECIMAL-beside-DECIMAL. [NATIVE-UNION-LITERAL-INT-1](native-union-literal-int-1-card-2026-11-13.md)
is an INT arm against an untyped literal. Neither names decimal-decimal widening.

## Not yet known

The seat. From reading, the candidate is the set-operation decimal coercion beside the `widen_decimal_against_float`
path the DECIMAL-DOUBLE-UNION-COERCION-1 card names: step 0 must find where a union of two decimals computes a common
type past precision 38 instead of narrowing as Spark does, and why the narrower pairs keep full scale.

## Append (2026-11-16): ruled

The owner's word, 2026-11-16 about 14:40 UTC, verbatim: "defaults". The rulings are recorded in full at
[../../wo/cast-literal-unwrap-1/rulings-2026-11-16.md](../../wo/cast-literal-unwrap-1/rulings-2026-11-16.md).
What they change for this card:

- **Renamed DECIMAL-COMMON-TYPE-1.** This file keeps its name. The unit is one unit for Spark's whole decimal
  least-common-type family: unions, comparisons, `IN`, `CASE`, `coalesce` and the other positions where Spark picks
  one decimal type for several operands, not `UNION ALL` alone.
- **Label-blocking.** It blocks the scoped label ICE-V2-FLAT-AWS-LABEL-1. Severity stays S1.
- **Lossy comparison matches Spark.** RePark compares as Spark does, rounding to the common type: `DECIMAL(38,0)` 1
  equals `DECIMAL(10,2)` 1.25, because both become `decimal(38,0)`.
- **The order.** #1341 (DECIMAL-DOUBLE-UNION-COERCION-1 part 2), then CAST-LITERAL-UNWRAP-1, then this unit, then
  NAN-MINMAX-1 part 2, DOUBLE-DECIMAL-DATAFRAME-1 and SIGNED-ZERO-UPDATE-IN-LIST-1.
- **Depends on CAST-LITERAL-UNWRAP-1 for every cell with a constant.** Where a column is the operand that loses
  digits in the common type and the other side is a constant, DataFusion's simplifier moves the rounding cast onto
  the constant and compares exactly ([CAST-LITERAL-UNWRAP-1](cast-literal-unwrap-1-card-2026-11-16.md)). Those
  cells cannot pass until that unit lands; column-against-column cells do not depend on it.

Up: [map.md](map.md).
