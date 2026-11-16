# Card FLOAT-DIVISION-TYPE-1: with ANSI off, `1/<float>` is typed `float`, where Spark types it `double`

**Date:** 2026-11-16. **Filed by:** the records clerk, from the 2026-11-15 records backlog (item 6; #1337's
verifiers). Measured on main `a06c1088`'s package against Spark 4.1.2, both ANSI modes, the Spark door and the
native door.

**Status:** PROPOSED, not dispatched. Pre-existing: main types it the same way.

**Severity: S3.** The mistyped division also changes the value (`0.3333333432674408` for `1/3` against Spark's
`0.3333333333333333`), but the value is the correctly rounded FLOAT quotient: it follows the wrong type rather
than corrupting a right one. Precedent:
[DECIMAL-FLOAT-ARITH-1](decimal-float-arith-1-card-2026-11-13.md) rates the same kind of type-driven value
difference S3.

## Finding

Any division that touches a FLOAT is typed `float` on both RePark doors, in both ANSI modes, where Spark types it
`double`. The `1/3` quotient then prints the float rounding instead of the double rounding.

## Measured

`f6(x float)` holds one row, 1.0. Identical on both doors, both modes:

| cell | RePark | Spark 4.1.2 (both modes) |
|---|---|---|
| `SELECT 1/CAST(3 AS FLOAT)` | `float`, 0.3333333432674408 | `double`, 0.3333333333333333 |
| `SELECT CAST(1 AS FLOAT)/2` | `float`, 0.5 | `double`, 0.5 |
| `SELECT x/3 FROM f6` | `float`, 0.3333333432674408 | `double`, 0.3333333333333333 |

The second row shows the values can agree while the type still differs; the first and third rows show a quotient
whose float and double roundings differ, so the mistype moves the value too.

## Not covered elsewhere

[DECIMAL-FLOAT-ARITH-1](decimal-float-arith-1-card-2026-11-13.md) is `<decimal> <op> <float>` (addition measured);
this card is division with no decimal involved (`INT / FLOAT`, `FLOAT / INT`, column over literal). From reading,
the candidate seat is the same numerical coercion that card names (DataFusion's `mathematics_numerical_coercion`,
`(_, Float32) => Float32`); step 0 must confirm the division path reaches it.

Up: [map.md](map.md).
