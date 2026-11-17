# Card RANGE-DECIMAL-OFFSET-1: a `RANGE` window frame with a decimal offset refuses, where Spark answers

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Merge those and defaults." (2026-11-16, about 23:40 UTC), ruling B7 on DECIMAL-COMMON-TYPE-1 step 0 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-16.md)). **Source:** [domain table](../../../wo/decimal-common-type-1/domain-table.md), the Windows, PIVOT and grouping sets section and the "Other causes" table; [sketch §9 and §10 question 7](../../../wo/decimal-common-type-1/sketch.md). Parent: [DECIMAL-DECIMAL-UNION-WIDEN-1](../decimal-decimal-union-widen-1-card-2026-11-16.md).

**Status:** filed 2026-11-16, not dispatched. **Not label-blocking** (ruled B7).

**Severity: S3** (ruled B7). A refusal where Spark answers, on a `RANGE` frame whose offset is a decimal of another type.

## Finding

A `RANGE` frame whose offset is a DECIMAL of a type other than the key's refuses on main with `Invalid window frame: frame offsets for RANGE must be either a numeric value, a string value…`. It refuses the same-type offset too. A plain literal offset (`1.5`, `0.5`) agrees. Spark casts the offset to the key's type, not to a common type, so this is not the common-type family.

## Measured

| cell | Spark 4.1.2 | main |
|---|---|---|
| `ORDER BY a RANGE BETWEEN CAST('0.5' AS DECIMAL(10,2)) PRECEDING` | answers | refuses |
| `c` and `h` keys with a `DECIMAL(38,0)` offset (`w_range_*_dec`) | answers | refuses |
| side cell `range_same_type` (the same type as the key) | answers | refuses |

The table states no ANSI mode for these cells.

## Seat (not diagnosed)

Not diagnosed. The refusal is the window frame's offset check.

Up: [map.md](map.md).
