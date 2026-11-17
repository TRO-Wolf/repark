# Card ARRAY-UNION-TYPES-1: `array_union` accepts two element types, where Spark refuses

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Merge those and defaults." (2026-11-16, about 23:40 UTC), ruling B7 on DECIMAL-COMMON-TYPE-1 step 0 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-16.md)). **Source:** [domain table](../../../wo/decimal-common-type-1/domain-table.md), the position grid's `array_union` row and the "Other causes" table; [sketch §9 and §10 question 7](../../../wo/decimal-common-type-1/sketch.md). Parent: [DECIMAL-DECIMAL-UNION-WIDEN-1](../decimal-decimal-union-widen-1-card-2026-11-16.md).

**Status:** filed 2026-11-16, not dispatched. **Not label-blocking** (ruled B7).

**Severity: S3** (ruled B7). A lenient answer where Spark refuses, on an unusual shape.

## Finding

`array_union` over arrays whose element types differ answers on main. Spark refuses with `BINARY_ARRAY_DIFF_TYPES`.

## Measured

| cell | Spark 4.1.2 | main |
|---|---|---|
| `array_union` row of the position grid, pair `ab` | refuses | refuses too (`B`, both raise) |
| `array_union` row, pairs `ac`, `gh`, `i64h`, `i32w30` and the control `c0c` | refuses | answers (`M`, missed refusal) |

The grid note says ANSI on is identical cell for cell.

## Seat (not diagnosed)

Not diagnosed. The element-type check on `array_union` is the seat to find.

Up: [map.md](map.md).
