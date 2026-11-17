# Card NVL-DECIMAL-DOUBLE-1: `nvl` and `ifnull` over decimals answer a DOUBLE, where Spark keeps the decimal common type

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Merge those and defaults." (2026-11-16, about 23:40 UTC), ruling B7 on DECIMAL-COMMON-TYPE-1 step 0 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-16.md)). **Source:** [domain table](../../../wo/decimal-common-type-1/domain-table.md), the "Other causes" table and the position grid's `nvl` row; [sketch §9 and §10 question 7](../../../wo/decimal-common-type-1/sketch.md). Parent: [DECIMAL-DECIMAL-UNION-WIDEN-1](../decimal-decimal-union-widen-1-card-2026-11-16.md).

**Status:** filed 2026-11-16, not dispatched. **Not label-blocking** (ruled B7). Linked to [NVL-TYPE-COERCION-2](../nvl-type-coercion-2-card-2026-10-02.md), parked: its mode-aware `nvl` rewrite is the other half of the same typing, so `nvl` is fixed once, one way.

**Severity: S2** (ruled B7). A wrong type, and wrong values past 15 to 17 digits.

## Finding

`nvl` and `ifnull` over DECIMAL operands answer `double` on main. DataFusion's `nvl` signature lists no decimal type, so the decimal common type is lost. Spark answers the decimal common type.

## Measured

| cell | Spark 4.1.2 | main |
|---|---|---|
| `nvl` row of the position grid, pairs `ab`, `ac`, `gh`, `i64h`, `i32w30` | the decimal common type | wrong answer on each (`W`) |
| `nvl` row, control pair `c0c` | the decimal common type | wrong type, `double` (`T`) |
| side cells `nvl_same`, `nvl_c0c`, `ifnull_c0c` | the decimal common type | `double` (the table's "Other causes" row) |

The grid note says ANSI on is identical cell for cell. The table states no ANSI mode for the side cells.

## Seat (not diagnosed)

The cause the table names is DataFusion's `nvl` signature, which lists no decimal. Related: NVL-TYPE-COERCION-2, the parked mode-aware rewrite of the `nvl` family.

Up: [map.md](map.md).
