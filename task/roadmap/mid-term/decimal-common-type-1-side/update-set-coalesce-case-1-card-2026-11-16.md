# Card UPDATE-SET-COALESCE-CASE-1: `UPDATE … SET x = coalesce(CASE …, y)` refuses, where Spark answers

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Merge those and defaults." (2026-11-16, about 23:40 UTC), ruling B7 on DECIMAL-COMMON-TYPE-1 step 0 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-16.md)). **Source:** [domain table](../../../wo/decimal-common-type-1/domain-table.md), the "Other causes" table (row `UPDATE … SET x = coalesce(CASE …, y)`); [sketch §9 and §10 question 7](../../../wo/decimal-common-type-1/sketch.md). Parent: [DECIMAL-DECIMAL-UNION-WIDEN-1](../decimal-decimal-union-widen-1-card-2026-11-16.md).

**Status:** filed 2026-11-16, not dispatched. **Not label-blocking** (ruled B7).

**Severity: S3** (ruled B7). A refusal where Spark answers, on an `UPDATE` whose assignment is a `coalesce` over a `CASE`.

## Finding

An `UPDATE … SET x = coalesce(CASE …, y)` refuses on main with `__common_expr_1`, for the same type too. Spark answers.

## Measured

| cell | Spark 4.1.2 | main |
|---|---|---|
| `update_set_coalesce` (the position cell) and its side cell, the same-type control included | answers | refuses (`__common_expr_1`) |

The table states no ANSI mode for these cells.

## Seat (not diagnosed)

Not diagnosed. The refusal names an internal common-expression alias.

Up: [map.md](map.md).
