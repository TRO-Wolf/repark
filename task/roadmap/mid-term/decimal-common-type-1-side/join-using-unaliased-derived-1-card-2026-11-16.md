# Card JOIN-USING-UNALIASED-DERIVED-1: `JOIN … USING` over unaliased derived tables refuses, where Spark answers

**Date:** 2026-11-16. **Filed by:** the records clerk (`claude-haiku-5-5`), under the owner's ruling "Merge those and defaults." (2026-11-16, about 23:40 UTC), ruling B7 on DECIMAL-COMMON-TYPE-1 step 0 ([rulings](../../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-16.md)). **Source:** [domain table](../../../wo/decimal-common-type-1/domain-table.md), the "Other causes" table and revision M2; [sketch §9, §10 question 7 and §11](../../../wo/decimal-common-type-1/sketch.md). Parent: [DECIMAL-DECIMAL-UNION-WIDEN-1](../decimal-decimal-union-widen-1-card-2026-11-16.md).

**Status:** filed 2026-11-16, not dispatched. **Not label-blocking** (ruled B7).

**Severity: S3** (ruled B7). A refusal where Spark answers, on an unaliased pair of derived tables.

## Finding

`JOIN … USING (k)` over two **unaliased** derived tables refuses on main with `duplicate unqualified field name k`, for every join type and for the same types too. Spark answers. Aliased `USING` plans, and that is part of DECIMAL-COMMON-TYPE-1 (revision M2), not this card.

## Scope note (clerk reading, for the owner to confirm)

Ruling B7 names the `FULL OUTER JOIN … USING` form over unaliased, same-named derived tables. The measurement finds the refusal for inner, left and full joins alike, with distinct or duplicate names, and with the same type on both sides (seven cells, domain table revision M2). This card therefore names the unaliased shape, not one join type. The sketch's §9 says the same ("refuses for every join type").

## Measured

| cell | Spark 4.1.2 | main |
|---|---|---|
| `join_full_using` row of the position grid, pairs `ab`, `ac`, `gh`, `i64h`, `i32w30` | answers | refuses (`R`) |
| `join_full_using` row, control pair `c0c` | answers | refuses (`R`) |
| side cell `using_same_type`, and the revision cells `m2/using_*` without an alias (seven cells: inner, left, full, distinct, duplicate names) | answers | refuses, `duplicate unqualified field name k` |

The grid note says ANSI on is identical cell for cell.

## Seat (not diagnosed)

Not diagnosed. The refusal names a duplicate unqualified field on the unaliased side.

Up: [map.md](map.md).
