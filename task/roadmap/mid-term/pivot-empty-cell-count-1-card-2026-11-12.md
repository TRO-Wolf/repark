# Card PIVOT-EMPTY-CELL-COUNT-1: a pivoted count answers 0 where Spark answers NULL for an absent pair

**Date:** 2026-11-12. **Filed by:** the records clerk (`claude-opus-5-5`), under owner ruling Q8 of 2026-11-12 ("Go with
the defaults"). **Source:** SIGNED-ZERO-NORMALIZE-1 part B evidence (`task/wo/signed-zero-normalize-1/evidence/part-b/`,
filed with that unit): step 0 on main `ae247767`, re-measured by item 1 on main `276dc503`; Fable's review of
2026-11-12, §3.

**Status:** filed 2026-11-12, not dispatched. Severity **S2** (drafter and Fable agree): a silent wrong value on the
typed door. Not signed zero, and does not block the scoped label; Fable asks for a card and a known-divergences line.

**Measured on main `276dc503` against Spark 4.1.2** (cell GB-11, typed door; the SQL form of GB-11 is an integer
control and is EQUAL):

- The cell is `t.groupBy('g').pivot(<col>).agg(F.count(F.lit(1)))` over a seed in which some `(g, value)` pairs have
  no row. The `pivot(...).count()` shortcut was not measured separately.
- On the decimal twin (`pivot('m')`), Spark answers `g = 0: null → NULL, 0.00 → 2, 1.50 → 1, 9.99 → NULL` and
  `g = 1: null → 1, 0.00 → 2, 1.50 → NULL, 9.99 → 1`. RePark answers `0` in each of the three NULL places. The column
  order also differs; that is [PIVOT-COLUMN-ORDER-1](pivot-column-order-1-card-2026-11-12.md).
- The DOUBLE and FLOAT rows show the same zeros, beside the split zero columns that part B part 1 fixes. Their decimal
  twin is not EQUAL, so part B's table marks the float rows confounded until this card is fixed or the cell is re-cut
  to `sum`.
- Item 1 reproduced step 0's label (WRONG-READ) on all three types.

**Fix direction:** the facade lowers a pivot to one conditional aggregate per value
(`python/repark/src/repark/spark/dataframe/grouped_pivot.py`, `joins_columns.py`), and a conditional `count` of no rows
is 0. Spark aggregates per `(group, value)` first and then pivots, so a pair with no row is NULL. Either lower the
count arms so that a group with no matching row yields NULL, or lower the pivot as Spark does. The other aggregates
already yield NULL over no rows (by reading, not measured). Pin the decimal twin of GB-11 on the typed door.

**Close:** fixed with that pin, or declined by a recorded ruling.
