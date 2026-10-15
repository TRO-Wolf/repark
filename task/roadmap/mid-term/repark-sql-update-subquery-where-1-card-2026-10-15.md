# Card REPARK-SQL-UPDATE-SUBQUERY-WHERE-1: `repark.sql()` UPDATE with a subquery in WHERE updates every row

**Date:** 2026-10-15. **Filed by:** Muse Spark, clerk, under owner ruling 106 (a).

**Status:** open. **Critical.** Own unit next on the U-6 lane after #1062 merges.

**Retires:** when `repark.sql()` is routed through the ANSI door's dialect so it inherits
that door's refusal, with no third guard copy; if routing is more than one unit carries, a
refusal on that door is the stopgap and routing stays this card.

**Registry row:** [REPARK-SQL-UPDATE-SUBQUERY-WHERE-1](../../../docs/spark-sql-iceberg-parity.md)
(dated row 2026-10-15, owner ruling 106 (a)).

## Risk

Through `repark.sql()`, an `UPDATE` whose `WHERE` holds a subquery updates every row and reports success.

## Sequence

Through the public Python `repark.sql()` only (the `DataFusionDialect` door),
`UPDATE t SET v = 5 WHERE id IN (SELECT id FROM s)` and `WHERE id = (SELECT …)` update every
row of the table and report success; Spark updates only the matching rows. Main and PR #1062's
head are the same. The Spark SQL door and the Rust ANSI door refuse these statements.

## Note

Measured by U-6's re-verifier 3, 2026-10-15. A silent wrong value, the same class as F-04.
The 1.5.6 note names it until routing lands.
