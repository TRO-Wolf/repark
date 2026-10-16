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

## Round 2 (2026-10-16): the stopgap has landed; routing stays this card

- **2026-10-16, DELETE is in** (round 2, ruling Q4 of the
  [sketch](../../wo/repark-sql-update-subquery-where-1/sketch.md), provisional, the orchestrator on
  the sketch's lean). Through `repark.sql()`, 6 of the 8 measured `DELETE` forms with a subquery in
  `WHERE` emptied the table and reported success; a 7th deleted a row that does not match.
- **2026-10-16, the stopgap has landed** (ruling 106 (a)'s "refusal on that door only as a
  stopgap", sketch rulings Q1, Q5 and Q6). `repark.sql()` refuses every `UPDATE` and `DELETE`
  whose `WHERE` holds a subquery with the G3-E8 text verbatim and writes nothing, the five forms
  the other doors answer through their identity path included. The refusal lives once, in
  `repark_iceberg::write::predicate_dml::subquery_where`: the ANSI router calls it with the
  identity exemption, `DataFusionDialect` (the `repark.sql()` door) without, so there is no third
  guard copy. Ledger:
  [repark-sql-update-subquery-where-1](../../ledgers/staging/repark-sql-update-subquery-where-1-ledger.md).
- **Routing stays this card.** Routing `repark.sql()` through the ANSI door needs the owner's
  rulings on its mechanism and on the never-worse items N1 to N7 of the sketch (Q2 and Q3, with
  the owner). When it lands, `repark.sql()` answers the five identity forms with Spark's rows.
- **2026-10-16, observed, open:** the refused class is still reachable through `EXPLAIN` on
  `repark.sql()`. A plain `EXPLAIN` of a `DELETE` or `UPDATE` writes when its result is read,
  with any predicate (main the same), and `EXPLAIN ANALYZE` of a subquery-predicate statement
  writes every row on `repark.sql()` and on the ANSI door; the ledger's section 9b has the cells.
  The round-2 hand-back asks the owner where this goes.
- **Later tidy unit (ruling Q7, 2026-10-16):** fold the Spark SQL door's G3-E8 copy
  (`crates/repark-spark/src/normalize.rs`) into the shared home, with its target renderer as a
  parameter.
