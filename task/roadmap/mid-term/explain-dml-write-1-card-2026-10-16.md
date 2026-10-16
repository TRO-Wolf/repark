# Card EXPLAIN-DML-WRITE-1: a plain `EXPLAIN` of DML writes on `repark.sql()`, and `EXPLAIN ANALYZE` of DML writes on every door

**Date:** 2026-10-16. **Filed by:** Claude Opus 5.5, under owner ruling 131.

**Status:** open. **Critical.** The next Iceberg write unit after 106
(REPARK-SQL-UPDATE-SUBQUERY-WHERE-1). By ruling 126 (a), U-5's acceptance run and P2 report land
before this card's lane opens.

**Retires:** when a plain `EXPLAIN` of DML never writes on any door, `EXPLAIN ANALYZE` of DML
refuses on every door, and the pins below hold on all three doors.

## The goal, in the owner's words

Owner ruling 131, verbatim: "New critical card, next Iceberg write unit after 106: a plain
EXPLAIN of DML never writes on any door, and EXPLAIN ANALYZE of DML refuses on every door. Pins
on all three doors, so routing repark.sql() later cannot bring the write back. Named in the 1.5.6
known issues. 106 does not hold for it: a ready refusal does not wait behind a broader fix."

## Measured cells

Seeds: `ice.ns.t (id INT, v INT)` holds (1, 10), (2, 20), (3, 30), (4, 40); `ice.ns.s (id INT, w
INT)` holds (2, 20), (3, 99); a fresh memory catalog per cell. "Read" means the frame's result is
collected. Cells from the 106 ledger, sections 9b and 11, and its round-3 records, 2026-10-16.
The `repark.sql()` column comes from a Rust replica of the native door built from main and from the
106 modules, whose path for these statements is main's apart from the subquery-predicate refusal;
the `UPDATE … IN` row comes from the 106 module only. The ANSI door is a main build. The Spark SQL
door goes through the Python facade on the 106 modules, which do not touch that door.

| Statement | `repark.sql()`, main | ANSI door, main | Spark SQL door | Spark 4.1.2 |
|---|---|---|---|---|
| `EXPLAIN DELETE FROM ice.ns.t WHERE id = 2`, read | id 2 deleted | rows kept | rows kept | plan, rows kept |
| `EXPLAIN UPDATE ice.ns.t SET v = 5 WHERE id = 2`, read | id 2 written | rows kept | rows kept | plan, rows kept |
| `EXPLAIN DELETE … WHERE id IN (SELECT id FROM ice.ns.s)`, read | no row left | rows kept | rows kept | plan, rows kept |
| `EXPLAIN ANALYZE DELETE FROM ice.ns.t WHERE id = 2` | id 2 deleted | id 2 deleted | id 2 deleted | `ParseException` |
| `EXPLAIN ANALYZE UPDATE ice.ns.t SET v = 5 WHERE id = 2` | id 2 written | id 2 written | id 2 written | `ParseException` |
| `EXPLAIN ANALYZE DELETE … WHERE id IN (SELECT id FROM ice.ns.s)` | no row left | no row left | no row left | `ParseException` |
| `EXPLAIN ANALYZE UPDATE ice.ns.t SET v = 5 WHERE id IN (SELECT …)` | every row 5 | every row 5 | not measured | `ParseException` |

Since 106 round 3, `repark.sql()` refuses the subquery-predicate rows with the G3-E8 text and keeps
the rows; the other rows are unchanged there. Building the frame of a plain `EXPLAIN` without
reading it writes nothing on `repark.sql()`.

## Pins it owes, on all three doors

- A plain `EXPLAIN` of `DELETE`, `UPDATE`, `INSERT` and `MERGE` (each door's spellings), with a
  plain predicate and with a subquery predicate, read to the end: the rows, the snapshot and the
  warehouse files hold on `repark.sql()`, the ANSI door and the Spark SQL door.
- `EXPLAIN ANALYZE` of the same statements refuses on every door and writes nothing, `VERBOSE` and
  nested forms included.
- `EXPLAIN` of a `SELECT` still answers its plan on every door (the control).
- The pins on `repark.sql()` stay when it is routed through the ANSI door, so routing cannot bring
  the write back.

## Note

Named in the 1.5.6 known issues. 106 does not wait for this card; its `EXPLAIN` rule covers only
the subquery-predicate class it already refuses.
