# Card EXPLAIN-DML-WRITE-1: a plain `EXPLAIN` of DML writes on `repark.sql()`, and `EXPLAIN ANALYZE` of DML writes on every door

## 2026-10-17, owner ruling 136: the card corrected

Owner ruling 136, verbatim:

```
Q136 → correct the card. Plain EXPLAIN of DML on an Iceberg table does
not write; the write was on in-memory tables only, and the CRITICAL on
the Iceberg path is withdrawn. The card keeps two items: EXPLAIN
ANALYZE of DML executes on every door, and Spark refuses it, so RePark
refuses it too; and the PREPARE/EXECUTE path on the ANSI door. The
measurement lane is Muse xhigh, as item 3 of the overnight plan said.
No new model permission is needed. The table gains PREPARE/EXECUTE
rows on all three doors and labels any in-memory-table row as such.
You write the options from the table.
```

- **Plain `EXPLAIN` of DML on an Iceberg table does not write.** The write this card was filed
  for was measured on in-memory tables only: the seeds of 106's rounds 2 and 3 (`CREATE SCHEMA …
  WITH (location = …)` and `CREATE TABLE … AS SELECT` through `repark.sql()`) made DataFusion
  in-memory tables,
  whose delete and update run while the physical plan is built. The CRITICAL on the Iceberg path
  is withdrawn.
- **The card keeps two items.**
  1. `EXPLAIN ANALYZE` of DML executes the write on every door; Spark 4.1.2 refuses it with a
     `ParseException`, so RePark is to refuse it too.
  2. The `PREPARE`/`EXECUTE` path on the ANSI door: `PREPARE p AS DELETE … WHERE id IN (SELECT
     …)` and then `EXECUTE p` empties the table, where the bare statement answers Spark's rows.
- **The measurement lane is Muse xhigh** (item 3 of the overnight plan); no new model permission
  is needed.
- The text below this section stays as written on 2026-10-16.

### The table, 2026-10-17

Seeds as below; every cell is an Iceberg table (`IcebergTableScan`, the rows and the snapshot
count read back) unless its row says in-memory. "Read" means the result was collected; snapshot
counts go from 1. `repark.sql()` main is `14ad3c6d` (the 106 verifier's Iceberg cells,
2026-10-16) with the `PREPARE` rows re-measured on the merge of main into 106 before its round 4
(`5a51a758`, the same path); the 106 head is round 4's module. The ANSI door is the 106
verifier's Rust harness on Iceberg seeds (main = the 106 head; 106 does not touch that door). The
Spark SQL door is the Python facade on the 106 head's module, measured 2026-10-17 (106 does not
touch that door either). Spark 4.1.2 with Iceberg 1.11.0, `local[1]`, measured 2026-10-16 and
2026-10-17.

| Statement | `repark.sql()`, main | `repark.sql()`, 106 head | ANSI door | Spark SQL door | Spark 4.1.2 |
|---|---|---|---|---|---|
| `EXPLAIN DELETE … WHERE id = 2`, read | plan, rows kept, 1 → 1 | plan, rows kept, 1 → 1 | plan, rows kept | plan, rows kept, 1 → 1 | plan, rows kept |
| `EXPLAIN DELETE … WHERE id IN (SELECT id FROM ice.ns.s)`, read | plan, rows kept, 1 → 1 | plan, rows kept, 1 → 1 | plan, rows kept | plan, rows kept, 1 → 1 | plan, rows kept |
| `EXPLAIN UPDATE … SET v = 5 WHERE id IN (SELECT …)`, read | plan, rows kept, 1 → 1 | plan, rows kept, 1 → 1 | plan, rows kept | plan, rows kept, 1 → 1 | plan, rows kept |
| **in-memory table:** `EXPLAIN DELETE … WHERE a = 2`, read | row 2 deleted | row 2 deleted | not measured | not measured | — |
| **in-memory table:** `EXPLAIN DELETE … WHERE a IN (SELECT …)`, read | no row left | no row left (round 3 refused it) | not measured | not measured | — |
| **in-memory table:** `EXPLAIN UPDATE … SET a = 5 WHERE a = 2`, read | row 2 written | row 2 written | not measured | not measured | — |
| `EXPLAIN ANALYZE DELETE … WHERE id = 2` | id 2 deleted, 1 → 2 | id 2 deleted, 1 → 2 | id 2 deleted | id 2 deleted, 1 → 2 | `ParseException`, rows kept |
| `EXPLAIN ANALYZE UPDATE … SET v = 5 WHERE id = 2` | id 2 written, 1 → 2 | id 2 written, 1 → 2 | id 2 written | id 2 written, 1 → 2 | `ParseException`, rows kept |
| `EXPLAIN ANALYZE DELETE … WHERE id IN (SELECT …)` | no row left, 1 → 2 | G3-E8 refusal, rows kept | no row left | no row left, 1 → 2 | `ParseException`, rows kept |
| `EXPLAIN ANALYZE UPDATE … SET v = 5 WHERE id IN (SELECT …)` | every row 5, 1 → 2 | G3-E8 refusal, rows kept | every row 5 | every row 5, 1 → 2 | `ParseException`, rows kept |
| `PREPARE p AS DELETE …` with a plain predicate; `EXECUTE p` | id 2 deleted, 1 → 2 | id 2 deleted, 1 → 2 | ids 1 and 3 deleted (`IN (1, 3)`) | id 2 deleted, 1 → 2 | `ParseException`, rows kept |
| `PREPARE p AS DELETE … WHERE id IN (SELECT …)`; `EXECUTE p` | no row left, 1 → 2 | G3-E8 refusal at `PREPARE`; `EXECUTE` finds no `p`; rows kept | no row left | no row left, 1 → 2 (the bare `DELETE` keeps ids 1 and 4) | `ParseException`, rows kept |
| `PREPARE p AS UPDATE … SET v = 5 WHERE id IN (SELECT …)`; `EXECUTE p` | every row 5, 1 → 2 | G3-E8 refusal at `PREPARE`, rows kept | every row 5 | every row 5, 1 → 2 | `ParseException`, rows kept |
| `PREPARE p(INT) AS DELETE … WHERE id IN (SELECT id FROM ice.ns.s WHERE w > $1)`; `EXECUTE p(0)` | no row left, 1 → 2 | G3-E8 refusal at `PREPARE`, rows kept | no row left | no row left, 1 → 2 | `ParseException`, rows kept |
| `PREPARE p AS EXPLAIN ANALYZE DELETE … WHERE id IN (SELECT …)`; `EXECUTE p` | no row left, 1 → 2 | G3-E8 refusal at `PREPARE`, rows kept | no row left | no row left, 1 → 2 | `ParseException`, rows kept |
| `PREPARE p AS EXPLAIN DELETE … WHERE id IN (SELECT …)`; `EXECUTE p` | plan, rows kept, 1 → 1 | plan, rows kept, 1 → 1 | plan, rows kept | plan, rows kept, 1 → 1 | `ParseException`, rows kept |

The in-memory rows are `repark.sql()` over tables made by `CREATE TABLE … AS SELECT` outside an
Iceberg namespace: main from the 106 verifier's MemTable cells, the 106 head from round 4's probe
on its module, 2026-10-17; each table's scan is a `DataSourceExec`, not an `IcebergTableScan`. Round 3 refused the second one; round 4 answers a plain `EXPLAIN` again, as main does, by
the orchestrator's reading of ruling 135, and the owner may overrule that reading.

**Measured outside the two items:** the Spark SQL door carries the same `PREPARE`/`EXECUTE`
write as the ANSI door (the rows above, 2026-10-17). Item 2 names the ANSI door; whether it
covers the Spark SQL door too is the owner's call (option 2-D below).

### Options, written from the table

Item 1, `EXPLAIN ANALYZE` of DML refuses on every door:

- **1-A, one parse-level check on every door (lean).** The walk `repark.sql()` already uses,
  `repark_iceberg::write::statement_writes::executed_writes`, answers the writes a statement
  executes through `EXPLAIN ANALYZE` and `PREPARE`. Each door refuses before planning when a
  write sits under an `EXPLAIN ANALYZE` layer (DataFusion parses a leading `EXPLAIN` itself, so
  each door peels those layers as `repark.sql()` does). It refuses where Spark does, at parse
  time, and reaches `PREPARE p AS EXPLAIN ANALYZE …`. Pins: the four `EXPLAIN ANALYZE` rows and
  the `PREPARE … EXPLAIN ANALYZE` row, on all three doors.
- **1-B, a plan-level check in the shared belt.** `PreExecute::guard` refuses a
  `LogicalPlan::Analyze` over a `Dml` input. One check covers every door that runs the belt, but
  a prepared plan reaches execution through `EXECUTE`, after `PREPARE` passed the guard, so 1-B
  needs `PREPARE` checked too; and a door that does not run the belt needs its own call.
- **1-C, answer the plan without executing.** Departs from both DataFusion and Spark (Spark
  refuses); not recommended.

Item 2, the `PREPARE`/`EXECUTE` path on the ANSI door:

- **2-A, the ANSI router's G3-E8 check over the walk, with no identity exemption under
  `PREPARE` (lean).** The identity forms answer Spark's rows only through the router's identity
  path; `EXECUTE` runs DataFusion's DML planner instead, which writes every row (the table). So a
  subquery-predicate `DELETE` or `UPDATE` under `PREPARE` refuses with the G3-E8 text, and a
  plain-predicate one still prepares and executes. It loses no answer the table shows as right.
- **2-B, refuse `PREPARE` of any DML.** Simpler, and closer to Spark (which has no `PREPARE`),
  but it takes away the plain-predicate `PREPARE` rows, which RePark answers right today.
- **2-C, refuse `PREPARE` entirely.** Spark's answer, at the cost of every `PREPARE` RePark
  answers today (`SELECT` included); a never-worse census decides how many.
- **2-D, scope.** The Spark SQL door's `PREPARE` rows match the ANSI door's; extend item 2 to
  that door, or leave it to its own card.

### Pins it owes now

- On all three doors, Iceberg tables: `EXPLAIN ANALYZE` of `DELETE` and `UPDATE`, plain and
  subquery predicates, refuses and leaves rows and snapshots unchanged.
- On the ANSI door (and the Spark SQL door if 2-D extends it): `PREPARE` of a subquery-predicate
  `DELETE`, `UPDATE`, parameterised `DELETE` and `EXPLAIN ANALYZE DELETE` refuses, rows and
  snapshots unchanged; a plain-predicate `PREPARE`/`EXECUTE` still writes the matching rows.
- Controls: a plain `EXPLAIN` of DML answers its plan with the Iceberg table unchanged, on every
  door; any pin on an in-memory table says so in its name.

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
