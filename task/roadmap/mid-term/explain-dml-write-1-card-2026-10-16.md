# Card EXPLAIN-DML-WRITE-1: `EXPLAIN ANALYZE` and `PREPARE`/`EXECUTE` run DML on every RePark door, and with a subquery predicate they change every row

**Date:** 2026-10-16, corrected 2026-10-17. **Filed by:** Claude Opus 5.5, under owner rulings 131,
136, 138 and 139.

**Status:** open. **Critical** for a `DELETE` or `UPDATE` whose `WHERE` holds a subquery, run under
`EXPLAIN ANALYZE` or `PREPARE` then `EXECUTE` (owner ruling 139). The unit takes 106's Opus slot the
day 106 (REPARK-SQL-UPDATE-SUBQUERY-WHERE-1) merges or parks. If 106 merges, its P2 report is filed
first.

**Retires:** when `EXPLAIN ANALYZE` of DML refuses on every door, a `PREPARE` whose body is a write
refuses on every door while `PREPARE` of a query still works, and the wrapper pins below hold on
every door.

## 2026-10-17: owner rulings 136, 138 and 139, and the measured table

The earlier text, kept as written at the end, said a plain `EXPLAIN` of DML writes on
`repark.sql()`. A measurement on real Iceberg tables showed it does not: that write happens on
in-memory tables only. It also showed `PREPARE`/`EXECUTE` writing on every RePark door, not only on
the ANSI door.

### The owner's rulings, verbatim

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

```
Q138 → (a). PREPARE whose body is a write refuses on every door;
PREPARE of a query keeps working. Spark refuses it, so RePark
refuses it. It is not a lane of its own: it lands inside the
EXPLAIN-DML-WRITE-1 unit from Q139, since both edit the same walks.
The refusal text names the statement and says PREPARE of a query
still works; it names no data-deleting action.
```

```
Q139 → (a), CRITICAL for the wrapper-plus-subquery form, with the
scope corrected after I looked at main:
  - repark-spark's two doors share one strict pregate: the
    statement path calls one function that today matches only a
    bare DELETE or UPDATE. 106's fix round makes that one function
    the walk-level refusal (this is the Q135 shape, placed on the
    function every repark-spark caller passes), so the Spark door
    closes with 106, not later. 106's verifier pins both
    repark-spark doors. If the Spark door turns out not to reach
    that function, the fix-round sketch says so, and the Spark
    door joins the ANSI door in the unit below.
  - The ANSI door is a different crate with its own twin guard,
    called on the bare DELETE and UPDATE paths only, and the
    door-to-door ban means it cannot share repark-spark's walk.
    That twin is the EXPLAIN-DML-WRITE-1 unit: the walk-level
    refusal on the ANSI door, plus Q138's PREPARE-of-DML refusal
    on every door.
  - The unit takes 106's Opus slot the day 106 merges or parks.
    If 106 merges, its P2 report is filed first, then the lane
    opens. If 106 parks, the unit branches from main and carries
    the repark-spark placement itself. No fourth lane, and nothing
    branched from 106's round-4 head; (c) is refused.
  - Step-0 sketch before step 1: it names the exact call sites per
    door on main. Then one Opus xhigh round, then the verifier,
    which pins each wrapper on each door in the IN-subquery form,
    on tables created through the Iceberg catalog with the
    provider guard, the same test shape as 106. P2 report on merge.
  - Docs PR today, docs-only, no verifier: the measured table on
    the card and the 1.5.6 known-issue line naming both wrappers
    and both doors.
```

### Where each part lands

| Part | Lands in |
|---|---|
| `repark.sql()`: the subquery refusal on every statement the walk returns, `PREPARE` and `EXPLAIN ANALYZE` included | 106, ruling 135 |
| repark-spark's two doors: the strict pregate's `refuse_dml_subquery_predicate_in_statement` becomes the walk-level refusal | 106's fix round, ruling 139. A wrapper path that does not reach that function joins this unit. |
| The ANSI door: the walk-level refusal on its twin guard | this unit |
| `PREPARE` whose body is a write refuses; `PREPARE` of a query works | this unit, every door, ruling 138 |
| `EXPLAIN ANALYZE` of DML refuses, as Spark refuses it | this unit, every door, ruling 136 |
| A plain `EXPLAIN` of DML on an in-memory table writes | not this card, ruling 136 |

The orchestrator's reading of main `56057f65`, for the step-0 sketch to confirm or correct:
- The repark-spark statement path parses in `execute_passthrough_inner`
  (`crates/repark-spark/src/spark_ast.rs`). It tries `try_execute_identity_dml` first, then runs the
  strict pregate, whose `refuse_dml_subquery_predicate_in_statement`
  (`crates/repark-spark/src/normalize.rs`) matches only a bare `Statement::Delete` or
  `Statement::Update`.
- That function skips the refusal for the allow-listed shapes `try_allowed_delete_in` and
  `try_allowed_update_in` accept. Those include `id IN (SELECT …)`, which the bare statement runs
  through RePark's own executor with the matching rows. Under a wrapper, the same shape goes to
  DataFusion's DML planner, which is where every row changes. So the allow-list exemption cannot
  carry over to a statement reached under a wrapper.
- A `PREPARE` arrives at that function as `Statement::Prepare` and passes its catch-all arm.
  DataFusion's own `EXPLAIN [ANALYZE]` takes the `DfStatement::Explain` arm of
  `execute_passthrough_inner` and does not reach that function on main.
- The ANSI door's twin is `guards::refuse_dml_subquery_predicate` (`crates/repark-sql/src/guards.rs`),
  called from `crates/repark-sql/src/router.rs` on its bare `DELETE` and `UPDATE` paths.

### Measured cells, main `56057f65`

The EXPLAIN-DML-WRITE-1 measurement lane (Muse xhigh, 2026-10-17) took the release module built
from a clean main, sha256 `cbd7c6d7…`. Each cell had a fresh memory catalog:
- **Seeds:** `ice.ns.t (id INT, v INT)` holds (1, 10), (2, 20), (3, 30), (4, 40), and
  `ice.ns.s (id INT, w INT)` holds (2, 20), (3, 99). So the subquery matches ids 2 and 3.
- **Iceberg tables:** created through the catalog (namespace, `CREATE TABLE` with columns, then
  `INSERT`), and checked as Iceberg before each cell by an `IcebergTableScan` in the plan.
- **In-memory column:** the `repark.sql()` cells repeated on `CREATE SCHEMA … WITH (location …)`
  plus CTAS tables, which are in-memory tables.
- **Doors:** the ANSI door ran from Rust through `repark_sql::AnsiDialect`, and the Spark SQL door
  through the Python facade's `ReparkSession.sql()`.
- **Reading:** every `EXPLAIN` cell was read to the end. Building the frame without reading it
  wrote nothing on any RePark door or table kind. For `PREPARE`, the cell shows `EXECUTE p` read to the
  end, and `PREPARE` alone wrote nothing on any door.

101 cells were measured and none was unreached. The orchestrator re-ran nine of them by hand, and
all nine matched.

| Statement | repark.sql(), Iceberg | repark.sql(), in-memory table | ANSI door | Spark SQL door | Spark 4.1.2 |
|---|---|---|---|---|---|
| `EXPLAIN DELETE … WHERE id = 2` | plan; no write, snapshots 1→1 | plan; 1 row deleted | plan; no write, snapshots 1→1 | plan; no write, snapshots 1→1 | plan; no write, snapshots 1→1 |
| `EXPLAIN UPDATE … WHERE id = 2` | plan; no write, snapshots 1→1 | plan; 1 row set | plan; no write, snapshots 1→1 | plan; no write, snapshots 1→1 | plan; no write, snapshots 1→1 |
| `EXPLAIN DELETE … WHERE id IN (SELECT id FROM s)` | plan; no write, snapshots 1→1 | plan; **all 4 rows deleted** | plan; no write, snapshots 1→1 | plan; no write, snapshots 1→1 | plan; no write, snapshots 1→1 |
| `EXPLAIN UPDATE … WHERE id IN (SELECT id FROM s)` | plan; no write, snapshots 1→1 | plan; **all 4 rows set** | plan; no write, snapshots 1→1 | plan; no write, snapshots 1→1 | plan; no write, snapshots 1→1 |
| `EXPLAIN ANALYZE DELETE … WHERE id = 2` | plan; 1 row deleted, snapshots 1→2 | plan; 1 row deleted | plan; 1 row deleted, snapshots 1→2 | plan; 1 row deleted, snapshots 1→2 | refused ParseException; no write, snapshots 1→1 |
| `EXPLAIN ANALYZE UPDATE … WHERE id = 2` | plan; 1 row set, snapshots 1→2 | plan; 1 row set | plan; 1 row set, snapshots 1→2 | plan; 1 row set, snapshots 1→2 | refused ParseException; no write, snapshots 1→1 |
| `EXPLAIN ANALYZE DELETE … WHERE id IN (SELECT id FROM s)` | plan; **all 4 rows deleted**, snapshots 1→2 | plan; **all 4 rows deleted** | plan; **all 4 rows deleted**, snapshots 1→2 | plan; **all 4 rows deleted**, snapshots 1→2 | refused ParseException; no write, snapshots 1→1 |
| `EXPLAIN ANALYZE UPDATE … WHERE id IN (SELECT id FROM s)` | plan; **all 4 rows set**, snapshots 1→2 | plan; **all 4 rows set** | plan; **all 4 rows set**, snapshots 1→2 | plan; **all 4 rows set**, snapshots 1→2 | refused ParseException; no write, snapshots 1→1 |
| `PREPARE p AS DELETE … WHERE id = 2; EXECUTE p` | PREPARE ok, EXECUTE ok; 1 row deleted, snapshots 1→2 | PREPARE ok, EXECUTE ok; 1 row deleted | PREPARE ok, EXECUTE ok; 1 row deleted, snapshots 1→2 | PREPARE ok, EXECUTE ok; 1 row deleted, snapshots 1→2 | PREPARE and EXECUTE refused (ParseException); no write, snapshots 1→1 |
| `PREPARE p AS UPDATE … WHERE id = 2; EXECUTE p` | PREPARE ok, EXECUTE ok; 1 row set, snapshots 1→2 | PREPARE ok, EXECUTE ok; 1 row set | PREPARE ok, EXECUTE ok; 1 row set, snapshots 1→2 | PREPARE ok, EXECUTE ok; 1 row set, snapshots 1→2 | PREPARE and EXECUTE refused (ParseException); no write, snapshots 1→1 |
| `PREPARE p AS DELETE … WHERE id IN (SELECT id FROM s); EXECUTE p` | PREPARE ok, EXECUTE ok; **all 4 rows deleted**, snapshots 1→2 | PREPARE ok, EXECUTE ok; **all 4 rows deleted** | PREPARE ok, EXECUTE ok; **all 4 rows deleted**, snapshots 1→2 | PREPARE ok, EXECUTE ok; **all 4 rows deleted**, snapshots 1→2 | PREPARE and EXECUTE refused (ParseException); no write, snapshots 1→1 |
| `PREPARE p AS UPDATE … WHERE id IN (SELECT id FROM s); EXECUTE p` | PREPARE ok, EXECUTE ok; **all 4 rows set**, snapshots 1→2 | PREPARE ok, EXECUTE ok; **all 4 rows set** | PREPARE ok, EXECUTE ok; **all 4 rows set**, snapshots 1→2 | PREPARE ok, EXECUTE ok; **all 4 rows set**, snapshots 1→2 | PREPARE and EXECUTE refused (ParseException); no write, snapshots 1→1 |
| `EXPLAIN SELECT * FROM t WHERE id = 2 (control)` | plan; no write, snapshots 1→1 | plan; no write | plan; no write, snapshots 1→1 | plan; no write, snapshots 1→1 | plan; no write, snapshots 1→1 |

For comparison, the orchestrator ran the bare statements by hand on the same module:
- On the Spark SQL door, the bare `DELETE … WHERE id IN (SELECT id FROM s)` deletes ids 2 and 3,
  and the bare `UPDATE` sets those two rows.
- On `repark.sql()`, the bare `DELETE` deletes all four rows; 106 refuses it.
- The ANSI door's bare forms were not measured here.

Spark 4.1.2 has neither `EXPLAIN ANALYZE` nor `PREPARE`. It refuses both at parse with
`PARSE_SYNTAX_ERROR`, and for `EXECUTE p` it asks for `EXECUTE IMMEDIATE`.

### Pins this unit owes

The verifier pins each wrapper on each door in the `IN`-subquery form. The tables are created
through the Iceberg catalog, and a provider guard asserts they are Iceberg, the same test shape as
106. Each pin asserts the refusal, the rows and the snapshot count:
- `EXPLAIN ANALYZE` of `DELETE` and `UPDATE`, with a plain and with a subquery predicate, refuses
  on every door and writes nothing.
- `PREPARE p AS DELETE …` and `PREPARE p AS UPDATE …` refuse on every door, with a plain and with a
  subquery predicate. The refusal names the statement, says `PREPARE` of a query still works, and
  names no data-deleting action. `PREPARE p AS SELECT …` then `EXECUTE p` still answers.
- A plain `EXPLAIN` of DML on an Iceberg table answers its plan and writes nothing, on every door.
- `EXPLAIN SELECT` answers its plan on every door (the control).

**2026-10-17, 106 round 4:** 106 closes `repark.sql()`: its G3-E8 refusal runs on every statement
the shared walk `repark_iceberg::write::statement_writes::executed_writes` returns, so a `DELETE` or
`UPDATE` whose `WHERE` holds a subquery refuses bare, under `PREPARE` (alone, parameterised, or
around `EXPLAIN ANALYZE`) and under `EXPLAIN ANALYZE`, while a plain `EXPLAIN` answers its plan; a
`PREPARE` with a plain predicate still runs. 106 also closes both repark-spark doors (the
`SparkDialect` session door and the router API): their strict pregate,
`refuse_dml_subquery_predicate_in_statement`, refuses each `DELETE` and `UPDATE` the same walk
returns, with the allow-list exemption kept for the top-level bare statement only, so `PREPARE` of
a subquery-predicate `DELETE` or `UPDATE`, alone or around `EXPLAIN ANALYZE`, refuses there too,
while the bare `IN` forms keep the matching rows. The sketch (106 ledger §13.2) sends one form to
this unit: DataFusion's own `EXPLAIN ANALYZE <DML>` on both repark-spark doors takes the
`DfStatement::Explain` arm and never reaches that function, so it still writes there. Ledger
sections 12 and 13.

## Earlier text, 2026-10-16, as written (headings one level down)

**Title then:** Card EXPLAIN-DML-WRITE-1: a plain `EXPLAIN` of DML writes on `repark.sql()`, and `EXPLAIN ANALYZE` of DML writes on every door

**Date:** 2026-10-16. **Filed by:** Claude Opus 5.5, under owner ruling 131.

**Status:** open. **Critical.** The next Iceberg write unit after 106
(REPARK-SQL-UPDATE-SUBQUERY-WHERE-1). By ruling 126 (a), U-5's acceptance run and P2 report land
before this card's lane opens.

**Retires:** when a plain `EXPLAIN` of DML never writes on any door, `EXPLAIN ANALYZE` of DML
refuses on every door, and the pins below hold on all three doors.

### The goal, in the owner's words

Owner ruling 131, verbatim: "New critical card, next Iceberg write unit after 106: a plain
EXPLAIN of DML never writes on any door, and EXPLAIN ANALYZE of DML refuses on every door. Pins
on all three doors, so routing repark.sql() later cannot bring the write back. Named in the 1.5.6
known issues. 106 does not hold for it: a ready refusal does not wait behind a broader fix."

### Measured cells

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

### Pins it owes, on all three doors

- A plain `EXPLAIN` of `DELETE`, `UPDATE`, `INSERT` and `MERGE` (each door's spellings), with a
  plain predicate and with a subquery predicate, read to the end: the rows, the snapshot and the
  warehouse files hold on `repark.sql()`, the ANSI door and the Spark SQL door.
- `EXPLAIN ANALYZE` of the same statements refuses on every door and writes nothing, `VERBOSE` and
  nested forms included.
- `EXPLAIN` of a `SELECT` still answers its plan on every door (the control).
- The pins on `repark.sql()` stay when it is routed through the ANSI door, so routing cannot bring
  the write back.

### Note

Named in the 1.5.6 known issues. 106 does not wait for this card; its `EXPLAIN` rule covers only
the subquery-predicate class it already refuses.
