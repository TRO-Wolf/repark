# task/wo/explain-dml-write-1/evidence/ — the never-worse census of EXPLAIN-DML-WRITE-1

Step 6 of [the order](../order.md), recorded 2026-10-21. Data only; the reading is in the unit
ledger's §6 ([../../../ledgers/staging/explain-dml-write-1-ledger.md](../../../ledgers/staging/explain-dml-write-1-ledger.md)).

- [census-cells.txt](census-cells.txt) — the 106 cells, one block each: the cell id, then its
  statements in order. Eleven predicates (subquery `IN`, `NOT IN`, `EXISTS`, correlated
  `EXISTS` and `NOT EXISTS`, correlated `IN`, `= (SELECT …)`, `IN … AND v > 25`, `= ANY`, and two
  plain ones), each as a bare `DELETE` and `UPDATE`, a plain `EXPLAIN DELETE`, `EXPLAIN ANALYZE`
  of both verbs and a `PREPARE`d `DELETE` and `UPDATE` then `EXECUTE`; plus `PREPARE` around
  `EXPLAIN [ANALYZE]`, parameterised `PREPARE`, `PREPARE` of a query, `INSERT` and `MERGE` bare,
  under `EXPLAIN [ANALYZE]` and `PREPARE`, a CTE-carried `DELETE` in the three forms,
  `VERBOSE`, a `WHERE`-less `DELETE` under both wrappers, `EXPLAIN ANALYZE PREPARE`, nested
  `EXPLAIN`, `SELECT` and `EXPLAIN [ANALYZE] SELECT` with a subquery, and a CTAS.
- `census-<door>-main.txt`, `census-<door>-head.txt` — one line per step: the cell, the step
  (`seed`: the `IcebergTableScan` guard on the seeded table; `0`, `1`: each statement's answer or
  refusal, error text trimmed at a word; `state`: the rows of `ice.ns.t` and its snapshot count
  after the cell). Doors: `repark-sql` (`repark.sql()`, Python, main's module `60286bb3…`, the
  head's `11e5bd48…`), `spark-session` (the Spark SQL session, the same modules), `ansi`
  (`AnsiDialect`, Rust) and `router` (`repark_spark::execute`, Rust); the Rust "main" runs are
  the head tree with the three call sites restored to `origin/main`.
- [mutant-trailing-semicolon.txt](mutant-trailing-semicolon.txt) — fix round 1 (2026-10-21,
  verifier S1-1): the red line of each door's suite with the refusal skipped on a trailing `;` at
  all three call sites.

Up: [../map.md](../map.md).
