# task/wo/explain-dml-write-1/ — the work order for EXPLAIN-DML-WRITE-1

EXPLAIN-DML-WRITE-1 (card 2026-10-16, rulings 136, 138, 139): `EXPLAIN ANALYZE` of DML and
`PREPARE` of a write refuse on every RePark door, as Spark 4.1.2 refuses both. Documents only;
no product code and no test.

- [order.md](order.md) — the grade B order (2026-10-20, awaits the owner's adopt of the 2026-10-20 plan, P3): why and out of scope, the rulings, the three doors and where each wrapper enters on main `085d77e8`, one shared refusal beside 106's walk in `statement_writes.rs`, the test modules under the subsystem directories, steps from the step-0 sketch to the records, gates, halts (a second seam; a statement main answers that the head refuses outside the card's table), hand-back. Opus 5.5 build, one scoped Opus 5.5 verifier.
- [sketch.md](sketch.md) — **step 0 (2026-10-21, on main `c91f69c5`, by reading):** the twelve wrapper statements per door from entry to `execute_logical_plan`; the one seam `statement_writes::refuse_wrapped_write(&DFStatement)` and its three one-line call sites (`dialect.rs`, ANSI `router.rs`, `spark_ast.rs`); ruling 138's `PREPARE` text and the proposed `EXPLAIN ANALYZE` text; the pins by name; the existing pins that flip under rulings 136 and 138; questions Q1 (`INSERT` and `MERGE` reach the seam, halt condition 6) to Q4.

- [evidence/](evidence/map.md) — **step 6 (2026-10-21):** the never-worse census, 106 cells on each of four doors (`repark.sql()`, the ANSI door, the Spark SQL session, the router API), main and head; every changed cell is a wrapped write that now refuses with the ruled text and holds the table.

Up: [../map.md](../map.md).
