# WRITE-BODY-CARRIERS-1 D4: wrapper routes store the wall as UTC (measured, no product change)

Measured 2026-10-14 on the main release module (repark 1.5.5, memory catalog),
session zone `America/New_York`, into a top-level `timestamptz_ns` column of a
format-version-3 table from the naive nanosecond wall
`CAST('2026-01-02 03:04:05.123456789' AS timestamp_ns)`.
Recorder: `/tmp/wbc1_d4.py` (scratch, uncommitted). Ticks are the stored
`timestamp[ns, tz=UTC]` values cast to int64, one row per route.

| route | statement | stored tick |
|---|---|---|
| plain INSERT | `INSERT INTO d_plain VALUES (1, <wall>)` | 1767341045123456789 |
| EXPLAIN ANALYZE INSERT | `EXPLAIN ANALYZE INSERT INTO d_ea VALUES (1, <wall>)` | 1767323045123456789 |
| EXPLAIN ANALYZE INSERT OVERWRITE | `EXPLAIN ANALYZE INSERT OVERWRITE d_eo VALUES (1, <wall>)` | 1767323045123456789 |
| PREPARE + EXECUTE INSERT | `PREPARE wbc_d4_pi AS INSERT INTO d_pi VALUES (1, <wall>)` then `EXECUTE wbc_d4_pi` | 1767323045123456789 |
| PREPARE + EXECUTE INSERT OVERWRITE | `PREPARE wbc_d4_po AS INSERT OVERWRITE d_po VALUES (1, <wall>)` then `EXECUTE wbc_d4_po` | 1767323045123456789 |

The wall read as UTC is 1767323045123456789. Plain INSERT stores
1767341045123456789, exactly 18_000_000_000_000 nanoseconds (5 hours) later:
INSERT localises the naive wall in the session zone (New York is UTC-5 in
January). All four wrapped routes store the wall as if it were UTC, the same
instant as each other and 5 hours before INSERT's. This matches the card's
2026-10-10 section; this round changes none of these routes.

The bare INSERT's write reaches `execute_insert_routed` at
`crates/repark-spark/src/router.rs:411` through the `Statement::Insert` arm at
`crates/repark-spark/src/router.rs:377`. `EXPLAIN ANALYZE`, `PREPARE` and
`EXECUTE` match no router arm and fall to the passthrough at
`crates/repark-spark/src/router.rs:407`, so none reaches `execute_insert_routed`.
Inside `execute_passthrough_inner` at `crates/repark-spark/src/spark_ast.rs:144`,
a bare statement takes the `DfStatement::Statement` branch at
`crates/repark-spark/src/spark_ast.rs:163` with the full pre-execution rewrites,
while `EXPLAIN ANALYZE` takes the `DfStatement::Explain` branch at
`crates/repark-spark/src/spark_ast.rs:191`, which only lowers timestamp casts.
Downstream, `after_analysis` at `crates/repark-spark/src/insert_timestamp_ns.rs:68`
(and `before_analysis` at `:16`) matches only a bare `LogicalPlan::Dml`; the
`Analyze`-wrapped DML plan returns unchanged, so the localisation conform never
fires. `PREPARE` is planned by DataFusion itself
(`datafusion-sql-54.1.0/src/statement.rs:810`), so the stored inner INSERT plan
is built without the Spark-door rewrites, and `EXECUTE` runs that stored plan.

wrapper routes: part at crates/repark-spark/src/router.rs:407
