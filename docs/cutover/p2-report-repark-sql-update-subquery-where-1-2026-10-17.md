# P2 report for #1090, REPARK-SQL-UPDATE-SUBQUERY-WHERE-1 (106), 2026-10-17

## Verifier verdicts, by time

- 2026-10-16 22:13 EDT — final verifier (head `996f4d5c`): verdict FAIL. V106-F1 (critical):
  `PREPARE`/`EXECUTE` carried a subquery-predicate `DELETE` or `UPDATE` past the `repark.sql()`
  refusal and wrote every row. V106-F2 to F7: an in-memory `EXPLAIN` write, a surviving
  `GenericDialect` mutant, pins on in-memory tables, and records. Owner rulings 135 and 136 gave
  one fix round with the shape changed; rulings 138 and 139 widened it to both repark-spark doors.
- 2026-10-17 18:57 EDT — final verifier, Opus Max (head `054c3fbb`): verdict PASS. All nine
  stages hold. Two records findings: VF-1 (low) and VF-2 (info), under Follow-ups below.
- 2026-10-17 19:24 EDT — merged as `0b9195ca` (squash), under owner ruling 132: verifier
  PASS and CI green.

## Acceptance run

Run id [39078930301](https://github.com/TRO-Wolf/repark/actions/runs/39078930301). Head sha
`0b9195ca7e3e21e35cab46461d73a67ff677a5cd` (the #1090 squash on main). Start
`2026-10-17T23:24:56Z` (2026-10-17 19:24:56 EDT). End `2026-10-17T23:49:25Z` (2026-10-17
19:49:25 EDT). Conclusion `success`.

Jobs:

- `tier-2 live AWS acceptance (Glue + S3 Tables, scratch-only)` — `success`
- `ICE-READ-PERF bench (S3 Tables + Glue, dispatch-only, scratch-only)` — `skipped`

Cell count: 12. The acceptance module has 11 and the dbt gold module has 1. All 12 PASSED
(0 FAILED, 0 SKIPPED). The skipped bench job is a job conclusion and has no PASSED, FAILED, or
SKIPPED line.

### Acceptance module

Summary: `11 passed in 382.96s (0:06:22)`.

- `python/repark/tests/test_aws_acceptance.py::test_process_silver_acceptance_against_glue` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_process_silver_acceptance_against_s3tables` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_mor_merge_compact_expire_against_glue` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_mor_merge_compact_expire_against_s3tables` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_v3_dv_dml_maintenance_against_glue` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_v3_dv_dml_maintenance_against_s3tables` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_sql_harden_cutover_against_glue` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_sql_harden_cutover_against_s3tables` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_create_or_replace_twice_against_glue` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_create_or_replace_twice_against_s3tables` — PASSED
- `python/repark/tests/test_aws_acceptance.py::test_u12_s3_path_write_against_scratch_prefix` — PASSED

### dbt gold module

Summary: `1 passed, 97 warnings in 59.94s`.

- `python/dbt-repark/tests/test_aws_acceptance_gold.py::test_gold_stage_on_glue` — PASSED

## What changed in the write path

- **One walk for every door.** `executed_writes` moved from `repark-spark`'s
  `router/nested_ns.rs` to `repark_iceberg::write::statement_writes`, and now also returns
  `DELETE`. The router's nested-write gate calls it unchanged: the verifier diffed the walk
  against main's and found only `pub`, `#[must_use]` and the `DELETE` arm.
- **`repark.sql()`** (`crates/repark-core/src/dialect.rs`): the G3-E8 refusal runs, with no
  exemption, on every statement the walk returns. A `DELETE` or `UPDATE` whose `WHERE` holds a
  subquery is refused bare, under `PREPARE` (alone, parameterised, or around `EXPLAIN ANALYZE`) and
  under `EXPLAIN ANALYZE`, and nothing is written. A plain `EXPLAIN` answers its plan, as on main,
  and writes nothing on an Iceberg table (ruling 136).
- **Both repark-spark doors** (the `SparkDialect` session door and the router API, ruling 139):
  their strict pregate, `refuse_dml_subquery_predicate_in_statement`, keeps its allow-list
  exemption for the top-level bare statement only, and refuses each `DELETE` and `UPDATE` the walk
  returns. `PREPARE` of a subquery-predicate `DELETE` or `UPDATE`, alone or around `EXPLAIN
  ANALYZE`, refuses there. The bare `IN` forms keep the matching rows.
- **Ruling 146:** `WITH c AS (…) DELETE/UPDATE … WHERE id IN (SELECT id FROM c)` now gets the
  G3-E8 refusal on the repark-spark doors, where it got DataFusion's NotImplemented. Both refuse
  and write nothing. The owner's ruling, verbatim:

  ```
  Q146 → (a). Accept. Before and after, RePark refuses the two
  CTE-prefixed forms and writes nothing; the only change is which
  refusal they get, and the G3-E8 one is the one the pin's own
  failure text asked for. The pin now asserts the G3-E8 text and
  the unchanged table row count. (b) is refused: keeping the old
  text would pull DML inside a query back out of the walk, which is
  the loophole 106 exists to close. The subquery-free CTE form keeps
  its old error, as you say.
  ```
- **Never worse (the verifier's census, on Iceberg seeds):**
  - On `repark.sql()`, 25 of 165 cells lose their answer, all subquery-predicate `UPDATE` or
    `DELETE` forms (bare, under `PREPARE`, or under `EXPLAIN ANALYZE`).
  - On the repark-spark doors, 72 census cells lose theirs, all `PREPARE` forms of the same class.
  - Spark 4.1.2 refuses the wrapped forms with a `ParseException`. The bare forms refused on
    `repark.sql()` are ruling 106's stopgap, and the Spark SQL session answers them with the
    matching rows. No other answer is lost.
  - `cargo test` on the four crates: no test green on main is red on head.

## Follow-ups

- **EXPLAIN-DML-WRITE-1** (critical for the subquery form, ruling 139): the ANSI door's
  walk-level refusal, ruling 138's `PREPARE`-of-a-write refusal on every door, and DataFusion's
  own `EXPLAIN ANALYZE` on the repark-spark doors, which never reaches the pregate. Under
  ruling 139 the unit takes 106's slot after this report. The owner has halted new lanes, so it
  waits for the owner.
- **VF-1 (low):** `crates/repark-sql/tests/ansi_subquery_where_identity.rs` seeds its tables with
  the ANSI door's CTAS and calls no provider guard, while the registry row says every pin asserts
  an `IcebergTableScan`. The seed measures Iceberg today. The guard and the narrower sentence
  belong to EXPLAIN-DML-WRITE-1's ANSI-door pins, which reuse this seed.
- **VF-2 (info):** the 1.5.6 known-issue line says `PREPARE` refused "instead of changing every
  row at EXECUTE". On main, 20 of the 26 prepared forms changed every row; others deleted the
  wrong rows or refused. The ledger's §13.6 wording is the measured one, and the release line
  should take it.
- **Card EXPLAIN-DML-WRITE-1's 1.5.6 line** still says `PREPARE` of the subquery form changes
  every row on the Spark SQL session. Since this merge that holds only for DataFusion's own
  `EXPLAIN ANALYZE` there.
