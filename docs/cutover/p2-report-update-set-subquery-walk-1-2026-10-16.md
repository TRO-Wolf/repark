# P2 report for #1083, UPDATE-SET-SUBQUERY-WALK-1, 2026-10-16

## Verifier verdicts, by time

- 2026-10-16 08:28 EDT — scoped verifier for UPDATE-SET-SUBQUERY-WALK-1 (PR #1083, head
  `4d5af84c`): verdict PASS, with two info findings (V-001, V-002).
- 2026-10-16 09:16 EDT — the same verifier on owner ruling 117's three items: verdict PASS,
  with one new low finding (V-003).
- 2026-10-16 09:40 EDT — merged as `41cdd957` (squash), under ruling 117 (a): verifier PASS
  and CI green.

## Acceptance run

Run id [38909759805](https://github.com/TRO-Wolf/repark/actions/runs/38909759805). Head sha
`41cdd9576bb9892b4a0979e44dbe61979870b0e5` (the #1083 squash on main). Start
`2026-10-16T13:41:10Z` (2026-10-16 09:41:10 EDT). End `2026-10-16T14:05:34Z` (2026-10-16
10:05:34 EDT). Conclusion `success`.

Jobs:

- `tier-2 live AWS acceptance (Glue + S3 Tables, scratch-only)` — `success`
- `ICE-READ-PERF bench (S3 Tables + Glue, dispatch-only, scratch-only)` — `skipped`

Cell count: 12. The acceptance module has 11 and the dbt gold module has 1. All 12 PASSED
(0 FAILED, 0 SKIPPED). The skipped bench job is a job conclusion and has no PASSED, FAILED, or
SKIPPED line.

### Acceptance module

Summary: `11 passed in 364.79s (0:06:04)`.

- `python/repark/tests/parity/test_aws_acceptance.py::test_process_silver_acceptance_against_glue` — PASSED
- `python/repark/tests/parity/test_aws_acceptance.py::test_process_silver_acceptance_against_s3tables` — PASSED
- `python/repark/tests/parity/test_aws_acceptance.py::test_mor_merge_compact_expire_against_glue` — PASSED
- `python/repark/tests/parity/test_aws_acceptance.py::test_mor_merge_compact_expire_against_s3tables` — PASSED
- `python/repark/tests/parity/test_aws_acceptance.py::test_v3_dv_dml_maintenance_against_glue` — PASSED
- `python/repark/tests/parity/test_aws_acceptance.py::test_v3_dv_dml_maintenance_against_s3tables` — PASSED
- `python/repark/tests/parity/test_aws_acceptance.py::test_sql_harden_cutover_against_glue` — PASSED
- `python/repark/tests/parity/test_aws_acceptance.py::test_sql_harden_cutover_against_s3tables` — PASSED
- `python/repark/tests/parity/test_aws_acceptance.py::test_create_or_replace_twice_against_glue` — PASSED
- `python/repark/tests/parity/test_aws_acceptance.py::test_create_or_replace_twice_against_s3tables` — PASSED
- `python/repark/tests/parity/test_aws_acceptance.py::test_u12_s3_path_write_against_scratch_prefix` — PASSED

### dbt gold module

Summary: `1 passed, 97 warnings in 61.25s (0:01:01)`.

- `python/dbt-repark/tests/test_aws_acceptance_gold.py::test_gold_stage_on_glue` — PASSED

## What changed in the write path

#1083 makes the ruling-90 guard walk the whole `UPDATE … SET` expression. It applies rulings
99 (c) and 110 (a). `assigns_a_subquery` in `crates/repark-core/src/pre_execute.rs` is now
`expr.contains_scalar_subquery()`, replacing the earlier walk through alias, cast, `arrow_cast`
and `__repark_*`. Any scalar subquery anywhere in a `SET` value refuses outside the identity
path, with the one ruling-90 text. That holds on the Spark SQL session, the ANSI door and
`repark.sql()`.

Ruling 117's items, as the verifier measured them:
- **The identity path still answers.** 128 of 128 answered identity cells are byte-identical
  on main and head, and equal Spark 4.1.2's rows: 64 on each of the Spark and ANSI doors, plus
  the same session through the Python facade. `repark.sql()` has no identity path.
- **A refusal leaves no new snapshot and no metadata change** (ruling 95). Every refusal leaves
  identical before and after:
  - the metadata location;
  - the metadata JSON's sha256;
  - the `metadata/` and `data/` file lists;
  - the snapshot count and id.

  That holds for 308 refusals on the Spark session, 298 on the ANSI door, and 402 on
  `repark.sql()` against an on-disk Iceberg table.
- **One error text.** All 1,721 refusals carry the identical ruling-90 string.

Never worse:
- `cargo test --no-fail-fast -p repark-core -p repark-spark -p repark-sql`: head 4,895 passed
  and 0 failed; main 4,893 passed and 0 failed. The difference is one renamed test and two new
  pins.
- The `update or subquery` Python subset: head 1,253 passed; main 1,252 passed.
- No test green on main is red on head.
- Seven mutants, all killed.

The verifier lists two correct answers that main gave and that now refuse. In both, main was
right by coincidence:
- `CASE WHEN (SELECT v FROM src) > 0 THEN 1 ELSE 0 END` on the Spark door, in three `WHERE`
  forms. Main tested the target's own `v`, not the subquery's value, so another threshold makes
  it diverge from Spark.
- `abs((SELECT …))` on `repark.sql()`, under an `IN`-subquery `WHERE` that matches no row.

## Follow-ups

- **V-003 (low).** The `repark.sql()` pin in `python/repark/tests/writes/test_update_set_subquery_walk_1.py`
  and its `map.md` row say "Iceberg table", but that door's `CREATE SCHEMA` and CTAS build
  DataFusion in-memory tables. The pin still goes red on main, so it holds the guard. It does not
  hold the Iceberg path, which the verifier measured separately. To be filed for a clerk.
- **V-001 and V-002 (info).**
  - `IN (SELECT …)` and `EXISTS (SELECT …)` used as a `SET` value fail with an internal error,
    not ruling 90's text, and write nothing, as on main.
  - `CASE WHEN v IN ((SELECT …), 1)` does not parse on the Spark door or on `repark.sql()`.
- **Next on this lane:** [REPARK-SQL-UPDATE-SUBQUERY-WHERE-1](../../task/roadmap/mid-term/repark-sql-update-subquery-where-1-card-2026-10-15.md),
  ruling 106. It opens after this report.
