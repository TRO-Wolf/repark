# P2 report for #1082, ICE-TSNS-TYPE-PLANNER-1, 2026-10-16

## Verifier verdicts, by time

- 2026-10-16 05:54:23 EDT — Verifier 1 for ICE-TSNS-TYPE-PLANNER-1 (U-1, R-017; PR #1082, head
  `bedd799b`): verdict PASS. Six findings: F-1 medium, F-2 low, F-3 low, F-6 low, F-4 and F-5
  info.
- 2026-10-16 07:40 EDT — merged as `c7121437` on the owner's word ("U-1 merge word: go"), after
  ruling 111's checklist.

## Acceptance run

Run id [38895333180](https://github.com/TRO-Wolf/repark/actions/runs/38895333180). Head sha
`c71214376d7978750f0559e11ca6b03115065883` (the #1082 squash on main). Start
`2026-10-16T11:40:30Z` (2026-10-16 07:40:30 EDT). End `2026-10-16T12:05:41Z` (2026-10-16
08:05:41 EDT). Conclusion `success`.

Jobs:

- `tier-2 live AWS acceptance (Glue + S3 Tables, scratch-only)` — `success`
- `ICE-READ-PERF bench (S3 Tables + Glue, dispatch-only, scratch-only)` — `skipped`

Cell count: 12. The acceptance module has 11 and the dbt gold module has 1. All 12 PASSED
(0 FAILED, 0 SKIPPED). The skipped bench job is a job conclusion and has no PASSED, FAILED, or
SKIPPED line.

### Acceptance module

Summary: `11 passed in 439.61s (0:07:19)`.

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

Summary: `1 passed, 97 warnings in 63.82s (0:01:03)`.

- `python/dbt-repark/tests/test_aws_acceptance_gold.py::test_gold_stage_on_glue` — PASSED

## What changed in the write path

#1082 gives RePark one type planner for timestamps:
- A written `TIMESTAMP` plans as Spark's microsecond instant on the Spark door, and as the
  microsecond wall on the Rust ANSI door (ruling 94).
- `timestamp_ns` and `timestamptz_ns` plan as nanoseconds.
- One analyzer rule, `SparkTimestampUnitWiden`, gives a NULL or a coarser timestamp beside a
  nanosecond value the nanosecond type, so `coalesce(ns, NULL)` keeps all nine digits on every
  door. That closes R-017.
- Two extensions of that rule go beyond amendment 1's §2a text. The owner authorized both under
  ruling 100 (ruling 119 (a)), and they are recorded as dated rows in amendment 1's position
  table:
  - a `DATE` or string beside a zoned timestamp;
  - a `VALUES` column retyped by an expression-shape test.

Ruling 111's never-worse checklist, as measured by the verifier and the orchestrator and put to the owner before the merge word:
- Every one of the 78 carried breaks traces to one carried commit, and none was dropped. That
  is 51 Rust rows and 27 facade rows.
- The 11 ruling-100 re-pins each name their source.
- On today's main, 10,336 of 11,288 statements answer. The head answers all of them except
  108, all inside ruling 38's carve-out for the pandas and Arrow doors.
- Ruling 101's eight array cells are green on main and on the head.
- The Q6 cell passes.
- `spark.sql.execution.pandas.convertToArrowArraySafely` still reads `'true'`.
- Rust passes 6,850 with 0 failures, and the facade passes 19,998 with 0 failures. No test
  changed status.

## Follow-ups filed

- [ICE-TSNS-ANSI-CAST-FLOOR-1](../../task/roadmap/mid-term/ice-tsns-ansi-cast-floor-1-card-2026-10-16.md),
  register row ICE-TSNS-SQL-1-R-025 (the verifier's F-1, owner ruling 120 (a)). On the ANSI
  doors, `CAST(ns AS TIMESTAMP)` truncates a pre-1970 sub-microsecond value toward zero instead
  of flooring. Stored values equal main's, and it does not block U-1.
- The open items that already have rows or cards keep them:
  - R-024, an encoded nanosecond source into `TIMESTAMP`;
  - [ICE-TSNS-UPDATE-NTZ-CAST-1](../../task/roadmap/mid-term/ice-tsns-update-ntz-cast-1-card-2026-10-15.md),
    a written `CAST(ns AS TIMESTAMP_NTZ)` in `UPDATE … WHERE`.
