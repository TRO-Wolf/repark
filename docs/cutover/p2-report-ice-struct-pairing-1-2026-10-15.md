# P2 report for #1062, ICE-STRUCT-PAIRING-1, 2026-10-15

## Verifier verdicts, by time

- 2026-10-14 19:00:31.747355480 -0400 (EDT) — Verifier 1 for ICE-STRUCT-PAIRING-1 (PR #1062, head e3fc32ab): verdict FAIL.
- 2026-10-15 02:14:43.231247941 -0400 (EDT) — Re-verifier of ICE-STRUCT-PAIRING-1 (U-6, R-016) build round 3 at 5b97a250: verdict FAIL.
- 2026-10-15 18:29:18.654650860 -0400 (EDT) — Re-verify of ICE-STRUCT-PAIRING-1 exception round at ab925ec1: verdict PASS (verdict.json).
- 2026-10-15 18:51 EDT — merged as `5531311b`.

## Acceptance run

Run id [38820033176](https://github.com/TRO-Wolf/repark/actions/runs/38820033176). Head sha
`5531311b8b13a5d34ea452048f5edec6f642296e` (the #1062 squash on main). Start
`2026-10-15T22:52:37Z` (2026-10-15 18:52:37 EDT). End `2026-10-15T23:15:30Z` (2026-10-15
19:15:30 EDT). Conclusion `success`.

Jobs:

- `tier-2 live AWS acceptance (Glue + S3 Tables, scratch-only)` — `success`
- `ICE-READ-PERF bench (S3 Tables + Glue, dispatch-only, scratch-only)` — `skipped`

Cell count: 12. The acceptance module has 11. The dbt gold module has 1. All 12 are
PASSED (0 FAILED, 0 SKIPPED). The skipped bench job is a job conclusion and has no
PASSED, FAILED, or SKIPPED line.

### Acceptance module

Summary: `11 passed in 456.58s (0:07:36)`.

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

Summary: `1 passed, 97 warnings in 61.87s (0:01:01)`.

- `python/dbt-repark/tests/test_aws_acceptance_gold.py::test_gold_stage_on_glue` — PASSED

## What changed in the write path

#1062 puts one rule, `pair_store_value`
(`crates/repark-iceberg/src/write/struct_pairing.rs`), on every write door. It decides
which source field feeds which field of a struct column, as Spark 4.1.2 does: by position
on `INSERT … VALUES`, `INSERT … SELECT`, `INSERT OVERWRITE`, `REPLACE WHERE` and
`insertInto`; by name on `INSERT … BY NAME`, every MERGE arm, `UPDATE`,
`writeTo().append()`, `overwritePartitions()` and `saveAsTable` (append), at every depth
and in every container, following `spark.sql.caseSensitive`. Where the rule cannot be met
the write refuses with Spark's text; it never stores a NULL nobody wrote. The named
exception, owner ruling 58, row `ICE-STRUCT-PAIRING-1-STATIC-PARTITION`, is both PARTITION
overwrite spellings: a struct whose fields do not already sit in the table's order refuses
with a hint that names the workaround (write the fields with the table's names in the
table's order). Per route, 52 cells where Spark stores now refuse; 38 of them are cells
where main stored Spark's value (19 per format version, v2 and v3), and 14 where main
stored another value.

## Follow-ups filed

Cards on main from U-6's out-of-scope findings, by ID:

- [REPARK-SQL-UPDATE-SUBQUERY-WHERE-1](../../task/roadmap/mid-term/repark-sql-update-subquery-where-1-card-2026-10-15.md)
  (critical). Through `repark.sql()`, an `UPDATE` whose `WHERE` holds a subquery updates
  every row and reports success. Filed 2026-10-15 under owner ruling 106 (a).
- [ICE-STRUCT-INLINE-TABLE-FROM-1](../../task/roadmap/mid-term/ice-struct-inline-table-from-1-card-2026-10-15.md).
  An inline table in `FROM` stores a reordered struct by name, where Spark pairs by
  position. Filed 2026-10-15 under owner ruling 107 (a).
- Ruling 99, the full SET-expression walk, unit `UPDATE-SET-SUBQUERY-WALK-1`, is under
  way. This main has no card file for that unit.
