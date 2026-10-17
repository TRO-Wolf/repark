# P2 report for #1095, RP-58: the fork repin to `409ff330` (ICE-COMMIT-REBASE-DUP-1), 2026-10-17

## Verifier verdicts, by time

The fork change was verified on the fork branch `fix/ice-commit-rebase-dup-1`, before the RePark
bump existed.

- 2026-10-16 13:19 EDT — verifier (head `d36896aa`): verdict FAIL.
- 2026-10-16 22:31 EDT — re-verifier (head `3e3aad98`): verdict FAIL.
- 2026-10-17 09:35 EDT — scoped re-verifier of ruling 137 (head `46af9dc8`): verdict FAIL, on two
  test gaps only (R137-1, R137-2). The behaviour held on every shape it measured.
- 2026-10-17 10:56 EDT — scoped re-verifier of ruling 140 (head `ca358550`): verdict PASS. Its
  four non-blocking test gaps are recorded in the fork ledger as R140-1 to R140-4 (`3b01a858`).
- 2026-10-17 16:49 EDT — fork #368 squash-merged into fork main as `409ff330`, on `Tests (default)`
  and `Tests (doc)` green (owner ruling 141 (a)). Its tree equals the verified branch head.
- 2026-10-17 17:27 EDT — RePark #1095 merged as `1ce895a0` (TREE-EQUAL), CI green.

## Acceptance run

Run id [39071066809](https://github.com/TRO-Wolf/repark/actions/runs/39071066809). Head sha
`1ce895a086277001f6a8e0d3c521f64a4675b33e` (the #1095 merge on main). Start
`2026-10-17T21:28:10Z` (2026-10-17 17:28:10 EDT). End `2026-10-17T21:54:21Z` (2026-10-17
17:54:21 EDT). Conclusion `success`.

Jobs:

- `tier-2 live AWS acceptance (Glue + S3 Tables, scratch-only)` — `success`
- `ICE-READ-PERF bench (S3 Tables + Glue, dispatch-only, scratch-only)` — `skipped`

Cell count: 12. The acceptance module has 11 and the dbt gold module has 1. All 12 PASSED
(0 FAILED, 0 SKIPPED). The skipped bench job is a job conclusion and has no PASSED, FAILED, or
SKIPPED line.

### Acceptance module

Summary: `11 passed in 340.82s (0:05:40)`.

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

Summary: `1 passed, 97 warnings in 59.07s`.

- `python/dbt-repark/tests/test_aws_acceptance_gold.py::test_gold_stage_on_glue` — PASSED

## What changed in the write path

#1095 moves the five `rev =` lines from `076d5f98` to `409ff330` and nothing else in code. The lock
changes the six fork packages' source lines and adds `aws-runtime` and `aws-smithy-async` under
the two catalog crates, both already in the lock at the versions the fork asks for. The fork
brings two commits:

- **#368, F-COMMIT-REBASE-DUP-1** (RePark owner rulings 84, 115, 127 to 130, 137 and 140):
  - **The re-base guard.** After a retryable commit rejection, `Transaction::commit` refreshes
    the table. It returns the table when it already holds a snapshot id any attempt of this
    transaction sent. A lost reply, the SDK's own re-send and a stale rejection no longer append
    the same rows twice on Glue or S3 Tables. A genuine conflict still re-bases.
  - **One send per commit call.** Glue `UpdateTable` and S3 Tables
    `UpdateTableMetadataLocation` run with the SDK retry off; reads keep their retries.
  - **Ambiguous failures reach the read-back.** A reply lost on a reset or a closed connection, a
    5xx (modeled or not), a timeout, or an unreadable reply surfaces as `CommitStateUnknown` only
    after the fork has read the table back for the sent ids. A refused connect and a construction
    failure are never sent, and are not reconciled.
  - **Throttles.** A throttle resends unchanged with backoff, up to three sends, and then says
    it was throttled on each send and that nothing was applied. A client built without a
    `sleep_impl` still resends; a client with its own sleep keeps it.
- **#367, rider:** CI moves from MinIO to RustFS. The fork's own lock takes rustls 0.23.46, which
  RePark's lock already held.

RePark's acceptance cells commit through both catalogs. The double append needs a lost reply, and
the cells do not inject one; the offline drills on the fork are where that sequence runs.

## Follow-ups

- **ENC-1, #1071.** Ruling 141 had ENC-1 merge main after this bump, so its final verifier
  measures at `409ff330`. The merge was clean, and no re-key was needed: the ambiguous-outcome
  arm keys on `ErrorKind::CommitStateUnknown`.
- **Ruling 143's fork-side card:** the fork exposes the snapshot ids it sent, so RePark can check
  its own id instead of the `engine.operation-id`. It does not grow into DUP-1.
- **The U-9 live drill** (#1067, held): the lost-reply sequence on live Glue and S3 Tables is
  still offline only.
