# Charter ledger — MW-10 · the S3 Tables merge-on-read leg (the intake's "MW-4b"), measure-first on OD-3b

**Date:** 2026-08-28 · **Branch:** `feat/mw-10-s3tables-mor` (opens when the owner confirms this
gate) · **Base:** `main` after #256 · **Policy:** [../../../AGENTS.md](../../../AGENTS.md)
"Verify before done" and the tier-2 runbook [../../../docs/tier2-aws.md](../../../docs/tier2-aws.md)
· **Path:** STANDARD (one new acceptance test plus a bounded retry around the maintenance calls;
the measurement runs only on the owner's `aws-acceptance` dispatch or the nightly after merge).
**Named MW-10** because the ledger id `mw-4b` is taken by the archived Glue metadata-rewrite unit
(2026-08-23); the roadmap intake's "MW-4b — S3 Tables MOR leg" row points here.

**Retires:** moved to `completed/` in this unit's departure commit.

**Why now.** OD-3b was ruled in on 2026-08-25 and the owner applied the scoped S3 Tables IAM
statements (`docs/tier2-aws.md` §2) on 2026-08-28. Nothing in the harness measures what that
policy allows: the S3 Tables leg today is create-only (`test_process_silver_acceptance_against_s3tables`)
and the merge-on-read → compact → expire helper (`run_mor_merge_compact_expire`) runs only against
Glue. The open question the ruling itself names — whether `s3tables:PutTableData` authorizes
removing expired snapshot files on table storage, since AWS's object-API mapping names no action
for `DeleteObject` there — is answered by this unit's first clause, and **a denial is a stop, not a
design**: the harness never widens the policy. This is also the row the v1.0 gate's "Live: Glue +
S3 Tables v3 legs" depends on for its IAM, so the answer serves both.

## PROPOSITION LEDGER — MW-10 — 2026-08-28

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | **OD-3b measured.** On the scratch table bucket, `run_mor_merge_compact_expire` against `s3tables_catalog` runs `CALL expire_snapshots(older_than => <future>, retain_last => 1)` and the outcome is one of exactly two recorded states: the expired CTAS snapshot's files are gone from table storage and the live row set is unchanged (the dual probe: that snapshot readable before expire, unreadable after), or the service denies the delete — in which case the run fails loud, the denial (action, resource, message with the account masked) becomes a dated registry row, and the unit stops. | The first owner dispatch (or the first nightly after merge) with the run log attached here; the dual probe from MW-1; the registry row if denied. | OPEN | Which error does S3 Tables return when table-storage delete is not authorized — `AccessDenied` on the object API, or a `CommitFailed` from the catalog? Record whichever appears verbatim. |
| C-002 | **The Glue leg's twin.** `test_mor_merge_compact_expire_against_s3tables` in `python/repark/tests/test_aws_acceptance.py` runs the shared helper against `S3TABLES_CATALOG` on a fresh `testing_mw10_mor_<uuid>` table in `testing_repark_acceptance` (never-teardown: tables accumulate under the scratch prefix), asserts through `assert_mor_maintenance_outcome`, and SKIPs — never fails — when `TABLE_BUCKET_ARN` is absent so a Glue-only run is unaffected; the module stays skipped in tier 1 (`REPARK_AWS_ACCEPTANCE` unset). | The test; `make preflight` green with the module skipped; the dispatch log. | OPEN | The S3 Tables namespace is created without `location` (the bucket is the storage) — the Glue location guard is not called, as the create-only leg already does. |
| C-003 | **Service-side maintenance is a conflict hazard, handled as routine retry.** S3 Tables compacts and expires concurrently with the engine (fork engine contract §8): a `CommitFailed` requirement mismatch or a `validate_data_files_exist` trip during the MERGE or maintenance steps is retried a bounded number of times (the count named in the test, default 3) and the retry count is recorded in the outcome; an exhausted retry is a failure, not a skip. The run also records whether the service committed during the sequence (snapshot-log operations the engine did not write). | A bounded retry helper in `_acceptance.py`, pinned in the always-run memory analog with an injected `CommitFailed`; the dispatch log's retry count and service-commit observation. | OPEN | Does service compaction between the MERGEs and `rewrite_position_delete_files` empty the engine's work? If it does, the helper's `deletes_after < deletes_before` assertion must accept "the service compacted first" as a recorded outcome rather than a failure — decided by the first run. |
| C-004 | **Automatic snapshot management interplay recorded.** The scratch tables carry no branch, tag or `history.expire.*` property, so S3 Tables' own snapshot management stays enabled on them; the unit records what the service's expiry does to the CTAS snapshot's readability window and whether the engine's `expire_snapshots` and the service's ever disagree on the table's current snapshot. | Snapshot log before / after; the maintenance runbook gains the S3 Tables paragraph. | OPEN | — |
| C-005 | The documents say what the run proved: `docs/tier2-aws.md` §2 carries the applied date and the measured answer to the `PutTableData` question; the north star §5 OD-3b bullet and the "Live: Glue + S3 Tables" row point at this evidence (this unit is format v2 — it proves the permission, not the v3 leg); the maintenance runbook's S3 Tables section; the roadmap intake's MW-4b row marked chartered as MW-10; STATUS and the slate; maps in lockstep; a registry row iff denied. | `make check-map-sync`, `check-docs-compaction`, `check-ledger-grammar` green; the rulings meta-pin (`test_od_3b_is_ruled_in_and_the_runbook_carries_the_scoped_statement`) still green. | OPEN | Closes on the departure commit. |
| C-006 | Green on the whole surface: `make preflight` with the acceptance module skipped, the parity harness, and one green owner dispatch of `aws-acceptance` on merged code (or the recorded denial with the unit stopped at C-001). | Gate output; the dispatch link. | OPEN | Closes at readiness. |

VERDICT: OPEN — 6 clauses, 0 PROVEN, 0 REJECTED. The gate passes when every row is PROVEN with its
pin (`pins: mw-10-s3tables-mor/C-NNN`) and the owner confirms.

## 2. Sequence

1. Pickup ritual; the retry helper and its memory-analog pin (C-003) first, since the Glue leg
   benefits from it too.
2. The S3 Tables test (C-002), skipped locally; `make preflight`.
3. Departure docs (C-005) written for both outcomes with the measured one filled after the
   dispatch; PR opened; **owner dispatches `aws-acceptance`** (or the nightly runs) — that run is
   C-001 / C-006's evidence and is attached to this ledger before the PR is called ready.
4. If denied: registry row, tier2-aws note, unit stops; the v1.0 gate's S3 Tables rows inherit
   the dated gap until the owner rules again.

## 3. Owner actions

- Confirm this gate.
- After the PR opens: dispatch `aws-acceptance` (environment approval), read the result. The
  role keeps no `s3tables:DeleteTable` / `DeleteNamespace` / `DeleteTableBucket`; scratch tables
  accumulate under `testing_` and are cleaned by lifecycle policy or by hand with owner credentials.
