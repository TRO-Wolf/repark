# Charter ledger — MW-10 · the S3 Tables merge-on-read leg (the intake's "MW-4b"), measure-first on OD-3b

**Date:** 2026-08-28 · **Branch:** `feat/mw-10-s3tables-mor` (opens when the owner confirms this
gate) · **Base:** `main` after #256 · **Policy:** [../../../AGENTS.md](../../../../AGENTS.md)
"Verify before done" and the tier-2 runbook [../../../docs/tier2-aws.md](../../../../docs/tier2-aws.md)
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
| C-001 | **OD-3b measured.** On the scratch table bucket, `run_mor_merge_compact_expire` against `s3tables_catalog` runs `CALL expire_snapshots(older_than => <future>, retain_last => 1)` and the outcome is one of exactly two recorded states: the expired CTAS snapshot's files are gone from table storage and the live row set is unchanged (the dual probe: that snapshot readable before expire, unreadable after), or the service denies the delete — in which case the run fails loud, the denial (action, resource, message with the account masked) becomes a dated registry row, and the unit stops. | The first owner dispatch (or the first nightly after merge) with the run log attached here; the dual probe from MW-1; the registry row if denied. | **PROVEN** | Measured 2026-08-30: **allow**. The owner dispatched `aws-acceptance` on merged `main` (`afe3b807`, #264) at 20:18 UTC — [run 33333274383](https://github.com/TRO-Wolf/repark/actions/runs/33333274383) — and the acceptance step finished `4 passed in 145.15s` with `test_mor_merge_compact_expire_against_s3tables` green: the dual probe held (CTAS snapshot readable before expire, unknown-snapshot needle after), the live row set was unchanged, and the expired files were removed from table storage. No denial signature appeared, so no registry row is filed and the which-error question is unanswerable by construction (record it only if a future run denies). pins: mw-10-s3tables-mor/C-001 |
| C-002 | **The Glue leg's twin.** `test_mor_merge_compact_expire_against_s3tables` in `python/repark/tests/test_aws_acceptance.py` runs the shared helper against `S3TABLES_CATALOG` on a fresh `testing_mw10_mor_<uuid>` table in `testing_repark_acceptance` (never-teardown: tables accumulate under the scratch prefix), asserts through `assert_mor_maintenance_outcome`, and SKIPs — never fails — when `TABLE_BUCKET_ARN` is absent so a Glue-only run is unaffected; the module stays skipped in tier 1 (`REPARK_AWS_ACCEPTANCE` unset). | The test; `make preflight` green with the module skipped; the dispatch log. | **PROVEN** | Test landed. Namespace created without `location` (AST: `create_namespace` has two positional args and no `location` keyword — F-7). Glue location guard is not called. Denial path is `pytest.fail(format_denial_failure(...))` (F-4; fail→skip goes red). Module skipif on `REPARK_AWS_ACCEPTANCE`; extra skip on absent `TABLE_BUCKET_ARN` with the create-only wording. Dual probe is the shared helper's `require_snapshot_readable` / `require_snapshot_expired`, plus `assert_engine_expire_removed_ctas` (F-6). pins: mw-10-s3tables-mor/C-002 |
| C-003 | **Service-side maintenance is a conflict hazard, handled as routine retry.** S3 Tables compacts and expires concurrently with the engine (fork engine contract §8): a `CommitFailed` requirement mismatch or a `validate_data_files_exist` trip during the MERGE or maintenance steps is retried a bounded number of times (the count named in the test, default 3) and the retry count is recorded in the outcome; an exhausted retry is a failure, not a skip. The run also records whether the service committed during the sequence (snapshot-log operations the engine did not write). | A bounded retry helper in `_acceptance.py`, pinned in the always-run memory analog with an injected `CommitFailed`; the dispatch log's retry count and service-commit observation. | **PROVEN** | `retry_on_commit_conflict` (default 3). Per-call cap is `max_call_retries <= attempts`; the sum may exceed attempts (`assert_retry_counts`, F-1). `service_commits` is the union of both expire logs minus engine-credited ids; a vanished pre-expire service id counts; a two-id step window is ambiguous not guessed (F-2). MERGE and each CALL wrap `retry_on_commit_conflict` (AST + injection: first MERGE and first CALL conflict once → `retry_count == 2`, `max_call_retries == 1`; wrap removal goes red — F-3). Denial signatures win over conflict (F-5). Dispatch 2026-08-30 ([run 33333274383](https://github.com/TRO-Wolf/repark/actions/runs/33333274383)): green under `pytest -q`, which prints no per-field values; the pass proves the asserted bounds — no per-call budget exhausted, `service_commits` recorded non-negative, compact reduced delete files (so service compaction did not empty the engine's work on this run). Exact counts stay in the outcome object; a future dispatch with `-rA` would print them. pins: mw-10-s3tables-mor/C-003. |
| C-004 | **Automatic snapshot management interplay recorded.** The scratch tables carry no branch, tag or `history.expire.*` property, so S3 Tables' own snapshot management stays enabled on them; the unit records what the service's expiry does to the CTAS snapshot's readability window and whether the engine's `expire_snapshots` and the service's ever disagree on the table's current snapshot. | Snapshot log before / after; the maintenance runbook gains the S3 Tables paragraph. | **PROVEN** | Outcome records `snapshot_log_before_expire`, `snapshot_log_after_expire`, `current_snapshot_matches_engine`, and `ambiguous_engine_windows`. `assert_engine_expire_removed_ctas` requires the CTAS id in the before-expire log and absent after (F-6). Memory analog asserts shape; the guide's S3 Tables paragraph states automatic snapshot management stays on, concurrent expire, and bounded retry. Live interplay measured 2026-08-30 ([run 33333274383](https://github.com/TRO-Wolf/repark/actions/runs/33333274383), green): automatic snapshot management stayed on, the CTAS id was in the before-expire log and absent after, and `current_snapshot_matches_engine` was recorded without tripping any assertion. pins: mw-10-s3tables-mor/C-004 |
| C-005 | The documents say what the run proved: `docs/tier2-aws.md` §2 carries the applied date and the measured answer to the `PutTableData` question; the north star §5 OD-3b bullet and the "Live: Glue + S3 Tables" row point at this evidence (this unit is format v2 — it proves the permission, not the v3 leg); the maintenance runbook's S3 Tables section; the roadmap intake's MW-4b row marked chartered as MW-10; STATUS and the slate; maps in lockstep; a registry row iff denied. | `make check-map-sync`, `check-docs-compaction`, `check-ledger-grammar` green; the rulings meta-pin (`test_od_3b_is_ruled_in_and_the_runbook_carries_the_scoped_statement`) still green. | **PROVEN** | Every slot filled from the 2026-08-30 dispatch in the departure commit: `docs/tier2-aws.md` §2 (allow, run link), north-star OD-3b bullet and Live row (permission green, v3 legs still unmeasured), `docs/guide/iceberg-guide.md` interplay paragraph, STATUS, the intake MW-4b row, roadmap maps; no registry row (allow outcome). Doc gates green on the departure commit. pins: mw-10-s3tables-mor/C-005 |
| C-006 | Green on the whole surface: `make preflight` with the acceptance module skipped, the parity harness, and one green owner dispatch of `aws-acceptance` on merged code (or the recorded denial with the unit stopped at C-001). | Gate output; the dispatch link. | **PROVEN** | `make preflight` green pre-merge (PR #264, all checks passed); parity harness 49 passed / 4 skipped on the unit branch; the owner dispatch is [run 33333274383](https://github.com/TRO-Wolf/repark/actions/runs/33333274383) — `aws-acceptance` on merged `main` `afe3b807`, success in 13m43s, acceptance step `4 passed in 145.15s`. pins: mw-10-s3tables-mor/C-006 |

VERDICT: PROVEN — 6 clauses, 6 PROVEN, 0 OPEN, 0 REJECTED (C-001, C-005, C-006 closed by the
2026-08-30 owner dispatch, [run 33333274383](https://github.com/TRO-Wolf/repark/actions/runs/33333274383)). The
gate passes when every row is PROVEN with its pin (`pins: mw-10-s3tables-mor/C-NNN`) and the owner
confirms.

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

## Denial row draft

The orchestrator files this in `docs/spark-sql-iceberg-parity.md` only if the first owner
dispatch is a table-storage delete denial. Do not add a new `s3tables:*` action.

### S3T-2 — S3 Tables `expire_snapshots` cannot remove expired snapshot files (OD-3b denial)

- **repark** — `CALL expire_snapshots` against the scratch S3 Tables catalog is denied when
  removing expired snapshot files from table storage. The acceptance helper fails loud with
  the action, the resource, and the message (12-digit account ids masked as `<ACCOUNT>`).
  The live row set is unchanged. The harness does not widen IAM.
- **Apache Spark** — not a Spark divergence: this is an AWS S3 Tables permission gap on
  whether `s3tables:PutTableData` authorizes object delete on table storage.
  *(oracle: live — first owner `aws-acceptance` dispatch after MW-10 merge; paste the
  masked action / resource / message here.)*
- **Pin** —
  `python/repark/tests/test_aws_acceptance.py::test_mor_merge_compact_expire_against_s3tables`
- **Rationale** — DECLARED dated IAM gap. A denial is a stop, not a design. The owner must
  re-rule before any policy widen.
  Measured result (2026-08-30): the first dispatch was **allow**, so this row was NOT filed;
  the draft stays here as the recorded procedure should a future run deny.

## Closing (orchestrator, 2026-08-30)

```yaml
COVERAGE_ATTESTATION:
  pr_unit: mw-10-s3tables-mor
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: >
        Walked C-001..C-006 against the charter. C-001, C-005 and C-006 were closed only on
        the live dispatch's raw log (4 passed in 145.15s on merged main afe3b807), never on
        the Actor's claim; the Grok Critic's F-1..F-7 were confirmed in source before the
        remediation round and re-verified after it.
      artifacts: [python/repark/tests/test_aws_acceptance.py]
    - id: AT-2
      status: ATTACKED
      evidence: >
        Edge inputs pinned as always-run memory analogs: injected first-MERGE and first-CALL
        conflicts (retry_count 2, max_call_retries 1), a pre-expire service id that vanishes,
        the ambiguous two-id step window, denial-over-conflict precedence, CTAS absent from
        the before-expire log naming auto-expiry.
      artifacts: [python/repark/tests/test_acceptance_helpers.py]
    - id: AT-3
      status: ATTACKED
      evidence: >
        State writes are scratch-only by construction: never-teardown namespace, no
        DROP/DELETE path in the harness, IAM statement unchanged by the unit (a denial is a
        stop, not a design), account ids masked before any failure text can carry them.
      artifacts: [docs/tier2-aws.md]
    - id: AT-4
      status: ATTACKED
      evidence: >
        The concurrency surface is the unit's subject: service compaction/expiry commits
        alongside the engine; bounded retry with per-call cap, service commits recorded from
        the union of both expire logs, engine windows never guessed when ambiguous.
      artifacts: [python/repark/tests/_acceptance.py]
    - id: AT-5
      status: N/A
      justification: no renames, moves, or visibility changes in this unit.
    - id: AT-6
      status: ATTACKED
      evidence: >
        make preflight green pre-merge (PR #264, CI all pass); parity harness 49 passed /
        4 skipped on the unit branch; focused helper suites re-run by the orchestrator after
        remediation; the owner dispatch green on merged main (run 33333274383).
      artifacts: [.github/workflows/aws-acceptance.yml]
    - id: AT-7
      status: N/A
      justification: no dependency, lockfile, or workflow change in this unit.
    - id: AT-8
      status: ATTACKED
      evidence: >
        Pre-push forbidden-literal scan over added lines and messages, 0 hits; worker commit
        identity byte-exact with the Grok trailer on every Actor commit; the dispatch log
        carries no unmasked account id into any filled slot.
      artifacts: [python/repark/tests/_acceptance.py]
    - id: AT-9
      status: ATTACKED
      evidence: >
        The user-facing documents state the measured outcome, not the hoped one: tier2-aws
        s2 carries the allow with the run link, the guide's maintenance paragraph states the
        retry posture and the measured interplay, the north star keeps the v3 live legs
        unmeasured (this unit proves the permission on format v2 only).
      artifacts: [docs/guide/iceberg-guide.md]
    - id: AT-10
      status: ATTACKED
      evidence: >
        Quantified claims remeasured from the raw run log (4 passed, 145.15s, 13m43s job),
        not from a summary; retry/service-commit phrasing limited to what a quiet pytest run
        proves — exact counts are recorded in the outcome object, not claimed from the log.
      artifacts: [docs/tier2-aws.md]
  complete: true
```
