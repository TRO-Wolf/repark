# Unit ledger — CLUSTER-CODEC-TEST-1 · the one cluster-feature test that fails on main

**Date:** 2026-10-10 · **Branch:** `fix/cluster-codec-pushdown-test-1` · **Base:** `9b230aed` (`main`)
**Model:** muse-spark-1.3-contributor · **Policy:** [../../../AGENTS.md](../../../AGENTS.md).
**Path:** STANDARD. **risk_tier: standard.**
**Card:** [cluster-feature-ci-1-card-2026-10-09.md](../../roadmap/mid-term/cluster-feature-ci-1-card-2026-10-09.md).

**Retires:** this ledger moves to `../completed/` when the unit's PR merges.

**Why now.** The card records that `date_and_timestamp_predicates_measure_the_pushdown_surface`
in `crates/repark-distributed/tests/codec.rs` fails on main, and no CI job runs the
`cluster` feature, so the red went unseen. The owner ruled (2026-10-10) that the failing
test is fixed in its own small PR before the CI job is added, so the job is green on day
one.

**Not in this unit:** product code (the brief forbids it), `.github/`, `Cargo.toml`,
`STATUS.md`, the CI job itself (the card stays open for it).

## PROPOSITION LEDGER — CLUSTER-CODEC-TEST-1 — 2026-10-10

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | The TIMESTAMP half measures the new surface: `ts = TIMESTAMP '2024-01-02 03:04:05'` scans Ok with `predicate:[]`, and the predicate-less node encodes, decodes identically, and answers the same rows on the cluster as on the local executor. | The updated test's `timestamp-predicate` half (`scan` Ok, the `predicate:[]` assertion, `encode_or_refusal` travel-or-refusal, `assert_same_scan`, `cluster_batches`). | PROVEN | Throwaway probe (deleted after measuring): `parse_sql_expr` yields `ts = CAST(Utf8 AS Timestamp(Nanosecond))`; the scan succeeds with `predicates()` None and both rows, equal to the unfiltered scan. The refusal became a drop in `e67cad70` (RP-27, fork `8477b249` → `fdaa82d4`, fork #294), which added the `predicate_binds_soundly` gate: the space-separated string fails the String→Timestamp bind check, so the filter drops instead of erroring at bind. Nothing is lost across the codec: None encodes to zero filter bytes and decodes to the same unfiltered scan. |
| C-002 | The DATE half measures the pushed surface: `d = DATE '2024-01-01'` scans with `predicate:[d = 2024-01-01]`, and the pushed node encodes, decodes identically, and answers the SQL filtered answer on both executors. | The updated test's `date-predicate` half (the `predicate:[d = 2024-01-01]` assertion, travel-or-refusal, `assert_same_scan`, `cluster_batches`). | PROVEN | Throwaway probe: `parse_sql_expr` yields `d = CAST(Utf8 AS Date32)`; the scan carries `Eq(d, Date(19723))` and returns only row 1, equal to `SELECT * ... WHERE d = DATE '2024-01-01'`. The drop became a push in the same `e67cad70`, which evaluates Cast literals with `cast_to` instead of refusing casts to date. |

## The measured surface

| Filter | At test birth (`e756c8e0`, fork `85db42f`) | At head (fork `076d5f98`) | Rows |
|---|---|---|---|
| `ts = TIMESTAMP '2024-01-02 03:04:05'` | `scan` errors, message names `timestamp` | `scan` Ok, `predicate:[]`, `predicates()` None | both rows, equal to the unfiltered scan |
| `d = DATE '2024-01-01'` | `scan` Ok, `predicate:[]` (dropped) | `scan` Ok, `predicate:[d = 2024-01-01]` | row 1 only, equal to the SQL filtered answer |

Both halves moved in one commit, `e67cad70` (2026-09-18, RP-27), read off the fork
checkouts (the `predicate_binds_soundly` gate and literal `cast_to` both first appear at
fork `fdaa82d4`, absent at `8477b249`). No bisect build was run. The verdict is
expectation-stale on both halves: the product pushes more (DATE, correctly) and refuses
less (TIMESTAMP, dropped soundly under the fork's Inexact pushdown contract, so DataFusion
re-applies the filter above the scan — the SQL answers are unchanged and correct).

## Coverage attestation

```yaml
COVERAGE_ATTESTATION:
  pr_unit: cluster-codec-test-1
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: Both clauses are the brief's steps 2 and 3; the updated test pins each half's surface.
      artifacts: [crates/repark-distributed/tests/codec.rs]
    - id: AT-2
      status: N/A
      justification: No new input; two fixed filter literals keep their inputs and gain new expected surfaces.
    - id: AT-3
      status: ATTACKED
      evidence: Both travel-or-refusal arms stay live; a refusal must still name the predicate field.
      artifacts: [crates/repark-distributed/tests/codec.rs]
    - id: AT-4
      status: N/A
      justification: No shared state; each test builds its own warehouse and session.
    - id: AT-5
      status: N/A
      justification: No privileged actions, no secrets, no unsafe code.
    - id: AT-6
      status: ATTACKED
      evidence: Pushed DATE rows equal the SQL filtered answer; cluster rows equal local rows on both halves.
      artifacts: [crates/repark-distributed/tests/codec.rs]
    - id: AT-7
      status: N/A
      justification: One extra scan and cluster run inside the test; no loops, no growth.
    - id: AT-8
      status: ATTACKED
      evidence: The fork scan surface is re-measured at the pinned rev, not presumed.
      artifacts: [crates/repark-distributed/tests/codec.rs, crates/repark-distributed/tests/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: Both new assertions print the actual scan display on mismatch.
      artifacts: [crates/repark-distributed/tests/codec.rs]
    - id: AT-10
      status: ATTACKED
      evidence: The test was red on the old expectation; the new values were measured first with an independent probe.
      artifacts: [crates/repark-distributed/tests/codec.rs]
  complete: true
```
