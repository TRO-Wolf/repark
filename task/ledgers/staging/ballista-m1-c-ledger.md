# Unit ledger — BALLISTA-M1-C · multi-stage queries, shuffle, retry, metrics (step 1)

**Retires:** this ledger moves to `../completed/` in this unit's last commit.
This file closes when BALLISTA-M1-C merges, or when the owner closes the slate row.

**Unit:** BALLISTA-M1-C step 1 · **Date:** 2026-09-10 · **Executor:** Grok (grok-4.6), Actor, step 1 ·
**Branch:** `feat/ballista-m1-c` · **Base:** `8647f59f`
**Model:** grok-4.6
**risk_tier:** standard.
**Path:** STANDARD.

Step 1 lands D-1: three multi-stage physical-plan shapes in
`crates/repark-distributed/tests/multi_stage.rs`, each built once, run through
`LocalDataFusionExecutor`, then through a two-executor `ReparkClusterExecutor`, compared
after sorting rows. Step 2 (not this commit) is D-2 retry, D-3 metrics, D-4 spill-dir
cleanup, and `docs/design/distributed-m1.md`.

**Not in this unit this step:** `src/cluster.rs` retry/metrics surface, the design doc,
Iceberg providers, `STATUS.md`, `briefs/next-sequence.md`, `.github/`, `Cargo.toml`,
`Cargo.lock`, `datafusion-proto`.

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Three shapes, each equal to `LocalDataFusionExecutor` on the same physical plan: hash aggregate over 4 partitions (Partial+Final), hash join of two tables with a `RepartitionExec` on both sides, sort-merge join with `prefer_hash_join=false` and a `RepartitionExec` on both sides. Cluster row order is not a contract; rows are sorted before compare. | `hash_aggregate_over_four_partitions_matches_local_executor`, `hash_join_of_two_tables_matches_local_executor`, `sort_merge_join_with_prefer_hash_join_false_matches_local_executor` in `crates/repark-distributed/tests/multi_stage.rs`. | **PROVEN** | Red first: `cargo test -p repark-distributed --features cluster --offline --test multi_stage` compile exit 101, `error[E0106]: missing lifetime specifier` on `join_named`. Green after the named lifetime: 3 passed in 0.51 s. CollectLeft thresholds set to 0 so small MemTables still shuffle both sides (otherwise DF broadcasts and the join is not the partitioned three-stage shape). pins: ballista-m1-c/C-001 |
| C-002 | Retry: kill one executor's task mid-shuffle; the job completes on the remaining executor and the status carries the retried stage count. | Step 2 pin in `tests/multi_stage.rs`. | **OPEN** | Step 2. |
| C-003 | `JobStatus::Completed` carries per-stage rows, bytes shuffled, and wall time from the scheduler's metrics; shuffle bytes > 0 on the two-stage hash-aggregate shape. | Step 2 pin in `tests/multi_stage.rs`. | **OPEN** | Step 2. Needs a `JobStatus::Completed` payload change in `src/cluster.rs` / `src/executor.rs`. |
| C-004 | Shuffle data lands under the session's spill temp dir and is removed when the job completes or is cancelled; the test checks the directory. | Step 2 pin in `tests/multi_stage.rs`. | **OPEN** | Step 2. |

VERDICT: 4 clauses, 1 PROVEN, 3 OPEN (step 2), 0 REJECTED.

## Red-first evidence (step 1, 2026-09-10)

Command: `cargo test -p repark-distributed --features cluster --offline --test multi_stage -- --nocapture`

First compile of the new pin (lifetime of `join_named` return not named):

```
error[E0106]: missing lifetime specifier
   --> crates/repark-distributed/tests/multi_stage.rs:183:56
    |
183 | fn join_named(plan: &dyn ExecutionPlan, name: &str) -> &dyn ExecutionPlan {
    |                     ------------------        ----     ^ expected named lifetime parameter
    |
    = help: this function's return type contains a borrowed value, but the signature does not say whether it is borrowed from `plan` or `name`

error: could not compile `repark-distributed` (test "multi_stage") due to 1 previous error
```

Exit 101.

Green after `fn join_named<'plan>(plan: &'plan dyn ExecutionPlan, name: &str) -> &'plan dyn ExecutionPlan`:

```
running 3 tests
test hash_aggregate_over_four_partitions_matches_local_executor ... ok
test sort_merge_join_with_prefer_hash_join_false_matches_local_executor ... ok
test hash_join_of_two_tables_matches_local_executor ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.51s
```

The M1-B cluster executor already answered the three shapes; the pin did not need product-code edits. Cluster vs local comparison sorts cell-rows (`sorted_row_keys`) so an incidental partition order is not asserted.

## Residue

- M1-B C-004 (codec round-trip over the five shuffle nodes) stays OPEN / PARKED; this step did not hit that wall. Shuffle nodes in these three shapes ran through the installed Ballista codecs.
- D-2 / D-3 / D-4 are step 2.
