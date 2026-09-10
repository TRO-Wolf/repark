# Unit ledger — BALLISTA-M1-C · multi-stage queries, shuffle, retry, metrics (step 2)

**Retires:** this ledger moves to `../completed/` in this unit's last commit.
This file closes when BALLISTA-M1-C merges, or when the owner closes the slate row.

**Unit:** BALLISTA-M1-C step 2 · **Date:** 2026-09-10 · **Executor:** Grok (grok-4.6), Actor, step 2 ·
**Branch:** `feat/ballista-m1-c` · **Base:** `6398ced2`
**Model:** grok-4.6
**risk_tier:** standard.
**Path:** STANDARD.

Step 1 (commit `6398ced2`) landed D-1. Step 2 lands D-3 (`JobStatus::Completed` per-stage
metrics), D-4 (session spill dir has no shuffle files after complete or cancel), and
`docs/design/distributed-m1.md`. D-2 retry stays **OPEN**.

**Not in this unit:** Iceberg providers, `STATUS.md`, `briefs/next-sequence.md`, `.github/`,
`Cargo.toml`, `Cargo.lock`, `datafusion-proto`, `arrow_flight`.

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | Three shapes, each equal to `LocalDataFusionExecutor` on the same physical plan: hash aggregate over 4 partitions (Partial+Final), hash join of two tables with a `RepartitionExec` on both sides, sort-merge join with `prefer_hash_join=false` and a `RepartitionExec` on both sides. Cluster row order is not a contract; rows are sorted before compare. | `hash_aggregate_over_four_partitions_matches_local_executor`, `hash_join_of_two_tables_matches_local_executor`, `sort_merge_join_with_prefer_hash_join_false_matches_local_executor` in `crates/repark-distributed/tests/multi_stage.rs`. | **PROVEN** | Red first: `cargo test -p repark-distributed --features cluster --offline --test multi_stage` compile exit 101, `error[E0106]: missing lifetime specifier` on `join_named`. Green after the named lifetime: 3 passed in 0.51 s. CollectLeft thresholds set to 0 so small MemTables still shuffle both sides (otherwise DF broadcasts and the join is not the partitioned three-stage shape). pins: ballista-m1-c/C-001 |
| C-002 | Retry: kill one executor's task mid-shuffle; the job completes on the remaining executor and the status carries the retried stage count. | Deterministic fail-once of one executor task, then complete on the remaining executor with `retried_stages > 0`. | **OPEN** | Ballista 54.1.0 `ChaosExec` (`ballista.testing.chaos_execution.*`) injects a retryable `DataFusionError::IoError`, but `should_fail` is `seed.wrapping_add(partition)` and does not change on retry — probability 1.0 fails every attempt up to `task_max_failures` (4) and the job fails. Stopping one executor requires Flight + poll-loop `JoinHandle`s. `new_standalone_executor_from_builder` returns `()` and spawns both internally (`standalone.rs` 137–145). Reimplementing that startup needs `arrow_flight`, which this crate does not depend on and must not add. Scheduler `remove_executor` is `pub(crate)`. `Completed.retried_stages` is still wired from `stage_attempt_num` for a later unit. No pin: a pin that cannot fail the task once would prove nothing. |
| C-003 | `JobStatus::Completed` carries per-stage rows, bytes shuffled, and wall time from the scheduler's metrics; shuffle bytes > 0 on the two-stage hash-aggregate shape. | `completed_two_stage_hash_aggregate_reports_shuffle_bytes` in `crates/repark-distributed/tests/multi_stage.rs`. | **PROVEN** | Red first: `JobStatus::Completed` became a struct variant, `cargo test -p repark-distributed --features cluster --offline --no-run` compile exit 101, `error[E0533]: expected value, found struct variant JobStatus::Completed` at `local.rs:156` and `cluster.rs:526`. Green: that pin `ok`; stages.len() >= 2, sum(bytes_shuffled) > 0, sum(rows) > 0. Numbers come from the execution graph (`ShuffleWritePartition.num_rows` / `num_bytes`, `TaskInfo` start/end exec times), not `SchedulerMetricsCollector` (job-level timestamps only; default collector is a no-op without the prometheus feature). Wall time is carried; the pin does not require wall_time_ms > 0. Local executor reports `Completed { stages: [], retried_stages: 0 }` (no graph). pins: ballista-m1-c/C-003 |
| C-004 | Shuffle data lands under the session's spill temp dir and is removed when the job completes or is cancelled; the test checks the directory. | `session_spill_dir_has_no_shuffle_files_after_complete_and_after_cancel`. | **PROVEN** | Real behaviour, not the original wording. Session spill dir (`datafusion.runtime.temp_directory`) has no `data*.arrow` files before the job, after the two-stage hash aggregate completes, or after cancel of `range(100000000)`. Ballista 54.1.0 standalone sets `work_dir` from `TempDir::new()` (`standalone.rs:118`) and drops the `TempDir` in that statement; shuffle files land there, not in the session spill dir. `clean_shuffle_data_loop` is only in `executor_process.rs` and is not started on the standalone path. We do not hold that work_dir (doing so needs `arrow_flight`). pins: ballista-m1-c/C-004 |

VERDICT: 4 clauses, 3 PROVEN, 1 OPEN (C-002), 0 REJECTED.

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
- D-2 stays OPEN (see C-002). `Completed.retried_stages` is present and will be > 0 if a later unit can inject a fail-once.
- D-4 original wording ("shuffle lands under the session spill dir and is removed") is false for Ballista 54.1.0 standalone. The pin is the real behaviour: the session spill dir never holds those files.

## Red-first evidence (step 2, 2026-09-10)

Command: `cargo test -p repark-distributed --features cluster --offline --no-run` after `JobStatus::Completed` gained a payload and before call sites were updated.

```
error[E0533]: expected value, found struct variant `JobStatus::Completed`
   --> crates/repark-distributed/src/local.rs:156:56
    |
156 |                     mark_terminal(&this.jobs, this.id, JobStatus::Completed);
    |                                                        ^^^^^^^^^^^^^^^^^^^^ not a value

error[E0533]: expected value, found struct variant `JobStatus::Completed`
   --> crates/repark-distributed/src/cluster.rs:526:59
    |
526 |             Some(job_status::Status::Successful(_)) => Ok(JobStatus::Completed),
    |                                                           ^^^^^^^^^^^^^^^^^^^^ not a value

error: could not compile `repark-distributed` (lib test) due to 2 previous errors
```

Green after the payload fill, existing pin updates, and the two new pins:

```
running 5 tests
test hash_aggregate_over_four_partitions_matches_local_executor ... ok
test completed_two_stage_hash_aggregate_reports_shuffle_bytes ... ok
test sort_merge_join_with_prefer_hash_join_false_matches_local_executor ... ok
test session_spill_dir_has_no_shuffle_files_after_complete_and_after_cancel ... ok
test hash_join_of_two_tables_matches_local_executor ... ok

test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.58s
```
