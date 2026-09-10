# Unit ledger — BALLISTA-M1-A · the `repark-distributed` crate and the local executor (step 1)

**Retires:** this ledger moves to `../completed/` in this unit's last commit.
This file closes when BALLISTA-M1-A merges, or when the owner closes the slate row.

**Unit:** BALLISTA-M1-A step 1 · **Date:** 2026-09-10 · **Executor:** Grok (grok-4.6), Actor, step 1 ·
**Branch:** `feat/ballista-m1-a` · **Base:** `a1e7e329`
**Model:** grok-4.6
**risk_tier:** standard.
**Path:** STANDARD.

Step 0 (seed) is already on this branch. Step 1 lands `executor.rs` (D-3), `local.rs` (D-4),
the three local-executor pins, and the `cluster` feature compile proof. Step 2 (chained in
this session, separate commit) adds the ARCHITECTURE.md paragraph and DAG figure row, the
`repark-distributed → repark-core` DAG sentence in `crates/map.md`, and the D-3/D-4 design
note on the crate map.

**Not in this unit:** cluster executor code (BALLISTA-M1-B), Iceberg providers, `STATUS.md`,
`briefs/next-sequence.md`, `.github/`, `Cargo.toml`, `Cargo.lock`.

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | A `range(1000)` sum through `LocalDataFusionExecutor` equals the direct DataFusion collect of the same SQL. | `range_1000_sum_through_local_executor_equals_direct_datafusion_answer` in `crates/repark-distributed/tests/local_executor.rs`. | **PROVEN** | Red first on the seed tree (`a1e7e329`, empty `lib.rs`): `cargo test -p repark-distributed --offline` exit 101, `error[E0432]: unresolved imports ... DistributedExecutor, JobStatus, LocalDataFusionExecutor`. Green after: that test `ok`. pins: ballista-m1-a/C-001 |
| C-002 | `status` is `Queued` or `Running` before the stream drains and `Completed` after. | `status_is_queued_or_running_before_drain_and_completed_after`. | **PROVEN** | Same red compile on the seed. Green after: that test `ok`. pins: ballista-m1-a/C-002 |
| C-003 | `cancel` on a long `range` returns within 1 s and `status` is `Cancelled`. | `cancel_on_a_long_range_returns_within_one_second_and_status_is_cancelled` (`SELECT value FROM range(100000000)`). | **PROVEN** | Same red compile on the seed. Green after: that test `ok` in 0.15 s for the file. pins: ballista-m1-a/C-003 |
| C-004 | The `cluster` feature compiles with `ballista`, `ballista-core`, `ballista-scheduler`, and `ballista-executor` linked and no cluster code this round. | `cargo build -p repark-distributed --features cluster`. | **PROVEN** | `cargo build -p repark-distributed --features cluster --offline` exit 0 (`Finished dev profile ... in 7.18s`). No `cluster.rs` / `session_provider.rs` / `codec.rs`. pins: ballista-m1-a/C-004 |

VERDICT: 4 clauses, 4 PROVEN, 0 OPEN, 0 REJECTED.

## Red-first evidence (step 1, 2026-09-10)

Command: `cargo test -p repark-distributed --offline` on the seed crate (empty `src/lib.rs`, tests already written). Exit 101.

```
error[E0432]: unresolved imports `repark_distributed::DistributedExecutor`, `repark_distributed::JobStatus`, `repark_distributed::LocalDataFusionExecutor`
 --> crates/repark-distributed/tests/local_executor.rs:9:26
  |
9 | use repark_distributed::{DistributedExecutor, JobStatus, LocalDataFusionExecutor};
  |                          ^^^^^^^^^^^^^^^^^^^  ^^^^^^^^^  ^^^^^^^^^^^^^^^^^^^^^^^ no `LocalDataFusionExecutor` in the root
  |                          |                    |
  |                          |                    no `JobStatus` in the root
  |                          no `DistributedExecutor` in the root

error: could not compile `repark-distributed` (test "local_executor") due to 1 previous error
```

Green after `executor.rs` + `local.rs` + `lib.rs` wiring:

```
running 3 tests
test status_is_queued_or_running_before_drain_and_completed_after ... ok
test range_1000_sum_through_local_executor_equals_direct_datafusion_answer ... ok
test cancel_on_a_long_range_returns_within_one_second_and_status_is_cancelled ... ok

test result: ok. 3 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.15s
```

## Coverage attestation (Actor, step 1)

```yaml
COVERAGE_ATTESTATION:
  pr_unit: ballista-m1-a
  categories:
    - id: AT-1
      status: ATTACKED
      evidence: D-3 trait and D-4 local executor walked against the three behavioral pins plus the cluster compile; range(1000) SUM equals the direct DataFusion collect of the same SQL; status is Queued/Running before drain and Completed after; cancel on range(1e8) returns under 1s with Cancelled.
      artifacts: [crates/repark-distributed/tests/local_executor.rs, crates/repark-distributed/src/executor.rs, crates/repark-distributed/src/local.rs]
    - id: AT-2
      status: ATTACKED
      evidence: The cancel pin uses a 1e8-row range so the job is still producing batches when cancel runs; the sum pin uses the card's range(1000); status is sampled before the first poll, after the first batch, and after end-of-stream.
      artifacts: [crates/repark-distributed/tests/local_executor.rs]
    - id: AT-3
      status: ATTACKED
      evidence: Stream errors mark Failed without overwriting Cancelled; unknown job ids return Error::DataFusion; cancel of a live job is idempotent on the cancelled flag and returns Ok. No spawn, so drop of the stream is DataFusion's abort.
      artifacts: [crates/repark-distributed/src/local.rs]
    - id: AT-4
      status: ATTACKED
      evidence: Job table is a Mutex never held across await; cancel uses an AtomicBool the stream poll checks before and after the inner poll so a concurrent drain cannot flip Cancelled back to Completed.
      artifacts: [crates/repark-distributed/src/local.rs]
    - id: AT-5
      status: N/A
      justification: In-process executor, no network, no auth, no path, no deserialization of untrusted bytes.
    - id: AT-6
      status: ATTACKED
      evidence: The sum pin compares whole RecordBatches against DataFusion's own collect of the same SQL, value and schema together.
      artifacts: [crates/repark-distributed/tests/local_executor.rs]
    - id: AT-7
      status: N/A
      justification: Cancel is a flag store plus a status write; no unbounded buffer. The 1e8-row pin is cancelled, not collected.
    - id: AT-8
      status: ATTACKED
      evidence: Errors fold through repark_core::engine_err; execute uses DataFusion execute_stream on the session TaskContext; cluster feature is compile-only this round with no Ballista API calls.
      artifacts: [crates/repark-distributed/src/local.rs, crates/repark-distributed/src/map.md]
    - id: AT-9
      status: ATTACKED
      evidence: Unknown-job errors name the job id; Failed stores the DataFusion error string; JobStatus is queryable by id after execute returns.
      artifacts: [crates/repark-distributed/src/local.rs]
    - id: AT-10
      status: ATTACKED
      evidence: Pins compiled red on the empty seed lib.rs (E0432) then ran green; assert_eq was replaced so the brief's clippy -D disallowed_methods gate on tests stays honest. Cluster proof is the feature build with no cluster module.
      artifacts: [crates/repark-distributed/tests/local_executor.rs, crates/repark-distributed/tests/map.md, crates/repark-distributed/src/map.md]
  complete: true
```
