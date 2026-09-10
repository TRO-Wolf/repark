# Unit ledger — BALLISTA-M1-B · the cluster executor (step 2)

**Retires:** this ledger moves to `../completed/` in this unit's last commit.
This file closes when BALLISTA-M1-B merges, or when the owner closes the slate row.

**Unit:** BALLISTA-M1-B step 2 · **Date:** 2026-09-10 · **Executor:** Grok (grok-4.6), Actor, step 2 ·
**Branch:** `feat/ballista-m1-b` · **Base:** `2bcf53c5`
**Model:** grok-4.6
**risk_tier:** standard.
**Path:** STANDARD.

Step 1 landed `cluster.rs` (D-1), `session_provider.rs` (D-2 seat / D-4 table), `codec.rs`
(D-3 wrapper), and the two-executor pin (`2bcf53c5`). Step 2 adds the UDF-on-executor pin
(C-003), the cancel path (C-005), and C-006 (codec install + shuffle-through-codecs). C-004
(round-trip serde over the five shuffle nodes) stays **OPEN / PARKED**.

**Not in this unit:** Iceberg providers, `STATUS.md`, `briefs/next-sequence.md`, `.github/`,
`Cargo.toml`, `Cargo.lock`, `datafusion-proto`.

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `ReparkClusterExecutor` with one scheduler and two in-process executors on ephemeral ports answers `SELECT sum(x) FROM t` equal to `LocalDataFusionExecutor`, both executors run at least one task, and `status` walks Queued → Running → Completed. | `two_executors_sum_matches_local_and_status_walks_queued_running_completed` in `crates/repark-distributed/tests/cluster_two_executors.rs`. | **PROVEN** | Red first on the M1-A base tree (`f09ac97b`): `cargo test -p repark-distributed --features cluster --offline --test cluster_two_executors` compile exit 101, `error[E0432]: unresolved imports ... ReparkClusterExecutor, ReparkSessionProvider`. Green after: that test `ok` in 0.33 s. pins: ballista-m1-b/C-001 |
| C-002 | The test data is an in-memory table `t` registered through `ReparkSessionProvider` on the session used to build every executor. | Same pin: `register_table_t` then `ReparkSessionProvider::from_session`. | **PROVEN** | Same red compile. Green: the pin builds the provider from a RePark session that already has `t`. pins: ballista-m1-b/C-002 |
| C-003 | A RePark UDF registered through the session provider resolves on an executor. A cluster whose provider is a vanilla DataFusion session fails to resolve the same UDF. | `repark_udf_registered_through_session_provider_resolves_on_two_executors` in `crates/repark-distributed/tests/cluster_two_executors.rs`. | **PROVEN** | Red first: the new pin's negative arm panicked because `execute` returned `Ok` and the stream error was treated as "resolved". Stream error was `PhysicalExtensionCodec is not provided for scalar function repark_times_ten`. Green after asserting empty batches + error names `repark_times_ten`. Positive arm: `SELECT sum(repark_times_ten(x)) FROM t` equals the local executor. `ReparkClusterExecutor::new` requires a provider, so "without the provider" is a vanilla `SessionContext` provider. pins: ballista-m1-b/C-003 |
| C-004 | The codec wrapper forwards the five Ballista shuffle node types and registers no RePark write or commit node; a round-trip serde test over every default node survives an upgrade. | Round-trip pin over every default Ballista shuffle node. | **OPEN** (PARKED) | Orchestrator 2026-09-10: do not add `datafusion-proto`. Ballista 54.1.0 does not re-export `PhysicalExtensionCodec`; the wrapper cannot `impl` the trait. Residue unchanged from step 1. Owner question in `crates/repark-distributed/src/map.md`. |
| C-005 | A long cluster query cancelled mid-flight: `status` becomes `Cancelled`, and no executor is left running a task after 5 s. | `cancel_mid_flight_sets_cancelled_and_no_running_tasks_within_five_seconds`. | **PROVEN** | Red first: `error[E0599]: no method named \`running_executor_task_counts\` found for struct \`ReparkClusterExecutor\``. Green after the method: that test `ok`. Bounded poll, no `#[ignore]`. pins: ballista-m1-b/C-005 |
| C-006 | `repark_ballista_codec()` installs Ballista's own physical and logical codecs into `BallistaCodec::new`, and the two-executor job (which shuffles through those codecs) completes. | `repark_ballista_codec_installs_ballista_default_physical_and_logical_codecs` plus `two_executors_sum_matches_local_and_status_walks_queued_running_completed`. | **PROVEN** | Codec Debug of the installed codecs equals `Repark*ExtensionCodec::into_inner()` (Ballista defaults). Two-executor job already green from step 1; re-run `ok` this step. pins: ballista-m1-b/C-006 |

VERDICT: 6 clauses, 5 PROVEN, 1 OPEN (C-004 PARKED), 0 REJECTED.

## Red-first evidence (step 1, 2026-09-10)

Command: `cargo test -p repark-distributed --features cluster --offline --test cluster_two_executors` on the M1-A tree (no cluster modules). Compile failed.

```
error[E0432]: unresolved imports `repark_distributed::ReparkClusterExecutor`, `repark_distributed::ReparkSessionProvider`
 --> crates/repark-distributed/tests/cluster_two_executors.rs:17:62
  |
17 |     DistributedExecutor, JobStatus, LocalDataFusionExecutor, ReparkClusterExecutor,
   |                                                              ^^^^^^^^^^^^^^^^^^^^^ no `ReparkClusterExecutor` in the root
18 |     ReparkSessionProvider,
   |     ^^^^^^^^^^^^^^^^^^^^^ no `ReparkSessionProvider` in the root

error: could not compile `repark-distributed` (test "cluster_two_executors") due to 1 previous error
```

Green after `cluster.rs` + `session_provider.rs` + `codec.rs` + `lib.rs` wiring:

```
running 1 test
test two_executors_sum_matches_local_and_status_walks_queued_running_completed ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.33s
```

## Red-first evidence (step 2, 2026-09-10)

C-005 compile red, method not yet on `ReparkClusterExecutor`:

```
error[E0599]: no method named `running_executor_task_counts` found for struct `ReparkClusterExecutor` in the current scope
   --> crates/repark-distributed/tests/cluster_two_executors.rs:359:37
    |
359 |         let running = match cluster.running_executor_task_counts(job).await {
    |                                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
```

C-003 negative-arm red (positive arm already matched the local executor). First assertion treated a stream error as "resolved":

```
thread 'repark_udf_registered_through_session_provider_resolves_on_two_executors' panicked at crates/repark-distributed/tests/cluster_two_executors.rs:302:13:
vanilla cluster resolved repark_times_ten; batches=[] errors=["Execution error: job LbESGLO failed: Job failed due to stage 1 failed: Task failed due to runtime execution error: DataFusionError(NotImplemented(\"PhysicalExtensionCodec is not provided for scalar function repark_times_ten\"))\n"]
test result: FAILED. 3 passed; 1 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.42s
```

Green after `running_executor_task_counts` plus the negative-arm stream-error assert:

```
running 4 tests
test repark_ballista_codec_installs_ballista_default_physical_and_logical_codecs ... ok
test cancel_mid_flight_sets_cancelled_and_no_running_tasks_within_five_seconds ... ok
test two_executors_sum_matches_local_and_status_walks_queued_running_completed ... ok
test repark_udf_registered_through_session_provider_resolves_on_two_executors ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.37s
```

## API residue versus the 2026-09-08 audit

- Audit §26.E names a `SessionProvider` hook at `scheduler_server/mod.rs` line 65. Ballista 54.1.0 exports `SessionBuilder` (`Arc<dyn Fn(SessionConfig) -> Result<SessionState> + Send + Sync>`) at that seat. The card said the API wins; `ReparkSessionProvider` fills `SessionBuilder`.
- Audit R-1's delegating wrapper would `impl PhysicalExtensionCodec`. That trait lives in `datafusion-proto`, which is not a direct `repark-distributed` dependency and this unit cannot add one. The wrapper newtype holds `BallistaPhysicalExtensionCodec` and is installed via `BallistaCodec::new`. Orchestrator 2026-09-10 PARKED C-004 on that residue. C-006 pins the install + shuffle path that does not need the trait. Owner question (both options, no choice): M1-C takes a `datafusion-proto` dev-dependency and the wrapper becomes a real delegating codec with the round-trip pin, or the wrapper is deleted and RePark uses Ballista's codec until a RePark node needs serialising.
- `ReparkClusterExecutor::new` requires `ReparkSessionProvider`. The C-003 negative case is a provider built from `SessionContext::new()` (no RePark UDF). Submit succeeds; the executor decode fails with `PhysicalExtensionCodec is not provided for scalar function repark_times_ten`.
