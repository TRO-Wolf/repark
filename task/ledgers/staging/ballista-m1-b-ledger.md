# Unit ledger — BALLISTA-M1-B · the cluster executor (step 1)

**Retires:** this ledger moves to `../completed/` in this unit's last commit.
This file closes when BALLISTA-M1-B merges, or when the owner closes the slate row.

**Unit:** BALLISTA-M1-B step 1 · **Date:** 2026-09-10 · **Executor:** Grok (grok-4.6), Actor, step 1 ·
**Branch:** `feat/ballista-m1-b` · **Base:** `f09ac97b`
**Model:** grok-4.6
**risk_tier:** standard.
**Path:** STANDARD.

Step 1 lands `cluster.rs` (D-1), `session_provider.rs` (D-2 seat / D-4 table), `codec.rs`
(D-3 wrapper), and the two-executor pin. Step 2 (separate commit) adds the UDF-on-executor
pin, the codec round-trip pin, and the cancel path.

**Not in this unit:** Iceberg providers, `STATUS.md`, `briefs/next-sequence.md`, `.github/`,
`Cargo.toml`, `Cargo.lock`.

## Proposition ledger

| Clause | Proposition (checkable) | Proof obligation | Verdict | Evidence / open question |
|---|---|---|---|---|
| C-001 | `ReparkClusterExecutor` with one scheduler and two in-process executors on ephemeral ports answers `SELECT sum(x) FROM t` equal to `LocalDataFusionExecutor`, both executors run at least one task, and `status` walks Queued → Running → Completed. | `two_executors_sum_matches_local_and_status_walks_queued_running_completed` in `crates/repark-distributed/tests/cluster_two_executors.rs`. | **PROVEN** | Red first on the M1-A base tree (`f09ac97b`): `cargo test -p repark-distributed --features cluster --offline --test cluster_two_executors` compile exit 101, `error[E0432]: unresolved imports ... ReparkClusterExecutor, ReparkSessionProvider`. Green after: that test `ok` in 0.33 s. pins: ballista-m1-b/C-001 |
| C-002 | The test data is an in-memory table `t` registered through `ReparkSessionProvider` on the session used to build every executor. | Same pin: `register_table_t` then `ReparkSessionProvider::from_session`. | **PROVEN** | Same red compile. Green: the pin builds the provider from a RePark session that already has `t`. pins: ballista-m1-b/C-002 |
| C-003 | A RePark UDF registered through the session provider resolves on an executor. | Step 2 UDF-on-executor pin. | **OPEN** | D-2's UDF pin is step 2. The `SessionProvider` seat is filled this step as `ReparkSessionProvider` (`SessionBuilder` in Ballista 54.1.0; the audit's `SessionProvider` name is not a 54.1.0 type). |
| C-004 | The codec wrapper forwards the five Ballista shuffle node types and registers no RePark write or commit node; a round-trip serde test over every default node survives an upgrade. | Step 2 round-trip pin. | **OPEN** | `ReparkPhysicalExtensionCodec` / `ReparkLogicalExtensionCodec` wrap the Ballista defaults and `repark_ballista_codec()` installs them. Round-trip pin is step 2. Residue: Ballista 54.1.0 does not re-export `PhysicalExtensionCodec`; `datafusion-proto` is not a direct crate dep and this unit cannot add one, so the wrapper cannot `impl` the trait. The seat is occupied by passing the inner `BallistaPhysicalExtensionCodec` into `BallistaCodec::new`. |

VERDICT: 4 clauses, 2 PROVEN, 2 OPEN, 0 REJECTED.

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

## API residue versus the 2026-09-08 audit

- Audit §26.E names a `SessionProvider` hook at `scheduler_server/mod.rs` line 65. Ballista 54.1.0 exports `SessionBuilder` (`Arc<dyn Fn(SessionConfig) -> Result<SessionState> + Send + Sync>`) at that seat. The card said the API wins; `ReparkSessionProvider` fills `SessionBuilder`.
- Audit R-1's delegating wrapper would `impl PhysicalExtensionCodec`. That trait lives in `datafusion-proto`, which is not a direct `repark-distributed` dependency and this unit cannot add one. The wrapper newtype holds `BallistaPhysicalExtensionCodec` and is installed via `BallistaCodec::new`.
