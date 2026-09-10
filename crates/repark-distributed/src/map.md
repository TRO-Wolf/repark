# map — repark-distributed/src

## Purpose

Source for the distributed-execution crate. The crate seed (BALLISTA-M1-A step 0) carries an
empty `lib.rs`; step 1 adds `executor.rs` (the `DistributedExecutor` trait, `JobHandle`,
`JobStatus`) and `local.rs` (`LocalDataFusionExecutor`). BALLISTA-M1-B step 1 adds
`cluster.rs`, `session_provider.rs` and `codec.rs` behind the `cluster` feature.

## Contents

- `lib.rs` — the crate manifest: the module list and the public re-exports.
- `executor.rs` — D-3: `DistributedExecutor`, `JobHandle`, `JobId`, `JobStatus`.
  pins: ballista-m1-a/C-001
- `local.rs` — D-4: `LocalDataFusionExecutor` runs a plan on the session `SessionContext`
  in-process; `status` is `Completed`/`Failed` after the stream drains; `cancel` sets
  `Cancelled` and stops the stream. pins: ballista-m1-a/C-002, C-003
- `cluster.rs` (`cluster` feature) — D-1: `ReparkClusterExecutor` starts one in-process
  scheduler and N executors on ephemeral ports; `execute` submits a physical plan; `status`
  walks Queued → Running → Completed; `executor_task_counts` reads the scheduler job graph.
  pins: ballista-m1-b/C-001
- `session_provider.rs` (`cluster` feature) — D-2 seat / D-4: `ReparkSessionProvider` builds
  every executor `SessionState` from a RePark session (catalog, UDFs, analyzer rules). The
  two-executor pin registers in-memory table `t` here. pins: ballista-m1-b/C-002
- `codec.rs` (`cluster` feature) — D-3: `ReparkPhysicalExtensionCodec` /
  `ReparkLogicalExtensionCodec` wrap the Ballista defaults (five shuffle nodes, no RePark
  write/commit node) and `repark_ballista_codec()` installs them. Round-trip pin is step 2.
  pins: ballista-m1-b/C-004

## Pointers

- Up: [../map.md](../map.md)
