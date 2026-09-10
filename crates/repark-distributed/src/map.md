# map — repark-distributed/src

## Purpose

Source for the distributed-execution crate. The crate seed (BALLISTA-M1-A step 0) carries an
empty `lib.rs`; step 1 adds `executor.rs` (the `DistributedExecutor` trait, `JobHandle`,
`JobStatus`) and `local.rs` (`LocalDataFusionExecutor`). BALLISTA-M1-B step 1 adds
`cluster.rs`, `session_provider.rs` and `codec.rs` behind the `cluster` feature. Step 2
adds `running_executor_task_counts` on the cluster executor. BALLISTA-M1-D step 1 adds
`iceberg_provider.rs` (`IcebergScanSpec` provider codec) behind the same feature.

## Contents

- `lib.rs` — the crate manifest: the module list and the public re-exports.
- `executor.rs` — D-3: `DistributedExecutor`, `JobHandle`, `JobId`, `JobStatus`,
  `StageMetrics`. `Completed` carries per-stage rows, shuffle bytes, wall time, attempt
  number, and `retried_stages`. pins: ballista-m1-a/C-001, ballista-m1-c/C-003
- `local.rs` — D-4: `LocalDataFusionExecutor` runs a plan on the session `SessionContext`
  in-process; `status` is `Completed { stages: [], retried_stages: 0 }` / `Failed` after
  the stream drains; `cancel` sets `Cancelled` and stops the stream.
  pins: ballista-m1-a/C-002, C-003
- `cluster.rs` (`cluster` feature) — D-1: `ReparkClusterExecutor` starts one in-process
  scheduler and N executors on ephemeral ports; `execute` submits a physical plan; `status`
  walks Queued → Running → `Completed { stages, retried_stages }` (per-stage rows, shuffle
  bytes, wall time, and attempt number from the execution graph's `ShuffleWritePartition`
  and `TaskInfo` times; `retried_stages` counts stages with `stage_attempt_num > 0`).
  `executor_task_counts` reads the scheduler job graph;
  `running_executor_task_counts` counts only in-flight tasks (or reports the job still
  queued/running when the graph is not yet in completed state).
  pins: ballista-m1-b/C-001, C-005, ballista-m1-c/C-003
- `session_provider.rs` (`cluster` feature) — D-2 seat / D-4: `ReparkSessionProvider` builds
  every executor `SessionState` from a RePark session (catalog, UDFs, analyzer rules). The
  two-executor pin registers in-memory table `t` here; the UDF pin registers `repark_times_ten`
  on that same session so the executor registry has it.
  pins: ballista-m1-b/C-002, C-003
- `codec.rs` (`cluster` feature) — D-3: `ReparkPhysicalExtensionCodec` /
  `ReparkLogicalExtensionCodec` wrap the Ballista defaults and `repark_ballista_codec()`
  installs those inner codecs into `BallistaCodec::new`. The wrappers add no encode/decode
  behaviour today. Owner question (do not choose here): either BALLISTA-M1-C takes a
  `datafusion-proto` dev-dependency and the wrapper becomes a real delegating codec with a
  round-trip pin over the five shuffle nodes, or the wrapper is deleted and RePark uses
  Ballista's codec until a RePark plan node needs serialising. C-004 stays OPEN / PARKED
  until that choice; C-006 pins what is true without the trait.
  pins: ballista-m1-b/C-006
- `iceberg_provider.rs` (`cluster` feature) — BALLISTA-M1-D D-1: `IcebergScanSpec` is the
  DataFusion-level provider codec (audit R-4). It serialises
  `(catalog config, table identifier, snapshot id, projection, filters)` as `RPIC` bytes
  and rebuilds the Iceberg `TableProvider` from the session catalog (never ambient
  authority). `IcebergTableScan` cannot travel through Ballista 54.1.0 without
  `PhysicalExtensionCodec` (`datafusion-proto`, the same wall as M1-B C-004); scans
  distribute as parquet file groups rewritten from the Iceberg `$files` list. Writes
  and a commit coordinator are out of scope (ADR-0004 / Milestone 3).
  pins: ballista-m1-d/C-001, C-002

## Pointers

- Up: [../map.md](../map.md)
