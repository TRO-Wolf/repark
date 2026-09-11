# map — repark-distributed/src

## Purpose

Source for the distributed-execution crate. The crate seed (BALLISTA-M1-A step 0) carries an
empty `lib.rs`; step 1 adds `executor.rs` (the `DistributedExecutor` trait, `JobHandle`,
`JobStatus`) and `local.rs` (`LocalDataFusionExecutor`). BALLISTA-M1-B step 1 adds
`cluster.rs`, `session_provider.rs` and `codec.rs` behind the `cluster` feature. Step 2
adds `running_executor_task_counts` on the cluster executor. BALLISTA-M1-D step 1 adds
`iceberg_provider.rs` (`IcebergScanSpec` provider codec) behind the same feature.
BALLISTA-M2-B adds `predicate_expr.rs` (typed `Predicate` → `Expr`) behind that feature.

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
- `codec.rs` (`cluster` feature) — BALLISTA-M2-B D-1/D-3: `ReparkPhysicalExtensionCodec`
  still delegates every unowned node to `BallistaPhysicalExtensionCodec`. Encode of
  `IcebergTableScan` downcasts the node (`node.as_any` / `downcast_ref`) and reads the
  spec from typed accessors (`table().identifier()`, `resolved_snapshot_id()`,
  `projection()`, `predicates()`). The encode-time rebuild-and-compare guard is retired.
  Decode still rebuilds through `IcebergScanSpec::scan` on the codec-carried session
  context and refuses when the rebuilt frozen snapshot differs from the spec's.
  Catalog spec recovery stays a session-catalog probe (`session_catalog_spec` /
  `catalog_spec_from_debug`): `IcebergTableScan` / `Table` have no catalog-spec accessor
  at this fork pin (measured 2026-09-11). Wire `RPIC` version is 2 so a v1 payload
  refuses on decode. pins: ballista-m2-b/C-001, C-004, ballista-m2-a/C-001, C-003,
  ballista-m1-b/C-006
- `predicate_expr.rs` (`cluster` feature) — BALLISTA-M2-B D-2: typed
  `iceberg::expr::Predicate` → `datafusion::logical_expr::Expr` (And/Or/Not, unary
  IsNull/NotNull/IsNan/NotNan, binary comparisons including StartsWith/NotStartsWith,
  In/NotIn, Datum → ScalarValue for the fork's primitive pushdown types). An
  inexpressible shape (AlwaysTrue/AlwaysFalse, AboveMax/BelowMin, Unknown, a StartsWith
  prefix carrying LIKE wildcards) refuses loud naming the shape. The Expr travels as
  `datafusion-proto` bytes. pins: ballista-m2-b/C-002
- `iceberg_provider.rs` (`cluster` feature) — BALLISTA-M1-D D-1 plus M2-B: `IcebergScanSpec`
  serialises catalog config, table identifier, snapshot id, projection, and either SQL
  filter text (local constructor) or typed Expr bytes (codec path) as `RPIC` v2. Rebuild
  looks up the Iceberg table from the session catalog. File-group rewrite stays the
  fallback for a node with no codec entry. pins: ballista-m1-d/C-001, C-002, ballista-m2-b/C-001, C-004

## Pointers

- Up: [../map.md](../map.md)
- M1 record: [../../../docs/design/distributed-m1.md](../../../docs/design/distributed-m1.md)
  (Iceberg reads, success list line 17, open question 4).
