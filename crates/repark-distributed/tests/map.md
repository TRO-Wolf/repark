# map — repark-distributed/tests

## Purpose

Integration pins for the local DataFusion executor (BALLISTA-M1-A step 1), the in-process
cluster executor (BALLISTA-M1-B), multi-stage cluster shapes (BALLISTA-M1-C step 1), and
Iceberg reads through the executors (BALLISTA-M1-D step 1). The crate-root `lib.rs` gate
forbids inline `#[cfg(test)]` modules, so the pins live here.

## Contents

- `local_executor.rs` — three pins against `LocalDataFusionExecutor`:
  `range(1000)` sum equals the direct DataFusion collect (C-001);
  `status` is `Queued`/`Running` before drain and `Completed` after (C-002);
  `cancel` on `range(100000000)` returns within 1 s and status is `Cancelled` (C-003).
  pins: ballista-m1-a/C-001, C-002, C-003
- `cluster_two_executors.rs` (`feature = "cluster"`) — one scheduler plus two in-process
  executors: `SELECT sum(x) FROM t` equals the local executor; both executors ran at least
  one task; `status` walks Queued → Running → Completed. Table `t` is an in-memory table
  registered through `ReparkSessionProvider`. Step 2 adds: `repark_times_ten` registered on
  the RePark session resolves on the executors and equals the local answer; a cluster whose
  provider is a vanilla `SessionContext` fails to resolve the same UDF (stream error names
  `repark_times_ten`); cancel of `range(100000000)` mid-flight sets `Cancelled` and
  `running_executor_task_counts` reaches empty within 5 s; `repark_ballista_codec()` Debug
  matches the wrapped Ballista default codecs.
  pins: ballista-m1-b/C-001, C-002, C-003, C-005, C-006
- `multi_stage.rs` (`feature = "cluster"`) — D-1: three physical-plan shapes, each built
  once, run through `LocalDataFusionExecutor`, then through a two-executor
  `ReparkClusterExecutor`, compared after sorting rows (cluster partition order is not
  a contract). Hash aggregate over a 4-partition `sales` table (Partial+Final,
  `target_partitions=4`); hash join of `left_t`/`right_t` with CollectLeft thresholds
  zeroed so both sides `RepartitionExec`; sort-merge join of the same tables with
  `prefer_hash_join=false` and a `RepartitionExec` on both children.
  Step 2 adds: `Completed` on the two-stage hash aggregate reports per-stage rows and
  shuffle bytes > 0; the session spill directory has no `data*.arrow` shuffle files after
  that job completes and after a long-range cancel.
  pins: ballista-m1-c/C-001, C-003, C-004
- `iceberg_scan.rs` (`feature = "cluster"`) — BALLISTA-M1-D: `IcebergScanSpec` round-trip
  of catalog config, table identifier, snapshot id, projection, and filters; truncated
  payload refuses; rebuild of the Iceberg provider from a RePark session that registered
  the memory catalog, with a vanilla `SessionContext` refusing (no ambient catalog);
  two-executor pin: a memory-catalog table with 8 files answers the same `count(*)`
  (with `id + 0 >= 0` so stats cannot constant-fold), `sum(id)`, and `id >= 4` filter
  as `LocalDataFusionExecutor`, and both executors ran a task. Cluster plans rewrite
  `IcebergTableScan` to parquet file groups because Ballista cannot encode that node
  without `datafusion-proto`.
  pins: ballista-m1-d/C-001, C-002, C-003

## Pointers

- Up: [../map.md](../map.md)
- M1 record: [../../../docs/design/distributed-m1.md](../../../docs/design/distributed-m1.md)
