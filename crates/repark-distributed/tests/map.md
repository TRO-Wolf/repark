# map — repark-distributed/tests

## Purpose

Integration pins for the local DataFusion executor (BALLISTA-M1-A step 1) and the in-process
cluster executor (BALLISTA-M1-B step 1). The crate-root `lib.rs` gate forbids inline
`#[cfg(test)]` modules, so the pins live here.

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

## Pointers

- Up: [../map.md](../map.md)
