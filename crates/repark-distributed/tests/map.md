# map — repark-distributed/tests

## Purpose

Integration pins for the local DataFusion executor (BALLISTA-M1-A step 1). The crate-root
`lib.rs` gate forbids inline `#[cfg(test)]` modules, so the pins live here.

## Contents

- `local_executor.rs` — three pins against `LocalDataFusionExecutor`:
  `range(1000)` sum equals the direct DataFusion collect (C-001);
  `status` is `Queued`/`Running` before drain and `Completed` after (C-002);
  `cancel` on `range(100000000)` returns within 1 s and status is `Cancelled` (C-003).
  pins: ballista-m1-a/C-001, C-002, C-003

## Pointers

- Up: [../map.md](../map.md)
