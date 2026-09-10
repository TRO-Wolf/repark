# map — repark-distributed/src

## Purpose

Source for the distributed-execution crate. The crate seed (BALLISTA-M1-A step 0) carries an
empty `lib.rs`; step 1 adds `executor.rs` (the `DistributedExecutor` trait, `JobHandle`,
`JobStatus`) and `local.rs` (`LocalDataFusionExecutor`), and the later cards add `cluster.rs`,
`session_provider.rs`, `codec.rs` and `iceberg_provider.rs` behind the `cluster` feature.

## Contents

- `lib.rs` — the crate manifest: the module list and the public re-exports.
- `executor.rs` — D-3: `DistributedExecutor`, `JobHandle`, `JobId`, `JobStatus`.
  pins: ballista-m1-a/C-001
- `local.rs` — D-4: `LocalDataFusionExecutor` runs a plan on the session `SessionContext`
  in-process; `status` is `Completed`/`Failed` after the stream drains; `cancel` sets
  `Cancelled` and stops the stream. pins: ballista-m1-a/C-002, C-003
- No cluster module this round. `cargo build -p repark-distributed --features cluster`
  links the four Ballista crates with no cluster code. pins: ballista-m1-a/C-004

## Pointers

- Up: [../map.md](../map.md)
