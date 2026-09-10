# map — repark-distributed/src

## Purpose

Source for the distributed-execution crate. The crate seed (BALLISTA-M1-A step 0) carries an
empty `lib.rs`; step 1 adds `executor.rs` (the `DistributedExecutor` trait, `JobHandle`,
`JobStatus`) and `local.rs` (`LocalDataFusionExecutor`), and the later cards add `cluster.rs`,
`session_provider.rs`, `codec.rs` and `iceberg_provider.rs` behind the `cluster` feature.

## Contents

- `lib.rs` — the crate manifest: the module list and the public re-exports.

## Pointers

- Up: [../map.md](../map.md)
