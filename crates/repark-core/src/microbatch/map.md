# map — repark-core/src/microbatch

## Purpose

The Session-owned micro-batch driver (MB-3, release 1.7): one tracked task per streaming query,
one batch in flight, the `availableNow`, `Once` and processing-time triggers, CC-1's four
shutdown rules and the `StreamingQuery` progress surface, over MB-1's
[`MicroBatchSource`](../time_travel/microbatch_source.rs) and MB-2a's sink scope in
`repark-iceberg`. Owned by the [MB-3 order](../../../../task/wo/microbatch/mb-3-driver.md) under
the [design sketch](../../../../task/wo/microbatch/mb-design-2026-10-06.md) §3.5 and §3.6 and the
[North Star](../../../../task/roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md).
Progress: the [MB-3 ledger](../../../../task/ledgers/staging/mb-3-ledger.md).

## Contents

- `mod.rs` — `#![forbid(unsafe_code)]` (NS-17) and the module declarations. No re-exports:
  callers use full paths.
- `driver.rs` — the driver's types (`Trigger`, `QueryState`, `ShutdownOutcome`, `BatchBody`,
  `SinkSpec`, `StreamSpec`, `RecordedLocation`), the `StreamingQueryManager` and the
  `QueryHandle`. **The Session seam (sketch §3.5):** `StreamingQueryManager::of` installs the
  manager lazily as a DataFusion config extension through `Session::context()`
  (`state_ref().write().config_mut().set_extension`), so `session.rs` does not grow; the check
  and the install run under one write lock. `register` resolves the sink through the session's
  catalog registry, loads it, and derives the `QueryId` from the sink's table uuid and the
  `queryName` (`QueryId::derive`); it does not start the query (CC-1 rule 1). Registering is
  async because the identity needs the sink's uuid, a dated difference from the sketch's
  synchronous signature (ledger D-1). `RecordedLocation` has no accessor that returns its text,
  and its `Debug` prints `RecordedLocation(<redacted>)`.
  pins: mb-3/C-003
- `driver_tests.rs` — the driver's pins (`#[cfg(test)] #[path]` from `driver.rs`).
- `testing.rs` — the test fixture: a session over a memory catalog with the `sales.orders`
  source, the `sales.silver` sink and a spare `sales.other` table.
