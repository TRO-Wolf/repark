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

- `mod.rs` — `#![forbid(unsafe_code)]` (NS-17) and the module declarations: `pub mod driver;`,
  `pub mod progress;`, the private `run`, and the test-only modules. No re-exports: callers use full paths.
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
  **Slice 2 (2026-10-07):** the lifecycle. `start` refuses a sink on a local filesystem catalog
  (`LocationPolicy::TempFallbackAllowed`, MBE-8), a second start and a second active query with
  the same id, then spawns the query's one tracked task (`#[expect(clippy::disallowed_methods)]`
  with its lifecycle stated) and keeps its `AbortHandle`. `stop` wakes the trigger wait, waits up
  to `stopTimeout` (none or zero waits forever) for the in-flight batch and returns the
  `ShutdownOutcome` with the durable `SinkRecord`; past the timeout it aborts the task, re-reads
  the sink and yields `RecoveryRequired(StopTimeout)` (CC-1 rules 2 and 3). Dropping a handle
  never stops the query (rule 4): the manager keeps it until it terminates. Every lock is a
  `std::sync::Mutex` held for one read or write, never across an await.
  pins: mb-3/C-004, C-005
- `run.rs` — the driver task. It resumes from the sink alone (`read_resume_point`: the next epoch,
  the recorded offset and generation; another recorded input refuses `InputsChanged`), then runs
  one batch in flight per trigger: `availableNow` fixes its end with the uncapped walk at start and
  drains capped batches up to it, `Once` runs one uncapped batch, and a processing-time trigger runs
  back to back at `ProcessingTime(0)` (an idle trigger waits `pollingDelay`) or once per interval.
  Each batch re-reads the resume point (an epoch already durable under this run is skipped, under
  another run it is `Fenced`), enters the `BatchScope`, runs the door, and requires the scope's
  outcome to be `Committed`. The `toTable` door writes through a private batch session cloned from
  the session state with the scope token installed as
  `spark.sql.iceberg.snapshot-property.repark.cdc.scope-token` (MB-2a D-10, ledger D-3), and an
  unknown commit outcome goes to `resolve_unknown_outcome`. The `foreachBatch` door runs the body on
  the user's session without the token, then stamps once through `commit_stamp_only` (ledger D-2).
  pins: mb-3/C-004, C-005
- `progress.rs` — the `StreamingQuery` progress surface (sketch §3.6, MB0-T3):
  `StreamingQueryProgress`, `DurationMs`, `SourceProgress`, `SinkProgress`, `QueryStatus` and
  `StatusMessage`, serialised with T3's camelCase names, and the crate-private `ProgressLog` (the
  `recentProgress` ring, the status, and Spark's `ProgressReporter` rates and idle-progress
  throttle). The driver records one report per trigger; a draining trigger reports no trailing
  idle progress after a batch (MB0b-R14, R15).
  pins: mb-3/C-006
- `progress_tests.rs` — the progress pins (`#[cfg(test)] #[path]` from `progress.rs`).
  pins: mb-3/C-006
- `driver_tests.rs` — the driver's pins (`#[cfg(test)] #[path]` from `driver.rs`).
- `run_tests.rs` — the trigger-loop and lifecycle pins (`#[cfg(test)] #[path]` from `run.rs`).
  pins: mb-3/C-004
- `foreach_tests.rs` — the `foreachBatch` door and shutdown pins, with a Rust `BatchBody` that
  writes the sink through the session's resolved write options.
  pins: mb-3/C-005
- `testing.rs` — the test fixture: a session over a memory catalog with the `sales.orders`
  source, the `sales.silver` sink and a spare `sales.other` table.
