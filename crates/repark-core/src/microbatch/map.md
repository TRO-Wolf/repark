# map — repark-core/src/microbatch

## Purpose

The Session-owned micro-batch driver (MB-3, release 1.7): one tracked task per streaming query,
one batch in flight, the `availableNow`, `Once` and processing-time triggers, CC-1's four
shutdown rules and the `StreamingQuery` progress surface, over MB-1's
[`MicroBatchSource`](../time_travel/microbatch_source.rs) and MB-2a's sink scope in
`repark-iceberg`. Owned by the [MB-3 order](../../../../task/wo/microbatch/mb-3-driver.md) under
the [design sketch](../../../../task/wo/microbatch/mb-design-2026-10-06.md) §3.5 and §3.6 and the
[North Star](../../../../task/roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md).
Progress: the [MB-3 ledger](../../../../task/ledgers/staging/mb-3-ledger.md). The round's gates,
the crash gate's 3 passed and 2 ignored included, are recorded there. pins: mb-3/C-009

## Contents

- `mod.rs` — `#![forbid(unsafe_code)]` (NS-17) and the module declarations: `pub mod driver;`,
  `pub mod progress;`, `pub mod relation;`, the private `run`, and the test-only modules. No re-exports: callers use full paths.
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
  **Fold 1 (2026-10-07):**
  - *One lifecycle lock.* The not-yet-started work (`Pending`) lives inside `Lifecycle`, so
    `start` (take the work, admit, set `Running`, spawn, keep the `AbortHandle`) and `stop` on a
    registered query (drop the work, set `Stopped`) are each one transition under the same
    lock. There is no window in which `stop` reports `Stopped` and `start` then spawns. Lock
    order: the query's own lifecycle, then the manager's registry, then the lifecycle of a
    query already in the registry. `start` holds its own lifecycle while it is admitted, and
    a query is in the registry only after that, so no path takes the registry while it holds
    the lifecycle of a registered query. pins: mb-3/C-012
  - *A panic ends the query `Failed`.* `Run::drive` catches an unwind of the whole trigger
    loop (`catch_unwind` around the loop's future, not around one call), and
    `QueryShared::panicked` turns the payload into `BatchFailed { epoch, cause }` with the
    in-flight epoch, or the next one when the panic came before a batch began. The outcome,
    the done signal and the freed query id then follow the body-error path. pins: mb-3/C-011
  - *One active query per sink per session.* `admit` refuses `SinkBusy` at start when an
    active query of this session already targets the sink's table uuid (registry row
    `MB-3-SINK-BUSY-1`); the refused query keeps its work and can start later.
    pins: mb-3/C-016
  - *The session's end stops its queries.* An explicit session stop is
    `StreamingQueryManager::stop_all`, which stops each active query and waits for it as
    `stop` does. A session dropped without a stop is seen through a `Weak` to the session's
    catalog registry (`Session::catalogs`, an `Arc` only the session's handles hold): every
    stop check reads it, and the trigger wait and the scope wait poll it every 100 ms
    (`SESSION_WATCH`), so each query ends `Stopped` after its in-flight batch and the task
    drops the source, the context and the catalog handles. A poll, not a drop hook, because
    `MicroBatchSource` owns a `SessionContext` clone, which keeps the session state and
    with it the manager alive for as long as the task runs, so the manager's `Drop` cannot
    fire first; `session.rs` is not edited (ledger D-11). A handle the caller still holds
    keeps the sink's catalog handle until it is dropped. pins: mb-3/C-013
  - `StreamSpec` gains `catalog_timeout` (`DEFAULT_CATALOG_TIMEOUT`, 60 s), today the bound on
    the scope wait only (ledger C-010 stays open for the per-call bound).
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
  **Fold 1 (2026-10-07):**
  - *The schedule.* With an interval the next trigger starts at the next wall-clock multiple
    of the interval after the trigger began (`until_next_trigger`, Spark's
    `ProcessingTimeExecutor.nextBatchTime`: `now / interval * interval + interval`, in epoch
    milliseconds); a batch that overruns the boundary is followed at once by the next
    trigger. pins: mb-3/C-017
  - *The scope wait.* `enter_scope` loads the sink and enters the `BatchScope`; when another
    query in the process holds the sink's scope (two sessions on one sink), it waits
    `pollingDelay`, reloads the sink and tries again, up to `catalog_timeout`, and only then
    fails `SinkBusy`. The sink is loaded again after each wait because the other query's
    commit moved it, and the resume-point check runs after the scope is held. A stop or a
    dropped session ends the wait with no batch run. pins: mb-3/C-016
  - *The replay window is not durable.* A restart plans the failed batch's window again
    (registry row `MB-3-REPLAY-WINDOW-1`). pins: mb-3/C-014
- `progress.rs` — the `StreamingQuery` progress surface (sketch §3.6, MB0-T3):
  `StreamingQueryProgress`, `DurationMs`, `SourceProgress`, `SinkProgress`, `QueryStatus` and
  `StatusMessage`, serialised with T3's camelCase names, and the crate-private `ProgressLog` (the
  `recentProgress` ring, the status, and Spark's `ProgressReporter` rates and idle-progress
  throttle). The driver records one report per trigger; a draining trigger reports no trailing
  idle progress after a batch (MB0b-R14, R15).
  pins: mb-3/C-006
- `progress_tests.rs` — the progress pins (`#[cfg(test)] #[path]` from `progress.rs`).
  pins: mb-3/C-006
- `relation.rs` — the streaming frame and the plan template (sketch §3.5, Q3, MBE-6):
  `streaming_frame` returns a frame over a `StreamingRelation` placeholder whose scan refuses a
  batch action; `PlanTemplate::from_frame` refuses a stateful operator above the stream
  (aggregation, `dropDuplicates`, sort, global limit, window function, stream-stream join) and
  accepts a static side; `bind` swaps each batch's provider in for the placeholder;
  `check_output_mode` refuses `complete` and `update`; `explain` renders the source, its reader
  options and the plan.
  pins: mb-3/C-008
  **Fold 1 (2026-10-07):** the template runs once per batch, so a shape whose output keeps
  rows of a static frame would land them again on every batch. `static_side_operator` refuses
  what Spark's `UnsupportedOperationChecker` refuses (cell MB3-J1): a union of the stream and a
  static frame, a full outer join, an outer join whose preserved side is the static frame, and
  a semi or anti join whose output side is the static frame (DataFusion's `LeftMark` and
  `RightMark` follow the semi rule). A streaming frame inside a subquery expression refuses
  too (unmeasured). Registry row `MB-3-STATIC-SIDE-1`.
  pins: mb-3/C-015
- `relation_tests.rs` — the template pins (`#[cfg(test)] #[path]` from `relation.rs`).
  pins: mb-3/C-008
- `driver_tests.rs` — the driver's pins (`#[cfg(test)] #[path]` from `driver.rs`).
- `run_tests.rs` — the trigger-loop and lifecycle pins (`#[cfg(test)] #[path]` from `run.rs`).
  pins: mb-3/C-004
- `foreach_tests.rs` — the `foreachBatch` door and shutdown pins, with a Rust `BatchBody` that
  writes the sink through the session's resolved write options.
  pins: mb-3/C-005
- `lifecycle_tests.rs` — the fold-1 pins (2026-10-07), with a recording `BatchBody` (`Probe`).
- `table_door_tests.rs` — the `toTable` door pins: the stamped append with the Spark keys, the
  start check on a shared catalog, the unknown-outcome reconcile and walk over a fault-injecting
  catalog wrapper (`FaultCatalog`, the `crash_tests.rs` shape), and fencing by another run of the
  same query across two sessions. `FaultCatalog` also takes a load hook (`on_load`), which the
  fold-1 pins use to panic or to hold a table load. The door's exactly-once guarantee against a racing driver is
  not claimed until `F-APPEND-PIN-BASE-1` lands (ledger C-002).
  pins: mb-3/C-007
- `testing.rs` — the test fixture: a session over a memory catalog with the `sales.orders`
  source, the `sales.silver` sink and a spare `sales.other` table.
