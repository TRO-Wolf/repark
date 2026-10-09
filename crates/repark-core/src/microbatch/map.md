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
the crash gate's 3 passed and 2 ignored included, are recorded there (round 1's count; since
the append fence merged, 2026-10-08, the crash gate reads 5 passed and 0 ignored).
pins: mb-3/C-009
Fold 1's gates (2026-10-07) are recorded there too. pins: mb-3/C-019
Round 2's gates (2026-10-08) likewise. pins: mb-3/C-023
Fold 2's gates (2026-10-08) likewise. pins: mb-3/C-029
Round 3's gates (2026-10-08, on the tree merged with the append fence) likewise.
pins: mb-3/C-031

## What each door guarantees (round 3, 2026-10-08, ledger C-002 and D-19)

- **`toTable`: exactly-once per epoch.** A batch is one append commit that carries its rows
  and its stamp, and that commit goes through the append fence
  (`repark-iceberg/src/write/sink_offsets/append_fence.rs`). An epoch lands once across a
  restart, across a second driver of the same query in this process (the sink's `BatchScope`
  serialises the two and the batch's resume-point check fences the later one before it
  writes), and across a driver in another process or one committing from a stale table handle
  (the fence refuses at the commit: `Fenced` naming the winner's run, or `AlreadyCommitted`
  for the run's own re-delivery). The refused driver's rows do not land. A commit whose
  outcome cannot be learned ends `RecoveryRequired`, never a silent second delivery.
- **`foreachBatch`: the stamp is exactly-once per epoch, the body is at-least-once.** The
  trailing stamp goes through the same scope, check and fence. The body's own sink writes are
  not stamped (OQ-2a-2, ledger D-2) and run before the stamp, so a body that died, or whose
  stamp was refused by a racing commit from another process, has already written and its epoch
  replays or is taken over. A driver fenced at the batch's resume-point check never runs the
  body.

## Contents

- `mod.rs` — `#![forbid(unsafe_code)]` (NS-17) and the module declarations: `pub mod driver;`,
  `pub mod progress;`, `pub mod relation;`, the private `run`, and the test-only modules
  (`race_tests` joined them in MB-4 items 13 and 14).
  **MB-4 round 2b (2026-10-08):** re-exports `MicroBatchError` and its direct field types
  (`RecoveryReason`, the offset ids, `Generation`, `iceberg::spec::Operation`) so the binding
  names the mapper's input without a `repark-iceberg` edge, following the three
  `repark_iceberg::write` re-export precedents (`partition_overwrite_mode.rs`,
  `session/writer_layout.rs`, `error_map.rs`).
  pins: mb-4/C-022
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
    `QueryShared::panicked` turns the payload into `DriverPanicked { epoch, message }` (its own
    variant since round 2, 2026-10-08; the message is the panic's, credential-masked) with the
    in-flight epoch, or the next one when the panic came before a batch began. The outcome,
    the done signal and the freed query id then follow the body-error path.
    pins: mb-3/C-011, C-020
  - *One active query per sink per session.* `admit` refuses `SinkBusy` at start when an
    active query of this session already targets the sink's table uuid (registry row
    `MB-3-SINK-BUSY-1`); the refused query keeps its work and can start later.
    pins: mb-3/C-016
  - *The session's end stops its queries.* An explicit session stop is
    `StreamingQueryManager::stop_all`, which stops each active query and waits for it as
    `stop` does. A session dropped without a stop drops its state, the state drops the
    manager, and the manager's `Drop` sends the stop signal to every query it holds (round 2,
    2026-10-08). Nothing a query owns keeps the session state alive: the source, the driver
    task and a registered handle's pending work hold a `WeakSessionState`
    (`time_travel/microbatch_source.rs`) and take a state snapshot per batch. The signal
    misses one case: a state snapshot that outlives the session (a `DataFrame` the caller still
    holds keeps the manager alive). For that case the trigger wait and the scope wait also
    watch a `Weak` to the session's catalog registry (`Session::catalogs`), polled once a
    second (`SESSION_WATCH`; fold 2, 2026-10-08, ledger D-18), and a planning error after the
    session ended is a stop, not a failure; the `toTable` door stops before its write when the
    state is gone. A registered, never started handle whose manager is gone concludes
    `Stopped` when it is awaited (`orphaned`, the same one-second watch), so
    `await_termination` on it returns. A handle the caller still holds keeps the sink's
    catalog handle until it is dropped. pins: mb-3/C-013, C-021, C-028
  - *A stop from inside a body never waits on itself* (fold 2, 2026-10-08, ledger D-17). The
    driver task runs inside the task-local `DRIVING`, a `Weak` to its own query. `stop` always
    sends the signal first. Called from a driver task, it records a wait edge from that task's
    query to the target (`WaitEdge`) and follows the target's edges: when they lead back to
    the caller (the target is the caller's own query, or two bodies stop each other), it does
    not wait and returns `Stopped` with the durable record known so far; the query then ends
    `Stopped` after the body returns and its batch is stamped. Otherwise it waits as any
    `stop` does, which is how `stop_all` from a body waits for the other queries and not for
    itself. `await_termination` on the caller's own query refuses `AwaitFromDriver`, Spark's
    answer. A body that stops its query from another task or thread must run that call inside
    the driver's task-local scope; MB-4 owns that for the Python body. pins: mb-3/C-024
  - *Every catalog call is bounded* (round 2, 2026-10-08, ledger C-010). `StreamSpec` carries
    `catalog_timeout` (`DEFAULT_CATALOG_TIMEOUT`, 60 s; MB-4 maps `repark.cdc.catalog-timeout`
    onto it). `bounded` wraps one call and fails `CatalogTimeout { call, waited }`. `register`
    bounds the sink load and the source open and hands the limit to the source
    (`with_catalog_timeout`, fold 2), and `stop` bounds its re-read of the sink after a
    stop timeout, falling back to the durable record it knows. pins: mb-3/C-010
  **MB-4 round 3 (2026-10-08):** the R-12/OQ-5 private seam for the facade pins:
  `start_below_catalog_check` is `pub` and no longer `cfg(test)` (the `Skip` arm with it),
  so the `_native` doors start on the memory catalogs the pins run on; `start` keeps the
  MBE-8 refusal. pins: mb-4/C-024
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
  - *Stop between planning and the body.* The loop reads the stop flag again after
    `next_batch`, so a stop that arrives while a trigger plans never starts that batch's body.
    pins: mb-3/C-018
  - *The scope wait.* `enter_scope` loads the sink and enters the `BatchScope`; when another
    query in the process holds the sink's scope (two sessions on one sink), it waits
    `pollingDelay`, reloads the sink and tries again, up to `catalog_timeout`, and only then
    fails `SinkBusy` naming the sink's table, as the refusal at start does (fold 2). The sink is
    loaded again after each wait because the other query's
    commit moved it, and the resume-point check runs after the scope is held. A stop or a
    dropped session ends the wait with no batch run. pins: mb-3/C-016
  - *The replay window is not durable.* A restart plans the failed batch's window again
    (registry row `MB-3-REPLAY-WINDOW-1`). pins: mb-3/C-014
  - *An unstamped batch.* The `NotCommitted` check after the door is reachable when the sink
    table is replaced under a `foreachBatch` body: the trailing stamp lands on the new table,
    outside the batch's scope, and the query ends `RecoveryRequired(UnstampedSinkCommit)`.
    Since fold 2 (2026-10-08) each door hands back the snapshot its commit produced, and the
    check names that snapshot without another catalog call, so a stalled catalog cannot turn
    this ending into a retryable error. pins: mb-3/C-018, C-026
  **Round 2 (2026-10-08):**
  - *The bounded calls.* Each catalog call of the task runs under `catalog_timeout`: the sink
    load (`load_sink`: at start, at each batch's scope and after a `foreachBatch` body) and
    the source's one catalog load inside each planning call (`load the source`, bounded inside
    `MicroBatchSource` since fold 2, 2026-10-08, so the manifest walk after the load is not
    under the timeout). A timed-out read
    fails the batch with `CatalogTimeout` before the offset moves. A commit that times out
    (the trailing stamp, the `toTable` append) is an unknown outcome: it goes to
    `resolve_unknown_outcome` with no operation id, which finds the landed commit by its
    record or ends `RecoveryRequired(CommitOutcomeUnknown)`; the walk is bounded too
    (ledger D-16). pins: mb-3/C-010, C-025, C-027
  - *A fresh sink for every batch.* `enter_scope` loads the sink for each batch, the `toTable`
    door stages and commits on that handle, and the `foreachBatch` door loads it again after
    the body before the trailing stamp. No handle is kept across batches, so an epoch never
    commits from a handle older than the run's previous commit (the append fence's rule,
    PR #996). pins: mb-3/C-022
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
  **MB-4 round 2b (2026-10-08):** `is_streaming_frame` exposes the plan predicate over
  `DataFrame::logical_plan` for the binding's action guard and `isStreaming` door.
  pins: mb-4/C-023
- `relation_tests.rs` — the template pins (`#[cfg(test)] #[path]` from `relation.rs`).
  pins: mb-3/C-008
- `driver_tests.rs` — the driver's pins (`#[cfg(test)] #[path]` from `driver.rs`).
- `run_tests.rs` — the trigger-loop and lifecycle pins (`#[cfg(test)] #[path]` from `run.rs`).
  pins: mb-3/C-004
- `fence_tests.rs` — the racing-driver pins (round 3, 2026-10-08), on both doors: two drivers
  of one query in two sessions over twenty rounds (each epoch stamped once, the rows once, no
  fenced body run, the fenced driver's restart adds nothing); a commit that lands between the
  driver's load and its commit, issued from `FaultCatalog`'s load hook inside the fork's own
  refresh, which is how another process's driver looks from here (`Fenced` for another run,
  `AlreadyCommitted` for the run's own re-delivery, `RecoveryRequired` when the refreshed sink
  needs recovery); and a driver that resumed before another run committed. The restart of the
  refused query continues at the next epoch each time.
  pins: mb-3/C-002, C-030
- `race_tests.rs` — the two race pins carried from MB-3 (MB-4 items 13 and 14, 2026-10-08).
  **Item 13, `two_sessions_racing_one_query_land_every_row_exactly_once`:** fifty iterations,
  each on a fresh warehouse with three source files of two rows. Two sessions over the one
  memory catalog register the same sink and `queryName` (one `QueryId`, two `RunId`s) on the
  `toTable` door, and two threads start them off a `std::sync::Barrier`. No commit is injected:
  both commits are the drivers' own. Each iteration asserts the sink holds ids 1 to 6 once each,
  epochs 0, 1 and 2 are each stamped once over every retained snapshot and on the main lineage,
  one run stamped all three, that run drained, and the other driver either drained with
  nothing to do or ended `Fenced`, or `RecoveryRequired` with a durable record, naming that run.
  *Why the catalog is slowed.* In one process the sink's `BatchScope` serialises the two
  drivers, and a driver that enters the scope with a current view of the sink is fenced by the
  batch's resume-point check in `run.rs` before it writes; measured on the unslowed catalog,
  none of fifty iterations reached the append fence. `SlowCatalog` wraps each session's catalog
  and holds every loaded view of the sink for a seeded 0 to 12 ms, which is what a catalog
  round trip gives a driver in another process: a view that is stale by the time it commits.
  With it the loser stages its file and is refused at the commit in 46 to 48 of fifty
  iterations (three runs). The pin counts those iterations, as parquet files under the sink
  that no snapshot added, and fails under a floor of ten, so it cannot go quiet if the race
  stops reaching the fence. The seed fixes the pauses and not the scheduler; a failure prints
  the iteration, the seed, both endings, the sink's rows and the summary history. The door is
  `toTable` only: the `foreachBatch` body is at-least-once by contract (see the door
  guarantees above), so "each row once in the sink" is not its promise.
  pins: mb-4/C-113
  **Item 14, `a_restart_after_the_fences_recovery_required_ending_refuses_by_name`:** on both
  doors, drives a query to the ending of `fence_tests.rs`'s
  `a_fence_refusal_that_needs_recovery_ends_recovery_required` (a foreign commit sets this
  query's offsets property, with no stamped snapshot, inside the driver's commit refresh), then
  registers the same sink and `queryName` on a fresh session and manager over the same catalog
  and starts, twice. The answer is the refusal the sketch names for a property with no stamped
  snapshot behind it (Q10 and §3.4, `read_resume_point`): `RecoveryRequired` with
  `StampedSnapshotExpired`, epoch 0 and the property's record as the durable offset, the same
  on the exception, on the `ShutdownOutcome` and, since the round-4 race-lane Q1 ruling
  (2026-10-09, `conclude` adopts the reported record when the lifecycle holds none), on
  `QueryHandle::durable()`. It does not resume: no body runs, the sink
  keeps no snapshot, no stamp and no row, so nothing is duplicated and nothing is skipped past.
  The helpers that build the ending repeat `fence_tests.rs`'s private ones, because that file
  is outside this slice's footprint.
  pins: mb-4/C-114, C-029
- `run.rs`, `body_statements.rs`, `exactly_once_tests.rs`, `fence_tests.rs`,
  `lifecycle_tests.rs`, `timeout_tests.rs`, `race_tests.rs` — **MB-4-FOREACH-EO fold 2
  (2026-10-09, orchestrator ruling after the re-verify: an invariant on the sink's lineage,
  checked by the driver, not a list of routes).** The entry below describes fold 1; where
  the two differ this one holds.
  - **Check (a), `Run::refuse_moved_sink`**, on the `foreachBatch` door only: at the start of
    the trigger loop and again on the sink each batch loads, before the body. The table under
    the sink's name must still be the one the query registered on, and no unstamped snapshot
    may sit above the query's newest stamp. A query with no stamp yet is measured from the
    head it found at start (`Cursor::baseline`), so a restart of a query that never committed
    batch 0 cannot refuse; it replays, and check (b) stops it again if the stray write
    recurs.
  - **Check (b), in `Run::foreach_batch`**: a `SinkMark` is taken before the body, and after
    the body, returned or raised, the sink is loaded once and compared. Any violation ends
    `RecoveryRequired` before the epoch is recorded durable, and it wins over the body's own
    error and over a typed refusal. This overturns fold 1's two rules "a failed body is not
    audited" and "a foreign snapshot beside a stamped epoch is tolerated". The load replaces
    the one the stamp-only commit made, so a body with no sink write costs the same catalog
    calls as before and a body with one costs one load more.
  - **`body_statements.rs`**: `refuse_unstamped_sink_dml` is called from
    `PreExecute::execute`, the one place a DataFusion-planned statement runs on all three
    doors. Inside a body scope it finds every `Dml` node the statement will execute (it does
    not descend into a plain `EXPLAIN`) and refuses `UnstampedSinkWrite` when the target is
    the sink. That is how `EXPLAIN ANALYZE INSERT`, a bare-dialect `INSERT` and any other
    statement that reaches the fork's table provider are refused before they land; it does not
    match on the word `EXPLAIN`.
  - **Pins.** `exactly_once_tests.rs` is rewritten around one `Shaped` body (a stray write of
    six kinds, at a chosen epoch, with the body's own append before or after it, or a raise):
    a stray with no stamped commit; a stray before and after the stamped commit, and what the
    restart does in each; a failed body audited and two restarts refused with no body run;
    the epoch-0 limit; rows already in the sink and a seeded restart; a foreign snapshot
    between runs; a property change beside the stamped commit; the guarded property change;
    the planned `INSERT` and `EXPLAIN ANALYZE INSERT` refused and a plain `EXPLAIN` passing.
    `fence_tests.rs` gains `a_writing_body_that_loses_its_epoch_ends_fenced_and_lands_no_row`
    (the racer's stamp injected at the body's own load and at its commit). The seeded
    `foreachBatch` race keeps its invariants and loses its floor: the audit's load lets the
    waiting driver see the winner's commit before it enters, so the loss now mostly lands on
    the resume check (measured 2 of 50 at a body's staged write), and a floor of zero is no
    assertion, so the test asserts the per-iteration invariants only. The replaced-sink pins in
    `lifecycle_tests.rs` and `timeout_tests.rs` read `UnstampedSinkChange`, and no stamp
    lands on the new table.
  - **Cost (measured 2026-10-09, 200 epochs, `availableNow`, one file per batch, a memory
    catalog, medians of three runs on a shared box).** Before the fold and after it, run one
    after the other: a body that appends to the sink 60.6 s and 61.3 s (+1.1 %); a body that
    writes nothing 7.8 s and 8.7 s (+10.8 %, over the 5 % line); `toTable`, whose code did
    not change, 55.8 s and 57.3 s (+2.6 %). The mark's snapshot set was then made lazy and
    the final build measured alone twenty minutes later: 63.4 s, 6.5 s and 57.2 s. The
    no-write reading did not reproduce (it is 16 % under the "before" figure) and the
    writing body read 4.5 % over it, so the box does not resolve a difference under about
    5 %, and no reading puts the audit above it twice.
  pins: mb-4-foreach-eo/C-016, C-017, C-018, C-019, C-023, C-025, C-026
- `run.rs`, `exactly_once_tests.rs`, `foreach_tests.rs`, `race_tests.rs`, `testing.rs` —
  **MB-4-FOREACH-EO (2026-10-09, owner ruling "FIX IT" on the MB-4 verify's S1):** the
  `foreachBatch` door is exactly-once on the declared sink. This overturns MB-3 D-2 and
  C-005 (b), (c), (e): the body's own sink commit carries the stamp, and the trailing
  `commit_stamp_only` runs only for a body that made no sink commit.
  - **`Run::foreach_batch`** polls the body inside `BatchScopeGuard::scope_body`, so a commit
    the body issues on the driver's task claims the batch stamp at the stamped arms of
    `repark-iceberg` (the design is in
    [write/sink_offsets/map.md](../../../repark-iceberg/src/write/sink_offsets/map.md)).
  - **After the body**, in this order. A stamped commit the scope recorded is the batch's
    commit. A commit whose outcome the guard saw as unknown goes through
    `resolve_unknown_outcome`, the `toTable` door's rule: found is durable, not found ends
    `RecoveryRequired(CommitOutcomeUnknown)`. A body that raised fails the query, but an epoch
    that committed is recorded durable first, so the restart resumes after it and the body is
    not replayed (skipped entirely, as the sketch's §3.5 step 2 and Flink's committer do). The
    error is the scope's typed refusal when it has one (`Fenced` from a lost race,
    `SinkCommittedTwice`, `UnstampedSinkWrite`), else `BatchFailed` with the body's masked
    text. A body that returned with no stamped commit while an unstamped snapshot landed on
    the sink above the batch's base ends `RecoveryRequired(UnstampedSinkCommit)` and writes no
    stamp; otherwise the stamp-only commit runs as before.
  - **A failed body is not audited for unstamped snapshots.** The check costs a sink load, and
    a body's own error is the answer the caller needs (MBE-16). A write that slips the scope is
    caught on the first batch that returns.
  - **`exactly_once_tests.rs`**: the second write (`SinkCommittedTwice`, the first lands once),
    the body with no sink write, a sink write from a spawned task with and without the body's
    own stamped commit, a guarded property change (`UnstampedSinkWrite`, nothing lands), a
    bare-dialect `INSERT` the guard cannot see (`UnstampedSinkCommit`), the two unknown
    outcomes over `FaultCatalog`, and the scope ending with the batch.
  - **`foreach_tests.rs`**: three pins rewritten for the new contract. The stamp pin reads
    `stamp 0 with 3 rows, stamp 1 with 2 rows` and no trailing snapshot; the failed-body pin
    fails before any sink write and replays its epoch; the pin that read "replays at least
    once" is now `a_body_that_fails_after_its_sink_write_leaves_the_epoch_durable`.
  - **`race_tests.rs`**: `race_once` takes the door, and
    `two_sessions_racing_foreach_bodies_land_every_row_exactly_once` runs the 50 seeded
    iterations with a body that appends to the sink. Its floor is 10 iterations that refused a
    staged write (measured 2026-10-09: 43 of 50).
  - **`testing.rs`** gains `append_frame` (the body's append, through the registry snapshot
    the statement funnel builds) and `SinkWriter`, a `BatchBody` that appends N times.
  - **Mutants (2026-10-09).** Eight hand mutants, eight red; the table is in the
    MB-4-FOREACH-EO ledger.
  pins: mb-4-foreach-eo/C-001, C-002, C-004, C-007, C-009, C-010, C-011, C-013
- `foreach_tests.rs` — the `foreachBatch` door and shutdown pins, with a Rust `BatchBody` that
  writes the sink through the session's resolved write options.
  pins: mb-3/C-005
- `lifecycle_tests.rs` — the fold-1 pins (2026-10-07): a panic in the body, in plan execution
  and during planning; `start` racing `stop` (300 rounds, the task count and a later source
  append); the session dropped and the session stopped; two queries on one sink in one
  session and across two sessions; a stop during planning; a zero `stopTimeout`; a sink
  replaced under the body; the replay window over a grown source; the trigger on the interval
  boundary. The boundary pin sets the interval to a wall-clock instant two seconds ahead in
  epoch milliseconds, so the first multiple of the interval is that instant and the schedule
  is exact without an injected clock; the arithmetic itself is pinned on fixed instants in
  `run_tests.rs`. Round 2 (2026-10-08) adds a registered handle that does not keep the session
  alive and the wake pin: eight rounds of a query in a one-hour wait, at least seven of which
  must end within 25 ms of the session's drop, which a 100 ms poll cannot do. Fold 2
  (2026-10-08) adds the scope wait that outlives the timeout, the session gone behind a live
  frame (a waiting query and a busy one), and the registered handle whose session is gone.
  pins: mb-3/C-011, C-012, C-013, C-014, C-016, C-017, C-018, C-020, C-021, C-027, C-028
- `timeout_tests.rs` — the catalog-timeout pins (round 2, 2026-10-08), one per call site over
  `FaultCatalog`'s load hook and its two stalling commit modes, with a 100 ms bound: `register`,
  the six read sites of the task, the reload after a body, a stalled commit on both doors
  (landed and lost), and `stop` over a stalled catalog. Fold 2 (2026-10-08) adds a walk longer
  than the timeout (thirty snapshots; the walk is timed first and the timeout set to a third
  of it, so the pin scales with the machine; the stamp runs under the same timeout, so the
  pin asserts the planned batch and accepts a drained or an unknown-outcome ending), the unstamped batch over a stalled catalog, and a
  stalled unknown-outcome walk after a stalled commit.
  pins: mb-3/C-010, C-025, C-026, C-027
- `self_stop_tests.rs` — the self-stop pins (fold 2, 2026-10-08), with a body that holds its
  own query handle: `stop` and `stop_all` from the body under the default `stopTimeout`,
  `stop_all` from a body with another query running, two bodies stopping each other, and
  `await_termination` on the body's own query.
  pins: mb-3/C-024
- `reload_tests.rs` — the fresh-sink pin (round 2, 2026-10-08): `FaultCatalog`'s event log
  counts the sink loads before each commit over three batches, on both doors. One load per
  commit is the fork's own refresh inside the commit (`FORK_REFRESH`, measured 2026-10-08), so
  a fork repin that changes it shows here. Re-measured after the append fence merged
  (2026-10-08): unchanged, because the fence reads `TableCommit::base_table` and loads nothing
  on the driver's paths.
  pins: mb-3/C-022
- `table_door_tests.rs` — the `toTable` door pins: the stamped append with the Spark keys, the
  start check on a shared catalog, the unknown-outcome reconcile and walk over a fault-injecting
  catalog wrapper (`FaultCatalog`, the `crash_tests.rs` shape), and fencing by another run of the
  same query across two sessions. `FaultCatalog` also takes a load hook (`on_load`), which the
  fold-1 pins use to panic or to hold a table load; round 2 adds two stalling commit modes
  and an event log of loads and commits. The door's exactly-once guarantee against a racing driver is
  claimed since the append fence merged (round 3, 2026-10-08, ledger C-002) and pinned in
  `fence_tests.rs`.
  pins: mb-3/C-007
- `testing.rs` — the test fixture: a session over a memory catalog with the `sales.orders`
  source, the `sales.silver` sink and a spare `sales.other` table.
