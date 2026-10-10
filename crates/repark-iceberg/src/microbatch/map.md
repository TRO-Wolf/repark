# map — repark-iceberg/src/microbatch

## Purpose

The micro-batch track's Rust home: offset and identifier types, the offset
JSON the sink stamp carries, and the track's one error enum. Owned by slice
MB-1 under the [design sketch](../../../../task/wo/microbatch/mb-design-2026-10-06.md),
the [MB-1 order](../../../../task/wo/microbatch/mb-1-source.md), and the
[North Star](../../../../task/roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md).
Round 1 landed `mod.rs`, `offset.rs` and `error.rs`; round 2 adds the
window and provider. Fold 1 (2026-10-07) reworks the window after the
verifier's FAIL on PR #979, against the measured MB0b-R14…R16 cells.
Progress: the [MB-1 ledger](../../../../task/ledgers/completed/mb-1-ledger.md).

## Contents

- `mod.rs` — `#![forbid(unsafe_code)]` (NS-17) plus `pub mod error;`,
  `pub mod offset;`, `pub mod provider;` and `pub mod window;`. No
  re-exports: callers use full paths. MB-2a adds `#[cfg(test)] mod crash_tests;`.
  pins: mb-1/C-007, mb-1/C-013
- `crash_tests.rs` — the crash harness of the
  [sketch's §5](../../../../task/wo/microbatch/mb-design-2026-10-06.md), in Rust over the memory
  catalog (correction H-1): five pins, all green since MB-2c's closing slice (2026-10-07,
  the append fence in `write/sink_offsets/append_fence.rs`; mb-2c C-010). Every pin enters a `BatchScope` and carries the guard's token through the
  session snapshot property `spark.sql.iceberg.snapshot-property.repark.cdc.scope-token`, as
  MB-3's driver will (D-10).
  `FaultCatalog` is the `UnknownOutcomeCatalog` shape over the memory catalog with three faults:
  `Race` commits a stamped racer inside the next `update_table`; `UnknownAfterLanding` lands and
  `UnknownWithoutLanding` drops the commit, and both answer `CommitStateUnknown` and fail the next
  reload once.
  - Pin 1, `test_microbatch_kill_after_commit_resumes_1` (**MB-2a, 2026-10-07**, green guard):
    kill points (a) and (c); the sink equals Bronze, epochs 0 and 1 stamped once, the property
    equals the head's stamp. pins: mb-2a/C-010, C-013
  - Pin 2, `test_microbatch_duplicate_delivery_skips_1` (**harness, 2026-10-07**; green since MB-2c's closing slice): kill
    point (b) leaves staged files in no snapshot; epoch 0 re-delivered with its original stamp
    against the pre-commit `Table` must not commit, and a claim on the reloaded sink must return
    `AlreadyCommitted`. pins: microbatch-harness/C-002, mb-2c/C-010
  - Pin 3, `test_microbatch_two_drivers_one_sink_1` (green since MB-2c's closing slice): run A commits epoch 1 inside run B's
    `update_table`, on the append arm and on a copy-on-write `execute_merge`. B's commit must
    fail, epoch 1 and its rows land once, the property names A, and B's claim on the reload must
    be `Fenced { winner: A }`. pins: microbatch-harness/C-003, mb-2c/C-010
  - Pin 4, `test_microbatch_unknown_outcome_reconciles_1` (**MB-2c, 2026-10-07**, green):
    `commit_stamp_only` on the `foreachBatch` door with `commit.status-check.num-retries=0`. A
    landed stamp must resolve to its snapshot with one `update_table` and no replace; an unlanded
    one must refuse
    `RecoveryRequired(CommitOutcomeUnknown)` carrying epoch 0 as durable.
    pins: microbatch-harness/C-004, mb-2c/C-004
  - Pin 5, `test_microbatch_bronze_overwrite_refuses_1` (green guard, MB-1): R2 overwrite and
    R5 delete refuse `NonAppendSnapshot` naming the snapshot, the sink unchanged; R8's replace is
    skipped and row 4 streams. pins: microbatch-harness/C-005
  Pins 2 and 3 carry `#[ignore = "red until F-APPEND-PIN-BASE-1 + MB-2c fence: <scenario>"]`
  (ruling Q2, the split): with the epoch check in, both still fail where the stamped append
  re-bases past a moved base, and the closing slice deletes the two lines. The interim gate is
  3 passed, 2 ignored. pins: mb-2c/C-005
  Why each red pin has its shape: the [harness ledger](../../../../task/ledgers/staging/microbatch-harness-ledger.md)
  D-1…D-4. pins: microbatch-harness/C-001, C-006
- `offset.rs` — the sketch's §3.1. Seven newtypes, each `new`/`get`
  (NS-14), with the sketch's named constructors beside them:
  `TableUuid::of`, `QueryId::derive`, `RunId::fresh`, `Epoch::FIRST`/`next`,
  `OffsetFormatVersion::CURRENT`. `InputOffset`, `OffsetVector`,
  `SinkRecord` with its summary/property readers and writers,
  `SinkDoor`, `spark_source_offset_json`, and the nine key constants.
  Fold 1: the writers return `Result`, and both readers accept only the
  canonical version text `1`.
  MB-2a fold 1 (ruling V4, 2026-10-07): only a JSON integer in the property's
  `format-version` is a version; a string, a float, `null` or any other shape refuses
  `Catalog` as a corrupt stamp, not `UnsupportedOffsetFormat`.
  pins: mb-1/C-001, C-002, C-003, C-004, C-005, C-024
  pins: mb-2a/C-016
- `stray_remedy.rs` — **MB-4-FOREACH-EO fold 4 (2026-10-10, owner ruling on the third
  verify's first two S1):** what a stray refusal tells the operator, as data. `StrayRemedy`
  holds where the stray sits (`under`: the stamped snapshot above it, or none), how to
  discard its rows (`Discard::RollBack { to, then }`, `EmptyStart`, or `Unproven(why)`) and
  how to keep them (`Restart::NewName(position)` or `Unnamed`). Its `Display` is the text:
  a rollback names the snapshot, a new query name comes with its
  `repark.cdc.start-after-snapshot-id` position, and a recipe the driver cannot prove is
  replaced by the reason it is not printed. Fold 3's one constant sentence said "roll back
  to the newest stamped snapshot, or start under a new name"; the first half is false for a
  stray under a stamp and the second half re-delivered every stamped batch (16 of 32 rows
  duplicated in the verify). Three in-file pins hold the three shapes of the text.
  pins: mb-4-foreach-eo/C-033
- `error.rs` — **MB-4-FOREACH-EO fold 4 (2026-10-10):** `RecoveryReason::StraySinkCommit
  { snapshot, operation, remedy }` is the reason for an unstamped snapshot on the sink's main
  branch; it prints the fold-2 sentence, "Its rows are in the sink." and the remedy.
  `UnstampedSinkCommit` loses the remedy sentence: it remains for a batch that ended without
  its stamp and for a snapshot the audit finds off the main branch. `UnstampedSinkChange`
  no longer promises a refused restart or a rollback: it says the change is still in place
  and that a restart checks only the main branch's snapshots (the third verify's S3).
  pins: mb-4-foreach-eo/C-033
- `starting_mark.rs` — **MB-4-FOREACH-EO fold 4 (2026-10-10):** the head a query started on
  outlives the mark. The first stamped commit replaces the mark in the offsets property, so
  it carries the head as the summary key `repark.cdc.starting-head` (a snapshot id, or
  `none` for a sink that started empty): `summary_entry` and `from_summary`. This is the
  lower bound of the walk under a first stamp; without it a row that was in the sink before
  the query started could not be told from a stray. Still two durable items: the key rides
  the stamp. In-file pins: only epoch 0 can be pending (the third verify's surviving mutant
  N2), and the summary round trip.
  pins: mb-4-foreach-eo/C-032, C-039
- `window.rs` — **MB-4-FOREACH-EO fold 4 (2026-10-10):** `WindowPlanner::ends_snapshot`
  answers whether an offset has consumed every added file of its snapshot. Only then is
  `repark.cdc.start-after-snapshot-id` an exact continuation, so only then does a refusal
  offer a new query name. pins: mb-4-foreach-eo/C-033
- `starting_mark.rs` — **MB-4-FOREACH-EO fold 3 (2026-10-10, owner ruling D2):** the starting
  mark of a `foreachBatch` query, as the first value of its offsets property:
  `{"format-version":1,"pending-epoch":0,"starting-head":<snapshot id or null>}`. It is not a
  third durable item (NS-2): it lives under the offsets key and the first stamped commit
  replaces it. `StartingMark::from_property` answers none for a value that is not a mark (the
  offsets record), refuses a pending epoch other than 0 and a missing head as corrupt, and
  refuses another format version as `UnsupportedOffsetFormat`. A build older than this fold
  reads a mark as a corrupt offsets record and refuses the query; it does not misread it.
  pins: mb-4-foreach-eo/C-028
- `error.rs` — **MB-4-FOREACH-EO fold 3 (2026-10-10, owner ruling D3):** the two unstamped
  reasons name both remedies: roll the sink back to its newest stamped snapshot (to the head
  the query first started on, if no batch is stamped yet), or start the query under a new
  name. The text says a restart refuses while an unstamped snapshot sits above the newest
  stamped batch: a stray below the batch's own stamped commit leaves that batch durable, and
  the restart resumes after it. `UnstampedSinkCommit` without an operation (the `toTable`
  door's own check) keeps its text byte for byte.
  pins: mb-4-foreach-eo/C-030
- `error.rs` — **MB-4-FOREACH-EO fold 2 (2026-10-09):** `RecoveryReason::UnstampedSinkCommit`
  gains `operation: Option<String>` (rendered ` (append)` after the snapshot id when known;
  the text without it is unchanged) and `UnstampedSinkChange { what }` joins it for a change
  that made no snapshot. MBE-19's text no longer lists `DELETE` without its condition.
  pins: mb-4-foreach-eo/C-017, C-027
- `error.rs` — **MB-4-FOREACH-EO (2026-10-09):** `UnstampedSinkWrite { sink, epoch }`
  (MBE-19), the refusal of a commit to the declared sink that cannot carry the epoch stamp.
  Its text names the shapes the sink does take inside a `foreachBatch` body.
  pins: mb-4-foreach-eo/C-006
- `error.rs` — the sketch's §3.2: `MicroBatchError` with every variant,
  `thiserror`, `#[non_exhaustive]` (NS-15), plus `RecoveryReason`. Fold 1
  adds `OffsetPositionOutOfRange`, and `UnsupportedOffsetFormat.found`
  becomes the version text as read. Fold 2 adds `SnapshotNotInLineage` (G5).
  MB-3 round 2 (2026-10-08) adds `DriverPanicked { epoch, message }` and
  `CatalogTimeout { call, waited }`, and `SinkBusy` now reads `sink <sink> is busy: another
  streaming query or batch is active on it; one at a time per sink`, true for the scope's
  refusal and for the driver's refusal at start. pins: mb-3/C-020
  MB-3 fold 2 (2026-10-08) adds `AwaitFromDriver { query }` with Spark's text, for
  `await_termination` called from the query's own driver task. pins: mb-3/C-024
  MB-2a fold 1 (ruling V2, 2026-10-07) adds `RecoveryReason::StampNotInLineage`: a sink stamp
  that is retained but off the current lineage (a rollback), naming a new `queryName` or a
  restore; `StampedSnapshotExpired` keeps the stamp that is truly gone.
  MB-2c fold 2 (2026-10-07) adds `resume_refusal` to `RecoveryReason::CommitOutcomeUnknown`:
  the walk's reload read a resume point that itself refuses (an `OffsetMismatch` after a
  rollback), rendered as `; resuming will then refuse: <reason>`.
  MB-2a fold 2 (ruling Y3, 2026-10-07): `SinkCommittedTwice` names the loss (a restart resumes
  after the stamped epoch, so the refused write's rows never land) and the fix (one sink write
  per batch body, or a single combined write); it carries no scope token.
  MB-2c fold 1 (ruling Q3, 2026-10-07): `MergeIsolationRefused` carries the isolation `property`
  of the refused operation (MERGE, UPDATE or DELETE) and renders
  `stamped write into <sink> needs <property>=serializable`.
  pins: mb-1/C-006, C-020, C-024, C-036, mb-2c/C-007
  pins: mb-2a/C-014, C-022
  ENC-1 round 2 (2026-10-09) adds `EncryptedSinkRefused { sink }` with the house
  one-sentence text, raised by `commit_stamp_only` before scope claim.
  Fold 1 (2026-10-09): its text is `write::encryption::refusal_text`, the one function, and
  `From<EncryptedTableRefusal>` builds it.
  pins: enc-1/C-005
- `window.rs` — the sketch's §3.3: `ReadCaps`, `StartPosition`,
  `WindowLimit`, `PlannedFile`, `WindowPlan`, and
  `WindowPlanner::{new, named, initial_offset, next_window}` over a held
  table. Fold 2 (G8): `named` sets the table name its refusals carry, so a
  source's errors name the table as it was opened (`ice.sales.orders`); the
  offsets it writes keep the table identifier.
  Fold 1: the window walks `(from, head]` lazily, one snapshot at a time,
  checking each operation as it enters and planning an append's files only
  while the private `Window` has room. Per-snapshot append scans list the
  files, sorted by path. The fail-on-non-append scan and the skip builders
  are never called (O-5).
  pins: mb-1/C-008, C-009, C-010, C-011, C-017, C-018, C-019, C-020, C-023, C-024, C-032, C-034, C-035, C-036, C-037
- `window_tests.rs` — the `window.rs` pins, split out under `#[path]` when
  the file passed the 1000-line ceiling: the memory-catalog fixture with
  append/overwrite/delete/replace commits plus the 18 start, window, cap,
  fail-loud and guard pins.
  pins: mb-1/C-008, C-009, C-010, C-011, C-019
- `window_fold2_pins.rs` — fold 2's window pins, a sibling child of
  `window_tests.rs` that reuses its fixture and fold 1's helpers: the two
  limits at an overwrite or delete (G1) and the position range on a
  replace or overwrite start (G3), and the delete-as-head start that the
  registry row `MB-1-FL-9` cites (G4), the lineage refusal (G5), and
  `FromTimestamp` over a skewed history built through `TableMetadataBuilder`
  (G6).
  pins: mb-1/C-032, C-034, C-035, C-036, C-037
- `window_fold_pins.rs` — fold 1's window pins, a child of `window_tests.rs`
  that reuses its fixture: the non-append start snapshot, the timestamp past
  the head, deliver-first refusal, the planning count read through the
  `#[cfg(test)]` counter, the position bound, truncated history after a real
  `expire_snapshots`, the measured max-rows rule, and the missing record count.
  pins: mb-1/C-017, C-018, C-019, C-020, C-021, C-023, C-024
- `provider.rs` — the sketch's §3.3 `MicroBatchTableProvider` (`pub(crate)`):
  reads exactly the planned tasks with `ArrowReaderBuilder` and the crate's
  `conform_batch`, and takes the arrow schema from the end snapshot. Filters
  stay `Inexact` (the tasks are pre-planned, so DataFusion re-applies them)
  and projection is served by `conform_batch`, not by re-planning.
  **MB-1 round 3 (2026-10-07):** `provider_for_plan` beside `try_new` is the
  type-erased cross-crate door — the struct stays `pub(crate)` per the sketch
  and `repark-core` reads through `Arc<dyn TableProvider>` (ledger D-4).
  Fold 1: every task is re-stamped with the end snapshot's schema and its
  top-level field ids, so older files read under the end schema, matched by
  field id, with null for a column a file lacks.
  Fold 2 (G2, 2026-10-07): the caller passes the read schema, and the end
  snapshot is only checked to exist. Every task is re-stamped with that
  schema and its top-level field ids.
  MB-4 round 3c (2026-10-08): `read_batches` serves an empty projection
  through `zero_column_batch`, which rebuilds the batch with the incoming
  row count stated explicitly — `RecordBatch::try_new` cannot infer a row
  count from zero columns, so `count(*)` over a batch frame failed
  engine-Internal before. Non-empty projections still read through the
  crate's `conform_batch`, untouched.
  pins: mb-1/C-012, C-015, C-016, C-022, C-033
  pins: mb-4/C-027
  **EMPTY-PROJECTION-COUNT-1 step 1 (2026-10-09):** the red-first
  `provider_counts_rows_through_an_empty_projection` pin — `COUNT(*)` over a
  three-file window holding an empty file, read through the shared
  `conform_batch` with no product change on this branch.
  pins: empty-projection-count-1/C-005

## Design notes

Error texts follow the sketch's §4: the `Cannot process overwrite snapshot`
and `Cannot process delete snapshot` prefixes, the skip refusal, the
checkpoint refusal, the shared-catalog refusal, and the `Cannot resume: start
snapshot expired` shape are Spark's wording verbatim; every other text is
RePark-owned, dated 2026-10-06, and names the fix. `Catalog(String)` is the
enum's generic carrier: catalog-layer folds and unreadable stamp bytes both
refuse through it, always with the offending key named. `NonAppendSnapshot`
fires only for overwrite and delete; any other `Operation` renders through
the same template but never reaches the variant.

`from_summary` returns `None` only when the stamp discriminator
(`repark.cdc.format-version`) is absent. A present version whose text is not
exactly `1` refuses `UnsupportedOffsetFormat` carrying that text as read, so
`+1`, `01`, `-1`, `abc` and an out-of-range number all name themselves
(fold 1, F10, 2026-10-07). `from_property` compares the JSON rendering of
its `format-version` member the same way. Any missing or unreadable
companion key refuses `Catalog` naming the key (ledger FL-2). Neither reader
guesses.

`try_from_inputs` sorts by table uuid and refuses an empty vector or a
repeated table uuid, loud, through `Catalog` (ledger FL-1, 2026-10-07). The
stored array is therefore always sorted and duplicate-free, and
`from_summary` re-validates what it reads.

`QueryId::derive` feeds the sketch's canonical bytes into UUID v5. The
`u32` length prefix saturates at `u32::MAX` for names past 4 GiB; the full
name still feeds the digest, so distinct names keep distinct ids, and the
sketch's infallible signature holds. The pinned vectors in the tests match an
independent RFC-4122 computation, not this implementation's output.
`Epoch::next` saturates rather than wrapping past `u64::MAX`, so an epoch can
stick but never collide with `FIRST`.

`spark_source_offset_json` is a manual `format!`, not `serde_json`: the
object's key order is Spark's quoted shape, and `serde_json::Map` would sort
it. The module's own JSON (the offsets array, the property value) goes
through `serde_json::Value`, whose sorted keys keep every rendering
deterministic; those objects carry arbitrary table names, so a real JSON
library owns the escaping. `summary_entries` and `property` return the
serializer's error as `Catalog` rather than defaulting to an empty string
(fold 1, F10).

`Cargo.toml` carries three lines for this module: the granted
`thiserror.workspace = true`, the `v5` feature on the existing `uuid` line
(the sketch's `QueryId::derive` calls `Uuid::new_v5`, and no other crate in
the closure enables it), and `serde_json.workspace = true` in
`[dependencies]` for the stamp readers and writers (ledger D-1,
2026-10-07). No version moved; the lockfile change is edges only.

The window's walk is its single refusal decider (fold 1, F5, 2026-10-07).
It follows the parent chain from `from` toward the head. Before each
snapshot it checks room: a capped window stops once it holds `max-files`
files or its rows reach `max-rows` (`>=`), so the file that crosses the
row cap stays in, as MB0b-R15 measured on Spark. `replace` is skipped
without planning. An `overwrite` or `delete` ends a non-empty window just
before it, so the appends ahead of it are delivered. A window that would
start by entering one refuses `NonAppendSnapshot`. The restart advice,
`start-after-snapshot-id=<id>`, therefore loses nothing.

The two limits differ at an `overwrite` or `delete` (fold 2, G1, 2026-10-07).
`WindowLimit::Unbounded` is the walk the MB-3 driver uses to fix an
AvailableNow or Once target, and it refuses at the first `overwrite` or
`delete` in `(from, head]` even when the window already holds files. That is
Spark's `prepareForTriggerAvailableNow`, which precomputes the end offset
with an uncapped `latestOffset` walk and throws before batch 0 (MB0b-R17:
two appends, then an overwrite or a delete, `availableNow` with
`streaming-max-files-per-micro-batch=1`, no batch and no offset).
`WindowLimit::Capped` keeps deliver-first: it ends a non-empty window just
before the snapshot, as Spark's processing-time triggers with caps deliver
the appends ahead of it. Ledger D-5 states both.

The start snapshot (fold 1, F1 and F6). An append start lists its files and
resumes at `from.position`. Any other start counts the ADDED data entries in
its own manifests, those written by that snapshot. A position above that
count refuses `OffsetPositionOutOfRange`. An `overwrite` or `delete` start
whose position is below its count refuses `NonAppendSnapshot`, as Spark's
`shouldProcess` does on the start of every batch (MB0b-R16).
`AfterSnapshot(x)` sets the position to that count, so it steps past `x`'s
files for any operation. A delete adds no data files, so `(x, 0)` cannot say
whether `x` is unread or consumed. `Earliest` and `FromTimestamp` therefore
refuse at `initial_offset` when they land on an `overwrite` or `delete`
with no added data files (ledger FL-9). `FromTimestamp` mirrors Spark's
`MicroBatchUtils.determineStartingOffset` and `SnapshotUtil.oldestAncestorAfter`
(fold 2, G6, 2026-10-07): a head below T reads `None`; otherwise the walk goes
back from the head and lands on the snapshot after the first one below T, or
on one exactly at T, or on the oldest ancestor when none is below T. Under
timestamp skew (Iceberg allows a minute backwards) that can start later than
the oldest ancestor at or after T, never earlier; older snapshots never stream
(MB0b-R14, fold 1, F4). A `from` gone from the
metadata still refuses `SourceSnapshotExpired` (FL-3). A `from`, or an
`AfterSnapshot(x)`, whose snapshot is still in the metadata but is not an
ancestor of the head (for example after `rollback_to`) refuses
`SnapshotNotInLineage` at `next_window` or `initial_offset` (fold 2, G5),
never as expired. Its text opens with Spark's `SnapshotUtil.snapshotAfter`
words, `Cannot find snapshot after <id>: not an ancestor of table's current
snapshot`, and the rest is RePark-owned, dated 2026-10-07, and names the
fix. FL-4 and FL-5 are superseded.

The read schema (fold 2, G2, 2026-10-07) supersedes fold 1's end-snapshot
schema. It is the table's current schema when the source first resolves its
start, which is `MicroBatchSource::open` in `repark-core`, and the source
holds it for its lifetime. Every batch of a run therefore has one column set
and one type set, as Spark's `SparkScan.toMicroBatchStream` hands the scan's
`expectedSchema` to the stream once. Files are projected by field id, with
null for a field id a file lacks: a field dropped and re-added under the same
name never shows the dropped field's values. A schema change committed after
the source opened does not change the batch schema; a new column stays
unread and a renamed one keeps the name it had at open.

The provider struct stays `pub(crate)` per the sketch, so its
`#[allow(dead_code)]` stood until round 3 wired a caller. `catalog/mod.rs`
widened one word (`mod scan_batches` to `pub(crate)`) so the provider reads
through the crate's `conform_batch` (ledger D-3, 2026-10-07); no behaviour
changed. Round 3 (2026-10-07) wired the caller: `provider_for_plan` beside
`try_new` returns `Arc<dyn TableProvider>` over the existing allowed
core-to-iceberg edge (ledger D-4), and both `dead_code` allows lifted with
the first live caller.

## I want to...

| ...do this | go to |
|---|---|
| Read the offset encoding | `offset.rs` (`InputOffset`, `OffsetVector`, `SinkRecord`) |
| Read the refusal texts | `error.rs` (`MicroBatchError`, `RecoveryReason`) |
| Change a refusal text | the sketch's §4 first — the verbatim rows are oracle cells |
| Plan a window | `window.rs` (`WindowPlanner::initial_offset`, `next_window`) |
| Count the listings a window plans | `window.rs` `planned_listings` (`#[cfg(test)]` only) |
| Read a planned window | `provider.rs` (`provider_for_plan` from another crate, `try_new` inside it) |

## Pointers

- Up: [../map.md](../map.md)
- Design: the sketch's §3.1, §3.2, §3.3, §4 and Q6

## Debug

First check: `cargo test -p repark-iceberg --lib microbatch`. A stamp that
refuses names its key; compare the bytes against the Q6 shapes quoted in
`offset.rs`'s round-trip pins.
