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
- `crash_tests.rs` — **MB-2a (2026-10-07):** the crash harness of the
  [sketch's §5](../../../../task/wo/microbatch/mb-design-2026-10-06.md), in Rust over the memory
  catalog (correction H-1). It holds pin 1, `test_microbatch_kill_after_commit_resumes_1`, the
  green guard that resume-from-sink alone holds: Bronze takes two appends; epoch 0 plans the
  window, reads it through `provider_for_plan`, stages it into the empty sink and commits through
  the stamped append arm under a `BatchScope`; every in-memory value drops (kill point c). A
  reload and `read_resume_point` resume epoch 0's offset; epoch 1's window is planned and read,
  then dropped before staging (kill point a), and the next reload still reads epoch 0. Epoch 1
  then commits under a fresh run id, and a third trigger finds no window and commits nothing. The
  sink equals Bronze with each id once, epochs 0 and 1 each appear once in the summary history,
  and the property equals the head's stamp. Pins 2–4 (red until MB-2c) and pin 5 belong to the
  harness slice and are not here. Fold 1 (2026-10-07): the stamped append passes the guard's
  `ScopeToken` in its extras, the way MB-3's session config will.
  pins: mb-2a/C-010, C-013
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
- `error.rs` — the sketch's §3.2: `MicroBatchError` with every variant,
  `thiserror`, `#[non_exhaustive]` (NS-15), plus `RecoveryReason`. Fold 1
  adds `OffsetPositionOutOfRange`, and `UnsupportedOffsetFormat.found`
  becomes the version text as read. Fold 2 adds `SnapshotNotInLineage` (G5).
  MB-2a fold 1 (ruling V2, 2026-10-07) adds `RecoveryReason::StampNotInLineage`: a sink stamp
  that is retained but off the current lineage (a rollback), naming a new `queryName` or a
  restore; `StampedSnapshotExpired` keeps the stamp that is truly gone.
  pins: mb-1/C-006, C-020, C-024, C-036
  pins: mb-2a/C-014
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
  pins: mb-1/C-012, C-015, C-016, C-022, C-033

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
