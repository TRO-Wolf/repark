# map — repark-iceberg/src/microbatch

## Purpose

The micro-batch track's Rust home: offset and identifier types, the offset
JSON the sink stamp carries, and the track's one error enum. Owned by slice
MB-1 under the [design sketch](../../../../task/wo/microbatch/mb-design-2026-10-06.md),
the [MB-1 order](../../../../task/wo/microbatch/mb-1-source.md), and the
[North Star](../../../../task/roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md).
Round 1 landed `mod.rs`, `offset.rs` and `error.rs`; round 2 adds the
window and provider. Progress: the [MB-1 ledger](../../../../task/ledgers/completed/mb-1-ledger.md).

## Contents

- `mod.rs` — `#![forbid(unsafe_code)]` (NS-17) plus `pub mod error;`,
  `pub mod offset;`, `pub mod provider;` and `pub mod window;`. No
  re-exports: callers use full paths.
  pins: mb-1/C-007, mb-1/C-013
- `offset.rs` — the sketch's §3.1. Seven newtypes, each `new`/`get`
  (NS-14), with the sketch's named constructors beside them:
  `TableUuid::of`, `QueryId::derive`, `RunId::fresh`, `Epoch::FIRST`/`next`,
  `OffsetFormatVersion::CURRENT`. `InputOffset`, `OffsetVector`,
  `SinkRecord` with its summary/property readers and writers,
  `SinkDoor`, `spark_source_offset_json`, and the nine key constants.
  pins: mb-1/C-001, C-002, C-003, C-004, C-005
- `error.rs` — the sketch's §3.2: `MicroBatchError` with every variant,
  `thiserror`, `#[non_exhaustive]` (NS-15), plus `RecoveryReason`.
  pins: mb-1/C-006
- `window.rs` — the sketch's §3.3: `ReadCaps`, `StartPosition`,
  `WindowLimit`, `PlannedFile`, `WindowPlan`, and
  `WindowPlanner::{new, initial_offset, next_window}` over a held table.
  One fork scan with `with_fail_on_non_append(true)` decides the refusal;
  per-snapshot plain scans list the files, sorted by path. The skip builders
  are never called (O-5).
  pins: mb-1/C-008, C-009, C-010, C-011
- `window_tests.rs` — the `window.rs` pins, split out under `#[path]` when
  the file passed the 1000-line ceiling: the memory-catalog fixture with
  append/overwrite/delete/replace commits plus the 18 start, window, cap,
  fail-loud and guard pins.
  pins: mb-1/C-008, C-009, C-010, C-011
- `provider.rs` — the sketch's §3.3 `MicroBatchTableProvider` (`pub(crate)`):
  reads exactly the planned tasks with `ArrowReaderBuilder` and the crate's
  `conform_batch`, and takes the arrow schema from the end snapshot. Filters
  stay `Inexact` (the tasks are pre-planned, so DataFusion re-applies them)
  and projection is served by `conform_batch`, not by re-planning.
  **MB-1 round 3 (2026-10-07):** `provider_for_plan` beside `try_new` is the
  type-erased cross-crate door — the struct stays `pub(crate)` per the sketch
  and `repark-core` reads through `Arc<dyn TableProvider>` (ledger D-4).
  pins: mb-1/C-012, C-015, C-016

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
(`repark.cdc.format-version`) is absent. A present version that parses but is
not `CURRENT` refuses `UnsupportedOffsetFormat`; a version that does not parse and
any missing or unreadable companion key refuse `Catalog` naming the key
(ledger FL-2, 2026-10-07). `from_property` has no absent case and refuses the
same way. Neither reader guesses.

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
library owns the escaping. `to_string` over a string/number-only `Value`
cannot fail (no floats, no writer), and its `unwrap_or_default` is
unreachable.

`Cargo.toml` carries three lines for this module: the granted
`thiserror.workspace = true`, the `v5` feature on the existing `uuid` line
(the sketch's `QueryId::derive` calls `Uuid::new_v5`, and no other crate in
the closure enables it), and `serde_json.workspace = true` in
`[dependencies]` for the stamp readers and writers (ledger D-1,
2026-10-07). No version moved; the lockfile change is edges only.

The fork's `PreconditionFailed` is the window's single refusal decider. The
local ancestry walk on that path extracts the refused snapshot id and
operation for the `NonAppendSnapshot` struct; it never refuses on its own,
so dropping `with_fail_on_non_append(true)` turns the refusal pins red
(ledger C-010). The walk agrees with the fork by construction: both follow
the parent chain from the head, oldest-first, and neither refuses `Replace`.

Three bounds decided in round 2 (ledger FL-3, FL-4, FL-5, 2026-10-07). A
`from.snapshot` that sits in metadata but off the current ancestry refuses
`SourceSnapshotExpired` like a fully expired id. A `FromTimestamp` past the
head returns the head fully consumed, so later appends stream. The
`from.snapshot` itself stays outside the refusal range: its listing runs
without fail-loud and contributes no files when it is not an append.

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
| Read a planned window | `provider.rs` (`provider_for_plan` from another crate, `try_new` inside it) |

## Pointers

- Up: [../map.md](../map.md)
- Design: the sketch's §3.1, §3.2, §3.3, §4 and Q6

## Debug

First check: `cargo test -p repark-iceberg --lib microbatch`. A stamp that
refuses names its key; compare the bytes against the Q6 shapes quoted in
`offset.rs`'s round-trip pins.
