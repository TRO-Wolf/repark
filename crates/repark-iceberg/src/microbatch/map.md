# map — repark-iceberg/src/microbatch

## Purpose

The micro-batch track's Rust home: offset and identifier types, the offset
JSON the sink stamp carries, and the track's one error enum. Owned by slice
MB-1 under the [design sketch](../../../../task/wo/microbatch/mb-design-2026-10-06.md),
the [MB-1 order](../../../../task/wo/microbatch/mb-1-source.md), and the
[North Star](../../../../task/roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md).
Round 1 lands `mod.rs`, `offset.rs` and `error.rs`; round 2 adds the window
and provider. Progress: the [MB-1 ledger](../../../../task/ledgers/staging/mb-1-ledger.md).

## Contents

- `mod.rs` — `#![forbid(unsafe_code)]` (NS-17) plus `pub mod error;` and
  `pub mod offset;`. No re-exports: callers use full paths.
  pins: mb-1/C-007
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

## I want to...

| ...do this | go to |
|---|---|
| Read the offset encoding | `offset.rs` (`InputOffset`, `OffsetVector`, `SinkRecord`) |
| Read the refusal texts | `error.rs` (`MicroBatchError`, `RecoveryReason`) |
| Change a refusal text | the sketch's §4 first — the verbatim rows are oracle cells |
| Round 2: the window and provider | the sketch's §3.3 (not yet landed) |

## Pointers

- Up: [../map.md](../map.md)
- Design: the sketch's §3.1, §3.2, §4 and Q6

## Debug

First check: `cargo test -p repark-iceberg --lib microbatch`. A stamp that
refuses names its key; compare the bytes against the Q6 shapes quoted in
`offset.rs`'s round-trip pins.
