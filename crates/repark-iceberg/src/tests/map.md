# map — repark-iceberg/src/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

Crate-root test modules. `lib.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index (rustfmt module order).
- `tracing.rs` — shared tracing harness: one global subscriber, both capture layers
  (forced-edit class 6). Accessors used by `catalog/tests/catalog.rs` and
  `write/merge/tests/streaming_scan.rs`.
- `fork_pin.rs` — ADR-0001 fork-pin proof: names and exercises fork-only public API
  (`iceberg::plan_commit_base_load` / `CommitBaseLoadPlan`).
- `v3_types.rs` — **V3-6 C-001:** fork pin `33be9a0` read/write measurement for
  `timestamp_ns` / `timestamptz_ns` (parquet round-trip), `unknown` (Arrow Null;
  write commits; scan `DataInvalid` Null), binary `variant` (parquet builder
  refuse), and `write_default` (schema stores it; engine `append` still refuses a
  missing column). pins: v3-6-v3-types/C-001

## Pointers

- Up: [../map.md](../map.md)
