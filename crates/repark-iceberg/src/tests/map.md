# map — repark-iceberg/src/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed.

## Purpose

Crate-root test modules. `lib.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index (rustfmt module order).
- `tracing.rs` — shared tracing harness: one global subscriber, both capture layers
  (forced-edit class 6). Accessors used by `catalog/tests/catalog.rs` and
  `write/merge/tests/streaming_scan.rs`.
- `fork_pin.rs` — ADR-0001 fork-pin proof: names and exercises fork-only public API
  (`iceberg::plan_commit_base_load` / `CommitBaseLoadPlan`).

## Pointers

- Up: [../map.md](../map.md)
