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
  write commits; scan `DataInvalid` Null), and binary `variant` (parquet builder
  refuse). **C-002:** `fork_variant_scan_refuses_naming_the_type` — a real data file plus a
  variant projection refuses at the fork's reader guard (empty table streams cleanly);
  the §4 registry row `V3-VARIANT-SHRED-1` cites these pins and the STATUS v3 block
  truth-up rides the same landings (pins: v3-6-v3-types/C-007).
  **C-005 (2026-09-01):** `write_default` fills an omitted column on append
  (red-first vs the old refuse pin), a supplied column is kept, and `initial_default`
  reads into files missing the column. pins: v3-6-v3-types/C-001, C-002, C-005

## Pointers

- Up: [../map.md](../map.md)
