# map — repark-iceberg/src/write/merge/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

MERGE unit tests. `merge/mod.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index (rustfmt module order).
- `merge.rs` — primary unit battery.
- `occ_conflict.rs` — OCC-2 M19/M20 batteries B/C/E/F/G/H/I.
- `occ.rs` — OCC / commit conflict pins + M13 isolation parse + M19-A split.
- `parallel_write.rs` — concurrent file write pins.
- `streaming_scan.rs` — streaming target-scan pins + PERF-04 residual-push + MG-1.
- `streaming.rs` — stream write interleaving pins.

## Pointers

- Up: [../map.md](../map.md)
