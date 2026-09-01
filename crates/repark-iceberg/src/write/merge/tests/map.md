# map — repark-iceberg/src/write/merge/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Wrapped-line fragments rewritten as complete sentences (D-002).

## Purpose

MERGE unit tests. `merge/mod.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index (rustfmt module order).
- `merge.rs` — primary unit battery.
- `nmbs.rs` — DML-A `WHEN NOT MATCHED BY SOURCE` SQL-fragment pins and skip_cardinality
  with an NMBS clause present.
  pins: dml-a-merge-not-matched-by-source/C-002, C-003, C-004, C-005
- `occ_conflict.rs` — OCC-2 M19/M20 batteries B/C/E/F/G/H/I.
- `occ.rs` — OCC / commit conflict pins + M13 isolation parse + M19-A split.
  RP-5 C-007: snapshot isolation still commits through a concurrent append.
  pins: rp-5-fork-repin/C-007
- `occ_branch.rs` — RP-5 critic OCC-on-branch: concurrent branch append vs concurrent main.
  pins: rp-5-fork-repin/C-004
- `parallel_write.rs` — concurrent file write pins.
- `streaming_scan.rs` — streaming target-scan pins + PERF-04 residual-push + MG-1.
- `streaming.rs` — stream write interleaving pins.

## Pointers

- Up: [../map.md](../map.md)
