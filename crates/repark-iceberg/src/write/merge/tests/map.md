# map — repark-iceberg/src/write/merge/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001). Wrapped-line fragments rewritten as complete sentences (D-002).

## Purpose

MERGE unit tests. `merge/mod.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index (rustfmt module order).
- `merge.rs` — primary unit battery.
- `lineage.rs` — V3-7 rewrite-projection and scratch-schema pins for carried `_row_id`.
  pins: v3-7-merge-lineage/C-001
- `nmbs.rs` — DML-A `WHEN NOT MATCHED BY SOURCE` SQL-fragment pins and skip_cardinality
  with an NMBS clause present.
  pins: dml-a-merge-not-matched-by-source/C-002, C-003, C-004, C-005
- `occ_conflict.rs` — OCC-2 M19/M20 batteries B/C/E/F/G/H/I.
- `occ.rs` — OCC / commit conflict pins + M13 isolation parse + M19-A split.
  RP-5 C-007: snapshot isolation still commits through a concurrent append.
  pins: rp-5-fork-repin/C-007
- `occ_branch.rs` — RP-5 critic OCC-on-branch: concurrent branch append is
  `DataInvalid` / not retryable / `Found conflicting files…`; concurrent main append
  does not fail the branch commit.
  pins: rp-5-fork-repin/C-004
- `parallel_write.rs` — concurrent file write pins.
- `partition_sink.rs` — **RP-7 (2026-09-02):** the identity/MERGE target scan records each
  planned `FileScanTask`'s `(spec_id, partition)`, so the v3 DV close never re-walks the data
  manifests it just read. The pin compares the drained sink to the manifest truth on a
  three-partition table; mutation (make `record_scanned_partitions` a no-op) 1 red of 1.
  pins: rp-7-f18-repin/C-002
- `occ_partitions.rs` — **RP-7 (2026-09-02):** one battery through the PRODUCTION
  `commit_row_delta_kind_with_partitions` variant on a partitioned v3 table with a real partition
  map: the commit lands, and a stale `validate_from_snapshot` pin is still rejected with the
  table unmoved. `occ.rs` / `occ_conflict.rs` keep their spellings and exercise the empty-map
  wrappers, which have no production caller left and are `#[cfg(test)]`.
  pins: rp-7-f18-repin/C-002
- `streaming_scan.rs` — streaming target-scan pins + PERF-04 residual-push + MG-1.
- `streaming.rs` — stream write interleaving pins.

## Pointers

- Up: [../map.md](../map.md)
