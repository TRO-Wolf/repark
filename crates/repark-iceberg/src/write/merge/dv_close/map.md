# map — repark-iceberg/src/write/merge/dv_close/

## Purpose

The one submodule of `../dv_close.rs`, the v3 `RowDelta` deletion-vector container close. It sits
here rather than beside `dv_close.rs` because `../mod.rs` sits on an exact 1795-line ceiling that
only moves down, and a `mod` line there would have grown it.

## Contents

- `legacy_deletes.rs` — **V3-12 (2026-09-02):** `collect_superseded_legacy_deletes` returns, for
  the data files this commit gives a DV, the positions of every live NON-Puffin position delete
  that still applies plus the delete files themselves, so `plan_deletion_vectors` can union them
  into the new DV and remove them in the same commit. Full rationale — the ported file-scope
  test, the lazy data-manifest walk and the shapes deliberately left refusing — lives one level
  up in [../map.md](../map.md). **V3-12 C-006:** the snapshot it reads is the `snapshot_id` the
  caller resolved for the target scan, not the current snapshot, so a `to_branch` write collects
  the BRANCH's legacy deletes. Every helper below `collect_superseded_legacy_deletes` is
  private: the ported `is_deletion_vector` / `referenced_data_file_location` are read only here,
  the latter returning `Cow` so the common bounds leg borrows instead of allocating a `String`
  for every delete entry examined before the `touched` test,
  and keeping them private is what makes a future unused one a compile warning rather than a
  silently dead second copy of the fork's scoping rule.
  `positions_from_parquet` is the table-free seam its two unit tests drive: one proves positions
  are filtered to the referenced data file, the other that a leading `row` column is projected
  away without shifting the reserved columns.
  pins: v3-12-legacy-delete-merge/C-002, C-003, C-004, C-007
