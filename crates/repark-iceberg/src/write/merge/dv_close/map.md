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
  up in [../map.md](../map.md). Every helper below `collect_superseded_legacy_deletes` is
  private: the ported `is_deletion_vector` / `referenced_data_file_location` are read only here,
  and keeping them private is what makes a future unused one a compile warning rather than a
  silently dead second copy of the fork's scoping rule.
  pins: v3-12-legacy-delete-merge/C-002, C-003, C-004
