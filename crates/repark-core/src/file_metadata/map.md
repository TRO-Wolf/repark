# map — repark-core/src/file_metadata

## Purpose

File-backed kernels beside [`file_metadata.rs`](../file_metadata.rs): the
marker scan wrapper, the plan walk, the metadata struct UDF, the per-file
augmentation, the select/filter/sort planning entry points, and the error
contract. Split from `file_metadata.rs` at the 1000-line ceiling in
DF-METADATA-COL-1 round 3; behavior unchanged by the move.

## Modules

- [`scan.rs`](scan.rs) — `FileKind`, the delegating `FileMetadataScan`
  marker, `FileHit`, and the physical-plan file listing. pins:
  df-metadata-col-1/M-1, M-3
- [`status.rs`](status.rs) — plan walk (`Found` / `Realized` / dead ends)
  and the `Available` / `Shadowed` / `Absent` status. pins:
  df-metadata-col-1/M-2
- [`udf.rs`](udf.rs) — the hidden `_metadata` struct shape and the
  per-file struct-building UDF. pins: df-metadata-col-1/M-1
- [`augment.rs`](augment.rs) — per-file reread plus union branches with
  `row_index`, and the exact-schema finish. pins: df-metadata-col-1/M-1,
  M-3
- [`ensure.rs`](ensure.rs) — `ensure_file_metadata`, the `_metadata`
  reference rewrite, hidden passthrough widening, and scan marking. pins:
  df-metadata-col-1/M-2, M-4
- [`error.rs`](error.rs) — `FileMetadataError`: unresolved column,
  missing-attribute, and engine failure shapes. pins:
  df-metadata-col-1/M-5
- [`tests.rs`](tests.rs) — marker/status round-trips, join-missing error
  shape, and the narrowed-reselect plus second-hop pins.
  pins: df-metadata-col-1/M-2, M-4
