# map — repark-iceberg/src/catalog/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

Catalog adapter tests. `catalog/mod.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index.
- `catalog.rs` — AWS-free unit battery: CTAS reality, builder validation, live-list staleness,
  O(1) invalidation, scheme selection, span secret-hygiene, fork-patch proof, T6 residual pins.
  pins: listing-cost-flake-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
  The listing-cost pin counts catalog calls instead of wall-clock time: a call count is
  deterministic under box load, while an elapsed-time ratio cannot tell a regression from a
  noisy neighbour.
- `evolved_lineage_read.rs` — **ICE-EVO-DML-1 (2026-09-17):** a v3 table evolved by
  `ADD COLUMN` or a name swap after its only write, read through
  `LineageColumnsTableProvider`: `row_id_read_after_add_column_null_fills_the_added_column`
  and `row_id_read_after_swapping_two_names_reads_each_field_by_id` (projection and a
  `WHERE v = 'a'` filter) hold the rows under the current schema.
  pins: ice-evo-dml-1/C-016
- `io_stats.rs` — **ICE-READ-PERF-0 (2026-09-19):** the counting-layer pins. Every op kind
  counts once with its bytes (local FS under the wrapper); ranged reads count the RETURNED
  length (a stub `FileRead` whose `read(0..1000)` returns 10 bytes counts 1 request / 10 bytes —
  round 2, verifier finding F-MUT-5A: local in-bounds reads cannot tell the two apart); a ranged
  read on a data or delete file that ends on the Parquet `PAR1` / Puffin `PFA1` tail magic is a
  `footer_read`, every other ranged read (and every ranged read on another class) stays
  `ranged_read`; every counter cell is 64-byte aligned (`size_of` = cells × 64);
  `InputFile` / `OutputFile` / `writer()` obtained through the wrapper still count; the Glue and
  S3 Tables defaults are the fork's `s3a` / `s3` OpenDAL factories and the counted builders keep
  their prop errors; the classifier over literal paths, over every file a memory-catalog INSERT
  writes, and over the names the `pos-del` / `dv` generators produce; a scan reads data-file
  ranges through the counter; a second `load_table` with the metadata cache on reads fewer
  metadata JSON documents than with it off; clones share one counter set.
  The scan pin sums rows with `RecordBatch::num_rows` (clippy `redundant_closure_for_method_calls`).
  pins: ice-read-perf-0/C-001, C-002, C-003, C-004, C-005
- `namespace_scoped.rs` — G17 wrapper pins for `NamespaceScopedCatalog`.
  pins: rp-1-fork-repin/C-003
  pins: rp-4-fork-repin/C-002
- `lineage_columns.rs` — **V3-4 critic:** stored `_row_id` wins over `first_row_id +` pos;
  `WHERE id = lit` keeps matching lineage rows; `try_new_with_snapshot` is absent.
  pins: v3-4-serve-lineage-columns/C-017, C-019, C-020

## Pointers

- Up: [../map.md](../map.md)
