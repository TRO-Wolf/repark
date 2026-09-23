# map — repark-iceberg/src/catalog/tests

CC-3 (2026-08-30): comments condensed to one line; banners removed; truncated comments rewritten as complete sentences (D-001).

## Purpose

Catalog adapter tests. `catalog/mod.rs` declares `#[cfg(test)] mod tests;`.

## Contents

- `mod.rs` — thin index.
- `namespace_drop.rs` — **ICE-DROP-NS-1 (2026-09-19):** `refuse_non_empty_namespace_drop`
  refuses a table-holding namespace with the count, passes an empty one, passes after the
  table is dropped, and fails loud on a missing namespace — all against a memory catalog.
  pins: ice-drop-ns-1/C-007, C-008
  A child namespace refuses the parent's drop (`Contains 1 child namespace(s).`).
  pins: ice-drop-ns-1/C-011
  **ICE-VIEWS-1 (2026-09-20):** a view-only namespace refuses the same way
  (`Contains 1 view(s).`).
  pins: ice-views-1/C-013
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
  their prop errors (called with `CatalogCaches::disabled()` since ICE-CATALOG-CACHE-1); the classifier over literal paths, over every file a memory-catalog INSERT
  writes, and over the names the `pos-del` / `dv` generators produce; a scan reads data-file
  ranges through the counter; a second `load_table` with the metadata cache on reads fewer
  metadata JSON documents than with it off; clones share one counter set.
  The scan pin sums rows with `RecordBatch::num_rows` (clippy `redundant_closure_for_method_calls`).
  Round 3 (verifier finding F-MUT-D): the fork's `Storage` trait (`43fcd243`) defaults two
  methods, `write_new` (the default is `exists` then `write`) and `list` (the default is
  `FeatureUnsupported`). `every_defaulted_storage_method_delegates_to_the_inner_storage` wraps a
  stub whose `write_new` and `list` succeed and record the call, while its `exists` and `write`
  error. The wrapper must reach the stub's own `write_new` and `list` once each, and count one
  data-file write of 12 bytes and one list. Dropping either override, or re-spelling
  `write_new` as `exists` + `write`, reds it (2026-09-19).
  pins: ice-read-perf-0/C-001, C-002, C-003, C-004, C-005, C-010
  pins: rp-37-fork-pin/C-001
  pins: rp-38-fork-pin/C-001
- `cache_wiring.rs` — **ICE-CATALOG-CACHE-1 (2026-09-19):** the cache-wiring pins, offline. A
  recording `CacheWiredBuilder` proves what `wire_caches` passes (the session's metadata `Arc`,
  the manifest bytes, the credential context; nothing when disabled; each switch alone); real
  Glue and S3 Tables builds (region and static keys in props, no service call at build) raise the
  session handle's `strong_count` by one each and print no secret in `Debug`. The scope and
  staleness pins run on the memory catalog behind the same `wire_caches` (the AWS builders'
  pointer seam is `#[cfg(test)]` in the fork): two access keys adopting one metadata location
  hold two entries, one key shares; no selector keeps each instance apart; a second handle's
  commit is seen and a same-scope sibling at the old location never reads it; three 40 KiB
  documents under a one-entry budget evict and never serve a sibling; a cold scan counts the same
  manifest-list / manifest / data-file requests with the caches on as off (requests, not bytes:
  the beds' temp paths differ in length). The two AWS build futures are boxed (clippy
  `large_futures`). `the_door_trim_settles_before_it_reads_the_high_water_mark` (round 2, Q-001)
  loads four small tables under a one-entry bound, calls `trim()` with nothing settled first,
  then settles and requires zero retained entries: moka's `entry_count` still reads 0 before its
  pending tasks run, so a `trim` that reads the unsettled `metadata_len()` never clears (red 3/3
  as `left: 4, right: 0`). pins: ice-catalog-cache-1/C-001, C-003, C-005, C-006,
  C-008, C-010, C-012
  **ICE-FOOTER-CACHE-1 (2026-09-19):** the recorder also records the footer `Arc`: it reaches
  the builder by default, not when disabled, alone with a credential context, and not at
  `footerCacheBytes = 0`; `every_builder_holds_the_session_footer_cache` raises the footer
  handle's `strong_count` by one per memory, Glue and S3 Tables build. The counter pin now
  compares warm data-file PAGE reads (requests and bytes) on vs off and requires zero warm
  footer reads with the caches on (and some with them off). pins: ice-footer-cache-1/C-002, C-005
- `footer_cache.rs` — **ICE-FOOTER-CACHE-1 (2026-09-19):** the footer-cache scan pins on a local
  memory-catalog table (three data files of 60,000 rows written through DataFusion's
  `generate_series`, read by a second catalog built with the caches under test). A warm re-scan
  reads zero data-file footers with the cache on, and the cache's `fetches` equal the cold scan's
  footer reads; with `footerCacheBytes = 0` the warm scan reads as many footers as the cold one.
  The cold scan reads at most as many footers with the cache on as off (measured 3 vs 4: the
  cache also collapses a split file's second footer read). A filtered scan after an unfiltered
  one answers right (100 rows, their sum), raises `upgrades`, fetches nothing new and reads no
  footer; a repeat does not upgrade again. Warm page reads are identical on and off. The table
  is reached through `Arc<dyn Catalog>` method calls, so the module imports no `Catalog` trait.
  pins: ice-footer-cache-1/C-003, C-004, C-005
- `namespace_scoped.rs` — G17 wrapper pins for `NamespaceScopedCatalog`.
  pins: rp-1-fork-repin/C-003
  pins: rp-4-fork-repin/C-002
- `lineage_columns.rs` — **V3-4 critic:** stored `_row_id` wins over `first_row_id +` pos;
  `WHERE id = lit` keeps matching lineage rows; `try_new_with_snapshot` is absent.
  pins: v3-4-serve-lineage-columns/C-017, C-019, C-020

## Pointers

- Up: [../map.md](../map.md)
