# map — repark-core/src/local_fs_policy

## Purpose

File-backed unit tests for `../local_fs_policy.rs`, the one local-file write policy of both SQL
doors and `repark.sql()`. Every refusal has a test beside a control that proves the guard is not
refusing everything.

## Contents

- [`tests.rs`](tests.rs) — the `#[cfg(test)] mod tests;` declared in `../local_fs_policy.rs`.
  Classification: the byte-5/7/9 boundaries, `the_location_is_read_the_way_the_write_reads_it`
  (DataFusion's `ListingTableUrl::parse` reading: `%2e%2e`, `%2E%2E`, `.%2e` and `..` removed as
  the URL parser removes them, `file:x` and `file:./x` as `/x`, `%20` decoded, a query dropped, a
  leading space kept and read against the cwd, a `file://<host>` refused as unreadable) and
  `the_written_reading_never_keeps_a_dot_dot` (no spelling leaves a `..` in the written path).
  Containment, owner ruling 114 (b) of 2026-10-16, where the bytes land: the nearest existing
  ancestor resolved through a symlink, a `..` after a missing directory refused by the resolver, a
  dangling symlink, a metadata read failing other than not-found; **V-01, V-02 and V-03**, each
  named "writes outside on today's head" (a symlink then `..`, a percent-encoded dot segment, a
  relative `file:` URL) refuse; `the_cells_of_ruling_114_land_inside_where_the_written_path_lands`
  (`deep/../dd_in`, `deep/../m1/dd_in2`, `s2/../sb_in`, `file://…/x/%2e%2e/pin2`, `x/../pin_bare`,
  `file://…/x/../pin_url`, `inner/x/../y`, each with its computed landing path); **RV-01** (a `[`,
  `?` or `*` in the warehouse path refuses its parent, its siblings and a bare path the glob cuts,
  while the `file:` spelling writes inside), **RV-02** (a `..` over a symlink above the warehouse
  lands inside), **RV-03** (U+E000 to U+E002, literal or percent-encoded, are ordinary characters);
  a warehouse registered through a symlinked path (the macOS `/tmp` shape) contains a direct write,
  a write through an in-warehouse symlink and missing middle directories. The plan walk over
  `Prepare`, `Analyze` and an `Explain` of an `Analyze`; the conf read; the native dialect's
  refusals, which name `repark.sql() does not allow local-file DDL` and no conf, and stay closed
  even with a `repark.sql` extension set true (V-04, ruling 104).
  pins: pa-local-fs-policy-1/C-011, C-012, C-014, C-019, C-020, C-021, C-022, C-023, C-024, C-026, C-027, C-028, C-029

## Pointers

- Up: [src map](../map.md)
- The door pins: `crates/repark-spark/src/tests/pa_local_fs_policy.rs`,
  `crates/repark-sql/tests/pa_local_fs_policy.rs`,
  `python/repark/tests/test_pa_local_fs_policy_1.py`
