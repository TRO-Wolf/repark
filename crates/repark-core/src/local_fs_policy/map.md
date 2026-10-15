# map — repark-core/src/local_fs_policy

## Purpose

File-backed unit tests for `../local_fs_policy.rs`, the one local-file write policy of both SQL
doors and `repark.sql()`. Every refusal has a test beside a control that proves the guard is not
refusing everything.

## Contents

- [`tests.rs`](tests.rs) — the `#[cfg(test)] mod tests;` declared in `../local_fs_policy.rs`.
  Classification: the byte-5/7/9 boundaries, `the_location_is_read_the_way_the_write_reads_it`
  (DataFusion's `ListingTableUrl::parse` reading: `%2e%2e`, `%2E%2E`, `.%2e` and `..` resolved
  as the URL parser resolves them, `file:x` and `file:./x` as `/x`, `%20` decoded, a query
  dropped, a leading space kept and read against the cwd, a `file://<host>` refused as
  unreadable). Containment: the PA-03 resolver (nearest existing ancestor, a `..` after a
  missing directory, a dangling symlink, a metadata read failing other than not-found);
  **V-01, V-02 and V-03 (fix round 2026-10-15, owner ruling 103)**, each named "writes outside
  on today's head": a symlink then `..`, a percent-encoded dot segment and a relative `file:`
  URL now refuse; a `..` that cancels a symlink under the warehouse refuses even when the write
  would land inside, while a `..` that cancels a plain directory below an in-warehouse symlink
  stays contained; a warehouse registered through a symlinked path (the macOS `/tmp` shape)
  contains a direct write, a write through an in-warehouse symlink and missing middle
  directories. The plan walk over `Prepare`, `Analyze` and an `Explain` of an `Analyze`; the
  conf read; the native dialect's refusals, which name `repark.sql() does not allow local-file
  DDL` and no conf, and stay closed even with a `repark.sql` extension set true (V-04, ruling
  104). pins: pa-local-fs-policy-1/C-011, C-012, C-014, C-019, C-020, C-021, C-022, C-023, C-024, C-025

## Pointers

- Up: [src map](../map.md)
- The door pins: `crates/repark-spark/src/tests/pa_local_fs_policy.rs`,
  `crates/repark-sql/tests/pa_local_fs_policy.rs`,
  `python/repark/tests/test_pa_local_fs_policy_1.py`
