# map — repark-core/src/session/path_write

## Purpose

S3 path-write helpers too large to live inside
[../path_write.rs](../path_write.rs). That file owns the save-mode protocol
and the COPY commit and declares this directory.

## Contents

- `append.rs` — **TEXT-WRITE-TIMESTAMP-ZONE-1 re-verify 3 fold
  (2026-09-30):** the append schema-validation cohort
  (`ReparkSession::validate_append` plus `validate_append_parquet`,
  `validate_append_csv`, `validate_append_json`, `append_types_compatible`,
  `strip_partition_names` and `quoted_list`), moved verbatim from
  `../path_write.rs` when the rollback wiring pushed that file past its
  ceiling; the only delta is the method's `pub(super)` visibility, which the
  move requires. The cohort still refuses silent null-fill and schema drift
  on every format.
- `rollback.rs` — **TEXT-WRITE-TIMESTAMP-ZONE-1 re-verify 3 fold
  (2026-09-30):** the failed-write rollback. `commit_s3_write` snapshots the
  destination keys before the COPY and, on any failure, deletes every key
  missing from the snapshot, so a lazy render error (or any later commit
  failure) leaves no new objects behind on any mode, including partitioned
  layouts and empty parts; existing objects under append are untouched. A
  snapshot-listing failure refuses before the COPY, so it cannot strand
  partial output. A cleanup failure is appended to the original error text
  with the error variant preserved, never replacing it. The overwrite mode
  still deletes the old prefix before the COPY, so a failed overwrite loses
  the old data; that order predates this fold and is measured, not changed.
  pins: text-write-timestamp-zone-1/C-007

## Pointers

- Up: [../map.md](../map.md)
