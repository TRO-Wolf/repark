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
- `rollback.rs` — **TEXT-WRITE-TIMESTAMP-ZONE-1 re-verify 4 fold
  (2026-09-30):** the failed-write rollback. The re-verify 3 snapshot diff
  is gone: it deleted objects a concurrent writer committed during the
  write window, anywhere in the bucket at the bucket root. A failed CSV/JSON
  write now deletes exactly the output paths its own sink recorded (plus a
  materialized empty part, tracked separately because the sink never sees
  it); no listing-based deletion remains. `_SUCCESS` is never deleted on
  failure: its PUT is the last commit step, so a failed PUT created nothing
  and a surviving one belongs to a concurrent or earlier writer. A cleanup
  failure is appended to the original error text with the error variant
  preserved and the bare cleanup message carried once, never replacing the
  original error. Parquet keeps its pre-rollback commit with no cleanup, a
  known gap: a parquet-side rollback needs its own sink. The overwrite mode
  still deletes the old prefix before the COPY, so a failed overwrite loses
  the old data; that order predates this fold and is measured, not changed.
- `hive_dirs.rs` — **PARTITIONBY-NULL-WRITE-1 (2026-11-25):** Spark's partition
  directory names for COPY-written path writes. `hive_partition_frame` replaces
  each partition column whose type DataFusion's demux can name with a Utf8
  column of the demux's own text, except that a NULL or empty value becomes
  `__HIVE_DEFAULT_PARTITION__`; types the demux refuses (decimal, timestamp,
  binary) are left alone, so they still refuse. `spark_partition_segment`
  undoes `object_store`'s percent-encoding of a written `name=value` segment
  and escapes both halves with `text_partition::escape_partition_value`
  (Spark's `escapePathName` set); `respell_local_partition_dirs` renames the
  facade's staging directories to that depth, and
  `respell_store_partition_keys` renames S3 keys below the prefix. Floats keep
  the demux's Rust text (`1`, `inf`), which is still unlike Spark (`1.0`,
  `Infinity`).

## Pointers

- Up: [../map.md](../map.md)
