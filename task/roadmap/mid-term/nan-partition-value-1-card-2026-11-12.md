# Card NAN-PARTITION-VALUE-1 — a NaN in a FLOAT or DOUBLE partition column is written as NULL

**Date:** 2026-11-12 · **Filed by:** Claude Opus 5.5 (`claude-opus-5-5`), the second builder ·
**Source:** signed-zero part B, item 1 addendum §2 and §3 item 11 (branch
`fix/signed-zero-normalize-1-part-b`), and the owner's ruling of 2026-11-12 on Q10, verbatim
"Go with the defaults": a fix of two fork files or fewer lands before ICE-V2-FLAT-AWS-LABEL-1.
**Severity:** S1, wrong write. **Status:** FIXED on branch `fix/nan-partition-value-1` in both
repositories (fork PR #383, squash-merged 2026-11-13 as `92242cb4`, the tree of its verified head
`46d5db39`; built and measured as `945d3326` off `66901f02`, the same two-file change). Evidence:
[task/wo/nan-partition-value-1/](../../wo/nan-partition-value-1/map.md).

## What was wrong

A RePark `INSERT`, DataFrame `writeTo(...).append()` or `saveAsTable` into a table
partitioned by a FLOAT or DOUBLE column stored the partition value of the NaN file as NULL in
the manifest. `±Infinity` was stored as NULL the same way. The data file kept the NaN. So:

- RePark's own `WHERE isnan(d)` and `WHERE d = CAST('NaN' AS DOUBLE)` returned no rows: the
  scan pruned the file on its NULL partition.
- Spark reading the table returned NULL for the NaN row and lost it from `isnan`.
- `.files` showed two `d = NULL` files, and `.partitions` merged the NaN file with the NULL one.

`CREATE TABLE … AS SELECT` was not affected: it writes through RePark's own fan-out writer and
never crosses the JSON step below.

## The seat

The fork's DataFusion insert plan (`IcebergTableProvider::insert_into`,
`crates/integrations/datafusion/src/table/mod.rs:244`) hands each data file from
`IcebergWriteExec` to `IcebergCommitExec` as a JSON string
(`physical_plan/write.rs:370` → `physical_plan/commit.rs:323`). The string came from
`serialize_data_file_to_json` (`crates/iceberg/src/spec/manifest/mod.rs:121` at `66901f02`):
`serde_json` writes a non-finite `f32`/`f64` as `null`. The commit read the `null` back as a
NULL partition and wrote it to the Avro manifest. The Avro manifest itself stores NaN
losslessly (the CTAS cells and a fork unit test show it).

## The fix (fork, two files)

- `crates/iceberg/src/spec/manifest/_serde.rs`: the JSON transport writes a NaN or infinite
  FLOAT/DOUBLE partition slot as the string `"NaN"`, `"Infinity"` or `"-Infinity"`, which is
  what Java's `SingleValueParser.toJson` writes (measured on Iceberg 1.11.0). Reading back, a
  FLOAT/DOUBLE slot holding one of those strings becomes the float again. Any other slot,
  type or value goes through the old path unchanged; a file with no non-finite slot serializes
  to the same bytes as before. Java's own `fromJson` refuses `"NaN"`, so Java has no lossless
  JSON form to match; the transport is internal to one plan and never persisted.
- `crates/iceberg/src/spec/manifest/mod.rs`: `serialize_data_file_to_json` builds its
  `DataFileSerde` with `for_json`, and `deserialize_data_file_from_json` converts with
  `try_into_from_json` (one line each; the file stays at its 1237-line ceiling).
- RePark: the fork pin bump only (`Cargo.toml`, the fork source lines of `Cargo.lock`).

## Related

- **`ICEBERG-PARTITIONS-TABLE-FLOAT-1`** (S3, filed on the part B branch, not on main):
  `.partitions` merges `-0.0` with `0.0` and NaN with NULL. The NaN half was this wrong write;
  with it fixed, `.partitions` lists the NaN partition on its own. The signed-zero half stays
  with that card and part B.
- **Tables written by RePark 1.5.6 or earlier:** every file written by `INSERT`, `writeTo` or
  `saveAsTable` whose FLOAT or DOUBLE partition value was NaN or `±Infinity` carries a NULL
  partition in its manifest entry. Its rows are intact. A rewrite of those files with a fixed
  build writes a correct partition value: `CALL … rewrite_data_files` with the
  `rewrite-all` option (a bare call rewrote no file on a seeded broken table; verifier 2,
  2026-11-13), or delete and re-insert the rows. Until then, `isnan`, `= NaN` and partition filters miss those rows in every
  engine.
- **Out of this unit, not measured:** a NaN column default (`initial-default`/`write-default`)
  goes through `Literal::try_into_json` (`crates/iceberg/src/spec/values/literal.rs:630-640`)
  and would be written to table metadata as `null`; Java writes `"NaN"` and cannot read it back.

## Done when

- Every pin cell of `python/repark/tests/iceberg/test_nan_partition_value_1.py` passes on the
  fixed build, with the INSERT, `writeTo` and `saveAsTable` cells red on main's build.
- The fork tests at the seat pass in FLOAT and DOUBLE.
- The controls (INT and STRING NULL partitions, the unpartitioned table, the CTAS door, the
  signed-zero partition) are unchanged.
