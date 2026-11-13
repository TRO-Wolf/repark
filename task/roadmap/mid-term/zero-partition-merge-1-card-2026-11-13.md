# Card ZERO-PARTITION-MERGE-1: a `0.0` row and a `-0.0` row written together land in one DOUBLE partition

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** NAN-WRITE-CANONICAL-1 verifier
1, 2026-11-13 (#1325, finding C1, class (c), and its probe `sz.py`); NAN-WRITE-CANONICAL-1 verifier 2, 2026-11-13
(#1325, finding C1); owner, 2026-11-13, about 16:45 UTC, verbatim "Go with default".

**Status:** filed 2026-11-13, ruled label-blocking; **FIXED 2026-11-13** by this unit (fork `bc5a9211`, see "Fixed" below). Severity **S1**: a wrong value through every
reader. The owner's ruling makes it label-blocking for the scoped label ICE-V2-FLAT-AWS-LABEL-1: a small fork unit
after the current lanes free. Pre-existing: base and head of #1325 store the same layout.

## Measured

- **The defect.** On an identity-partitioned DOUBLE column, `-0.0` and `0.0` rows that reach the same writer batch
  land in one partition, under whichever zero arrives first.
- **The `INSERT` door** (verifier 1, `sz.py`): an `INSERT` of `-0.0`, `0.0` and `1.5`.
  - On 2 cores: files `{d: -0.0}` with two rows and `{d: 1.5}` with one; row 2 reads back `8000000000000000`
    (`-0.0`) on RePark and on Spark.
  - On 16 cores: three files, correct.
  - Spark's own table keeps two zero partitions.
- **More doors** (verifier 2): CTAS, RTAS, `writeTo` `create` and `createOrReplace`, `saveAsTable` overwrite (v2 and
  v3, Parquet, ORC and Avro), v3 MERGE (insert, update copy-on-write and update merge-on-read) and
  `rewrite_data_files` on a partitioned table store the `+0.0` row in the `-0.0` partition. One file with partition
  value `d = 8000000000000000` holds both rows. Both engines then read the `+0.0` row as `-0.0` (16 tables read by
  Spark). The v3 MERGE door does it even on 16 cores.
- **FLOAT beside it** (verifier 2): Spark reads a `+0.0` FLOAT as `-0.0` from such an ORC file, although RePark's
  stored bit is `00000000`; a Spark rewrite of that file stores `80000000`. Cause not found; confined to the merged
  file.

## Likely mechanism (inferred from the source, not traced)

The fork's partition splitter keys partitions by `Literal` / `OrderedFloat` equality, where `-0.0 == 0.0`.

## Consequence for #1325

The #1325 pin cell `avro-insert` fails deterministically on a 1- or 2-core host (row 8 reads `-0.0`); it passed on 3,
4 and 16 cores and in CI (4 vCPU). The failure is a true detection of this card, not a NaN regression. #1325's branch
at filing (`e51927b1`) reseeds that cell so no partition holds both zeros.

## Related

- ICEBERG-PARTITIONS-TABLE-FLOAT-1 (S3, filed on #1326's branch, not on main): the `.partitions` read side, which
  groups the two zero partitions into one row. [NAN-PARTITION-VALUE-1](nan-partition-value-1-card-2026-11-12.md)
  names it under "Related".
- [SIGNED-ZERO-WRITE-METADATA-1](signed-zero-write-metadata-1-card-2026-11-13.md): the other signed-zero write
  differences from the same verifier.

## Close

Every partitioned write door keeps `0.0` and `-0.0` in their own partitions on every core count, as Spark's table
does, and both engines read each row's own zero; pinned on a 1-core and a many-core run, the fork test at the splitter.

## Fixed (2026-11-13, ZERO-PARTITION-MERGE-1, `claude-opus-5-5`)

Step 0, the inventory critic's verdict (GAPS: one missed door, one non-`Struct` key, one sort key, two pinning
tests, four ordering helpers) and the records are in [the unit's directory](../../wo/zero-partition-merge-1/map.md).

- **Seats (three product files).**
  - Fork `crates/iceberg/src/spec/values/struct_value.rs`: `Struct` compares and hashes a FLOAT or DOUBLE leaf,
    top level or nested, by its bits after NaN canonicalisation, which is Java's `Double.equals` / `hashCode`.
    Every map and set keyed on a partition tuple now keeps `-0.0` and `0.0` apart. That covers the splitter, both
    partitioning writers, the rewrite plan and writers, the replace-partitions drop set,
    `rewrite_position_delete_files`, remove-dangling-deletes, commit-time delete matching, RePark's lineage and
    position-delete groups, and the read side's delete-file index. `PrimitiveLiteral` and `Datum` are unchanged.
  - Fork `crates/integrations/datafusion/src/physical_plan/sort.rs`: the write-order sort key on `_partition`
    canonicalises NaN only, so a write-ordered table sorts the two zeros apart instead of interleaving them.
  - RePark `crates/repark-iceberg/src/write/summary_collision.rs`: the dynamic-overwrite summary check matches
    replaced files by `(spec id, Struct)`, so its refusal counts only the replaced partition's files.
- **Pins.**
  - Fork unit tests at both seats.
  - Two fork tests that pinned the merge now assert two partitions:
    - `record_batch_partition_splitter.rs` `test_float_partition_values_stay_on_the_row_wise_split`, which now
      expects 3 parts;
    - `tests/sorted_insert.rs` `float_partition_signed_zeros_write_two_partitions_sorted_by_id`, renamed from
      `…_stays_sorted_by_id`.
  - The facade test `python/repark/tests/iceberg/test_zero_partition_merge_1.py` covers 18 doors x DOUBLE and
    FLOAT x 1 and 16 writers.
    - Red on main: 60 of 72.
    - Green on the fix: 72 of 72, on 1 CPU and on 10 CPUs.
    - Live tier: 146 of 146 green; main fails 120.
- **Orderings, not changed (follow-up, not carded).** Four hand-written comparators still answer `Equal` for the
  two zeros: fork `inspect/partition_values.rs:203-236`, `maintenance/partition_stats.rs:800-821` and
  `maintenance/rewrite_data_files_plan.rs:246-250`, and RePark `write/file_order.rs:25-31`. The two zero rows or
  groups come out in hash order there, where Java orders `-0.0` first. No pin depends on that order.
- **Owner rulings**, 2026-11-13, about 19:53 UTC, verbatim "Defaults, merge #1325 and #1327 when green":
  - **Q1, release note.** A table RePark wrote before this fix may hold a partition-granularity position-delete
    file stamped `+0.0` that covers rows in the `-0.0` partition. After this fix, RePark reads those deleted
    `-0.0` rows again, as Spark already does. Nothing hides them. A partition-stats file computed before the fix
    may seed a merged row.
  - **Q2, declared divergence.** On a write-ordered table, rows that interleave `-0.0` and `0.0` are refused by
    Spark 4.1.2's clustered writer (`ClusteredWriter.java:96`). RePark writes both partitions. See the parity
    registry row ZPM-1.
  - **Q3.** #1325 merged first. The pin to fork `bc5a9211` sits on fork main `8b6ff921`.
- **Not explained, confined to merged files.** Spark read the other float column of a merged ORC file as `-0.0`.
  After the fix no file holds both zeros.
