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
  names it under "Related". After this fix `.partitions` lists two zero rows, as Spark does.
- [SIGNED-ZERO-WRITE-METADATA-1](signed-zero-write-metadata-1-card-2026-11-13.md): the other signed-zero write
  differences from the same verifier. This fix closed its `.partitions` half.
- [SIGNED-ZERO-PARTITION-FILTER-1](signed-zero-partition-filter-1-card-2026-11-14.md): a filter on a float zero
  partition answers by partition on Spark-on-Iceberg and by IEEE on RePark (verifier 2, C1). This fix did not cause
  it; the old merge hid it on RePark's tables.

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
    replaced files by `(spec id, Struct)`, so its refusal counts only the replaced partition's files. Spark's count
    is `1` for a `0.0` row over one `0.0` file and `2` for a `-0.0` row over two `-0.0` files; main counts `3`
    (verifier 1, B1).
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
  - **Added in the delta round (2026-11-14), after both verifiers' FAIL:**
    - `test_overwrite_partitions_summary_counts_only_the_replaced_zeros_files` pins the RePark seat: the two cells
      first cited for it never reach `replaced_data_files` (verifier 1, B1). It asserts Spark's count in the
      refusal and the unchanged rows, on DOUBLE and FLOAT.
    - Door `spark_fanout_off`, a table with no write order and `write.spark.fanout.enabled = false` (ZPM-1's second
      half), and its live twin `test_spark_refuses_interleaved_zeros_with_its_fanout_off`.
    - The file on 16 CPUs: main fails 66 of 80 (the 60, the two 1-writer `spark_fanout_off` cells and the four
      summary cells, each answering `3`); the fix passes 80 of 80. The live cells, run once: Spark refuses on DOUBLE
      and FLOAT and reads RePark's `spark_fanout_off` tables with each row's own zero (10 of 10).
- **Orderings, not changed (follow-up, not carded).** Five hand-written comparators still answer `Equal` for the
  two zeros, through `PrimitiveLiteral::partial_cmp`. The lines were re-checked in the delta round on fork
  `bc5a9211` and RePark `1bf566ee`; the two verifiers had cited different ranges.
  - Fork `crates/iceberg/src/inspect/partition_values.rs:203-237`, used by `inspect/partitions.rs:314`.
  - Fork `crates/iceberg/src/maintenance/partition_stats.rs:788-822`, used at `:485` and `:875-876`, and by a
    test at `:3233-3234`.
  - Fork `crates/iceberg/src/maintenance/rewrite_data_files_plan.rs:223-256`, the primitive arm at `:248-249`,
    used at `:188`.
  - Fork `crates/iceberg/src/writer/partitioning/fanout_writer.rs:126-149` `ascending_partition_order` (verifier
    2, F2). Its three callers:
    - `FanoutWriter::close` (`:107-123`, the sort at `:110`): the file emission order;
    - datafusion `physical_plan/delete_position_deletes.rs:94-100`: the order of position-delete groups;
    - datafusion `physical_plan/commit.rs:341-344`: the data-file order, with a positional tiebreak.
  - RePark `crates/repark-iceberg/src/write/file_order.rs:10-32` (`compare_partitions`, `compare_values`), used by
    `ascending_partition_order` (`:5-8`) and `stable_commit_order` (`:34-43`).

  The two zero rows or groups come out in hash order there, where Java orders `-0.0` first. No pin depends on that
  order. The follow-up also carries verifier 2's F3: on head, the v3 `_row_id` of zero-partition rows varies from
  run to run. In four identical one-core runs of one `INSERT`, the DOUBLE `-0.0` rows took `_row_id` 1 and 2 once
  and 3 and 4 three times; FLOAT flips too. The cause is `ascending_partition_order` plus `write/file_order.rs`
  keeping the `HashMap` order of the two zero files. Main was deterministic (one merged file). Spark's `_row_id`
  order already differed from RePark's on every partition, so `_row_id` parity was absent before the fix.
- **Owner rulings**, 2026-11-13, about 19:53 UTC, verbatim "Defaults, merge #1325 and #1327 when green":
  - **Q1, release note.** A table RePark wrote before this fix may hold a partition-granularity position-delete
    file stamped with one of the two zeros, covering rows in both. Either zero can carry the stamp: main's
    partition-granularity DELETE stamps `-0.0`, and main's `rewrite_position_delete_files` stamps `+0.0`. After
    this fix, rows deleted in the other zero's partition read again, as on Spark. Nothing hides them. A
    partition-stats file computed before the fix may seed a merged row. The owner ruled the note's existence; the
    delta round corrected its facts (verifier 2, F1, measured on main's build).
  - **Q2, declared divergence.** On a write-ordered table, rows that interleave `-0.0` and `0.0` are refused by
    Spark 4.1.2's clustered writer (`ClusteredWriter.java:96`). RePark writes both partitions. See the parity
    registry row ZPM-1.
  - **Q3.** #1325 merged first. The pin to fork `bc5a9211` sits on fork main `8b6ff921`.
- **Owner words owed, PROPOSED (asked 2026-11-14).** Neither is ruled.
  - **ZPM-1's second half** (verifier 1, B2). Spark 4.1.2 also refuses interleaved zeros on a table with no write
    order whenever `write.spark.fanout.enabled = false`. RePark writes both partitions, and Spark reads them back
    with each row's own zero. ZPM-1 now states that half; whether Q2 covers it is the owner's word.
  - **ZPM-2** (verifier 2, F4). Remove-dangling-deletes drops the `-0.0` partition's dangling delete file, which
    Spark's own action keeps because it groups the two zeros. Reads are identical; on the `1.5`/`2.0` control both
    engines remove it. Parity row ZPM-2.
- **Not explained, confined to merged files.** Spark read the other float column of a merged ORC file as `-0.0`.
  After the fix no file holds both zeros.
