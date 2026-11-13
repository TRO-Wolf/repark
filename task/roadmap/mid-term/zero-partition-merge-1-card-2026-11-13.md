# Card ZERO-PARTITION-MERGE-1: a `0.0` row and a `-0.0` row written together land in one DOUBLE partition

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Sources:** NAN-WRITE-CANONICAL-1 verifier
1, 2026-11-13 (#1325, finding C1, class (c), and its probe `sz.py`); NAN-WRITE-CANONICAL-1 verifier 2, 2026-11-13
(#1325, finding C1); owner, 2026-11-13, about 16:45 UTC, verbatim "Go with default".

**Status:** filed 2026-11-13, ruled label-blocking, not dispatched. Severity **S1**: a wrong value through every
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
