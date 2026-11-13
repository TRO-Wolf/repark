# Card NAN-PARTITION-RAW-BITS-1: RePark's fan-out writer stores the first NaN's raw bits as the partition value

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** NAN-PARTITION-VALUE-1 (PR #1319,
fork PR #383) verifier 1, 2026-11-13 (finding c2); verifier 2, 2026-11-13 (finding c2).

**Status:** closed by [NAN-WRITE-CANONICAL-1](nan-write-canonical-1-card-2026-11-13.md) (2026-11-13, branch, not merged); filed 2026-11-13. Severity **S2**: RePark's own read of its table misses rows; Spark reads
the table right.

## Measured

- **Doors:** CTAS, `writeTo().create()`, `saveAsTable` overwrite and MERGE. These write through RePark's fan-out writer
  and never cross the JSON step that #1319 fixed.
- **Stored:** the partition value is the raw bits of whichever NaN came first: `fff8000000000000`,
  `7ff8deadbeef0001` or `7fc00123`. Spark stores the canonical `7ff8…` / `7fc0…`.
- **RePark's read:** `WHERE d = CAST('NaN' AS <type>)` misses the rows of that partition in 28 of 76 tables (verifier
  1). For example `t_l1_merge_double_v2` answers `[]` where Spark answers `[1, 2, 3]`.
- **MERGE at format v2** keeps a sign-set NaN in a separate partition (DOUBLE `fff8…`): `= NaN` answers `[1, 3]` for
  DOUBLE and `[1, 2]` for FLOAT, where Spark answers `[1, 2, 3]` (verifier 2).
- **Spark** reads these tables correctly.

**Builds:** #1319's head `6ed266ce` (fork `46d5db39`) and its base, main `a3f8c7d9` (fork `66901f0`): the same table
families on both.

## Fix direction

None given by the sources. Related: [MATH-NAN-CANONICAL-1](math-nan-canonical-1-card-2026-11-13.md)'s option (C), a
canonicalising seat at RePark's Iceberg write batch, would cover this writer if the owner takes it.

## Close

Every RePark write door stores a canonical NaN partition value, and `= NaN` and `isnan` find every NaN row on RePark's
own read, pinned per door at format v2 and v3.

**Closed by [NAN-WRITE-CANONICAL-1](nan-write-canonical-1-card-2026-11-13.md) (2026-11-13):** the fork's `PartitionValueCalculator` canonicalises every NaN partition value on
Parquet, ORC and Avro. On its head (`e100717f`, fork `d6592644`) every partitioned door stores one canonical NaN
partition value, as Spark does, and `= NaN` and `isnan` find all eight NaN rows on RePark's read
([table.md §5](../../wo/nan-write-canonical-1/evidence/step0/table.md)). Pinned at format v2 by
`test_nan_write_canonical_1.py`; the fork's calculator test covers every NaN bit pattern.
