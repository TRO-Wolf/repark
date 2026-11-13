# Card NAN-PARTITION-FILE-SPLIT-1: RePark writes each NaN bit pattern of one partition to its own data file

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** NAN-PARTITION-VALUE-1 (PR #1319,
fork PR #383) verifier 1, 2026-11-13 (finding c3); verifier 2, 2026-11-13 (finding c1).

**Status:** filed 2026-11-13, not dispatched. Severity **S4**: layout only.

## Measured

- RePark's splitter keys a NaN partition by its bits. A canonical NaN, a sign-set NaN and a payload NaN land in three
  data files, each with the partition value NaN `7ff8…`. Spark writes one file with the three rows.
- So `.files` has extra rows, and `.partitions.file_count` is 2 or 3 where Spark's is 1.
- Values, record counts and every read cell are equal.
- The base did the same, with NULL partitions.

**Builds:** #1319's head `6ed266ce` (fork `46d5db39`) and its base, main `a3f8c7d9` (fork `66901f0`).

## Fix direction

None given by the sources.

## Close

RePark writes one file per NaN partition, as Spark does, or a recorded ruling keeps the split, with the `.files` and
`.partitions` cells pinned.
