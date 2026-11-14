# Card PARQUET-NAN-EQUALITY-1: `= NaN` keeps no row on a `spark.read.parquet` frame, where Spark keeps the NaN row

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** FILTER-STRING-NULL-ORDER-1
(#1321) verifier 1, 2026-11-13, finding C1 (class (c), pre-existing; the verifier found no card for it).

**Status:** filed 2026-11-13, not dispatched. Severity **S2**: a silent wrong read, no error. **Re-scoped 2026-11-13**
(owner, "defaults", Q10 of the NAN-COMPARE-SPARK-1 sketch; see "Re-scope" below): the pruning half is recorded as
equal to Spark on the same file, with no writer change; the constant-column half moved to
[NAN-MINMAX-1](nan-minmax-1-card-2026-11-13.md) and is FIXED by its part 1 (unmerged), as is a third case its verifier
1 found (F1, below). Nothing else stays open here.

## Re-scope (2026-11-13, owner Q10)

The [NAN-COMPARE-SPARK-1 sketch](../../wo/nan-compare-spark-1/sketch.md) §3 found two causes:

1. **Pruning by NaN-free footer statistics** (inventory F2). `= NaN` keeps no row because the row group's min and max
   leave NaN out. Spark's own Parquet reader prunes the same files the same way: `= NaN` keeps nothing on Spark too
   (sketch §6 `pq_*_l1`, §9 `pa_const48_eq_nan`; NAN-MINMAX-1's step 0 `eq_nan_d`, 0 on both engines over every
   constant file). Recorded as **equal to Spark on the same file**; no writer change (Q10). The source's "Spark keeps
   the NaN row" does not hold when Spark reads the same Parquet file.
2. **The constant-column read** (inventory I2). On a file whose other float values are one value, RePark read every
   NaN as that value and Spark reads NaN. **Moved to NAN-MINMAX-1** and fixed by its part 1, the `Inexact` float
   statistics seat.
3. **A row group with no footer min or max** (NAN-MINMAX-1 verifier 1, F1). In a file whose float row group is all
   NaN, or NULL and NaN, DataFusion merged the other row groups' bounds into an exact file bound, and the file pruner
   dropped the whole file: `= NaN` answered 0 where Spark answers 2 (`R_one`). Spark's reader keeps that row group,
   so case 1's "equal to Spark on the same file" did not hold there. **Fixed by NAN-MINMAX-1 part 1's delta** (the
   file's float bounds turn inexact; pinned in `test_nan_minmax_stats_1.py`).

`= NaN` over an all-NaN file of sign-set or payload NaN (Spark 2, RePark 0) is RePark's comparison by bits,
NAN-COMPARE-SPARK-1's, not this card's.

## Measured

- **The frame.** A seven-row `VALUES` view with a DOUBLE column `kd` that holds one NaN row, written with RePark's
  `write.mode("overwrite").parquet(…)` and read back with `spark.read.parquet(…)` (verifier 1's probe `nanpq.py`).
- **Wrong on the parquet frame:** `filter("kd = CAST('NaN' AS DOUBLE)")`, `filter(F.col('kd') == float('nan'))` and
  `spark.sql("SELECT id FROM <view of the parquet frame> WHERE kd = CAST('NaN' AS DOUBLE)")` keep no row. Spark keeps
  the NaN row.
- **Right:** the same predicates on the `VALUES` frame and on an Iceberg frame, and `isnan(kd)` on the parquet frame.
- **Builds:** #1321's head `f4f27dcc` and its base, main `7dd0ca20`, identical (base = head), so it is not #1321's
  doing.

## Not known yet

- **The file's NaN bits are unmeasured.** The source does not record which NaN bit pattern RePark wrote into the
  Parquet file, nor the file's statistics.
- The cause. No source names one.

## Possibly related (no cause claimed)

- [ICEBERG-NAN-FILE-PRUNE-1](iceberg-nan-file-prune-1-card-2026-11-13.md): RePark's Iceberg scan drops a NaN row's
  file for `x > 0.5`.
- [NAN-COMPARE-SPARK-1](nan-compare-spark-1-card-2026-11-13.md): Spark treats every NaN as one value in `=`, while
  RePark compares by bits.

## Fix direction

None given by the source.

## Close

`= NaN` in each of the three forms keeps the NaN row on a `spark.read.parquet` frame, as Spark does, pinned against
the Spark oracle beside a `VALUES` control.
