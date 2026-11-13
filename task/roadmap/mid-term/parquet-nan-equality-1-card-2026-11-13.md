# Card PARQUET-NAN-EQUALITY-1: `= NaN` keeps no row on a `spark.read.parquet` frame, where Spark keeps the NaN row

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** FILTER-STRING-NULL-ORDER-1
(#1321) verifier 1, 2026-11-13, finding C1 (class (c), pre-existing; the verifier found no card for it).

**Status:** filed 2026-11-13, not dispatched. Severity **S2**: a silent wrong read, no error.

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
