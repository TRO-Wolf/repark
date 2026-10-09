# STREAM-SURFACE-RESIDUE-1 — streaming-frame action, writer and surface answers that differ from Spark

**Filed:** 2026-10-09 by the MB-4 lane (fold 1b), from the Opus verify S3
findings (`verdict.json` findings 6, 7 and 12; repros `p/guard.py`,
`p/guard2.py`, `p/guard_spark.py`, `p/cells.py`; cells
`p/guard_repark.json`, `p/guard_spark.json`, `p/cells_spark.json`,
`p/cells_repark.json`). One card for the three record-only S3 groups.

## Current behaviour (2026-10-09, MB-4 C-044)

No door returns rows and none hangs; the 31 divergences are class,
condition or text only. Guard ids below are the `guard_*.json` keys.

Batch writes (9 doors) fail through the generic scan refusal instead of
Spark's call guard: `write.csv`, `write.format.save`, `write.insertInto`,
`write.json`, `write.parquet`, `write.saveAsTable`, `writeTo.append`,
`writeTo.create`, `writeTo.overwritePartitions` answer `PySparkException`
(no condition) `datafusion engine error: External error: Queries with
streaming sources must be executed with writeStream.start(); ... is a
streaming source`. `df.write` itself answers a `DataFrameWriter`
(`write.property` ok).

Lazy-ok returns (3 doors): `cache()` and `persist()` return a frame
whose later `count`/`collect` raise the generic scan refusal, and
`df.write` answers a writer. Spark refuses all three at the call.

`explain` prints (2 doors): `explain()` and `explain(True)` refuse
`AnalysisException` `_LEGACY_ERROR_TEMP_3102`. Spark prints the plan
(`StreamingRelation`) and returns `None`.

Generic scan refusal instead of DM-3 (6 doors): `checkpoint`,
`localCheckpoint`, `transpose`, `inputFiles` and SQL `INSERT`/`CTAS`
from a streaming temp view answer the `PySparkException` no-condition
scan refusal. Spark answers `AnalysisException`
`_LEGACY_ERROR_TEMP_3102` on each.

`UnsupportedOperationException` instead of DM-3 (4 doors):
`createOrReplaceGlobalTempView`, `summary`, `toJSON` answer the
engine's unsupported shape; `summary()` with no action refuses where
Spark builds the frame (it reads `isStreaming` True). The
`summary`/`toJSON` shapes are shared with the batch doors, not
streaming-specific.

Condition variant (1 door): `crosstab` answers DM-3
`_LEGACY_ERROR_TEMP_3102`. Spark answers `AnalysisException`
`_LEGACY_ERROR_TEMP_3063` (`pivot is not supported on a streaming
DataFrames/Datasets`).

No-pandas artefacts (4 doors): `mapInArrow`, `mapInPandas`, `pandas_api`
and `toPandas` were measured against a Spark without pandas, so Spark's
recorded answer is `PySparkImportError` `PACKAGE_NOT_INSTALLED`. Those
four cells need a re-measure on a Spark with pandas before they can
close.

Miscellaneous (2 doors): `rdd` refuses `NOT_IMPLEMENTED` where Spark
answers DM-3; `__arrow_c_stream__` refuses DM-3 where Spark answers
`PySparkAttributeError` `ATTRIBUTE_NOT_SUPPORTED`.

The 53 matching doors answer DM-3 with the recorded `;\niceberg` tail
(MB-0c D1 verbatim per the text-as-recorded ruling); Spark's tail names
the table (`;\nlocal.db.src`).

The five absent members are declared, not implemented (MB-4 C-043):
`DataStreamWriter.partitionBy`, `StreamingQuery.processAllAvailable`,
`StreamingQuery.explain`, `StreamingQueryManager.addListener` and
`removeListener` raise `PySparkNotImplementedError` naming themselves.
The `partitionBy=` start keyword stays accepted and ignored: Spark
ignores it on existing v2 tables (the verify's three cells ran).

`fence_tests.rs::two_drivers_on_one_sink_land_each_epoch_exactly_once`
never reaches the append fence: 0 fence refusals in each of 5 runs, all
green. The race pin (`two_sessions_racing_one_query_land_every_row_
exactly_once`) is the only test that exercises the fence (47 refusals
over 50 iterations at the shipped delay, green at every delay). The
older test is recorded, not rewritten.

## The Spark-matching answer

The batch-write doors refuse at the property with `AnalysisException`
`CALL_ON_STREAMING_DATASET_UNSUPPORTED` (42KDE): `[CALL_ON_STREAMING_
DATASET_UNSUPPORTED] The method `write` can not be called on streaming
Dataset/DataFrame. SQLSTATE: 42KDE`. `cache`, `persist`, `checkpoint`,
`localCheckpoint`, `transpose`, `inputFiles` and SQL `INSERT`/`CTAS`
from a stream refuse `AnalysisException` `_LEGACY_ERROR_TEMP_3102`
with Spark's text. `explain()` prints the plan and returns `None`.
`crosstab` answers `_LEGACY_ERROR_TEMP_3063`. The five declared members
run: `partitionBy` lays out file-system partitions, `processAllAvailable`
drains the source, `explain` prints the query plans, the listeners
receive life cycle up-calls. The four no-pandas cells re-measure on a
Spark with pandas. Closing this card means routing each door to its
Spark shape; the DM-3 tail stays `;\niceberg` until the text-as-recorded
ruling is revisited.

## Residuals, recorded (2026-10-09)

- `summary()` with no action and `toJSON` share their batch-door
  unsupported shapes; a streaming fix rides the batch fix.
- The older fence test keeps its name and its passing runs; only its
  record says it never reaches the fence.
