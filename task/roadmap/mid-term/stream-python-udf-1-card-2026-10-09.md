# STREAM-PYTHON-UDF-1 — Python UDFs over a streaming frame run on Spark, refuse here

**Filed:** 2026-10-09 by the MB-4 lane (fold 1), from the Opus verify S2-UDF
(`verdict.json`; repro `p/guard3.py`; cells `p/cells_spark.json`, `p/cells_repark.json`).
One card for five doors with one shared site.

## Current behaviour (2026-10-09, MB-4 C-039)

`isStreaming` stays `True` through all five Python-executed doors (`udf`,
`pandas_udf`, `mapInArrow`, `mapInPandas`, `applyInPandas`): the facade walks
the map-bridge chain to the streaming scan underneath. Starting such a query
refuses on every start door (`start`, `start(path)`, `toTable`, `foreachBatch`
start) with `PySparkNotImplementedError` `[NOT_IMPLEMENTED] Python UDF over a
streaming DataFrame is not implemented` (registry row MBE-18, Python-raised:
the bridge chain is facade-local). Batch actions on such a frame answer the
DM-3 guard through the native guard against the streaming ancestor, exactly as
a plain streaming frame does.

## The Spark-matching answer

Spark runs all five doors (`cells_spark.json`, 2026-10-08): `plan.udf.select`,
`plan.udf.filter` and `plan.udf.toTable` land their rows; `plan.udf.outputmode`
runs under `append`; only `plan.udf.complete` refuses, with Spark's own
stateful answer (`AnalysisException` `_LEGACY_ERROR_TEMP_3102`,
`Complete output mode not supported when there are no streaming
aggregations`). `plan.udf.continuous` runs. Closing this card means executing
the Python function per micro-batch (the `foreachBatch` body already runs per
batch; the UDF bridge needs the same streaming-aware execution path instead of
its batch-only Arrow reader).

## Residuals, recorded (2026-10-09)

- Plan-building transforms over a UDF-stream frame (`.filter`, `.select`,
  `.withColumn` past the first UDF) still fail opening the upstream Arrow
  stream (`PySparkException`, no condition), as before this card: the plan
  snapshot executes the batch-only bridge. Spark builds them lazily.
- `spark.sql` with a registered Python UDF over a stream fails at build the
  same way; it is a sixth door on the same site, not yet walked.
- `cache()` / `persist()` on a UDF-stream frame stay lazy-ok as at base
  (Spark refuses DM-3); they share the plain-stream `ok` divergence, recorded
  under STREAM-SURFACE-RESIDUE-1.
