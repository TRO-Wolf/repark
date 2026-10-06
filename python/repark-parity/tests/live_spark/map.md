# map — python/repark-parity/tests/live_spark

## Purpose

MB-0 (2026-10-06): the streaming oracle for the micro-batch track (1.7). It records
24 cells, MB0-R1…R13, W1…W8 and T1…T3, on live Spark 4.1.2 + Iceberg 1.11.0 over a
local Hadoop catalog. The micro-batch design sketch answers packet Q1–Q4 from these
recorded cells, not from documentation. Order:
[mb-0-oracle.md](../../../../task/wo/microbatch/mb-0-oracle.md). Nothing here is
collected by pytest, and no RePark code runs.

## Contents

- `mb0_streaming_oracle.py` is the recorder. There is one function per cell, and
  `record_all(bench)` returns one entry per cell id while printing each entry as one
  JSON line. Run it through the managed interpreter only (order §4 step 2), with
  `MB0_WAREHOUSE` and `MB0_CHECKPOINTS` pointing at empty private directories.
  `MB0_OUT` redirects the JSON for a re-run comparison, and `MB0_ICEBERG_JAR`
  overrides the jar search under `~/.ivy2` and `~/.m2`.
- `mb0_streaming_oracle.json` is the recording: `{"preamble": …, "cells": [24 entries]}`,
  pretty-printed with sorted keys. Each entry carries exactly `cell`, `statement`, `kind`,
  `answer` and `field`. The preamble names the versions and the catalog (`hadoop`).
- `mb0_streaming_oracle.sha256` holds `sha256sum` of the JSON. Check it with
  `sha256sum -c` from this directory.

## Recording choices

- `statement` is the cell function's own source followed by the shared helpers it
  calls (`inspect.getsource`), so the recorded code cannot drift from the code that ran.
- Errors record Spark's error class (`getCondition()`), the Python exception, the
  SQLSTATE, the JVM cause chain and the full `str(exc)` text.
- Three path prefixes are replaced: `$WAREHOUSE`, `$CHECKPOINTS` and `$RECORDER` (this
  file's path in Python tracebacks). `SPARK_LOCAL_HOSTNAME=localhost` keeps the host
  name in task-failure text stable.
- `spark.sql.catalog.local.cache-enabled=false` is set because a `foreachBatch` write
  commits through another catalog instance, and a cached table in the driver session
  would read stale (W4 recorded zero rows with the cache on).
- Each snapshot kind is produced as follows. `overwrite` comes from `INSERT OVERWRITE`.
  `delete` is a merge-on-read `DELETE` that adds one position-delete file. `replace`
  comes from `rewrite_data_files` with `rewrite-all`. Every read cell also records the
  table's operation log, so the window contents can be checked.
- W6 kills the batch mid-flight without killing the process: a Python UDF raises on
  `id = 3` while a flag file exists. Offsets for batch 1 are then written but its
  commit is not, and the restart replays the batch.
- R10 records two timestamps, midway between the first two commits and exactly at
  the second commit. T1 also records `streaming-max-files-per-micro-batch=1` under
  `once`.

## Volatile fields

A re-run is byte-identical except for these fields:

- query ids and run ids, plus the snapshot ids inside every error text
  (R2, R4, R5, R7, W6);
- R13: the checkpoint `metadata` id, `batchTimestampMs`, and `snapshot_id` in each offset;
- W6: the `SparkMicroBatchStream@<hash>` object identity;
- W7: `app-id`, `spark.app.id` and `spark.sql.streaming.queryId`;
- T3: `id`, `runId`, `timestamp`, `batchDuration`, every `durationMs.*`, both
  `processedRowsPerSecond`, `sources[0].description` and `endOffset.snapshot_id`.

## Pointers

- Up: [../map.md](../map.md)
- Packet: [packet.md](../../../../task/wo/microbatch/packet.md)
- North Star: [cdc-microbatch-north-star-2026-10-05.md](../../../../task/roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md)
