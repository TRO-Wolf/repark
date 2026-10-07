# map — python/repark-parity/tests/live_spark

## Purpose

MB-0 (2026-10-06): the streaming oracle for the micro-batch track (1.7). It records
24 cells, MB0-R1…R13, W1…W8 and T1…T3, on live Spark 4.1.2 + Iceberg 1.11.0 over a
local Hadoop catalog. MB-0b (2026-10-07, MB-1 fold 1) adds three read cells,
MB0b-R14…R16, so the recording holds 27. The micro-batch design sketch answers packet Q1–Q4 from these
recorded cells, not from documentation. Order:
[mb-0-oracle.md](../../../../task/wo/microbatch/mb-0-oracle.md). Nothing here is
collected by pytest, and no RePark code runs.

D-M2 (2026-10-06): the Postgres JDBC oracle for the connect track (1.6). It records
58 cells, `DM2-T01…T30` (types), `DM2-V01…V10` (values) and `DM2-S01…S04`
(shapes), on live Spark 4.1.2 reading Postgres 16.15 through pgjdbc 42.7.13.
The C-2 registry takes its Spark halves from these recorded cells, not from
documentation. Nothing here is collected by pytest, and no RePark code runs.

## Contents

- `mb0_streaming_oracle.py` is the recorder. There is one function per cell, and
  `record_all(bench)` returns one entry per cell id while printing each entry as one
  JSON line. Run it through the managed interpreter only (order §4 step 2), with
  `MB0_WAREHOUSE` and `MB0_CHECKPOINTS` pointing at empty private directories.
  `MB0_OUT` redirects the JSON for a re-run comparison, and `MB0_ICEBERG_JAR`
  overrides the jar search under `~/.ivy2` and `~/.m2`. `MB0_CELLS` (a comma list
  of cell ids) records only those cells and merges them into the existing JSON at
  the output path; every other entry and the preamble stay byte-identical, and the
  run refuses if Spark, Iceberg, the catalog or the master moved.
- `mb0_bench.py` is the recorder's plumbing, split out when MB-0b took the recorder
  past the 1000-line ceiling (2026-10-07): the jar search, the Spark session, `Bench`,
  `Collect`, the table and snapshot-log helpers, and the stream error and progress
  readers. Cell functions and the helpers named in a cell's `statement` stay in the
  recorder, so every recorded `statement` is unchanged.
- `mb0_streaming_oracle.json` is the recording: `{"preamble": …, "cells": [27 entries]}`,
  pretty-printed with sorted keys. Each entry carries exactly `cell`, `statement`, `kind`,
  `answer` and `field`. The preamble names the versions and the catalog (`hadoop`).
- `mb0_streaming_oracle.sha256` holds `sha256sum` of the JSON. Check it with
  `sha256sum -c` from this directory.
  pins: mb-1/C-025
- `c2_jdbc_oracle.py` is the D-M2 recorder. Cells are data (`define_cells`),
  one entry per `(code, tz)` id, and `record_all` prints each entry as one
  JSON line. Run it through the managed interpreter only (brief step 2),
  with `DM2_PG_URL` (from `make pg-url`), `DM2_WAREHOUSE` pointing at an
  empty private directory, and `DM2_PGJDBC_JAR` when the jar lives outside
  the D-M2 jars directory. `DM2_OUT` redirects the JSON for a re-run
  comparison. Missing Spark, psql, jar or database prints `SKIP` and exits 0.
- `c2_jdbc_oracle.json` is the D-M2 recording: the Spark, pgjdbc (version
  and SHA-256), Postgres and date preamble plus `setup_preamble`, then the
  58 cells in recorder order, pretty-printed with sorted keys. Each entry
  carries exactly `id`, `sql_setup`, `read`, `tz`, `schema`, `rows` and
  `error` (`class`, `error_class`, `sqlstate`, `message`); only `DM2-S03`
  adds `explain` and `push_down_limit`.
- `c2_jdbc_oracle.sha256` holds `sha256sum` of the JSON. Check it with
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
- The MB0b read cells run through `option_run`, which records the batches, the
  progress rows and each checkpoint offset as `(snapshot_ordinal, position)`, the
  ordinal being the snapshot's index in commit order (`null` for Spark's
  `START_OFFSET`, snapshot `-1`). R14 sets `stream-from-timestamp` one hour past
  the head, then appends below it: once before the append, once resumed from the
  same checkpoint after it, and once fresh. Its later-landing half sets the
  timestamp six seconds past the head, appends below it, waits past it, and
  appends again. R15 runs `streaming-max-rows-per-micro-batch` at 3, 4 and 5 over
  three one-file 2-row snapshots and over one 3-file snapshot of 2-row files. R16's
  first snapshot is an `INSERT OVERWRITE` on the empty table.

## D-M2 recording choices

- Every read sets `.option("driver", "org.postgresql.Driver")` next to
  `spark.jars`: analysis-time `DriverManager.getDriver` cannot see
  `spark.jars` jars, and without the option every cell fails with
  `No suitable driver`. The explicit driver routes through Spark's
  `DriverRegistry`, which loads it from the Spark classloader.
- The recorder creates its tables through the `psql` CLI, one
  `ON_ERROR_STOP=1` invocation per cell, because `/tmp/sparkenv` has no
  `psycopg`. Each `CREATE` runs once; tz-repeat, `DM2-T12` and `DM2-S02`
  cells re-read an earlier cell's table, and their `sql_setup` repeats the
  creating statements as provenance.
- `read` records `url` as `<pg-url>` and omits `user`/`password`; the
  password never reaches the JSON. `$WAREHOUSE` and `$RECORDER` are
  scrubbed like the MB-0 prefixes.
- `schema` is kept when only `collect` fails, so a failing value still
  pins the type mapping. `error_class`/`sqlstate` come from the Python
  getters first, then the `java_exception` getters; the `message` is the
  first 400 characters verbatim.
- `DM2-S03` also records the formatted plan and the `push_down_limit`
  triple: whether the external-engine query carries a whole-word `LIMIT`,
  the unset `JDBCOptions.pushDownLimit` read off the analyzed plan, and a
  reflection error slot. A whole-word match is used because the table is
  named `t_limit100`.
- A re-run is byte-identical, including py4j object ids and plan
  attribute ids: the call sequence is fixed, so both id streams repeat.

## D-M2 measured notes (2026-10-06, read from the recording)

- The four flagged numerics match the C-2a reflection: `(39,1)`, `(50,10)`
  and `(1000,40)` read as `decimal(38,0)`, `(50,45)` as `decimal(38,33)`,
  with fraction digits rounded half up into the target scale.
- `numeric(5,-2)` reads as `decimal(38,38)` live, not `decimal(7,0)` as
  the reflection predicted, and its `collect` fails with
  `NUMERIC_VALUE_OUT_OF_RANGE` (`22003`). `bpchar(5)` reads as `string`,
  not `char(5)`: live pgjdbc reports it as `VARCHAR`, while the verifier
  assumed the `CHAR` type code.
- Every UTC repeat answers identically to its `America/New_York` twin,
  so the JDBC timestamp path follows the JVM default time zone
  (`America/New_York` here, recorded as `jvm_timezone`), not the session
  time zone.
- `V1` JDBC pushes no limit: the plan keeps `CollectLimit` above the
  scan with a bare external query, while the unset option reads back
  `true`.
- `NaN`/`Infinity` numerics fail in `PgResultSet.toBigDecimal`
  (`Bad value for type BigDecimal`); date/timestamp infinities return
  boundary dates instead of failing. The DST gap reads as `03:30`, the
  overlap as `01:30` with `fold=1`, and `time '24:00:00'` as next-day
  midnight. An unknown JDBC option is silently ignored.

## Volatile fields (MB-0)

An MB-0 re-run is byte-identical except for these fields:

- query ids and run ids, plus the snapshot ids inside every error text
  (R2, R4, R5, R7, W6);
- R13: the checkpoint `metadata` id, `batchTimestampMs`, and `snapshot_id` in each offset;
- R16: the snapshot id and the `SparkMicroBatchStream@<hash>` identity in the error text;
- W6: the `SparkMicroBatchStream@<hash>` object identity;
- W7: `app-id`, `spark.app.id` and `spark.sql.streaming.queryId`;
- T3: `id`, `runId`, `timestamp`, `batchDuration`, every `durationMs.*`, both
  `processedRowsPerSecond`, `sources[0].description` and `endOffset.snapshot_id`.

## Pointers

- Up: [../map.md](../map.md)
- Packet: [packet.md](../../../../task/wo/microbatch/packet.md)
- North Star: [cdc-microbatch-north-star-2026-10-05.md](../../../../task/roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md)
