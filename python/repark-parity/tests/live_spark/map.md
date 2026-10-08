# map — python/repark-parity/tests/live_spark

## Purpose

MB-0 (2026-10-06): the streaming oracle for the micro-batch track (1.7). It records
24 cells, MB0-R1…R13, W1…W8 and T1…T3, on live Spark 4.1.2 + Iceberg 1.11.0 over a
local Hadoop catalog. MB-0b (2026-10-07, MB-1 fold 1) adds three read cells,
MB0b-R14…R16, and MB-1 fold 2 adds MB0b-R17, so the recording holds 28. The micro-batch design sketch answers packet Q1–Q4 from these
recorded cells, not from documentation. Order:
[mb-0-oracle.md](../../../../task/wo/microbatch/mb-0-oracle.md). Nothing here is
collected by pytest, and no RePark code runs.

D-M2 (2026-10-06): the Postgres JDBC oracle for the connect track (1.6). It records
58 cells, `DM2-T01…T30` (types), `DM2-V01…V10` (values) and `DM2-S01…S04`
(shapes), on live Spark 4.1.2 reading Postgres 16.15 through pgjdbc 42.7.13.
The C-2 registry takes its Spark halves from these recorded cells, not from
documentation. Nothing here is collected by pytest, and no RePark code runs.

FOREACH-WRAP-1 (2026-10-07): the user-callback oracle. It records three cells,
`FW1-foreach`, `FW1-foreachPartition` and `FW1-transform`, on live Spark 4.1.2
classic, each callback raising `ValueError('fetch failed for http://u:<secret>@h/x')`.
Nothing here is collected by pytest, and no RePark code runs.

C-3 (2026-10-07): the JDBC partitioned-read oracle for the connect track (1.6). It records
41 cells, `C3-P`, `M`, `B`, `N`, `T`, `C`, `Q`, `J` and `L`, and a 740-triple stride grid on
live Spark 4.1.2 reading Postgres 16.15 through pgjdbc 42.7.13. The C-3 design note and the
`CONNECT-DECL-pg-partitioned-read` row take their Spark halves from these cells. Nothing here is
collected by pytest, and no RePark code runs.

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
- `mb0_streaming_oracle.json` is the recording: `{"preamble": …, "cells": [28 entries]}`,
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
- `fw1_callback_oracle.py` is the FOREACH-WRAP-1 recorder. `define_cells` names
  the three actions and `record_all` prints each entry as one JSON line. Run it
  through the managed interpreter only; `FW1_WAREHOUSE` points at an empty private
  directory and `FW1_OUT` redirects the JSON for a re-run comparison. Missing
  `pyspark` prints `SKIP` and exits 0. pins: foreach-wrap-1/C-004
- `fw1_callback_oracle.json` is the recording: the Spark, master and date
  preamble plus the three cells, pretty-printed with sorted keys. Each `error`
  carries `class`, `mro`, `error_class` (the `getErrorClass` / `getCondition`
  getters on the exception, then on its `java_exception`), `has_get_error_class`,
  the JVM cause chain, the Python `__cause__` chain, `context`, `is_original`
  (the raised object is the user's own `ValueError`), the message head up to
  `: Job aborted`, whether the secret is in `str` and in `repr`, and every message
  line carrying it, with the secret written `$SECRET`.
- `fw1_callback_oracle.sha256` holds `sha256sum` of the JSON. Check it with
  `sha256sum -c` from this directory.

- `c3_partition_oracle.py` is the C-3 recorder. `define_shapes` names one option bag per cell;
  `shape_cell` records each partition's `whereClause` (read off `JDBCRelation.parts()`), the
  ids each partition returned (`spark_partition_id`), the row count, or the error (`class`,
  `error_class`, `sqlstate`, the first 500 characters of the message). `grid_triples` is 40
  edge triples (the `i64` extremes, equal bounds, one stride, more strides than values,
  negative ranges, reversed bounds) followed by 700 triples from `random.Random(20261007)`.
  Run it through the managed interpreter only, with `C3_PG_URL` (from `make pg-url`) and
  `C3_PGJDBC_JAR` when the jar lives outside the D-M2 jars directory; `C3_OUT` and
  `C3_GRID_OUT` redirect the two outputs for a re-run comparison. Missing Spark, jar or
  database prints `SKIP` and exits 0. It creates and drops its own schema, `c3o`.
- `c3_partition_oracle.json` is the recording: the Spark, pgjdbc (name and SHA-256), Postgres
  and date preamble, the setup statements, the grid's seed, size and SHA-256, then the 41
  cells in recorder order. `url` and the credentials never reach it.
- `c3_stride_grid.txt` is the stride grid, one triple per line: `lower upper count cuts c1 c2 …`
  (the upper bound of every stride but the last, in order; no cut means one unpartitioned
  read) or `lower upper count refused`. `crates/repark-connect/tests/it/partition.rs` reads
  this file, so the Rust arithmetic is compared with Spark's own numbers.
- `c3_partition_oracle.sha256` holds `sha256sum` of both files. Check it with `sha256sum -c`
  from this directory. pins: c-3/C-001

## C-3 fold 1 cells (2026-10-08)

The recorder gains eleven cells, so the recording holds 52; the stride grid re-recorded byte for
byte. Two more tables: `c3o.mx` (one column `"Mixed"`) and `c3o.twins` (`"Mixed"` and `"mixed"`).

- `C3-C04`…`C07`: a quoted name in another case resolves (`"N"` against `n`, `"mixed"` against
  `"Mixed"`), as a bare name in another case does (`MIXED`), and the `whereClause` carries the
  column's real name.
- `C3-C08`…`C10`: a relation with two columns that differ only in case cannot be read at all,
  partitioned or not: `AnalysisException` `COLUMN_ALREADY_EXISTS` (`42711`).
- `C3-N07`: `numPartitions = 3000000000` is `NumberFormatException` (the option is a 32-bit
  `Int`). `C3-N08`: `2147483647` over a span of 3 shrinks to three strides. `C3-N09`: a padded
  `" 4"` is `NumberFormatException`.
- `C3-T11`: a `timestamp` column partitions with date text as bounds.
  pins: c-3/C-001

## C-3 measured notes (2026-10-07, read from the recording)

- Rows below `lowerBound`, above `upperBound` and NULL rows are all returned: the first stride
  is `"n" < 50 or "n" is null`, the last is `"n" >= 150` (`C3-P01`).
- `numPartitions` alone, `numPartitions` of `1`, `0` or `-1`, and equal bounds each read one
  unpartitioned partition with no error (`C3-M05`, `N01`…`N03`, `B02`). Reversed bounds refuse.
- When `upperBound - lowerBound < numPartitions` the count shrinks to the difference (`C3-B03`).
- The column resolves in another case and when quoted (`C3-C02`, `C03`). `text` and `boolean`
  refuse; `date`, `timestamp`, `timestamptz`, `numeric` and `float8` partition (`C3-T03`…`T10`).
- `query` with `partitionColumn` refuses and names the `dbtable` subquery form, which partitions
  (`C3-Q01`, `Q02`).
- A `limit` over a partitioned V1 JDBC scan is not pushed (`C3-L01`).
- Every recorded cut list is strictly increasing, the `i64` extremes included.

## FOREACH-WRAP-1 measured notes (2026-10-07, read from the recording)

- `foreach` and `foreachPartition` raise `py4j.protocol.Py4JJavaError`, not a
  `PySparkException`. It has no `getErrorClass`, and the condition getters on its
  `java_exception` answer `null`. The JVM chain is `SparkException` over
  `org.apache.spark.api.python.PythonException`, the Python `__cause__` chain is
  empty, and `__context__` is `None`. The worker traceback, with the user's
  message, is inside `str(exc)`, so Spark shows the credential; `repr` does not.
- `transform` raises the user's own `ValueError` object unchanged: it is a plain
  Python call on the driver and never crosses the JVM.
- `local[1]` over a one-partition frame keeps stage and task numbers fixed, and
  the message head stops before them, so a re-run is byte-identical.

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
  first snapshot is an `INSERT OVERWRITE` on the empty table. R17 (fold 2) appends
  two one-file snapshots, then commits an `INSERT OVERWRITE` (table `r17`) or a
  `DELETE` of one whole file (table `r17_delete`), then runs `availableNow` with
  `streaming-max-files-per-micro-batch=1`.

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
- R17: the query ids, run ids and snapshot ids in both error texts;
- W6: the `SparkMicroBatchStream@<hash>` object identity;
- W7: `app-id`, `spark.app.id` and `spark.sql.streaming.queryId`;
- T3: `id`, `runId`, `timestamp`, `batchDuration`, every `durationMs.*`, both
  `processedRowsPerSecond`, `sources[0].description` and `endOffset.snapshot_id`.

## Pointers

- Up: [../map.md](../map.md)
- Packet: [packet.md](../../../../task/wo/microbatch/packet.md)
- North Star: [cdc-microbatch-north-star-2026-10-05.md](../../../../task/roadmap/epic-term/cdc-microbatch-north-star-2026-10-05.md)
