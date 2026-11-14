# Card NATIVE-DATAFRAME-WRITE-SILENT-1: a native-door DataFrame write reports success and stores nothing

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** NATIVE-CTAS-ICEBERG-REFUSE-1
(PR #1317) verifier 2, 2026-11-13, finding F4.

**Status:** **FIXED 2026-11-13** by this unit (branch `fix/native-dataframe-write-silent-1`, PR #1331): every
native-door DataFrame write that would store nothing refuses before any effect. See "Step 0", "Fix", "Delta"
and "Delta 2" below. Filed and ruled the same day. Severity **S1**, a silent lost write, as rated at filing; the verifier
recorded it as S2 and outside its unit's scope.
**Ruled 2026-11-13** (below): measure first, then a refusal guard; it rides v1.5.7.

## Measured

On the native door, on the memory catalog `ice`, a frame from `repark.sql(…)` written through any of these returns
without an error, and the target's rows are unchanged afterwards:

- `.write.mode("append").saveAsTable("ice.ns.src")`;
- `.write.insertInto(…)`;
- `.writeTo(…).append()`.

On the same door, `writeTo(…).create()`, `writeTo(…).createOrReplace()` and `saveAsTable` into a new table or with
overwrite fail to parse on `USING`.

**Builds:** #1317's head `ed9f678c` and its base, main `a3f8c7d9`, identical.

## Not known yet

- The cause. The verifier did not root-cause it.
- Whether the hadoop catalog, Glue and S3 Tables behave the same. Only `ice` is named in the source.

## Owner ruling, 2026-11-13

The owner, verbatim "Defaults", to the board's decision as Fable refined it (Fable guidance, 2026-11-13); the record
is [rulings-2026-11-13.md](../../wo/ice-v2-flat-aws-label-1/design/rulings-2026-11-13.md), item 5.

1. **A measured table first:** every writer verb on every target kind (beside the measured memory catalog, at
   least the hadoop catalog, Glue and S3 Tables listed above), the overwrite forms included. An overwrite that stores nothing may also remove rows; #1317's first
   premise was wrong for the same reason.
2. **Then a refusal guard like #1317's** (NATIVE-CTAS-ICEBERG-REFUSE-1): each verb that cannot store the frame's
   rows refuses loudly before any effect.
3. **It rides v1.5.7,** queued after the label blockers and before the v1.5.7 tag, as #1317 was.
4. **The known-issues text names it now:** `STATUS.md` (Known correctness issues) and the parity registry row
   NATIVE-DATAFRAME-WRITE-SILENT-1 in `docs/spark-sql-iceberg-parity.md`: on the native door, `repark.sql(…)`
   DataFrame appends report success and store nothing in 1.5.6.

## Fix direction

The refusal guard above, after the measured table. No source gives the cause.

## Close

Each of the three calls either stores Spark's rows or refuses loudly before any effect, pinned per catalog kind.

## Step 0 (measured, main `1d9d2808`)

Every writer verb was run on every target kind the native door reaches, with seven frame shapes:
1568 native cells and 868 Spark-door cells. The table is
[evidence/map.md](../../wo/native-dataframe-write-silent-1/evidence/map.md); the raw cells are
under its `step0/`.

- **SILENT, 308 cells.** These return success and leave rows, snapshots and files unchanged:
  - on existing Iceberg tables (`ice` memory, `hd` hadoop): `saveAsTable` append,
    `format("iceberg").save(name)` append and overwrite, `insertInto` (both forms, and with
    `partitionBy`), `writeTo().append()` (plain, `tableProperty`, `partitionedBy`) and
    `overwritePartitions()`;
  - on in-memory tables: `insertInto`;
  - on a new path: every Parquet/CSV/JSON mode and spelling, plus an existing path's append.
- **S1, 63 cells.** A Parquet/CSV/JSON overwrite of an existing path, plain, partitioned or
  through `save`, deletes the rows already there and leaves one empty file.
- **28 cells** of `partitionBy(c).insertInto` return unchanged where Spark refuses.
- **No Iceberg overwrite removes rows.**
- **Every other verb refuses before any effect, or stores:** `saveAsTable` ignore as a no-op,
  and `.text`.
- **Not measured:**
  - A partitioned Iceberg table cannot be reached on the native door.
  - Glue, S3 Tables and REST do not run locally.
- **Cause:**
  - `writer_layout.run_through_temp_view` (`writer_layout.py:112`) discards the frame that
    `session_sql_with_write_options` returns.
  - On the native door, `DataFusionDialect` → `ctx.execute_logical_plan`
    (`pre_execute.rs:57`) leaves a `Dml` insert or `Copy` lazy, so that discarded frame is
    still pending.
  - The Spark door collects such statements (`spark_ast.rs:271`).
  - For the path S1, the empty staging directory then replaces the destination
    (`writer_readwriter.py:429-466`).

## Fix

- **Seat:** the pyfunction `session_sql_with_write_options`
  (`crates/repark-python/src/session_write_options.rs`), the facade writers' only entry. When the
  planned frame's root is a pending write (a `Dml` insert or a `Copy`), it raises
  `UnsupportedOperationException` before anything runs.
- **The message names the target:**
  - a table by `TableReference::to_quoted_string()`;
  - a path as the user wrote it, recovered from the staging segment: `repark-staging-<32 hex>-<name>`,
    or `repark-staging-<32 hex>` with no suffix for a nameless destination (`.`). Any other URL is
    named raw.
- **The workaround is one that stores when followed literally** (delta and delta 2, below):
  register the frame as a temporary view `<view>`, then
  - an append: a collected native `INSERT INTO <t> (<frame columns>) SELECT <frame columns> FROM
    <view>` for a by-name verb, or `INSERT INTO <t> SELECT * FROM <view>` for `insertInto`;
  - a static overwrite or `overwritePartitions` on an Iceberg table: `INSERT OVERWRITE` in the
    same two forms, which on an unpartitioned table replaces what `overwritePartitions` would;
  - either overwrite of an in-memory table: a collected `CREATE OR REPLACE TABLE <t> AS SELECT
    <frame column> AS <table column>, … FROM <view>` (the replaced table takes the frame's column types and drops `NOT NULL`; delta-2 re-check F8);
  - a path: `spark.createDataFrame(frame.collect(), frame.schema)` on a `ReparkSession`.
- **A view or a database-source table** gets no statement: the message says the native door
  cannot write it.
- Plain `repark.sql()` and the Spark door do not pass the check, and neither does `mergeInto`.
  `mergeInto` keeps its loud native refusal as a control pin.
- **Pins:**
  - `crates/repark-python/src/session_write_options_tests.rs`;
  - `python/repark/tests/writes/test_native_dataframe_write_silent_1.py`, over all 2436 cells:
    the 399 guarded cells refuse with the exact text and leave rows, snapshots, files and
    staging unchanged, and every other cell keeps main's outcome, rows, snapshot count and file
    shape (main's table re-recorded on `1d9d2808` in the delta, outcomes and rows identical).
- **Residue, recorded for the records batch:** `note_local_write_root` keeps the staging path
  allow-listed after a refusal. Also recorded there: the S3 overwrite deletes before it copies
  (`path_write.rs:403`), a native path containing `'` is refused before the seat, and the Spark
  door stores `partitionBy(c).insertInto` rows. The list is in
  [evidence/map.md](../../wo/native-dataframe-write-silent-1/evidence/map.md).

## Delta (2026-11-13, after both verifiers)

Both verifiers of #1331 passed the guard (every silent or destructive cell refuses with no
effect; nothing else moved) and failed three S3 records-and-message findings, fixed here:

- **F1, the path name.** A destination whose last segment is `out` (`out`, `out/`, `data/out`,
  an absolute `…/out`) was named by its staging path, because `out` was also the stand-in for a
  nameless destination. The writer now stages a nameless destination with no suffix, a form no
  user name can take, so every spelling is named as written. Pinned: `out`, `out/`,
  `data/out`, an absolute `…/out`, `.`, `..` and three relative spellings, in both modes.
- **F2, the workarounds.** A `ReparkSession` cannot see a native-door table (`unknown catalog`,
  `TABLE_OR_VIEW_NOT_FOUND`), and `INSERT OVERWRITE` is not implemented for an in-memory table.
  The table messages now name only the collected statements above; each is pinned by following
  it literally from the message text and checking the rows.
- **F3, the records.** `STATUS.md` and the v1.5.6 release notes now state the destructive path
  overwrite, with the workarounds; both forms and every workaround were re-checked on the
  published 1.5.6 wheel.
- **N1, ruled consequence: an empty frame refuses.** A native write of an empty `repark.sql(…)`
  frame (a new path, an existing path's append or overwrite, a table append) refuses, where
  main was vacuously right and the Spark door stores. This follows from the ruling's "stored
  nothing": the guard reads the planned statement before any row exists, and on this door that
  statement is dropped whatever the frame holds. Workaround: for a path, the conversion the
  message names, `spark.createDataFrame(frame.collect(), frame.schema)`, which keeps an empty
  frame's columns and stores it; for a table, run the collected statement the message names.

## Delta 2 (2026-11-14, the owner's cap exception)

The one re-check of #1331 (head `b49403ee`) held F1–F3 and found the guard sound, with no class
(a) finding. It failed three new class (b) findings, all about the routes the message and the
1.5.6 notes name. The owner granted a second targeted delta on 2026-11-14 (about 00:55 UTC),
verbatim "merge and defaults", adopting option A: "a second, message-only fix round … A short
re-check follows". This follows the #1261 precedent. The guard is unchanged: the same 399 cells
refuse, before any effect, and every other cell keeps main's outcome. Only the message text
and its records moved.

- **F4 (S2), the by-name verbs.** `saveAsTable` append, `format("iceberg").save(name)` append
  and overwrite, `writeTo().append()` and `overwritePartitions()` pair columns by name, but the
  named `INSERT … SELECT` was positional, so a frame whose columns are in another order stored
  swapped values. The message now lists the frame's columns, in the frame's order, in both
  places: `INSERT INTO t (p, id) SELECT p, id FROM <view>`, and the same for
  `INSERT OVERWRITE`. `BY NAME` does not parse on this door.
  - **How the seat knows:** the writer's own statement names its columns exactly when it binds
    by name, so the seat parses it. It reads the pairs from the planned insert's projection and
    the frame's order from the view under it.
  - **Quoting:** a name is written bare only when it is lowercase and the session's parser
    reads `SELECT <name>` back as that column. So `current_date`, mixed case and an embedded
    quote are quoted.
  - **`insertInto` keeps the positional form**, `SELECT * FROM <view>`, as Spark's `insertInto`
    is positional.
  - **The in-memory overwrite:** it names `CREATE OR REPLACE TABLE … AS SELECT <frame column>
    AS <table column>, …`, so the columns pair as the verb pairs them and the table keeps its
    column names and order. It does not keep the table's column types or `NOT NULL`: the replaced table takes the frame's column types and drops `NOT NULL`, so a NULL row is stored where the Spark door refuses (delta-2 re-check F8).
- **F5 (S3), views.** `insertInto` on a native view (`datafusion.public.<view>`,
  `ice.ms.<view>`) refuses with "it is a view, and the native door cannot write through a view".
  It names no statement, since no route can store. The 1.5.6 wheel returns and stores nothing
  for both forms.
- **F6 (S3), the path route.** `spark.sql(…)` cannot read a table made in the native session.
  The message and the notes now name a conversion, `spark.createDataFrame(frame.collect(),
  frame.schema)` on a `ReparkSession`. Its limit is that it holds every row in memory. It also fails loudly, with nothing written, on the head build and on the 1.5.6 wheel, for a decimal value with 11 or more integer digits at any declared precision, and for ±Infinity in a DOUBLE or FLOAT column (delta-2 re-check F9).
  - **Measured on the head build and on the published 1.5.6 wheel:** it stores the frame's rows
    and column types. That covers int, string, date, both timestamps, decimal, boolean, double
    with NaN, array, struct, map and binary, an empty frame, a reordered frame, NULLs, and an
    append onto files of the frame's own types.
  - **`createDataFrame(frame.toPandas())` without the schema** widens int32 to int64, and an
    int with NULL to double, so that append refuses loudly. It also fails on an empty frame
    (`CANNOT_INFER_EMPTY_SCHEMA`).
  - **With `frame.schema`,** `toPandas()` turns NaN into NULL and fails on arrays and maps.
- **Postgres database sources.** A mounted Postgres source's table has no `insert_into`, and
  native DML into it is not refused before the seat, so the guard could reach one and would
  have named a failing `INSERT`.
  - The seat now gives such a target F5's wording ("it is a table of the database source
    <name>, and the native door cannot write into a database source"). It detects the target
    by its catalog being a registered database source.
  - **Not measured end to end:** no database server was used, and planning a write needs one to
    resolve the table. The branch is pinned on a planned statement over a source declared with
    an unroutable host, whose registration opens no connection.
- **Pins:**
  - Rust (`session_write_options_tests.rs`): the by-name and positional forms; the in-memory
    replace with positional aliases; each named statement followed and its rows checked; the
    quoting; a view; a database source.
  - Facade (`test_native_dataframe_write_silent_1.py`):
    - a frame in `(p, id)` order against a `(id, p)` table, on every by-name verb and on
      `insertInto`, on `ice` and `hd`, and `insertInto` on both in-memory kinds. Each follows
      the message literally and checks the rows against fixed expectations and against the
      Spark door's rows for the same verb;
    - native views, both forms;
    - the path route over a native-table frame: an overwrite, an append onto int32 files, and
      an empty frame, keeping int32.
- **Owner ruling and records (2026-11-14, about 09:50 UTC):** verbatim "Go with defaults", adopting option B. F8 and F9 of the delta-2 re-check are corrected in these records, and its F7 is carded as [NATIVE-WRITE-ROUTE-QUOTE-1](native-write-route-quote-1-card-2026-11-14.md).
