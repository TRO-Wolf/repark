# Card NATIVE-DATAFRAME-WRITE-SILENT-1: a native-door DataFrame write reports success and stores nothing

**Date:** 2026-11-13. **Filed by:** the records clerk (`claude-opus-5-5`). **Source:** NATIVE-CTAS-ICEBERG-REFUSE-1
(PR #1317) verifier 2, 2026-11-13, finding F4.

**Status:** filed 2026-11-13, not dispatched. Severity **S1**, a silent lost write, as rated at filing. The verifier
recorded it as S2 and outside its unit's scope.
**Ruled 2026-11-13** (below): measure first, then a refusal guard; it rides v1.5.7.
**FIXED 2026-11-13** by this unit (branch `fix/native-dataframe-write-silent-1`): every
native-door DataFrame write that would store nothing refuses before any effect. See "Step 0" and
"Fix" below.

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
  - a path by the user's destination, recovered from the staging segment
    `repark-staging-<32 hex>-<name>` only on an exact match, with the raw URL otherwise.
- **The workaround follows the insert:**
  - an append points to a collected native `INSERT INTO … SELECT` or a `ReparkSession` frame;
  - a static overwrite points to `INSERT OVERWRITE` for an Iceberg table, or a `ReparkSession`
    frame;
  - `overwritePartitions` and paths point to a `ReparkSession` frame.
- Plain `repark.sql()` and the Spark door do not pass the check, and neither does `mergeInto`.
  `mergeInto` keeps its loud native refusal as a control pin.
- **Pins:**
  - `crates/repark-python/src/session_write_options_tests.rs`;
  - `python/repark/tests/writes/test_native_dataframe_write_silent_1.py`, over all 2436 cells:
    the 399 guarded cells refuse with the exact text and leave rows, snapshots, files and
    staging unchanged, and every other cell keeps main's outcome.
- **Residue, recorded for the records batch:** `note_local_write_root` keeps the staging path
  allow-listed after a refusal. Also recorded there: the S3 overwrite deletes before it copies
  (`path_write.rs:403`), a native path containing `'` is refused before the seat, and the Spark
  door stores `partitionBy(c).insertInto` rows. The list is in
  [evidence/map.md](../../wo/native-dataframe-write-silent-1/evidence/map.md).
