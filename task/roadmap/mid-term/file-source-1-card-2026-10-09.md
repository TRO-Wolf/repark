# FILE-SOURCE-1 — streaming file sources feed the micro-batch engine (filed under MB-5, multi-source)

**Filed:** 2026-10-09 by the MB-4 lane on the W-Q5 ruling (owner's delegate, 2026-10-08 evening):
inventory cell `W-STREAM-WRITE-FILESRC` is the Iceberg sink fed by a file-source stream
(`readStream.format("parquet"|"csv").load(dir)` to `writeStream.format("iceberg")`), the W half
of inventory row IPI-47 ("streaming read and write of Iceberg tables"). File sources are outside
the MB slice; this card scopes them for MB-5 (multi-source). Disposition until then: registered
refusal MBE-7 (registry row MB-4-FORMAT-1, a ledger draft MB-4 item 15 files).

## What RePark answers today (MB-4 item 12 probe, 2026-10-09, public doors)

- Read door, `spark.readStream.format("parquet").load(dir)`:
  `PySparkNotImplementedError` `[NOT_IMPLEMENTED] readStream.format(parquet) is not implemented.`
  with `{feature: readStream.format(parquet)}`, no SQLSTATE.
- Write door, `stream.writeStream.format("parquet").start(path, ...)`:
  `PySparkNotImplementedError` `[NOT_IMPLEMENTED] writeStream.format(parquet) is not implemented.`
  with `{feature: writeStream.format(parquet)}`, no SQLSTATE.
- `toTable` into an Iceberg table ignores the format (Spark parity, MB-0c cells F9–F12), so the
  file-source combination is unreachable: the read door refuses before any stream exists.

## What Spark answers (the card's to match)

Recorded MB-0c cells (`python/repark-parity/tests/live_spark/mb0c_facade_oracle.json`): a
format-less `load()` with no path refuses `'path' is not specified` (F1); with a missing path it
refuses `PATH_NOT_FOUND` (F2); a format-less `start(path)` runs its 3 rows into one file (F14);
`load("parquet", <existing-table-name>)` refuses `PATH_NOT_FOUND`, reading the name as a path
(F15). The exact filed combination — a file-source stream into an Iceberg sink — is unmeasured;
step 0 below records it. File streams run on Spark (F14), so the expected answer is rows, not a
refusal.

## Step 0 — the oracle (before any design is ruled)

Record on Spark 4.1.2 + Iceberg 1.11.0: `readStream.format("parquet").load(dir)` over a directory
with committed parquet files, then `writeStream.format("iceberg")` to a table (both `start` and
`toTable`), availableNow: batches, rows, the sink's snapshot summaries, and what a restart
replays. Then the `csv` source the same way. Every design question below is answered from these
cells, not from the documentation.

## Design questions for MB-5 (unruled)

File discovery and offset tracking (what names a file batch, where the offset lives when the sink
is file-fed but the stamp still lands on Iceberg); which formats ride the first slice (`parquet`
only, or `parquet` + `csv`); how reader options for files (`maxFilesPerTrigger`, `latestFirst`)
map onto the micro-batch caps; whether `toTable` keeps ignoring the format once file streams
exist. This card closes when MB-5 lands file sources or the owner declines them.
