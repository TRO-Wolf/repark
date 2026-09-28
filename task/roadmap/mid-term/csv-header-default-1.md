# CSV-HEADER-DEFAULT-1 — `df.write.csv(path)` writes a header by default; Spark does not

**Filed:** 2026-09-28 by the orchestrating session on the owner's ruling of the same day (option B:
"file a card, decide later"). RePark 1.5.x keeps today's default; this card holds the decision for a
later minor release.

## The difference (measured)

- `DataFrameWriter.csv(path)` with no `header` option: RePark writes a header line first
  (`python/repark/src/repark/spark/dataframe/writer_readwriter.py`, the `header` default in `csv()`:
  `self.option("header", "true")` when no header key is set). Spark 4.1.2 defaults `header` to
  `false` and writes data rows only.
- Measured on the U12 oracle (`task/ledgers/staging/u12-probes/u12-spark.json`, Spark 4.1.2 against
  a local S3 emulator): every `W-PATH-S3-csv-*` cell differs only by this line — 9 of U12's 16
  residues (`R-S3-CSV-HEADER` in
  [s3-path-write-1-ledger.md](../../ledgers/staging/s3-path-write-1-ledger.md)). A headerless read
  of a RePark-written file returns the header as an extra data row; an empty frame writes a
  non-empty part where Spark writes 0 bytes.
- The difference is not S3-specific: local path writes default the same way.

## Why it waits

Changing the default changes the bytes existing RePark users already produce. The owner ruled
(2026-09-28) that a 1.5.x patch release must not change it silently.

## Done condition (when scheduled)

The default matches Spark (`header=false`); the U12 csv cells and a local csv cell replay EQUAL; a
release-notes line tells users to pass `header=True` to keep today's output; every existing pin
that relied on the implicit header states `header=True` explicitly.
