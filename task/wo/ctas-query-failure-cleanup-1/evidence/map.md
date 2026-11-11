# task/wo/ctas-query-failure-cleanup-1/evidence/ — AUD-025 measurement evidence

Step tables for CTAS-QUERY-FAILURE-CLEANUP-1: the scripts that measured them,
scrubbed of machine paths, plus the rendered tables. Raw JSON stays in the
lane logs; the committed `table.md` files are the record.

- [step0/](step0/table.md) — the unmodified-tree table: every F row times the
  spellings times hadoop and memory times v2 and v3, every K row, PREPARE,
  EXPLAIN ANALYZE, the cancel cell and the Spark F1/F3/F6 columns.
- [step0/cells_repark.py](step0/cells_repark.py) — the 228-cell RePark runner
  (warehouse root and output path as arguments).
- [step0/fresh_reads.py](step0/fresh_reads.py) — fresh-process catalog rereads
  of every step-0 table.
- [step0/cancel_probe.py](step0/cancel_probe.py) with
  [step0/cancel_worker.py](step0/cancel_worker.py) — the kill-mid-write cell.
- [step0/cells_spark.py](step0/cells_spark.py) — the Spark oracle columns.
- [step0/provenance.md](step0/provenance.md) — head SHA, profile, versions.

Up: [../map.md](../map.md).
