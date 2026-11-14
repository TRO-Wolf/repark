# task/wo/nan-minmax-1/evidence/

- [step0/](step0/map.md) — the constant-file table (reads, the statistics shortcut, the round trip, the Iceberg
  ingestion) on main and the head against Spark 4.1.2, and every read path with file:line.
- [cost/](cost/map.md) — `count(*)` and a selective filter over 100 Parquet files, and `max(d)` over a file with
  statistics, main against the head.

Up: [../map.md](../map.md).
