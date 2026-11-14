# task/wo/nan-minmax-1/evidence/

- [step0/](step0/map.md) — the constant-file table (reads, the statistics shortcut, the round trip, the Iceberg
  ingestion) on main and the head against Spark 4.1.2, and every read path with file:line.
- [cost/](cost/map.md) — `count(*)` and a selective filter over 100 Parquet files, and `max(d)` over a file with
  statistics, main against the head.
- [delta/](delta/map.md) — the delta round: F1 red and green, the new xfails, and pruning on the fix against main
  over verifier 1's sets.
- [delta2/](delta2/map.md) — the second delta round (`d7ae795a`): the Spark 4.1.2 answers for the pinned all-NULL and spread cells, the umbrella-key and `RESET` probe on both heads, and the C-012 like-for-like table.

Up: [../map.md](../map.md).
