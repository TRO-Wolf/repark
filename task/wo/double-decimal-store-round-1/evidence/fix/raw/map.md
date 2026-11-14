# task/wo/double-decimal-store-round-1/evidence/fix/raw/ — the build's records

- spark-pins-ansi-true.jsonl, spark-pins-ansi-false.jsonl — Spark 4.1.2's answer for every pinned cell.
- table-head-ansi-true.jsonl, table-head-ansi-false.jsonl — the step 0 probe re-run on the head package.
- fuzz-base-ansi-true.jsonl, fuzz-base-ansi-false.jsonl, fuzz-head-ansi-true.jsonl,
  fuzz-head-ansi-false.jsonl, fuzz-spark-ansi-true.jsonl, fuzz-spark-ansi-false.jsonl — the coercion fuzz
  on main, the head and Spark.

- cost-bd639c4e.jsonl, cost-3e093a45.jsonl — the cost samples, one JSON line per sample.
- union-string.txt — the UNION-to-string cells on `nwc/pkg-base`, main and the head.
- cost-finish-cast.jsonl, cost-finish-nocast.jsonl, cost-finish-nocast-hires.jsonl, cost-finish-samples.jsonl,
  cost-finish-samples-hires.jsonl — the finishing cost run (the merged head against main `6f9f2aaa`) and its
  fifteen-repeat no-cast re-run: one JSON line per probe sample, and each sample's exit code, wall time and load.

Up: [../map.md](../map.md).
