# task/wo/temp-view-cache-lazy-parse-1/built/evidence/probes/ — A1's probes

- [f8.py](f8.py) — Q6's `writeTo` verbs: `create`, `createOrReplace`, `append`, `overwritePartitions` into an Iceberg
  v2 table, over a cached frame (the write first; `count` under the creating zone first; `count` under the reading
  zone, then the write back under the creating zone) and over the uncached frame; five readers, two directions.
  `f8.py <engine> <warehouse parent> <out> _ true`; step 0's `cells.py` setup with the verb cells appended.
- [bar_cmp.py](bar_cmp.py) — pairs every recorded cell (step 0, the revision round, Fable's ten, F8) with Spark's and
  classes it for main and the head, by step 0's verdict rule; families `req:*` (the 188) and `fc:join` (the 128).
- [a1_cost.py](a1_cost.py) — the cost cases on a 2M-row JSON file: `count` then `collect`, `collect` first, a view
  then `count`, a selective child's `count`, a second child's `count`. `a1_cost.py <case> <build> <data> <out>`.
- [a1_cost_table.py](a1_cost_table.py) — the ten-round cost table from `a1_cost.py`'s samples and C-012's walls:
  `a1_cost_table.py <raw.jsonl> <samples.jsonl>`.
- [a1_perf.py](a1_perf.py) — one counted action (`fill_count`, `view_total`, `plain_count`) on the same 2M-row file,
  enabling `perf stat` or `perf record` through a control FIFO around the action only.
  `a1_perf.py <label> <case> <round> <data> <ctl fifo> <ack fifo> <out>`.
- [a1_perf_table.py](a1_perf_table.py) — medians of `perf stat -x,` outputs per package and case, as ratios to the
  first package: `a1_perf_table.py <dir> "<packages>" "<cases>"`.

Up: [../map.md](../map.md).
