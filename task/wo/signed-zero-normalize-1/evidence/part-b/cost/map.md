# task/wo/signed-zero-normalize-1/evidence/part-b/cost/ — part B part 1, the cost gates (order §8)

- [cost_probe.py](cost_probe.py) — one timed case per fresh process and warehouse: `n1` (200 `EXPLAIN`s after 10
  warm-ups of a 320-column plan with integer and string `GROUP BY`, join, window and `ORDER BY` keys, float columns as
  values only, and one integer-derived float key), `n2` (the step-0 `spark-sql` read cells), `a` (part A's case (a)
  statements), `f1` (10M rows, a quarter `-0.0`: group, join against a 1000-row zero-free build side, window,
  distinct, TopK, five repeats each, on the Spark door and on native), `f2` (the same with canonical NaN and no
  `-0.0`) and `f3` (delta round: a float-key join probing a 20-file hadoop-catalog Iceberg table, with one `EXPLAIN
  ANALYZE` recording whether the dynamic filter reaches the probe scan). Each record carries the module, the load average and the answers (the join count is identical on every
  build, guard (b)).
- [summarize.py](summarize.py) — medians per case and build (with `04d84ab9-aa`, a second run of the base package for
  the base-against-base check), the ratios against `04d84ab9` and `276dc503`, and a per-sample load table.
- [l4l_run.py](l4l_run.py) — the C-012 like-for-like ratio (PERF-1's work-equal set) per run and pair, from the
  replay harness's per-cell work times. Prepared; not yet run.
- `cost-raw.jsonl` — the interleaved runs (04d84ab9, 276dc503, head per case per run), written by the lane's runner
  under the exclusive measure lock; present once the runs finish.

The C-012 like-for-like set itself has no committed harness (it lives in an uncommitted scratch tree of the
ATTR-ID-1 lane); see the hand-back.

Up: [../map.md](../map.md).
