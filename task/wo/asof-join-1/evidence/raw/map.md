# task/wo/asof-join-1/evidence/raw/ — the cells, the plans, the cost samples

- `spark-<ansi>-<groups>-<type>.jsonl.gz`, `repark-<ansi>-<groups>-.jsonl.gz` — every cell per engine, mode and
  group: one JSON record per cell with its key (`g`, `ansi`, `typ`, `dir`, `exact`, `tol`, `how`, `on`, `case`,
  …), `surf` (`D`, `W1`, `W2`, `P`, `PD`, `SQL`, …) and sorted `rows`, `names`, `types` or `error`, and `ms`.
  Spark's grid is one file per type (ANSI on); `spark-true-EM-` holds the `E` and `M` groups rerun with `TZ=UTC`
  (their first-pass records were dropped from `spark-true-EMONTCSX-`). Scratch paths read `N`, `S`, `ORACLE`.
- `explain-spark.txt` — Spark's `QueryExecution` text (parsed, analyzed, optimized, physical) for `_joinAsOf`, one
  per direction, with key `k1` and without, tolerance 2, `left`, ANSI on.
- `explain-main.txt` — main's `explain(True)` for W1 and W2 on the same cells.
- `cost.jsonl.gz` — the cost samples: engine, surface, rows a side, keys, the seconds per repeat, peak RSS, the
  `count` and `sum(rid)` result (`probes/cost.py`).

Up: [../map.md](../map.md).
