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

- `critic-spark-false-G-BIGINT.jsonl.gz` — the grid with ANSI off, type BIGINT (324 records), Spark, from the critic's
  run of 2026-11-20; the same record format as the files above.
- `critic-spark-false-G-DATE.jsonl.gz` — the grid with ANSI off, type DATE (324 records), Spark, from the critic's run
  of 2026-11-20; the same record format as the files above.
- `critic-spark-false-G-DECIMAL102.jsonl.gz` — the grid with ANSI off, type DECIMAL102 (324 records), Spark, from the
  critic's run of 2026-11-20; the same record format as the files above.
- `critic-spark-false-G-DOUBLE.jsonl.gz` — the grid with ANSI off, type DOUBLE (324 records), Spark, from the critic's
  run of 2026-11-20; the same record format as the files above.
- `critic-spark-false-G-INT.jsonl.gz` — the grid with ANSI off, type INT (324 records), Spark, from the critic's run
  of 2026-11-20; the same record format as the files above.
- `critic-spark-false-G-TIMESTAMP.jsonl.gz` — the grid with ANSI off, type TIMESTAMP (324 records), Spark, from the
  critic's run of 2026-11-20; the same record format as the files above.
- `critic-spark-false-G-TIMESTAMP_NTZ.jsonl.gz` — the grid with ANSI off, type TIMESTAMP_NTZ (324 records), Spark,
  from the critic's run of 2026-11-20; the same record format as the files above.
- `critic-spark-false-off.jsonl.gz` — Spark, ANSI off, groups `E`, `M`, `O`, `N`, `Q9` and `S`, from the critic's run
  of 2026-11-20.
- `critic-spark-true-off.jsonl.gz` — Spark, ANSI on, groups `M`, `O`, `N`, `Q9` and `S`, from the critic's run of
  2026-11-20.
- `critic-main-false-off.jsonl.gz` — main, ANSI off, groups `M`, `O`, `N`, `Q9` and `S`, from the critic's run of
  2026-11-20.
- `critic-main-true-off.jsonl.gz` — main, ANSI on, groups `M`, `O`, `N`, `Q9` and `S`, from the critic's run of
  2026-11-20.

Up: [../map.md](../map.md).
