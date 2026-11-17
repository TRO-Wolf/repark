# task/wo/avg-decimal-round-1/evidence/raw/ — raw cells, verdicts, grids

Written by the probes in [../probes/](../probes/map.md); the SQL text of a cell is rebuilt from its key by
`cells.py`. Scratch paths are a placeholder (`<scratchpad>`).

- `spark-<ansi>-<conf>.jsonl` — Spark 4.1.2, ANSI `true` or `false`, conf `base` (defaults), `apl_false`
  (`spark.sql.decimalOperations.allowPrecisionLoss=false`) or `pmp_false`
  (`spark.sql.legacy.literal.pickMinimumPrecision=false`): every cell of `run_door.py` for that run, with the conf
  values read back (`conf/get/…`) and the stored data (`data/t`, `data/v`, `data/t4_files`).
- `repark-<ansi>-<conf>.jsonl` — RePark main, the same cells.
- `native.jsonl` — the native door `repark.sql()`, paired with both Spark `base` runs.
- `extra-spark-true.jsonl`, `extra-repark-true.jsonl` — `run_extra.py`, ANSI on (the SQL text is in `run_extra.py`).
- `explain-paths-repark.txt` — `explain_paths.py`'s plans and answers.
- [rev/](rev/map.md) — the revision's cells after the inventory critic (`run_revision.py`).
- `verdicts.jsonl` — `analyze.py`'s pairing: one row per RePark or native cell, mode and conf, with both result
  types, the verdict, the differing rows (first six), the count of differing rows and the per-value cause tags.
- `summary.md` — `analyze.py`'s count table by conf, mode, family, door and position.
- `grids.md` — `grids.py`'s letter grids: `avg` group by column, `avg` form by column, division cell by column, the
  result types that differ, and the totals.

Up: [../map.md](../map.md).
