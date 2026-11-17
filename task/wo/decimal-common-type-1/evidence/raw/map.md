# task/wo/decimal-common-type-1/evidence/raw/ — raw cells, verdicts, summaries

Written by the probes in [../probes/](../probes/map.md); the SQL text of a cell is rebuilt from its key by
`cells.py`. Paths are placeholders (`<scratchpad>`, `<home>`).

- `spark-false.jsonl`, `spark-true.jsonl` — Spark 4.1.2, ANSI off and on: every cell of `run_cells.py`.
- `repark-false.jsonl`, `repark-true.jsonl` — RePark main, the same cells.
- `native.jsonl` — the native door `repark.sql()`, paired with both Spark modes.
- `side-spark-false.jsonl`, `side-spark-true.jsonl`, `side-repark-false.jsonl`, `side-repark-true.jsonl` —
  `run_side.py` (these keep their SQL text).
- `revision-spark-false.jsonl`, `revision-spark-true.jsonl`, `revision-repark-false.jsonl`,
  `revision-repark-true.jsonl` — `run_revision.py`, after the critic.
- `explain-union-repark.txt` — `explain_union.py`: the nested binary unions of the initial plan.
- `verdicts.jsonl` — `analyze.py`'s pairing: one row per RePark or native cell and ANSI mode, with both answers, the
  verdict and the cause.
- `summary.md` — `analyze.py`'s tables: by engine, cause, group, door, pair and position, and every non-agreeing
  RePark cell outside the pair grid (ANSI off).
- `grids.md` — `grids.py`'s position-by-pair grids for both tables and doors, and the group tables.

Up: [../map.md](../map.md).
