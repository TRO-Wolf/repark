# task/wo/cast-literal-unwrap-1/evidence/raw/ — raw cells, verdicts, summaries

Written by the probes in [../probes/](../probes/map.md); the SQL text of a cell is rebuilt from its key by
`cells.py`. Paths are placeholders (`<scratchpad>`, `<home>`).

- `spark-false.jsonl`, `spark-true.jsonl` — Spark 4.1.2, ANSI off and on: every cell of `run_cells.py`.
- `repark-false.jsonl`, `repark-true.jsonl` — RePark main, the same cells, plus the `EXPLAIN` rows (`pos` `explain`).
- `native.jsonl` — the native door `repark.sql()` over a view per kind; the Parquet setup rows record its refusal.
- `held-spark-false.jsonl`, `held-spark-true.jsonl`, `held-repark-false.jsonl`, `held-repark-true.jsonl` — the
  kernel check (`run_held.py`).
- `verdicts.jsonl` — `analyze.py`'s pairing: one row per RePark or native cell and ANSI mode, with both answers and
  the verdict (`agree`, `both_raise`, `wrong_answer`, `wrong_write`, `missed_raise`, `over_raise`).
- `summary.md` — `analyze.py`'s tables: by kind, family, storage, door and position; the DML list; the kernel check;
  the headline cells.
- `explain-side-repark.txt`, `explain-side-spark.txt` — `explain_side.py` on each engine.
- `datafusion-54.1.0-vs-55.2.0.diff` — the non-test diff of `unwrap_cast.rs` (optimizer), `casts.rs` (expr-common)
  and `simplifier/unwrap_cast.rs` (physical-expr) between the two releases.

Up: [../map.md](../map.md).
