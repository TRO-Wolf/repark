# task/wo/avg-decimal-round-1/evidence/raw/rev/ — the revision's raw cells (after the inventory critic)

Written by `../../probes/run_revision.py` (cells in `rev_cells.py`), compacted by `compact.py` and paired by
`analyze.py` like the step 0 files one level up.

- `spark-true-base.jsonl`, `spark-false-base.jsonl` — Spark 4.1.2, ANSI on and off: `mean` at every `avg`
  position, the mixed-`DISTINCT` cells, the shapes, `mod`/`pmod`/`%`, `round`, the five `try_*` calls, `s04`, the
  DataFrame `mean`, `%`, `pmod`, `round(col / 3, 2)` and folding cells, the Iceberg `WHERE` and `mean` CTAS.
- `repark-true-base.jsonl`, `repark-false-base.jsonl` — RePark main, the same cells.
- `native.jsonl` — the native door's sample (mixed `DISTINCT`, `mean`, `mod`/`pmod`/`%`), paired with both modes.
- `verdicts.jsonl`, `summary.md` — `analyze.py`'s pairing and count table.

Up: [../map.md](../map.md).
