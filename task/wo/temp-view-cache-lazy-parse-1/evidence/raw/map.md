# task/wo/temp-view-cache-lazy-parse-1/evidence/raw/ — the cells, the verdicts, the trace, the cost samples

Scratch paths read `S`, `<scratchpad>`, `<home>` or `<tmp>`.

- `spark.jsonl.gz`, `main.jsonl.gz`, `base.jsonl.gz` — one JSON object per cell (`tag`, then `v`, `err` or
  `build_err`), every chunk of `cells.py` concatenated per engine: `R`, `Q`, `C`, `N`, `U`, `T`, `L`.
- `verdicts.jsonl.gz` — `cmp.py`'s verdict per cell for `main` and `base`, with the columns that differ.
- [summary.txt](summary.txt) — the counts per group and build, the moves, the non-EQUAL cells by door, action, reader
  and mode, and the door × action grid.
- [runs.txt](runs.txt) — each RePark log's `repark._native.__file__` line, then each chunk's exit code, seconds and
  line count.
- [trace-main.jsonl](trace-main.jsonl) — `trace.py` on main: per scenario, the answer and the wrapped calls with the
  live zone at each.
- [cost-samples.jsonl](cost-samples.jsonl) — `cost.py` on main: cumulative milliseconds at each phase, one sample per
  line (rounds 1 and 2; stopped there).

Up: [../map.md](../map.md).
