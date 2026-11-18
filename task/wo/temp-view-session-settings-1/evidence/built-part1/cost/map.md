# task/wo/temp-view-session-settings-1/evidence/built-part1/cost/ — the cost runner's scripts and samples

- `view_data.py` — `view_data.py <dir>` writes the two Parquet tables.
- `view_cost.py` — `view_cost.py <case> <label> <data> <out>` times one case (`match_*` or `differ_*`: `read`,
  `filter`, `join`, `nested`, `noview`) and appends a JSON row; `view_cost.py explain <label> <data> <out>` writes the
  five matching plans, normalised.
- `raw-view.jsonl` — every view sample (compacted); `samples.jsonl` — every sample's wall time and exit code, C-012
  included; `explain-plans.json` — the head's plans, identical to main's.
- Delta 1 (2026-11-18): `delta1-raw-view.jsonl`, `delta1-samples.jsonl` — the re-run on the final head (its C-012
  walls include lock waits); `delta1-c012-inlock.jsonl` — C-012 re-run with the clock inside the lock.

The runner is the AVG-DECIMAL-ROUND-1 one (`main`, `head`, `main-aa` per case, per run) pointed at these scripts and
the C-012 replay.

Up: [../map.md](../map.md).
