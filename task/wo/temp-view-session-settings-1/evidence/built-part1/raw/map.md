# task/wo/temp-view-session-settings-1/evidence/built-part1/raw/ — the runs, compacted

Written by the step 0's `compact.py` (timings dropped, scratch and home paths replaced by placeholders).

- `head-true.jsonl`, `head-unset.jsonl`, `main-true.jsonl`, `main-unset.jsonl` — `run.py` on the head and main.
- `rev-head.jsonl`, `rev-main.jsonl`, `rev-spark.jsonl` — `rev.py`, the three in one hour.
- `rc-head.jsonl`, `rc-main.jsonl`, `rc-spark.jsonl` — the re-critic's `rc.py`, the three in one hour.
- `bar-cells.jsonl`, `bar-summary.md` — `bar.py`'s pairing and summary.
- Delta 1 (2026-11-18): `delta1-sweep.json` — the sweep's 232 cells on `spark`, `main`, `part1` (as built) and `head`;
  `delta1-rev-{head,main,part1,spark}.jsonl`, `delta1-rc-{head,main,part1,spark}.jsonl` — `rev.py` and `rc.py`, the
  four in one window. The head's `run.py` answers equal `head-true.jsonl` and `head-unset.jsonl` cell for cell.

Up: [../map.md](../map.md).
