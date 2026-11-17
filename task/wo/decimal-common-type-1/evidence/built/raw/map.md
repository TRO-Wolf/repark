# task/wo/decimal-common-type-1/evidence/built/raw/ — the build's raw data

Compacted as step 0's (`../../probes/compact.py`: no SQL text, no timings, scratch and home paths replaced).

- `head-repark-false.jsonl`, `head-repark-true.jsonl`, `head-side-repark-*.jsonl`, `head-revision-repark-*.jsonl`,
  `head-native.jsonl` — the head's cells; pair them with step 0's Spark files in [../../raw/](../../raw/map.md).
- `main-native-rerun.jsonl` — main's native door run a second time (error texts that vary by row order).
- `bar-table.md` — `bar.py`'s output: counts per cause and verdict, the transitions, every non-EQUAL common-type
  cell named. `summary.md` — `analyze.py`'s summary for the head.
- `other-causes.md` — every non-EQUAL cell outside the common-type cause, cell for cell.
- `cost-samples.jsonl`, `cost-raw-*.jsonl`, `cost-table.md` — the cost runner's samples, the per-case timings and
  `analyze_cost.py`'s table.

Up: [../map.md](../map.md).
