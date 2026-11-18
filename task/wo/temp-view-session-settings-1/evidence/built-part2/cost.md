# TEMP-VIEW-SESSION-SETTINGS-1 part 2 — cost (delta 1: not completed)

The cost series on `pkg-view2-513208ce` against `pkg-view1-c54f5fc7` was started (the runner `cost/view_cost2.py`
with task 2's two join cases added, `exactjoin` and `crossjoin`, held at 1.05x on matching settings; 5 alternating
base/head/base runs; each sample's clock starts inside the exclusive lock: `time.perf_counter` inside the Python
process for the view rows, `date` inside the locked `bash -c` for C-012) and stopped at the turn budget. No row is a
result. The plans of the matching cases are in `cost/explain-base.json` and `cost/explain-head.json`.

Up: [map.md](map.md).
