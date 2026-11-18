# task/wo/temp-view-session-settings-1/evidence/built-part2/cost/ — the cost scripts and data (comment-free Python)

- `view_cost2.py` — `view_cost2.py <case> <label> <data> <out>`: one case in its own process, timed inside the
  process (inside the exclusive measure lock): the DataFrame view read, filter, join, view over a view, `LIMIT`, no
  view, `cache()` of a frame, the facade scratch-view operations and write doors, each `match_` (settings equal) or
  `differ_` (ANSI `SET` off after the views were made); `explain` writes the matching cases' plans.
- `samples.jsonl` — every sample's wall time (C-012 timed inside the lock); `raw-view.jsonl` — every view sample;
  `explain-base.json`, `explain-head.json` — the matching cases' plans.

Up: [../map.md](../map.md).
