# task/wo/temp-view-session-settings-1/evidence/built-part2/cost/ — the cost scripts and data (comment-free Python)

- `view_cost2.py` — `view_cost2.py <case> <label> <data> <out>`: one case in its own process, timed inside the
  process (inside the exclusive measure lock): the DataFrame view read, filter, join, view over a view, `LIMIT`, no
  view, `cache()` of a frame, the facade scratch-view operations and write doors, each `match_` (settings equal) or
  `differ_` (ANSI `SET` off after the views were made); `explain` writes the matching cases' plans. Delta 1 added
  the `exactjoin` and `crossjoin` cases; delta 2 reads `k2`'s second column, `w`, as their `b` (delta 1's
  `SELECT a, b FROM k2` failed every case at load).
- `samples.jsonl` — delta 2's run on `pkg-view2-9d71e030`: every sample's wall time (C-012 timed inside the lock); `raw-view.jsonl` — every view sample;
  `explain-base.json`, `explain-head.json` — the matching cases' plans.
- [delta3/](delta3/map.md) — delta 3's held-row samples and plans.

Up: [../map.md](../map.md).
