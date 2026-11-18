# task/wo/temp-view-session-settings-1/evidence/built-part1/probes/ — the bar scripts (comment-free Python)

- `bar.py` — `bar.py <out> <step 0 raw> <runs>`: pairs every cell of `run.py` (both start modes), `rev.py` and `rc.py`
  on main and the head with Spark's, gives it a scope by door (part 1, part 2, held, catalog, direct) and the step 0's
  verdict classes, and writes `bar-cells.jsonl` and `bar-summary.md` (counts, every move, every non-EQUAL part 1
  cell by class).
- `target.py` — `target.py <step 0 raw> <out>`: rebuilds part 1's view-gap set from the step 0's `verdicts.jsonl` and
  `rev-cells.jsonl` and says where each landed on the head.
- `rc.py` — the round 2 critic's probe, unchanged but for one wrapped literal; it imports the step 0's
  `../../probes/lib.py`. `rc.py <spark|repark> <warehouse> <jar> <out>`.
- Delta 1 (2026-11-18): `sweep_drive.py <tests dir> <spark|repark> <warehouse> <jar> <out>` runs the pin file's
  sweep (`_temp_view_session_settings_1_sweep.py`) alone in both ANSI modes; `sweep_cmp.py <runs.json>` gives each
  cell's verdict per build against Spark and the moves from main; `sweep_standing.py <spark> <head> <out>` writes the
  pin file's standing answers by class; `bar_moves.py <bar-cells.jsonl>` counts verdicts and moves per scope.

The step 0 probes ran unchanged from `../../probes/` (`run.py <engine> true|unset`, `rev.py <engine>`), each in its
own warehouse, every RePark run with the package on `PYTHONPATH`; `compact.py` copied the outputs here.

Up: [../map.md](../map.md).
