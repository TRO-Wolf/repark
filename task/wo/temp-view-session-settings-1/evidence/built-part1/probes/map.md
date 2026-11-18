# task/wo/temp-view-session-settings-1/evidence/built-part1/probes/ — the bar scripts (comment-free Python)

- `bar.py` — `bar.py <out> <step 0 raw> <runs>`: pairs every cell of `run.py` (both start modes), `rev.py` and `rc.py`
  on main and the head with Spark's, gives it a scope by door (part 1, part 2, held, catalog, direct) and the step 0's
  verdict classes, and writes `bar-cells.jsonl` and `bar-summary.md` (counts, every move, every non-EQUAL part 1
  cell by class).
- `target.py` — `target.py <step 0 raw> <out>`: rebuilds part 1's view-gap set from the step 0's `verdicts.jsonl` and
  `rev-cells.jsonl` and says where each landed on the head.
- `rc.py` — the round 2 critic's probe, unchanged but for one wrapped literal; it imports the step 0's
  `../../probes/lib.py`. `rc.py <spark|repark> <warehouse> <jar> <out>`.

The step 0 probes ran unchanged from `../../probes/` (`run.py <engine> true|unset`, `rev.py <engine>`), each in its
own warehouse, every RePark run with the package on `PYTHONPATH`; `compact.py` copied the outputs here.

Up: [../map.md](../map.md).
