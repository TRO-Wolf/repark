# task/wo/temp-view-session-settings-1/evidence/built-part2/probes/ — the bar scripts (comment-free Python)

- `bar2.py` — `bar2.py <out> <step 0 raw> <runs>`: pairs every cell of `run.py` (both start modes), `rev.py`, `rc.py`
  and the delta 1 sweep on main before part 1 (`pre`), main with part 1 (`part1`) and the head with Spark's, gives
  each a scope by door as part 1's `bar.py` does, and writes `bar2-cells.jsonl` and `bar2-summary.md` (counts per
  verdict and build, every move against `part1` and against `pre`, every non-EQUAL part 2 cell).
- `cmp2.py` — `cmp2.py <dir> <label>…`: the pin file's cells per build against Spark's recording, and each cell a
  build answers unlike Spark.
- `gen_standing.py` — `gen_standing.py <dir> <head> <out>`: the pin file's standing answers, every non-Spark head cell
  by named class (refuses an unclassed cell).

The step 0 probes ran unchanged from `../../probes/` (`run.py <engine> true|unset`, `rev.py <engine>`), part 1's
`rc.py` and `sweep_drive.py` from `../../built-part1/probes/`, each in its own warehouse, every RePark run with the
package on `PYTHONPATH`. The pin file's cells ran through
`python/repark/tests/session/_record_temp_view_session_settings_2.py <engine> <wh> <jar> <out> cells|explain`.

Up: [../map.md](../map.md).
