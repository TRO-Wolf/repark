# task/wo/temp-view-session-settings-1/evidence/built-part2/probes/ — the bar scripts (comment-free Python)

- `bar2.py` — `bar2.py <out> <step 0 raw> <runs>`: pairs every cell of `run.py` (both start modes), `rev.py`, `rc.py`
  and the delta 1 sweep on main before part 1 (`pre`), main with part 1 (`part1`) and the head with Spark's, gives
  each a scope by door as part 1's `bar.py` does, and writes `bar2-cells.jsonl` and `bar2-summary.md` (counts per
  verdict and build, every move against `part1` and against `pre`, every non-EQUAL part 2 cell).
- `cmp2.py` — `cmp2.py <dir> <label>…`: the pin file's cells per build against Spark's recording, and each cell a
  build answers unlike Spark.
- `gen_standing.py` — `gen_standing.py <dir> <head> <out>`: the pin file's standing answers, every non-Spark head cell
  by named class (refuses an unclassed cell).
- `wsweep.py` — `wsweep.py <engine> <wh> <out> <ansi> [doors]` (delta 2): one session under fixed settings writes
  every source of the write sweep into a fresh Iceberg table per cell through each write door, and records the
  refusal or the stored rows and types; Spark takes its runtime jar from `ICEBERG_SPARK_JAR`.
- `wsweep_cmp.py` — `wsweep_cmp.py <dir> <build> <build> [shown]`: the cells two builds answer differently.
- `race.py` — `race.py <wh> <out> [writes per door] [seconds] [captured]` (delta 3): the verifier's F1 probe, five
  write doors writing `-NULL` into `DATE` and `BOOLEAN` on five threads beside two threads looping `spark.conf.set`
  (keys outside the captured four, or with `captured` the ANSI flag and the zone too); a write count per door, or a
  duration for the hang run; records landed, refused and other answers, the rows stored and every write over 0.5 s.
- `probes_cmp.py` — `probes_cmp.py <runs> <delta 1 runs>`: the verifier's, Fable's, the reader, join and `smartCsv`
  sets per build against Spark, and the head's answers against delta 1's.

The step 0 probes ran unchanged from `../../probes/` (`run.py <engine> true|unset`, `rev.py <engine>`), part 1's
`rc.py` and `sweep_drive.py` from `../../built-part1/probes/`, each in its own warehouse, every RePark run with the
package on `PYTHONPATH`. The pin file's cells ran through
`python/repark/tests/session/_record_temp_view_session_settings_2.py <engine> <wh> <jar> <out> cells|explain`.

Up: [../map.md](../map.md).
