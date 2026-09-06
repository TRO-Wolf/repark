# map — python/repark-parity/bench/approxpct

## Purpose

The tracked probes behind
[docs/perf/approx-percentile-baseline.md](../../../../docs/perf/approx-percentile-baseline.md).
Round 2 (2026-09-06) replaces the throwaway `/tmp/bench_approx.py` the before/after
tables were recorded with: every probe refuses to run against a debug module or a
module from another tree.

This file closes when the H-3 campaign archives to `docs/history/`.

## Contents

- [run_cells.py](run_cells.py) — `run_cells.py ROWS ATTEMPTS [--control]` times one
  baseline row on a `range(1, ROWS+1)` scan: the sketch `percentile_approx(id, 0.5)`,
  or `count(id)` under `--control`. Prints one attempt per line (wall seconds,
  `ru_maxrss` peak MiB, the answer, start/end 1-minute load). The caller runs one
  fresh process per baseline row so peak RSS stays attributable.

## Pointers

- The baseline that quotes these cells:
  [docs/perf/approx-percentile-baseline.md](../../../../docs/perf/approx-percentile-baseline.md).
- Up: [../map.md](../map.md).
