# map — docs/perf/facade-5-display-baseline-2026-09-14

## Purpose

The runnable harness behind
[facade-5-display-baseline-2026-09-14.md](../facade-5-display-baseline-2026-09-14.md)
(FACADE-5 step 0). One session, one process, no JVM. The runner refuses to measure
a debug native module, builds the three fixture frames outside the timed region,
waits for an idle box (no cargo/rustc/maturin, 1-minute load under 6) before each
cell, and reports medians of five reps after one warmup. Each cell times the FETCH
leg (capped rows to an Arrow table) and the FORMAT leg (the formatter over the
pre-materialized table) separately, pairs each fetch rep with its format rep into
the wall median, then cross-checks the leg-composed string against the real
door's bytes (`door_check`).

## Contents

- [run_facade_5_display.py](run_facade_5_display.py) — the six-renderer battery
  (ASCII `show`, vertical `show`, `_repr_html_`, duckdb show, polars show, eager
  `repr`) at n ∈ {20, 1000} × truncate on/off × {flat 7-type, 50-column wide,
  nested struct/array/map} frames of 1,100 rows, plus styled cells at
  `repark.display.max_rows` ∈ {10, n}. Usage:
  `/tmp/f-types4/.venv/bin/python docs/perf/facade-5-display-baseline-2026-09-14/run_facade_5_display.py /tmp/facade-5-display-baseline.json`

## Pointers

- Up: [../map.md](../map.md)
- Baseline doc: [../facade-5-display-baseline-2026-09-14.md](../facade-5-display-baseline-2026-09-14.md)
- Ledger: [../../../task/ledgers/staging/facade-5-ledger.md](../../../task/ledgers/staging/facade-5-ledger.md)
