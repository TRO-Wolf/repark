# map — docs/perf

## Purpose

Committed performance baselines. A number whose environment was not recorded
is not a baseline (H-3). This directory is evidence plus the machine/profile
header; it is not a second measurement convention.

This file closes when the H-3 campaign archives to `docs/history/`.

## Contents

- [dynamic-flatten-baseline.md](dynamic-flatten-baseline.md) — **PERF-DYNFLATTEN-1
  (2026-09-04):** 1e5 and 1e6 per-fixture wall / RSS / walks, Spark explode wall,
  ratio, row-set equality, and the three H-3 candidate rankings. Release profile
  only: a debug module inverts the ranking, so debug numbers are not a baseline.
  **PERF-DYNFLATTEN-2 (2026-09-04)** appends an "after" section: its own before/after
  pair measured back to back on one quieter host, never overwriting the earlier tables.
  Two runs from different hours are not one table — each carries its own noise floor and
  its own 1-minute load, and a cost is read against the floor of the run it came from.
  pins: perf-dynflatten-1-measure/C-003, C-004

- [facade-boundary-baseline.md](facade-boundary-baseline.md) — **PERF-FACADE-1
  (2026-09-04):** the `collect()` and `withColumn`-chain cells of PERF-ANALYSIS-1 §7.3,
  before and after, with the boundary controls that must not move. The before column is one
  battery run at `origin/main`; the after column is the median of three repeats and the floor
  is the spread of those three, so a cell inside its floor is not a result. Section 4 repeats
  both halves inside one process on one module — the load-independent measurement — because the
  two battery columns come from different hours. Section 2 records why the projection collapse
  was measured (a perfect collapse tops out at 140 ms) and deliberately not built.
  pins: perf-facade-1/C-001, C-005, C-006

## Pointers

- Up: [../map.md](../map.md)
- Harness: [../../python/repark-parity/bench/dynflatten/map.md](../../python/repark-parity/bench/dynflatten/map.md)
