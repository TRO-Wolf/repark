# map — docs/perf/facade-3-cdf-baseline-2026-09-13

## Purpose

The runnable harness behind
[facade-3-cdf-baseline-2026-09-13.md](../facade-3-cdf-baseline-2026-09-13.md)
(FACADE-3 step 1). One session, one process, no JVM. The runner refuses to measure a
debug native module, builds every fixture outside the timed region, waits for an idle
box (no cargo/rustc/maturin) before each cell, and reports medians of three reps after
one warmup.

## Contents

- [run_facade_3_cdf.py](run_facade_3_cdf.py) — the eight-shape battery at 1e4 and 1e5
  rows (list of tuples, list of `Row`, list of dicts, tuples + DDL, tuples +
  `StructType`, nested, pandas and polars controls), each timed as `createDataFrame`
  alone and `createDataFrame(...).count()`, plus the floor cell and a cProfile split
  for the three slowest 1e5 shapes. Usage:
  `.venv/bin/python docs/perf/facade-3-cdf-baseline-2026-09-13/run_facade_3_cdf.py /tmp/facade-3-cdf-baseline.json`

## Pointers

- Up: [../map.md](../map.md)
- Baseline doc: [../facade-3-cdf-baseline-2026-09-13.md](../facade-3-cdf-baseline-2026-09-13.md)
- Ledger: [../../../task/ledgers/staging/facade-3-ledger.md](../../../task/ledgers/staging/facade-3-ledger.md)
