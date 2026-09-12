# map — python/repark-parity/bench/cast

## Purpose

PERF-CAST-1 step 1: a tracked harness that rebuilds the S2-21 reviewers' CAST
shapes — a 200k-row eager MemTable, CAST counts 50 / 250 / 2500, three plan
shapes — and writes one CSV row per cell (warmup, three timed repetitions,
medians). Measurement only; no product change.

This file closes when PERF-CAST-1 merges, or when the owner closes the slate
row.

## Contents

- `__init__.py` — package marker for `PYTHONPATH=.../bench` imports.
- [run_cast.py](run_cast.py) — CLI: `--out` CSV, `--shapes`, `--sizes`,
  `--rows` (default 200000), `--warmup` (1), `--repeats` (3), `--attribute`
  for the largest requested size of each shape. Prints per-cell medians and
  the log-log exponent fit across the requested sizes.
  pins: perf-cast-1/C-001
- [measure.py](measure.py) — session + source view, `session.sql` (plan) then
  `EXPLAIN ANALYZE` (execute, output discarded so a 2500-column projection is
  not copied into Python), CSV writer, exponent fit. The pin imports
  `measure_one_cell`.
  pins: perf-cast-1/C-001, C-004
- [shapes.py](shapes.py) — SQL builders. `standalone` is `CAST((id + i) AS
  VARCHAR)` over the source; `over_aggregate` is `CAST(sum(id + i) AS
  VARCHAR)` in one aggregate; `projection_over_aggregate` is the same CAST
  list over `SELECT sum(id) AS total`. The `+ i` keeps each CAST unique so
  common-subexpression elimination cannot collapse the grid to one expression.
- [models.py](models.py) — `CellRecord`, `PhaseSample`, `AttributionRecord`
  (Pydantic v2).
- [attribute.py](attribute.py) — C-002 probes: same-width literal SELECT
  (parse), `session.sql` (logical + optimizer + wrap), `EXPLAIN`, `EXPLAIN
  ANALYZE` plus `elapsed_compute`, `datafusion.optimizer.max_passes` 0/1/3,
  EXPLAIN VERBOSE rule names, optional `REPARK_LOG=info` `py.entry` span
  close times.
  pins: perf-cast-1/C-002, C-003
- `map.md` — this file.

## Reproduce

```
PYTHONPATH=python/repark-parity/bench .venv/bin/python \
  python/repark-parity/bench/cast/run_cast.py \
  --out docs/perf/cast-cost-2026-09-12.csv \
  --attribute \
  --attribute-json docs/perf/cast-cost-2026-09-12.attr.json
```

The committed tables live in [docs/perf/cast-cost-2026-09-12.md](../../../../docs/perf/cast-cost-2026-09-12.md).

## Pointers

- Up: [../map.md](../map.md)
- Ledger: [../../../../task/ledgers/staging/perf-cast-1-ledger.md](../../../../task/ledgers/staging/perf-cast-1-ledger.md)
- Pins: [../../tests/cast/map.md](../../tests/cast/map.md)
