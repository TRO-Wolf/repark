# map — python/repark-parity/bench/windows

## Purpose

**W-0 window-shape measurement** — sliding frames per aggregate class, constant
frame, unpartitioned `ORDER BY` at 1e7 rows, `lead`/`lag` over an unsorted
Iceberg scan, and a window over `memory_limit`. Two oracles: DuckDB 1.5.5 and
PySpark 4.1.2. Measurement only: nothing here changes engine behaviour.

Local filesystem and a memory catalog. Never AWS. The generator is checked in;
the data it makes never is. Scratch is deleted after a run unless `--keep-scratch`.

## Contents

- `roster.py` — Spark 4.1.2 probe roster (the C-002 / C-009 finite domain), SQL
  shapes, retractable-class names, scale constants (`FULL_UNPARTITIONED_ROWS = 1e7`).
- `datagen.py` — seeded Arrow/parquet generator (`id`, `ts`, `v`, `vi`, `v2`, `part`).
- `hardware.py` — machine-profile snapshot (cpu, cores, governor, ram).
- `models.py` — pydantic `ProbeRow` / `CellTiming` / `CellResult` / `RunResult`.
- `classify.py` — outcome classes (`ok` / `refuse` / `absent` / `oom` / `spill` /
  `error` / `crash` / `skip`) and `WIN-SLIDE-<name>` heading helper.
- `oracles.py` — DuckDB and PySpark 4.1.2 adapters (lazy imports).
- `measure.py` — RePark driver (native module). Not imported by `make py-test`.
- `report.py` — markdown renderer for the dated results document.
- `run_w0.py` — one CLI entry point.
- `requirements.txt` — `duckdb==1.5.5` and `pyspark==4.1.2` (workspace already
  pins DuckDB in the root dev group; PySpark is the parity `record` extra).
- `__init__.py` — package marker.
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Run the gate-scale battery | `run_w0.py --scale gate --scratch <dir> --out <json>` |
| Run the charter 1e7 unpartitioned cell | `run_w0.py --scale full --scratch <dir> --out <json> --report task/window-bench-report-2026-08-31.md` |
| Read the numbers | [../../../../task/window-bench-report-2026-08-31.md](../../../../task/window-bench-report-2026-08-31.md) |
| Pin the machinery without the native module | `python/repark-parity/tests/test_w0_window_bench.py` |

## Constraints

- Measure only. An engine defect this harness surfaces is a recorded outcome
  plus a registry row when it is a sliding-frame refuse, never a fix here.
- Never commit a scratch tree, a warehouse, or a result JSON.
- Wall-clock is one machine's number. Ratios are the deliverable.
