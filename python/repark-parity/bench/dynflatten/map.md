# map — python/repark-parity/bench/dynflatten

## Purpose

**PERF-DYNFLATTEN-1** measurement harness for `dynamicFlatten`. Generator is
checked in; parquet lands under `/tmp/oc-dynflatten-bed/` (never the repo).
Repark cells run one subprocess each (peak RSS). Spark explode + struct
expansion shares one JVM. Sequential Cartesian expansion is preserved;
DataFusion multi-column Unnest zip/pad is not a substitute.

pins: perf-dynflatten-1-measure/C-001, C-002, C-003, C-004

Docstrings here are one line each: `check_docstring_presence` (D101/D102/D103/D105/D107)
requires one, and nothing may say more. Reasons live in this map, not in the source.

## Contents

- `datagen` lives in [../../datasets/nested/bed.py](../../datasets/nested/bed.py)
  (shapes, seed, refuse-real-dataset).
- `models.py` — pydantic `EngineTiming` / `FixtureResult` / `CandidateShare` /
  `RunResult`.
- `spark_flatten.py` — PySpark 4.1.2 explode + struct expansion matching the
  repark rewrite (structs first, lists one-at-a-time).
- `cell_worker.py` — isolated repark cell (JSON in / JSON out).
- `measure.py` — orchestrator, ranking, markdown renderer.
- `run_dynflatten.py` — CLI (`--scale gate|quick|full --out DIR`).
- `__init__.py` — package marker.
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Generate the bed | `python python/repark-parity/datasets/nested/bed.py --scale gate --out /tmp/oc-dynflatten-bed` |
| Run the measurement | `python python/repark-parity/bench/dynflatten/run_dynflatten.py --scale gate --out /tmp/oc-dynflatten-bed` |
| Read the numbers | [../../../../docs/perf/dynamic-flatten-baseline.md](../../../../docs/perf/dynamic-flatten-baseline.md) |

## Debug

| Symptom | First check |
|---|---|
| `refusing real-dataset` | `--input` / `REPARK_DATASET*` are banned; synthetic only |
| Spark bind fail | one JVM on the box; retry after a minute |
| Walk counts missing in JSON | schema-only counters live in Rust `flatten_stats_*` pins |
| Row-set equal is empty | full equality only at `gate` (≤ 20_000 rows out) |

## Pointers

- Up: [../map.md](../map.md)
- Tests: [../../tests/test_dynflatten_bed.py](../../tests/test_dynflatten_bed.py)
- Live: `python/repark/tests/test_parity_live.py::test_live_dynflatten_matches_spark_explode`
