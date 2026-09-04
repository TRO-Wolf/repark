# map — python/

## Purpose

The Python tree: the uv workspace members. Compute lives in Rust; data crosses the PyO3 boundary
as Apache Arrow. This directory is a container — every package under it is a uv workspace member
declared in the root [`pyproject.toml`](../pyproject.toml), sharing one `uv.lock`.

CC-2 audits every tracked Python comment and docstring after the Rust crates finish. Public
docstrings keep their callable and PySpark contracts in the shortest complete form.
pins: comment-condensation-2/C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010, C-011.

## Contents

- [repark/](repark/map.md) — the wheel: maturin backend over `crates/repark-python`.
  Q1 re-home (2026-08-14): facade at `repark.spark`, alias at `repark.spark.sql`,
  `repark.sql()` the ANSI callable. Landed in phase-3 PR-5; it is the full-extras
  facade census cohort (`docs/design/python-facade.md` §6.3).
- [repark-parity/](repark-parity/map.md) — the Spark-parity differential harness (pure-pyarrow
  comparison core, no Spark and no JVM), the `compat/` PySpark-suite census machinery **including
  the report comparator that is the port's acceptance gate**, `bench/` (TPC-H / TPC-DS / write
  / fuzz / W-0 window-shape measurement scripts), and `datasets/` (torture-dataset generators; cache-root
  outputs). Landed in phase-3 PR-4; datasets arrive with conductor-18.

- [dbt-repark/](dbt-repark/map.md) — the dbt adapter (DBT-1, 2026-09-04): dbt's compiled SQL runs
  in process through `repark.sql()`. Its own `pyproject.toml`; **not** a uv workspace member and
  not in `uv.lock`, because nothing in `make ci` imports it and a member would pull `dbt-core`
  into the repo lock. Route, refusals and acceptance:
  [dbt-repark/map.md](dbt-repark/map.md).

## I want to...

| ...do this | go to |
|---|---|
| Build + run the facade wheel suite | [repark/map.md](repark/map.md) + `docs/design/python-facade.md` §6.3 |
| Run the parity harness | `PYTHONPATH=python/repark-parity/src pytest python/repark-parity/tests -q` |
| Compare two census reports (the acceptance gate) | [repark-parity/compat/map.md](repark-parity/compat/map.md) |
| Read the recorded census procedure | [../docs/port/census.md](../docs/port/census.md) |
| Understand what lands when in the Python tree | [../docs/design/python-facade.md](../docs/design/python-facade.md) §2.3, §9 |

## Pointers

- Up: [../map.md](../map.md)
- Related: the workspace root is [../pyproject.toml](../pyproject.toml) (virtual root — members,
  the `dev` dependency group, and the Ruff config); `uv.lock` is checked in from phase 3 on.

## Debug

| Symptom | First check |
|---|---|
| `uv lock` fails naming a missing member | The member list may only name directories that exist |
| `ModuleNotFoundError: repark_parity` | `PYTHONPATH` must include `python/repark-parity/src` (and `python/repark-parity` for `compat.*`) |

First checks: `uv lock --locked`, then the package's own `map.md`. Escalate to:
[../map.md#debug](../map.md).
