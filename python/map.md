# map — python/

## Purpose

The Python tree: the uv workspace members. Compute lives in Rust; data crosses the PyO3 boundary
as Apache Arrow. This directory is a container — every package under it is a uv workspace member
declared in the root [`pyproject.toml`](../pyproject.toml), sharing one `uv.lock`.

## Contents

- [repark-parity/](repark-parity/map.md) — the Spark-parity differential harness (pure-pyarrow
  comparison core, no Spark and no JVM), the `compat/` PySpark-suite census machinery **including
  the report comparator that is the port's acceptance gate**, and `bench/` (TPC-H / TPC-DS / write
  / fuzz measurement scripts). Landed in phase-3 PR-4.

Arriving later in phase 3: `repark/` — the PySpark facade wheel (maturin backend over
`crates/repark-python`, `repark.sql` alias package, `repark.ml`). It joins the workspace member
list in the facade PR; the parity package lands **first** because nine facade test files import
it and the wheel smoke step installs it explicitly.

## I want to...

| ...do this | go to |
|---|---|
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
| `uv lock` fails naming a missing member | The member list may only name directories that exist — the facade joins in its own PR |
| `ModuleNotFoundError: repark_parity` | `PYTHONPATH` must include `python/repark-parity/src` (and `python/repark-parity` for `compat.*`) |

First checks: `uv lock --locked`, then the package's own `map.md`. Escalate to:
[../map.md#debug](../map.md).
