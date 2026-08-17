# map — examples/

## Purpose

Runnable **examples** of repark in use, for humans reading the engine for the first time.
Examples are illustration, never a gate: every behavior an example shows is pinned by a
test somewhere under `python/repark/tests/` or `python/repark-parity/tests/`, and the
example points at that pin. No gate **executes** this directory today — `make ci` /
`verify` / `preflight` lint it (Ruff covers `.ipynb`) but never run it; execution gating
arrives with the examples-harness workstream.

Examples read and write only under a per-user cache root or a temporary directory; no
example writes data into the checkout, and no example carries credentials, real hosts, or
anything beyond `example.com`.

## Contents

- `notebooks/` — Jupyter notebooks. See [notebooks/map.md](notebooks/map.md).
- `map.md` — this file.

## I want to…

| I want to… | Go to |
|---|---|
| Tour the torture-dataset families through the facade | [notebooks/datasets_tour.ipynb](notebooks/datasets_tour.ipynb) |
| Find the gate that pins what an example shows | the "Where this is pinned" section each example ends with |
| Generate a dataset family without a notebook | [../python/repark-parity/datasets/map.md](../python/repark-parity/datasets/map.md) |
| Build the native module an example needs | [../DEVELOPMENT.md](../DEVELOPMENT.md) (`make develop`) |

## Pointers

- Up: [../map.md](../map.md)
- Related: [../docs/testing.md](../docs/testing.md) — why an example is never a substitute
  for a pin.

## Debug

| Symptom | First check |
|---|---|
| `ModuleNotFoundError: repark._native` | The wheel is not built into the running interpreter — `make develop` |
| `ModuleNotFoundError: repark_datasets` | The example must register the datasets tree by path (see the setup cell); it is not an installed package |
| An example writes into the checkout | It is not using the generator cache root — `python/repark-parity/datasets/_cache.py` refuses in-repo output |

## Constraints

- Zero new Python dependencies; examples run against the built wheel plus the generators.
- No CI wiring from this directory (no `.github/` edits) until the examples-harness
  workstream lands.
- Generated data is never committed.
