# map — python/repark

## Purpose

The `pip install repark` package: a near-drop-in PySpark facade (`from repark import
ReparkSession`, with `SparkSession` and `ReParkSession` kept as source-compatible aliases of the
same object) over the `repark._native` PyO3 module. Built by maturin from the
`crates/repark-python` cdylib; compute runs in Rust, rows cross as Apache Arrow (zero-copy).

Ported verbatim at the phase-3 port pin (`docs/design/python-facade.md` §2.3). This `map.md` is
**regenerated** against the real tree rather than ported: the pin's copy documented a flat
`session.py` / `dataframe.py` layout that the r26 region splits replaced, and omitted eight
modules (EC-7).

**Layout note (dated 2026-08-14):** Q1 re-home landed. `repark.spark` is the facade;
`repark.spark.sql` is the `pyspark`→`repark.spark` alias package; `repark.sql()` is the
ANSI-door callable; `import repark.sql` is not a module. `from repark import ReparkSession`
remains the deprecation shim. Native lazy DataFrame API is not this phase.

## Contents

- `pyproject.toml` — maturin build backend; `manifest-path = ../../crates/repark-python/Cargo.toml`;
  `module-name = repark._native`; `python-source = src`; `features = ["extension-module"]`.
  Runtime dep is exactly one (`pyarrow>=25`); `numpy` / `pandas` / `polars` / `ml-ext` are lazy
  extras. Version is `0.0.0` until the release PR makes it `dynamic` (design §4 Q6).
- `README.md` — package front door (the `readme` referenced by `pyproject.toml`).
- `src/` — the maturin `python-source` root; see [src/map.md](src/map.md).
- `src/repark/` — the importable package; see [src/repark/map.md](src/repark/map.md).
  Top level after Q1: `__init__.py` (`sql()` + shim), `errors.py`, `functions.py` (deprecation
  re-export), `py.typed`. Facade implementation: [src/repark/spark/map.md](src/repark/spark/map.md)
  (`session/`, `dataframe/`, `ml/`, `sql/` alias, functions/types/window, …).
- `tests/` — **134 `test_*.py` files** (plus committed `_record_*` drivers), the facade
  suite, ported minus the empirically generated deferral
  ledger ([../../task/port/deferred-python-tests.txt](../../task/port/deferred-python-tests.txt),
  EC-4). See [tests/map.md](tests/map.md). This suite is the full-extras facade census cohort
  (design §6.3) and is run against the **installed wheel**, never a source tree.

## I want to...

| ...do this | go to |
|---|---|
| Add/adjust a session method | `src/repark/spark/session/session_core.py` (+ the right region module) |
| Adjust builder / conf handling | `src/repark/spark/session/builder_conf.py` |
| Wire a reader (`spark.read.*`) | `src/repark/spark/session/reader.py` |
| Add/adjust a DataFrame action or interchange export | `src/repark/spark/dataframe/actions_export.py` |
| Add/adjust joins or column projection | `src/repark/spark/dataframe/joins_columns.py` |
| Add/adjust V2 `writeTo` / path writes | `src/repark/spark/dataframe/writer_readwriter.py` |
| Add/adjust a Spark function | `src/repark/spark/functions.py` (+ `src/repark/spark/sql/functions.py` alias) |
| Add/adjust an ML transformer or estimator | [src/repark/spark/ml/map.md](src/repark/spark/ml/map.md) |
| Change how the wheel builds | `pyproject.toml` `[tool.maturin]` |
| Build the wheel | `uvx maturin@1.14.1 build --out <dir>` from this directory |
| Run the facade suite against a built wheel | design §6.3 (bare venv outside the workspace, wheel by explicit file path, full extras) |
| See why a test is absent | [../../task/port/deferred-tests.md](../../task/port/deferred-tests.md) |

## Pointers

- Up: [../map.md](../map.md)
- Related: the native module is [../../crates/repark-python/map.md](../../crates/repark-python/map.md);
  the ML kernels are [../../crates/repark-ml/map.md](../../crates/repark-ml/map.md).
- Design: [../../docs/design/python-facade.md](../../docs/design/python-facade.md) (§2.3 the tree
  and the load-bearing ruff ignores; §3 EC-4/EC-7/EC-9; §6.3 the facade cohort).
- Ledger: [p3e-facade-ledger.md](../../docs/history/port-v2/p3e-facade-ledger.md).

## Debug

| Symptom | First check |
|---|---|
| `ModuleNotFoundError: repark._native` | The wheel is not installed (a source checkout alone has no extension module) |
| Maturin can't find the crate | `manifest-path` in `pyproject.toml` points at `crates/repark-python/Cargo.toml` |
| `pip install repark` gets the wrong package | The PyPI name is a reservation — always install the built wheel **by explicit file path** |
| Ruff reds on `import *` / `F821` / `E402` under `spark/session/` or `spark/dataframe/` | The root `pyproject.toml` per-file-ignore blocks are load-bearing (design §2.3); do not "clean up" the region splits |
| Conflicting engine knobs at `getOrCreate` | Dual spellings must agree; see `spark/session/builder_conf.py` |
| `import repark.sql` succeeds | leftover `sql/` or `sql.py` at the package root — must be gone |
| A facade test is missing vs the pin | It is either a declared deferral (`deferred-python-tests.txt`) or a defect — there is no third case |

First checks: rebuild the wheel and reinstall by path. Escalate to: [../map.md#debug](../map.md).
