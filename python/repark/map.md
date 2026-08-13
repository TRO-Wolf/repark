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

**Layout note (dated 2026-08-08):** `python/repark` is the *facade* package until the
post-milestone-one re-home designed in `docs/design/python-facade.md` §4 Q1 (`repark.spark`
becomes the facade; `repark.sql` becomes a callable). The target maps in `AGENTS.md` /
`PROJECT.md` describe that end state, not this tree.

## Contents

- `pyproject.toml` — maturin build backend; `manifest-path = ../../crates/repark-python/Cargo.toml`;
  `module-name = repark._native`; `python-source = src`; `features = ["extension-module"]`.
  Runtime dep is exactly one (`pyarrow>=25`); `numpy` / `pandas` / `polars` / `ml-ext` are lazy
  extras. Version is `0.0.0` until the release PR makes it `dynamic` (design §4 Q6).
- `README.md` — package front door (the `readme` referenced by `pyproject.toml`).
- `src/` — the maturin `python-source` root; see [src/map.md](src/map.md).
- `src/repark/` — the importable package, **53 modules**; see
  [src/repark/map.md](src/repark/map.md). Top level: `__init__.py`, `catalog.py`, `column.py`,
  `_csv_smart.py`, `errors.py`, `functions.py`, `_idents.py`, `merge.py`, `polars.py`, `row.py`,
  `_secrets.py`, `storage.py`, `ta.py`, `types.py`, `udtf.py`, `window.py`, `py.typed`.
  Sub-packages:
  - `session/` — the r26 region split of the old `session.py` (`__init__.py` `_wire()` loop,
    `session_core.py`, `builder_conf.py`, `catalog.py`, `create_dataframe.py`, `_funcs.py`,
    `reader.py`, `sql_udf.py`). Import paths are frozen — see the ruff per-file-ignore block.
  - `dataframe/` — the r26 region split of the old `dataframe.py` (`__init__.py`, `core.py`,
    `actions_export.py`, `joins_columns.py`, `writer_readwriter.py`).
  - `sql/` — the `pyspark.sql` alias package (`__init__.py`, `functions.py`, `types.py`,
    `window.py`): identity re-exports plus the loud `_PYSPARK_SQL_ABSENT` list.
  - `ml/` — the PySpark-shaped ML facade (`base.py`, `classification.py`, `clustering.py`,
    `evaluation.py`, `linalg.py`, `param.py`, `pipeline.py`, `regression.py`, `tuning.py`,
    `util.py`, `feature/`, `ext/`) over `crates/repark-ml`; `ext/` holds the lazy delegated
    backends (`_sklearn`, `_xgboost`, `_lightgbm`, `_persist`, `_deps`, `_arrow_util`).
- `tests/` — **134 `test_*.py` files** (plus committed `_record_*` drivers), the facade
  suite, ported minus the empirically generated deferral
  ledger ([../../task/port/deferred-python-tests.txt](../../task/port/deferred-python-tests.txt),
  EC-4). See [tests/map.md](tests/map.md). This suite is the full-extras facade census cohort
  (design §6.3) and is run against the **installed wheel**, never a source tree.

## I want to...

| ...do this | go to |
|---|---|
| Add/adjust a session method | `src/repark/session/session_core.py` (+ the right region module) |
| Adjust builder / conf handling | `src/repark/session/builder_conf.py` |
| Wire a reader (`spark.read.*`) | `src/repark/session/reader.py` |
| Add/adjust a DataFrame action or interchange export | `src/repark/dataframe/actions_export.py` |
| Add/adjust joins or column projection | `src/repark/dataframe/joins_columns.py` |
| Add/adjust V2 `writeTo` / path writes | `src/repark/dataframe/writer_readwriter.py` |
| Add/adjust a Spark function | `src/repark/functions.py` (+ `src/repark/sql/functions.py` alias) |
| Add/adjust an ML transformer or estimator | [src/repark/ml/map.md](src/repark/ml/map.md) |
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
| Ruff reds on `import *` / `F821` / `E402` under `session/` or `dataframe/` | The root `pyproject.toml` per-file-ignore blocks are load-bearing (design §2.3); do not "clean up" the region splits |
| Conflicting engine knobs at `getOrCreate` | Dual spellings must agree; see `session/builder_conf.py` |
| A facade test is missing vs the pin | It is either a declared deferral (`deferred-python-tests.txt`) or a defect — there is no third case |

First checks: rebuild the wheel and reinstall by path. Escalate to: [../map.md#debug](../map.md).
