# Delegated ML backends

## Purpose

`repark.spark.ml.ext` provides optional XGBoost, LightGBM, and scikit-learn
estimators. Package import is dependency-free. Touching a backend without
`repark[ml-ext]` raises an `ImportError` naming that extra.

These estimators may fit external libraries on Arrow-derived batches. Fitted
models retain model state and metadata, not training rows. Native ML estimators
remain in the Rust-backed modules.

## Modules

| Path | Role |
|---|---|
| `__init__.py` | Lazy public exports. |
| `_deps.py` | Optional dependency guards. |
| `_arrow_util.py` | Dense matrices, labels, prediction re-entry, and scratch-view ownership. |
| `_xgboost.py` | XGBoost regressors and classifiers with native booster-byte persistence. |
| `_lightgbm.py` | LightGBM regressors and classifiers with native text persistence. |
| `_sklearn.py` | RandomForest regressors and classifiers with explicit pickle refusal. |
| `_persist.py` | Envelope validation, atomic writes, version checks, path confinement, and refusal constants. |

## Contracts and known limitations

- Features must be non-null, dense, and one fixed width per input column.
- Sparse structs are densified only when size, width, and nonzero counts are at most 4096.
- Nullness and widths are checked before Arrow scalar materialization.
- Predictions must have one value per input row. Existing prediction columns are refused.
- Prediction re-entry uses an Arrow IPC MemTable. The returned frame owns scratch-view cleanup.
- XGBoost uses `save_raw`; LightGBM uses `model_to_string`. Both check the library major version.
- RandomForest save, write, read, and load refuse because pickle loading permits arbitrary code execution.
- Model envelopes require a positive feature count, a confined booster path, and a non-empty blob.

## Pointers

- Parent: [../map.md](../map.md)
- Feature transformers: [../feature/map.md](../feature/map.md)
- Tests: [../../../../../tests/map.md](../../../../../tests/map.md)
- Design: [../../../../../../../docs/design/python-facade.md](../../../../../../../docs/design/python-facade.md)
