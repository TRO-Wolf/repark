# map — python/repark/src/repark/spark/ml/ext

## Purpose

M4 **delegated ML backends** under `repark.spark.ml.ext` (Q1 re-home, 2026-08-14).
Optional extra `repark[ml-ext]` (xgboost / lightgbm / scikit-learn / numpy / pandas).
Bare `import repark.spark.ml.ext` succeeds without the extra; touching a class
without deps → loud `ImportError` naming `pip install 'repark[ml-ext]'`.

**Rust-rule exception (M4 only):** ext estimators may call an optional external
library's fit on Arrow/pandas frames derived from `to_arrow()`. Peak held data is
the training batch stream the lib requires — not a second repark-owned row cache.
Native (`repark.ml` non-ext) estimators remain under the M3 Rust rule. Library
deps are reachable only behind the optional import and never at `repark.ml` top
level.

**M8 persistence:** every fitted ext model is **save/load XOR pin-refuse** (no
silent third state). Booster-bytes via library OWN non-pickle serialization
(xgboost `save_raw`, lightgbm `model_to_string`); sklearn (only-pickle) refuses
with exact reason `pickle forbidden (arbitrary code execution on load)`. Atomic
write (M7 staging from `repark.ml.pipeline`); library-major version guard on load;
never training rows.

## Contents

| Path | Role |
|---|---|
| `__init__.py` | Lazy `__getattr__` exports; bare import OK; `PICKLE_FORBIDDEN_REASON` / `SKLEARN_SAVE_UNSUPPORTED` / `EXT_SAVE_UNSUPPORTED` |
| `_deps.py` | `require_xgboost` / `require_lightgbm` / `require_sklearn` / numpy / pandas |
| `_persist.py` | **M8** shared envelope helpers: atomic `write_ext_model_tree`, `load_ext_model_envelope`, blob path confinement, library-major version guard, pickle-forbid constants |
| `_arrow_util.py` | Dense feature matrix from Arrow (`MAX_EXT_FEATURES=4096` densify cap on **sparse size/width and nnz** + dense width); null/`len` probes **before** `as_py()` (octo C4-SAF-001); predict re-entry via Arrow MemTable with GC-owned `__repark_ml_ext_*` views |
| `_xgboost.py` | `XGBoostRegressor` / `XGBoostClassifier` + models; **M8** both models booster-bytes save/load (`save_raw` ubj + M1 envelope + atomic + version guard) |
| `_lightgbm.py` | LightGBM twins; **M8** both models `model_to_string` text blob save/load + `_LgbmPredictShell` post-load predict |
| `_sklearn.py` | sklearn `RandomForest*`; **M8** pin-refuse save/write with `PICKLE_FORBIDDEN_REASON` |

## I want to…

| Task | Go to |
|---|---|
| Add a booster wrapper | `_xgboost.py` / `_lightgbm.py` pattern + oracle in `test_ml_boost_oracle.py` |
| Change Arrow densify / re-entry | `_arrow_util.py` |
| Change ImportError wording | `_deps.py` `_ML_EXT_HINT` |
| Change save/load envelope / atomic / version guard | `_persist.py` |
| Optional deps list | `python/repark/pyproject.toml` `[project.optional-dependencies] ml-ext` |

## Pointers

Up: [../map.md](../map.md). Design: [docs/design/python-facade.md](../../../../../../docs/design/python-facade.md) §4 Q3.
Oracle: `python/repark/tests/test_ml_boost_oracle.py`.
Tuning (ParamGrid/CV): [../tuning.py](../tuning.py).

## Debug

- `ImportError` naming `repark[ml-ext]` on class touch → extra not installed (intended).
- `XGBoost*Model.save/load` → M8 booster-bytes (metadata.json + fitted/params.parquet + fitted/booster.raw via `save_raw`); atomic staging; `library_version` major mismatch → `IllegalArgumentException`. Predict-parity pin in `test_ml_boost_oracle.py`.
- `LightGBM*Model.save/load` → M8 `model_to_string` → `fitted/booster.txt`; post-load `_LgbmPredictShell`; same envelope guards.
- sklearn RF `save`/`write` → `UnsupportedOperationException` matching `pickle forbidden (arbitrary code execution on load)` (exact charter string).
- Hostile `booster_blob` with `..` / absolute path → `IllegalArgumentException` (octo M5 C1 via `_persist.safe_booster_blob_path`).
- Hostile task-type relabel (`fitted.classifier` vs reader) or `num_features<=0` → `IllegalArgumentException` (octo M8 C1).
- Missing/0-row `params.parquet` or empty booster blob → loud refuse.
- sklearn RF `load`/`read` pin-refuse same pickle reason as save/write (octo M8 C1).
- PipelineModel + ext stage still STOP-loud (no `_ml_fitted_state`; composition not booster-bytes).
- Feature null / mixed width / sparse size or **dense** width > 4096 → `IllegalArgumentException` from `_arrow_util`.
- Sparse **nnz** > 4096 (or nnz > size) refused before densify / without unbounded `list()` (octo C3-SAF-001).
- Null probe uses Arrow `is_valid`; dense width uses `len`/FixedSizeList `list_size` before `as_py` (octo C4-SAF-001).
- XGBoost*Model wrong feature width on transform → `IllegalArgumentException`.
- predictionCol collision → `AnalysisException` (no silent overwrite).
- Orphaned `__repark_ml_ext_*` after transform GC → success path must `_own_ext_temp_view` (octo C1-SAF-001).
- Lib-direct parity fails → seed / hyperparam mismatch vs oracle kwargs.
- **SQM round 7 (R7-1):** `__repark_ml_ext_*` is minted through
  `repark.spark._temp_views.scratch_view_name` (home-qualified spelling), so the IPC register →
  `SELECT *` pair survives a raw `SET datafusion.catalog.default_catalog`; ownership/drop are
  unchanged and prefix checks go through `local_view_name`.
