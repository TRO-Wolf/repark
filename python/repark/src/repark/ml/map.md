# map — python/repark/src/repark/ml

## Purpose

Near-drop-in `pyspark.ml` surface under `repark.ml` only (no `pyspark.ml` alias shim
tonight). Pipeline skeleton (M1), feature transformers (M2), native estimators (M3),
delegated backends + tuning (M4).
**Rust rule:** feature `fit` = session queries; estimator `fit` = multi-pass Rust Arrow
stream (params only); `transform` = plan expressions; Python never trains / never caches
training rows — **except** M4 `repark.ml.ext` (optional external lib on `to_arrow()`;
see [docs/design/python-facade.md](../../../../../docs/design/python-facade.md) §4 Q3).

## Contents

- **r23 QI1:** `clustering` / `classification` / `regression` / `evaluation` / `base` / `tuning` import `repark._idents.quote_ident` (always-quote SSOT; no local always-quote bodies).
| Path | Role |
|---|---|
| `__init__.py` | Public re-exports (`Pipeline`, `Estimator`, `Vectors`, mixins, `ParamGridBuilder`, `CrossValidator`, …) |
| `base.py` | `Estimator` / `Transformer` / `Model` / `UnaryTransformer`; Repark DataFrame gate; dense feature-width pre-check (`_require_dense_feature_width`, octo C3); outputCol collision refuse (octo C6) |
| `param.py` | `Param` / `Params` / `TypeConverters` + mixins (`HasInputCol`, `HasFeaturesCol`, …) |
| `linalg.py` | `DenseVector` / `SparseVector` / `Vectors` / `VectorUDT`; re-exports `ArrayType` from `repark.types` (X2; local twin removed) |
| `util.py` | `Identifiable`, uid `ClassName_<8hex>`, ML read/write bases |
| `feature/` | M2 + Q1 feature transformers ([feature/map.md](feature/map.md)) — OHE plural IO (M4), quantile family, RegexTokenizer, CV/IDF |
| `regression.py` | M3 `LinearRegression` / `LinearRegressionModel` (native OLS) |
| `classification.py` | M3 `LogisticRegression` / model (IRLS); deep `copy()` |
| `clustering.py` | M3 `KMeans` / model (Lloyd; initMode=random required); deep `copy()` |
| `evaluation.py` | M3/M5/M6/M7 evaluators (RMSE/accuracy; **areaUnderROC** Mann-Whitney; **areaUnderPR** average precision plan; dense list/array rawPrediction → `array_element(..., 1)`; **M7 sparse** `{size,indices,values}` → `element_at(values, array_position(indices, 1))` with missing→0.0 / null-cell+null-size+size&lt;2 → NULL (not densify-to-0); non-vector Map/String refuse with `AUC_VECTOR_RAW_GAP` (octo M7 C3); score projected to scalar before ORDER BY; multiclass f1 loud; empty-dataset refuse; R2 `isLargerBetter` case-insensitive) |
| `tuning.py` | M4/M6 `ParamGridBuilder` / `CrossValidator` / `CrossValidatorModel` (fold labels **materialized** once; NaN fold refuse; **M6 `parallelism`** thread-pool over independent fold×paramMap fits with main-thread metric sum for determinism) |
| `pipeline.py` | `Pipeline` / `PipelineModel` + repark-ml v1 save/load (stage path sanitize + `repark.ml` import allowlist, octo C2-SEC-001/002); **M7 atomic save** (staging sibling + rename; no rmtree-before-write — closes M4 C2-SAF-001; aside cleaned on concurrent-publish fail — octo M7 C1; file-target overwrite — octo M7 C5) |
| `ext/` | M4 delegated backends + **M8 save/load-or-pin-refuse** ([ext/map.md](ext/map.md)) — XGBoost/LightGBM booster-bytes; sklearn pickle-forbid; behind `repark[ml-ext]` |

## I want to…

| Task | Go to |
|---|---|
| Add a feature transformer | `feature/` (M2/Q1) + oracle in `python/repark/tests/test_ml_feature_oracle.py` |
| Quantile / percentile wiring | facade `functions.percentile_approx` + SQL aliases; transformers use `approx_percentile_cont` |
| Add / fix LinearRegression | `regression.py` + `crates/repark-ml` + `crates/repark-python/src/ml.rs` |
| Add evaluator metric | `evaluation.py` |
| Param grid / cross-validation | `tuning.py` + `test_ml_boost_oracle.py` |
| Delegated booster (XGBoost/…) | `ext/` + optional extra `repark[ml-ext]` |
| Change vector Arrow layout | `linalg.py` (layout home) + session `createDataFrame` hooks in `session/_funcs.py` |
| Change persistence layout | `pipeline.py` constants + greylight Q9 pins in `test_ml_skeleton_oracle.py` |
| Param / explainParams semantics | `param.py` |

## Pointers

Up: [../map.md](../map.md). Phase-3 scope of `repark-ml` (crate + facade package):
[docs/design/python-facade.md](../../../../../docs/design/python-facade.md) §4 Q3.
Layout / fit-rule / divergence facts live in-module (`linalg.py`, `base.py`,
`regression.py`, …) — not in that design record.
Oracles: `test_ml_skeleton_oracle.py`, `test_ml_feature_oracle.py`, `test_ml_estimators_oracle.py`,
`test_ml_boost_oracle.py` (M4).
Campaign: [phase-3-python-facade.md](../../../../../docs/history/port-v2/phase-3-python-facade.md).

## Debug

- `fit` / `transform` reject pandas/pyspark frames → intended (`base._require_repark_dataframe`).
- Persistence missing `metadata.json` or wrong `format` → `IllegalArgumentException`.
- Vector createDataFrame mixed widths → `AnalysisException` (fixed-width dense only;
  message cites `repark.ml.linalg`; pin `test_mixed_dense_widths_loud`).
- Stage load fails `_ml_from_save` → stage not registered for persistence yet.
- `LinearRegression` singular → intended loud refuse (no pinv); Spark may still fit.
- `KMeans` default initMode → set `initMode="random"` (no fake k-means||).
- `areaUnderROC` → Mann-Whitney (window RANK + aggregate) on scalar score col; non-0/1 labels refuse loud; dense list/array rawPrediction extracts index 1 (M6); **M7 sparse** struct extract via array_position/element_at (missing index → 0.0); size&lt;2 / short dense → loud length message (not degenerate-labels).
- `areaUnderPR` → M6 plan-built **score-group** average precision (distinct scores high→low; `n_pos_s · precision_after_group / n_pos`; order-independent under ties — octo M6 C3); same binary/degenerate guards as ROC; short dense/sparse extract → loud length message (octo M6 C4 / M7).
- `PipelineModel.save` overwrite is **atomic** (write staging → rename; rmtree old only after success; failed publish after move-aside cleans aside when target reoccupied — octo M7 C1).
- Native estimator fit on sparse `{size,indices,values}` → loud densify/sparseOutput boundary (octo M7 C2; not hollow "unsupported Struct").
- `CrossValidator.parallelism` → M6 thread-pool; sequential when 1; avgMetrics must match any parallelism; ctor `<1` refuses (octo M6 C1).
- Model `copy()` must deep-copy coefficients/centers (octo C2 — Params.copy is shallow).
- `maxIter=0` → zero optimization steps; `num_rows` still counted (octo C1/C2).
- Transform width uses `len(coefficients)` / center width; desynced `num_features` is loud (octo C4).
- `import repark.ml.ext` OK bare; class touch without extra → `ImportError` naming `repark[ml-ext]`.
- Ext model `.save` / `.write().save` → `UnsupportedOperationException` ("save not supported for ext estimators").
- `PipelineModel.save` with ext stage → STOP-loud (no hollow empty fitted parquet; octo C1-Q-001).
- Multiclass default `f1` → loud unsupported (`MULTICLASS_F1_SEED`); use `metricName="accuracy"`.
- CV fold SQL fails hash → falls back to sequential `ROW_NUMBER` modulo; fold frame always `materialize_as_temp_view` **and** subsequent SQL reads `__repark_cv_mat_*` (not call-only; octo C4-Q-001).
- `RegressionEvaluator.isLargerBetter` is case-insensitive (`R2` ≡ `r2`); must match `evaluate().lower()` (octo C2-L-001).
- CV fold NaN (e.g. R2 on constant-label fold) → loud `IllegalArgumentException`, never silent `bestModel=param_maps[0]` (octo C2-L-002).
- Model `copy(extra)` must apply Param overrides (LR/Logit/XGBRegressor/CV→bestModel) so `transform(df, params)` honors `predictionCol` (octo C4-L-002).
- Hostile `relative_path` / stage uid with `..` → `IllegalArgumentException` (octo C2-SEC-001); stage `class` outside `repark.ml.*` or under `repark.ml.ext` → load refuse (octo C2-SEC-002).
- M7 format/lint gate clean (ruff format + py-lint).
