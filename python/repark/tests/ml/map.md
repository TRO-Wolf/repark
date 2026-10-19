# map — python/repark/tests/ml

## Purpose

`repark.spark.ml`: params, pipelines, persistence, feature transformers, estimators, cross-validation and the boosting extension.

Part of the FACADE-TESTS-LAYOUT-1 (2026-10-19) layout; the directory rules live in
[../map.md](../map.md) "Layout". A new test goes in the directory of the subsystem it
pins.

## Contents

- [test_ml_boost_oracle.py](test_ml_boost_oracle.py) — M4 R-ML-BOOST oracles — XGBoostRegressor + ParamGrid/CV + OHE plural + ext gates.
- [test_ml_cv_folds.py](test_ml_cv_folds.py) — CrossValidator fold-degeneracy fallback pins.
- [test_ml_estimators_oracle.py](test_ml_estimators_oracle.py) — M3 R-ML-ESTIMATORS oracles — LinearRegression + evaluators + divergence pins.
- [test_ml_feature_oracle.py](test_ml_feature_oracle.py) — R-ML-FEATURE oracles — plan-built transformers + pipeline e2e.
- [test_ml_skeleton_oracle.py](test_ml_skeleton_oracle.py) — R-ML-SKELETON oracles — Param / Pipeline / uid / persistence / vectors.

## Pointers

- Up: [../map.md](../map.md)
