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

## Path expressions

The move put every file here one level below the old flat directory. These files had a
path expression repointed in the same unit and no other line changed:

| file | change |
|---|---|
| `test_ml_boost_oracle.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |
| `test_ml_estimators_oracle.py` | repository-relative anchors gain one level (`parents[N]` to `parents[N+1]`). |

## Pointers

- Up: [../map.md](../map.md)
