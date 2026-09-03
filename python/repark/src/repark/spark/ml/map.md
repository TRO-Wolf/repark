# map — python/repark/src/repark/spark/ml

## Purpose

This directory owns the direct Spark ML facade modules. Feature transformers and optional
delegated estimators live in [feature/map.md](feature/map.md) and [ext/map.md](ext/map.md).

## Modules

| Path | Role |
|---|---|
| `__init__.py` | Public ML exports. |
| `base.py` | Estimator, transformer, model, and frame validation contracts. |
| `classification.py` | Native binary logistic regression and model. |
| `clustering.py` | Native Lloyd k-means and model. |
| `evaluation.py` | Regression, binary, and multiclass evaluators. **FN-FIX-1:** sparse
  `array_position` not-found is `0`; skip `element_at(..., 0)`. pins: fn-fix-1-registry-rows/C-002 |
| `linalg.py` | Dense and sparse vectors and Arrow schema markers. |
| `param.py` | Parameters, converters, and shared parameter mixins. |
| `pipeline.py` | Pipeline composition and atomic persistence. |
| `regression.py` | Native ordinary least squares and model. |
| `tuning.py` | Parameter grids and deterministic cross-validation. |
| `util.py` | Uids and persistence interfaces. |

## Contracts and limitations

- Fits use session queries or native Rust streams. Models store parameters and fitted metadata,
  never training rows. Transforms remain lazy plan expressions.
- Dense vectors require one fixed width per column. Sparse vectors use `{size, indices, values}`.
- Native estimators refuse sparse feature structs and invalid dense widths.
- K-means requires `initMode="random"`; the Spark default is refused.
- Binary AUC rejects non-binary labels. Binary and multiclass accuracy accept comparable labels.
- Pipeline persistence allowlists `repark.spark.ml` stages. Directory targets use staging and
  move-aside replacement with best-effort restoration; existing files are unlinked before rename.
- `LinearRegressionSummary` exposes no computed metrics and refuses unknown fields.

## Pointers

- Parent package: [../map.md](../map.md)
- Feature transformers: [feature/map.md](feature/map.md)
- Delegated estimators: [ext/map.md](ext/map.md)
- Tests: [../../../../tests/map.md](../../../../tests/map.md)
- Design: [../../../../../../docs/design/python-facade.md](../../../../../../docs/design/python-facade.md)
