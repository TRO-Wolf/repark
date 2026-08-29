# map — repark-ml

## Purpose

Native ML estimator kernels (crate-DAG tier 3), with no internal dependencies. The crate provides
hand-rolled Cholesky, streaming OLS, IRLS logistic regression, and Lloyd KMeans with
`initMode="random"`. Models retain parameters only; the PyO3 binder streams Arrow rows, and the
Python facade builds transforms.

## Contents

| Path | Role |
|---|---|
| `Cargo.toml` | Workspace member; `unsafe_code=forbid` via workspace lints; dep: `thiserror` only |
| [src/](src/map.md) | Cholesky, OLS accumulator, IRLS logistic, Lloyd KMeans |

## I want to...

| Task | Go to |
|---|---|
| Change OLS / Cholesky | `src/linear_regression.rs` / `src/cholesky.rs` |
| Change IRLS logistic | `src/logistic_regression.rs` |
| Change KMeans init / Lloyd | `src/kmeans.rs` |
| Wire fit from Python | [`../repark-python/src/map.md`](../repark-python/src/map.md) `ml.rs` |
| Facade Estimator API | `python/repark` `ml/` package |

## Component contract

- **Owns:** native ML estimator kernels — Cholesky + streaming fit accumulators (OLS, IRLS logistic,
  Lloyd k-means with `initMode=random`). Models hold params only.
- **Does not own:** row extraction (the PyO3 binder streams Arrow batches in); the facade Estimator
  API (Python `repark` `ml/`); transform (plan-built on the facade).
- **Public inputs:** streamed feature / label rows (from repark-python `ml.rs`); solver params.
- **Public outputs:** fitted params — never training rows.
- **State & lifecycle:** streaming accumulators; params-only results; no full-row materialization.
- **Allowed internal deps:** **none internal** (a capability leaf). Third-party runtime: `thiserror` only.
- **Failure model:** `thiserror` estimator errors, including `Singular`, `FeatureDimTooLarge`,
  `KMeansInitModeDefault`, `ElasticNetUnsupported`, and `StandardizationUnsupported`.
- **Extension points:** change a solver (`linear_regression.rs` / `cholesky.rs` /
  `logistic_regression.rs` / `kmeans.rs`); fit wiring lives in repark-python `ml.rs`.
- **Test strategy:** `cargo test -p repark-ml` covers solvers and the error-message pin.
- **Known limitations:** `elasticNetParam != 0`, StandardScaler, and non-random KMeans init are
  unsupported; `MAX_FEATURES = 4096`. Pre-existing ML findings are single-homed in
  [src/map.md](src/map.md#known-limitations).

## Pointers

- Up: [../map.md](../map.md)
- Design: [../../docs/design/python-facade.md](../../docs/design/python-facade.md) §4 Q3

## Debug

| Symptom | First check |
|---|---|
| `Singular` on well-conditioned data | Feature collinearity / duplicate columns; check intercept column |
| `FeatureDimTooLarge` | p > 4096 — hard cap in `MAX_FEATURES` |
| `KMeansInitModeDefault` | User left Spark default; require `initMode="random"` |
| `ElasticNetUnsupported` | `elasticNetParam != 0` is unsupported |
| `StandardizationUnsupported` | Fit StandardScaler upstream; raw features only here |
