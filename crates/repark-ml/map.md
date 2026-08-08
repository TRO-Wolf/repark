# map — repark-ml

## Purpose

Native ML estimator kernels (crate-DAG tier 3, a capability leaf with **no internal deps**):
hand-rolled Cholesky + streaming fit accumulators for LinearRegression (must-land),
LogisticRegression (IRLS), and KMeans (Lloyd, initMode=random only). **Zero new crates.io deps**
(runtime: `thiserror` only). Models hold params only — never training rows. Fit streams
Arrow-extracted rows from the PyO3 binder; transform stays plan-built on the Python facade.
Ported verbatim from the v1 engine at the phase-3 port pin — see
[docs/design/python-facade.md](../../docs/design/python-facade.md) §4 Q3.

## Contents

| Path | Role |
|---|---|
| `Cargo.toml` | Workspace member; `unsafe_code=forbid` via workspace lints; dep: `thiserror` only |
| [src/](src/map.md) | Cholesky, OLS accumulator, IRLS logistic, Lloyd k-means |

## I want to...

| Task | Go to |
|---|---|
| Change OLS / Cholesky | `src/linear_regression.rs` / `src/cholesky.rs` |
| Change IRLS logistic | `src/logistic_regression.rs` |
| Change KMeans init / Lloyd | `src/kmeans.rs` |
| Wire fit from Python | `crates/repark-python` `ml` module (streams batches into accumulators) — **lands phase-3 PR-3**, not in the tree yet |
| Facade Estimator API | `python/repark` `ml/` package — **lands phase-3 PR-5**, not in the tree yet |

## Pointers

- Up: [../map.md](../map.md)
- Design: [../../docs/design/python-facade.md](../../docs/design/python-facade.md) (§1 edit
  classes — this crate is `none (verbatim)`; §2.1 tier row; §4 Q3 in-scope ruling)
- Brief: [../../briefs/phase-3-python-facade.md](../../briefs/phase-3-python-facade.md) §1 "PR-2"
- Ledger: [../../task/p3b-ml-ledger.md](../../task/p3b-ml-ledger.md)

## Debug

| Symptom | First check |
|---|---|
| `Singular` on well-conditioned data | Feature collinearity / duplicate columns; check intercept column |
| `FeatureDimTooLarge` | p > 4096 — hard cap in `MAX_FEATURES` |
| `KMeansInitModeDefault` | User left Spark default; require `initMode="random"` |
| `ElasticNetUnsupported` | `elasticNetParam != 0` is M4 |
| `StandardizationUnsupported` | Fit StandardScaler upstream; raw features only here |
