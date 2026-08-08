# map — repark-ml

## Purpose

Native ML estimator kernels (crate-DAG tier 3, a capability leaf with **no internal deps**;
NOTE: "tier-1" wording inside the verbatim-ported sources — `Cargo.toml` description,
`src/lib.rs` doc — is the source repo's M3 *estimator-tier* vocabulary, unrelated to the
crate-DAG tier this map cites):
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
| Wire fit from Python | [`../repark-python/src/map.md`](../repark-python/src/map.md) `ml.rs` (streams batches into accumulators; landed phase-3 PR-3) |
| Facade Estimator API | `python/repark` `ml/` package — **lands phase-3 PR-5**, not in the tree yet |

## Pointers

- Up: [../map.md](../map.md)
- Design: [../../docs/design/python-facade.md](../../docs/design/python-facade.md) (§1 edit
  classes — this crate is EC-7 on **this file only**, every `.rs` / `Cargo.toml` verbatim; §2.1
  tier row; §3 EC-6 second rider — the four `docs/ml-design.md` dead pointers, **DISCHARGED in
  PR-3**: `Cargo.toml:6`, `src/lib.rs:3`, `src/logistic_regression.rs:199` and the user-visible
  `src/error.rs` `Singular` `#[error(...)]` string now name the in-repo authority
  (`docs/design/python-facade.md` §4 Q3); the new message is pinned by
  `error::tests::singular_message_points_at_the_in_repo_ml_authority`, the crate's only
  post-PR-2 census addition; §4 Q3 in-scope ruling)
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
