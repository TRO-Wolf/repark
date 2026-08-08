//! Native tier-1 ML estimators for repark (M3).
//!
//! ## Design (docs/ml-design.md)
//!
//! * **Fit** may be multi-pass / multi-iter **Rust scans over Arrow batches** via the session
//!   stream. Peak held state is `O(p²)` (normal equations / IRLS weights) or `O(k·p)` (centers)
//!   — **never** the full training matrix in Rust or Python.
//! * **Models hold params only** (coefficients, intercept, centers). No cached training rows.
//! * **Transform** remains plan-built on the Python facade (expression projection).
//! * **Zero new crates.io deps** — hand-rolled Cholesky; no ndarray / nalgebra / linfa.
//!
//! ## Surface
//!
//! * [`linear_regression`] — streaming OLS (`XᵀX` / `Xᵀy` + Cholesky). **Must-land.**
//! * [`logistic_regression`] — IRLS reusing Cholesky.
//! * [`kmeans`] — Lloyd; default `initMode` errors (must set `initMode="random"`).
//!
//! The PyO3 binder in `repark-python` streams DataFusion batches into these accumulators.

#![forbid(unsafe_code)]

pub mod cholesky;
pub mod error;
pub mod kmeans;
pub mod linear_regression;
pub mod logistic_regression;

pub use error::{MlError, Result};
pub use kmeans::{
    KMeansPass, KMeansSolution, XorShift64, fit_kmeans_lloyd, nearest_center,
    random_center_indices, validate_init_mode,
};
pub use linear_regression::{
    LinearRegressionAccumulator, LinearRegressionSolution, validate_linear_regression_params,
};
pub use logistic_regression::{
    DEFAULT_MAX_ITER as LOGISTIC_DEFAULT_MAX_ITER, DEFAULT_TOL as LOGISTIC_DEFAULT_TOL,
    LogisticIrlsAccumulator, LogisticRegressionSolution, fit_logistic_irls, predict_probability,
};

/// Hard feature-dimension cap for design matrices (p ≤ 4096). Loud error naming this limit.
pub const MAX_FEATURES: usize = 4096;
