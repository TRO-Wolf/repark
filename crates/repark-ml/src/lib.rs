//! Native ML estimator kernels for repark.
//! The PyO3 binder streams Arrow batches; fits retain only `O(p²)` normal equations or `O(k·p)`
//! centers, never training rows. Models hold parameters; the Python facade builds transforms.

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
