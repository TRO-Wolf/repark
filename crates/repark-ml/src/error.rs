//! Error types for native ML estimators.

use thiserror::Error;

/// ===========================================================================================
/// Estimator and solver failures; singular systems never receive silent regularization.
/// ===========================================================================================
#[derive(Debug, Error, Clone, PartialEq)]
#[non_exhaustive]
pub enum MlError {
    /// Feature dimension exceeds [`crate::MAX_FEATURES`].
    #[error(
        "repark.ml: feature dimension p={actual} exceeds hard limit p≤{limit} \
         (LinearRegression / design-matrix cap; reduce features or wait for a later unit)"
    )]
    FeatureDimTooLarge {
        /// Observed feature count.
        actual: usize,
        /// Hard feature limit.
        limit: usize,
    },

    /// The stream is empty, or the design has no columns.
    #[error("repark.ml: cannot fit — {0}")]
    EmptyDesign(String),

    /// A stream row has a different feature width.
    #[error(
        "repark.ml: feature width mismatch: expected {expected} columns, got {actual} \
         (mixed dense widths are refused; assemble a fixed-width features column)"
    )]
    FeatureWidthMismatch {
        /// Expected width.
        expected: usize,
        /// Observed width.
        actual: usize,
    },

    /// A training feature or label is null or non-finite.
    #[error("repark.ml: non-finite or null {what} at row offset {row_offset}")]
    NonFinite {
        /// Field kind.
        what: &'static str,
        /// Stream-relative row offset.
        row_offset: u64,
    },

    /// Cholesky rejected a singular or ill-conditioned normal matrix.
    #[error(
        "repark.ml: singular or ill-conditioned design matrix (Cholesky failed at pivot {pivot}: \
         {detail}). repark refuses pseudoinverse / silent regularization (divergence vs Spark \
         solver path — see docs/design/python-facade.md §4 Q3)"
    )]
    Singular {
        /// Failing pivot.
        pivot: usize,
        /// Pivot detail.
        detail: String,
    },

    /// Elastic net is unsupported.
    #[error(
        "repark.ml: elasticNetParam={value} is unsupported in M3 (only elasticNetParam=0 / pure \
         least squares). Seed → M4 for elastic net / coordinate descent"
    )]
    ElasticNetUnsupported {
        /// Requested value.
        value: f64,
    },

    /// Feature standardization is unsupported during fit.
    #[error(
        "repark.ml: standardization=true is unsupported (raw features only). Fit a \
         StandardScaler stage before the estimator, or set standardization=false"
    )]
    StandardizationUnsupported,

    /// KMeans default `initMode` (`k-means||`) is unsupported; use `random`.
    #[error(
        "repark.ml: KMeans initMode default (k-means||) is not implemented. Set \
         initMode=\"random\" explicitly (no fake k-means||). Seeded random + assignment parity \
         up to label permutation when initMode=random"
    )]
    KMeansInitModeDefault,

    /// A KMeans `initMode` string is unsupported.
    #[error(
        "repark.ml: KMeans initMode={got:?} is unsupported (only initMode=\"random\" is \
         implemented in M3)"
    )]
    KMeansInitModeUnsupported {
        /// Requested mode.
        got: String,
    },

    /// A solver argument is invalid.
    #[error("repark.ml: {0}")]
    IllegalArgument(String),

    /// A named estimator capability is unsupported.
    #[error("repark.ml: {0}")]
    Unsupported(String),
}

/// Crate-local result alias.
pub type Result<T> = std::result::Result<T, MlError>;

#[cfg(test)]
mod tests {
    use super::MlError;

    #[test]
    fn singular_message_points_at_the_in_repo_ml_authority() {
        let rendered = MlError::Singular {
            pivot: 2,
            detail: "pivot 3.0e-18 below PIVOT_ABS_EPS".to_string(),
        }
        .to_string();

        assert!(
            rendered.contains("docs/design/python-facade.md §4 Q3"),
            "the user-visible message must point at the in-repo ML authority: {rendered}"
        );
        assert!(
            !rendered.contains("ml-design.md"),
            "the v1-only docs/ml-design.md path must not survive in a user-visible string: \
             {rendered}"
        );
        // Keep the pivot, detail, and refusal reason stable.
        assert!(
            rendered.contains("Cholesky failed at pivot 2")
                && rendered.contains("pivot 3.0e-18 below PIVOT_ABS_EPS")
                && rendered.contains("refuses pseudoinverse / silent regularization"),
            "the repoint must not disturb the diagnostic: {rendered}"
        );
    }
}
