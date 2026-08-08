//! Error types for native ML estimators.

use thiserror::Error;

/// ===========================================================================================
/// Estimator / solver failures (loud, never silent regularization).
/// ===========================================================================================
#[derive(Debug, Error, Clone, PartialEq)]
#[non_exhaustive]
pub enum MlError {
    /// Feature dimension exceeds the hard v1 cap (p ≤ [`crate::MAX_FEATURES`]).
    #[error(
        "repark.ml: feature dimension p={actual} exceeds hard limit p≤{limit} \
         (LinearRegression / design-matrix cap; reduce features or wait for a later unit)"
    )]
    FeatureDimTooLarge {
        /// Observed feature count (before intercept column).
        actual: usize,
        /// Hard limit ([`crate::MAX_FEATURES`]).
        limit: usize,
    },

    /// Empty design: zero rows after streaming, or zero-width features without intercept.
    #[error("repark.ml: cannot fit — {0}")]
    EmptyDesign(String),

    /// Feature vector width mismatch within a stream (mixed widths refused).
    #[error(
        "repark.ml: feature width mismatch: expected {expected} columns, got {actual} \
         (mixed dense widths are refused; assemble a fixed-width features column)"
    )]
    FeatureWidthMismatch {
        /// Width established by the first non-null row.
        expected: usize,
        /// Width of the offending row.
        actual: usize,
    },

    /// Null / non-finite feature or label in a training row.
    #[error("repark.ml: non-finite or null {what} at row offset {row_offset}")]
    NonFinite {
        /// `"feature"` or `"label"`.
        what: &'static str,
        /// Stream-relative row index (best-effort).
        row_offset: u64,
    },

    /// Cholesky failed: singular or ill-conditioned `XᵀX` (we refuse; Spark may pinv/regularize).
    #[error(
        "repark.ml: singular or ill-conditioned design matrix (Cholesky failed at pivot {pivot}: \
         {detail}). repark refuses pseudoinverse / silent regularization (divergence vs Spark \
         solver path — see docs/design/python-facade.md §4 Q3)"
    )]
    Singular {
        /// Zero-based pivot index where the diagonal was non-positive / below threshold.
        pivot: usize,
        /// Human-readable detail (e.g. pivot value).
        detail: String,
    },

    /// Elastic net / L1 path not in M3 (seed → M4).
    #[error(
        "repark.ml: elasticNetParam={value} is unsupported in M3 (only elasticNetParam=0 / pure \
         least squares). Seed → M4 for elastic net / coordinate descent"
    )]
    ElasticNetUnsupported {
        /// Requested `elasticNetParam`.
        value: f64,
    },

    /// Feature standardization during fit is unsupported (raw features only in M3).
    #[error(
        "repark.ml: standardization=true is unsupported (raw features only). Fit a \
         StandardScaler stage before the estimator, or set standardization=false"
    )]
    StandardizationUnsupported,

    /// `KMeans` default `initMode` (`k-means||`) is not implemented — user must set random.
    #[error(
        "repark.ml: KMeans initMode default (k-means||) is not implemented. Set \
         initMode=\"random\" explicitly (no fake k-means||). Seeded random + assignment parity \
         up to label permutation when initMode=random"
    )]
    KMeansInitModeDefault,

    /// Unsupported `initMode` string for `KMeans`.
    #[error(
        "repark.ml: KMeans initMode={got:?} is unsupported (only initMode=\"random\" is \
         implemented in M3)"
    )]
    KMeansInitModeUnsupported {
        /// User-provided `initMode`.
        got: String,
    },

    /// Generic illegal argument (k ≤ 0, empty centers, …).
    #[error("repark.ml: {0}")]
    IllegalArgument(String),

    /// Named unsupported / STOP seam (Logistic / stretch surfaces).
    #[error("repark.ml: {0}")]
    Unsupported(String),
}

/// Crate-local result alias.
pub type Result<T> = std::result::Result<T, MlError>;

#[cfg(test)]
mod tests {
    use super::MlError;

    /// EC-6 rider discharge (design §3, second rider; raised as p3b F-2). Four sources named
    /// `docs/ml-design.md`, a v1-only path with no counterpart in this repository — and ONE of
    /// them, this `Singular` variant, is inside an `#[error(...)]` format string, so the dead
    /// pointer is emitted to end users at runtime the moment PR-3 wires the binding. PR-2 kept
    /// all four byte-identical to protect a verbatim/identity port claim and assigned the fix
    /// here; the pointer now names the in-repo ML authority.
    ///
    /// The test pins the message's NEW text — the rider is only discharged if the thing users
    /// read actually changed, and nothing else in this repo would notice a silent revert.
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
        // The diagnostic content around the pointer is unchanged — this is a repoint, not a
        // rewrite: the refusal still names the pivot, the detail, and WHY repark refuses.
        assert!(
            rendered.contains("Cholesky failed at pivot 2")
                && rendered.contains("pivot 3.0e-18 below PIVOT_ABS_EPS")
                && rendered.contains("refuses pseudoinverse / silent regularization"),
            "the repoint must not disturb the diagnostic: {rendered}"
        );
    }
}
