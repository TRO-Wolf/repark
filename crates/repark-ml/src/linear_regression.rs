//! Streaming ordinary least squares via normal equations and Cholesky.
//! Accumulates `XᵀX` and `Xᵀy` without retaining rows. An optional intercept is column zero.
//! Singular or ill-conditioned designs fail without pseudoinversion or ridge regularization.

use crate::MAX_FEATURES;
use crate::cholesky::cholesky_factor_and_solve;
use crate::error::{MlError, Result};

/// ===========================================================================================
/// Fitted OLS parameters; training rows are not retained.
/// ===========================================================================================
#[derive(Debug, Clone, PartialEq)]
pub struct LinearRegressionSolution {
    /// Coefficients for the original feature columns.
    pub coefficients: Vec<f64>,
    /// Intercept, or `0.0` when disabled.
    pub intercept: f64,
    pub fit_intercept: bool,
    pub num_features: usize,
    pub num_rows: u64,
}

/// ===========================================================================================
/// Streaming OLS normal-equation accumulator with `O(p²)` state.
/// ===========================================================================================
#[derive(Debug, Clone)]
pub struct LinearRegressionAccumulator {
    num_features: usize,
    fit_intercept: bool,
    design_width: usize,
    xtx: Vec<f64>,
    xty: Vec<f64>,
    num_rows: u64,
    row_offset: u64,
}

impl LinearRegressionAccumulator {
    /// =======================================================================================
    /// Start an accumulator for raw feature columns.
    ///
    /// # Errors
    /// [`MlError::FeatureDimTooLarge`] when `num_features > MAX_FEATURES`.
    /// [`MlError::EmptyDesign`] when `num_features == 0` and `!fit_intercept`.
    /// =======================================================================================
    pub fn new(num_features: usize, fit_intercept: bool) -> Result<Self> {
        if num_features > MAX_FEATURES {
            return Err(MlError::FeatureDimTooLarge {
                actual: num_features,
                limit: MAX_FEATURES,
            });
        }
        if num_features == 0 && !fit_intercept {
            return Err(MlError::EmptyDesign(
                "num_features=0 and fit_intercept=false leaves an empty design".into(),
            ));
        }
        let design_width = num_features + usize::from(fit_intercept);
        Ok(Self {
            num_features,
            fit_intercept,
            design_width,
            xtx: vec![0.0; design_width * design_width],
            xty: vec![0.0; design_width],
            num_rows: 0,
            row_offset: 0,
        })
    }

    /// Feature dimension this accumulator was created for.
    #[must_use]
    pub fn num_features(&self) -> usize {
        self.num_features
    }

    /// Rows successfully observed so far.
    #[must_use]
    pub fn num_rows(&self) -> u64 {
        self.num_rows
    }

    /// =======================================================================================
    /// Observe one dense training row and label.
    ///
    /// # Errors
    /// Width mismatch, non-finite values.
    /// =======================================================================================
    pub fn observe_row(&mut self, features: &[f64], label: f64) -> Result<()> {
        if features.len() != self.num_features {
            return Err(MlError::FeatureWidthMismatch {
                expected: self.num_features,
                actual: features.len(),
            });
        }
        if !label.is_finite() {
            return Err(MlError::NonFinite {
                what: "label",
                row_offset: self.row_offset,
            });
        }
        for &value in features {
            if !value.is_finite() {
                return Err(MlError::NonFinite {
                    what: "feature",
                    row_offset: self.row_offset,
                });
            }
        }

        let width = self.design_width;
        let intercept_offset = usize::from(self.fit_intercept);

        for row in 0..width {
            let x_row = design_entry(features, row, self.fit_intercept, intercept_offset);
            self.xty[row] += label * x_row;
            for col in 0..=row {
                let x_col = design_entry(features, col, self.fit_intercept, intercept_offset);
                let add = x_row * x_col;
                self.xtx[row * width + col] += add;
                if row != col {
                    self.xtx[col * width + row] += add;
                }
            }
        }

        self.num_rows += 1;
        self.row_offset += 1;
        Ok(())
    }

    /// =======================================================================================
    /// Observe row-major dense rows and their labels.
    ///
    /// # Errors
    /// Same as [`Self::observe_row`], plus length mismatch on the flat buffer.
    /// =======================================================================================
    pub fn observe_dense(&mut self, features_flat: &[f64], labels: &[f64]) -> Result<()> {
        if labels.is_empty() {
            return Ok(());
        }
        if features_flat.len() != labels.len() * self.num_features {
            return Err(MlError::IllegalArgument(format!(
                "observe_dense: features_flat len {} != labels {} * p {}",
                features_flat.len(),
                labels.len(),
                self.num_features
            )));
        }
        for (row_index, label) in labels.iter().enumerate() {
            let start = row_index * self.num_features;
            let end = start + self.num_features;
            self.observe_row(&features_flat[start..end], *label)?;
        }
        Ok(())
    }

    /// =======================================================================================
    /// Consume the accumulator and solve its normal equations.
    ///
    /// # Errors
    /// Empty design (zero rows), singular / ill-conditioned `XᵀX`.
    /// =======================================================================================
    pub fn finish(self) -> Result<LinearRegressionSolution> {
        if self.num_rows == 0 {
            return Err(MlError::EmptyDesign(
                "zero training rows observed (empty stream)".into(),
            ));
        }
        if self.num_rows < self.design_width as u64 {
            // Let Cholesky report rank deficiency instead of rejecting by row count.
        }

        let mut xtx = self.xtx;
        let solution = cholesky_factor_and_solve(&mut xtx, self.design_width, &self.xty)?;

        let (intercept, coefficients) = if self.fit_intercept {
            let intercept = solution[0];
            let coefficients = solution[1..].to_vec();
            (intercept, coefficients)
        } else {
            (0.0, solution)
        };

        Ok(LinearRegressionSolution {
            coefficients,
            intercept,
            fit_intercept: self.fit_intercept,
            num_features: self.num_features,
            num_rows: self.num_rows,
        })
    }
}

/// Return a design-matrix entry, including the optional intercept.
#[inline]
fn design_entry(
    features: &[f64],
    index: usize,
    fit_intercept: bool,
    intercept_offset: usize,
) -> f64 {
    if fit_intercept && index == 0 {
        1.0
    } else {
        features[index - intercept_offset]
    }
}

/// ===========================================================================================
/// Validate estimator parameters before streaming.
///
/// # Errors
/// [`MlError::StandardizationUnsupported`] or [`MlError::ElasticNetUnsupported`].
/// ===========================================================================================
pub fn validate_linear_regression_params(
    elastic_net_param: f64,
    standardization: bool,
) -> Result<()> {
    if standardization {
        return Err(MlError::StandardizationUnsupported);
    }
    // Tolerate representation noise around the supported zero value.
    if elastic_net_param.abs() > 1e-15 {
        return Err(MlError::ElasticNetUnsupported {
            value: elastic_net_param,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn perfect_line_with_intercept() {
        let mut acc = LinearRegressionAccumulator::new(1, true).unwrap();
        for x in [0.0, 1.0, 2.0, 3.0, 4.0] {
            acc.observe_row(&[x], 2.0 + 3.0 * x).unwrap();
        }
        let sol = acc.finish().unwrap();
        assert!((sol.intercept - 2.0).abs() < 1e-9);
        assert!((sol.coefficients[0] - 3.0).abs() < 1e-9);
        assert_eq!(sol.num_rows, 5);
    }

    #[test]
    fn multi_feature_well_conditioned() {
        let mut acc = LinearRegressionAccumulator::new(2, true).unwrap();
        let rows = [
            ([1.0, 0.0], 3.0),
            ([0.0, 1.0], 0.5),
            ([1.0, 1.0], 2.5),
            ([2.0, 1.0], 4.5),
            ([1.0, 2.0], 2.0),
            ([3.0, 2.0], 6.0),
        ];
        for (features, label) in rows {
            acc.observe_row(&features, label).unwrap();
        }
        let sol = acc.finish().unwrap();
        assert!(
            (sol.intercept - 1.0).abs() < 1e-8,
            "intercept {}",
            sol.intercept
        );
        assert!(
            (sol.coefficients[0] - 2.0).abs() < 1e-8,
            "c0 {}",
            sol.coefficients[0]
        );
        assert!(
            (sol.coefficients[1] + 0.5).abs() < 1e-8,
            "c1 {}",
            sol.coefficients[1]
        );
    }

    #[test]
    fn no_intercept() {
        let mut acc = LinearRegressionAccumulator::new(1, false).unwrap();
        for x in [1.0, 2.0, 3.0] {
            acc.observe_row(&[x], 2.0 * x).unwrap();
        }
        let sol = acc.finish().unwrap();
        assert!((sol.intercept - 0.0).abs() < 1e-15);
        assert!((sol.coefficients[0] - 2.0).abs() < 1e-9);
    }

    #[test]
    fn singular_duplicate_features_loud() {
        let mut acc = LinearRegressionAccumulator::new(2, true).unwrap();
        for x in [1.0, 2.0, 3.0, 4.0] {
            acc.observe_row(&[x, x], x).unwrap();
        }
        let err = acc.finish().expect_err("singular");
        assert!(matches!(err, MlError::Singular { .. }));
    }

    #[test]
    fn p_cap_loud() {
        let err = LinearRegressionAccumulator::new(MAX_FEATURES + 1, true).unwrap_err();
        assert!(matches!(
            err,
            MlError::FeatureDimTooLarge {
                actual: _,
                limit: MAX_FEATURES
            }
        ));
    }

    #[test]
    fn elastic_net_and_standardization_gates() {
        assert!(matches!(
            validate_linear_regression_params(0.5, false),
            Err(MlError::ElasticNetUnsupported { .. })
        ));
        assert!(matches!(
            validate_linear_regression_params(0.0, true),
            Err(MlError::StandardizationUnsupported)
        ));
        assert!(validate_linear_regression_params(0.0, false).is_ok());
    }

    #[test]
    fn empty_stream_loud() {
        let acc = LinearRegressionAccumulator::new(1, true).unwrap();
        let err = acc.finish().unwrap_err();
        assert!(matches!(err, MlError::EmptyDesign(_)));
    }

    #[test]
    fn observe_dense_batch_matches_row_wise() {
        let data = [
            [1.0_f64, 0.0],
            [0.0, 1.0],
            [1.0, 1.0],
            [2.0, 1.0],
            [1.0, 2.0],
        ];
        let labels = [1.0, 2.0, 3.0, 4.0, 5.0];
        let mut a = LinearRegressionAccumulator::new(2, true).unwrap();
        let mut b = LinearRegressionAccumulator::new(2, true).unwrap();
        for (features, label) in data.iter().zip(labels.iter()) {
            a.observe_row(features, *label).unwrap();
        }
        let flat: Vec<f64> = data.iter().flatten().copied().collect();
        b.observe_dense(&flat, &labels).unwrap();
        let sa = a.finish().unwrap();
        let sb = b.finish().unwrap();
        assert_eq!(sa, sb);
    }

    #[test]
    fn non_finite_feature_and_label_loud() {
        let mut acc = LinearRegressionAccumulator::new(1, true).unwrap();
        let err = acc.observe_row(&[f64::NAN], 1.0).unwrap_err();
        assert!(matches!(
            err,
            MlError::NonFinite {
                what: "feature",
                ..
            }
        ));
        let err = acc.observe_row(&[1.0], f64::INFINITY).unwrap_err();
        assert!(matches!(err, MlError::NonFinite { what: "label", .. }));
    }

    #[test]
    fn width_mismatch_loud() {
        let mut acc = LinearRegressionAccumulator::new(2, true).unwrap();
        let err = acc.observe_row(&[1.0], 0.0).unwrap_err();
        assert!(matches!(
            err,
            MlError::FeatureWidthMismatch {
                expected: 2,
                actual: 1
            }
        ));
    }

    #[test]
    fn intercept_only_recovers_mean() {
        let mut acc = LinearRegressionAccumulator::new(0, true).unwrap();
        for label in [1.0, 2.0, 3.0] {
            acc.observe_row(&[], label).unwrap();
        }
        let sol = acc.finish().unwrap();
        assert!((sol.intercept - 2.0).abs() < 1e-12);
        assert!(sol.coefficients.is_empty());
    }
}
