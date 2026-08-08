//! Binary logistic regression via IRLS (iteratively reweighted least squares) + Cholesky.
//!
//! Multi-pass: each IRLS iteration re-streams design rows (caller supplies rows again, or the
//! binder re-executes the plan). State held between iterations is `O(p)` coefficients only —
//! never the full training matrix. M3 ships a fixed `max_iter` / `tol` loop.

use crate::MAX_FEATURES;
use crate::cholesky::cholesky_factor_and_solve;
use crate::error::{MlError, Result};

/// Default max IRLS iterations (Spark-like).
pub const DEFAULT_MAX_ITER: usize = 100;

/// Default convergence tolerance on coefficient max-abs delta.
pub const DEFAULT_TOL: f64 = 1e-6;

/// ===========================================================================================
/// Fitted logistic parameters (params only).
/// ===========================================================================================
#[derive(Debug, Clone, PartialEq)]
pub struct LogisticRegressionSolution {
    /// Coefficients for the original feature columns.
    pub coefficients: Vec<f64>,
    /// Intercept; `0.0` when `fit_intercept` was false.
    pub intercept: f64,
    pub fit_intercept: bool,
    pub num_features: usize,
    pub num_rows: u64,
    /// IRLS iterations actually run.
    pub iterations: usize,
    /// Whether the coefficient delta fell below `tol` before `max_iter`.
    pub converged: bool,
}

/// ===========================================================================================
/// One IRLS weighted least-squares step: accumulate `Xᵀ W X` and `Xᵀ W z` then Cholesky-solve.
///
/// `weights[i] = p_i (1 - p_i)`, `z_i = x·β + (y - p) / w` with `p = σ(x·β)`.
/// ===========================================================================================
#[derive(Debug, Clone)]
pub struct LogisticIrlsAccumulator {
    num_features: usize,
    fit_intercept: bool,
    design_width: usize,
    /// Current coefficients in design order ([intercept,] features…).
    beta: Vec<f64>,
    xtwx: Vec<f64>,
    xtwz: Vec<f64>,
    num_rows: u64,
    row_offset: u64,
}

impl LogisticIrlsAccumulator {
    /// =======================================================================================
    /// Start an IRLS pass from the current `beta` (design-ordered).
    ///
    /// # Errors
    /// [`MlError::FeatureDimTooLarge`] or beta / design-width mismatch.
    /// =======================================================================================
    pub fn new_pass(num_features: usize, fit_intercept: bool, beta: Vec<f64>) -> Result<Self> {
        if num_features > MAX_FEATURES {
            return Err(MlError::FeatureDimTooLarge {
                actual: num_features,
                limit: MAX_FEATURES,
            });
        }
        let design_width = num_features + usize::from(fit_intercept);
        if beta.len() != design_width {
            return Err(MlError::IllegalArgument(format!(
                "logistic beta length {} != design_width {}",
                beta.len(),
                design_width
            )));
        }
        Ok(Self {
            num_features,
            fit_intercept,
            design_width,
            beta,
            xtwx: vec![0.0; design_width * design_width],
            xtwz: vec![0.0; design_width],
            num_rows: 0,
            row_offset: 0,
        })
    }

    /// Zero coefficients of the right design width (cold start).
    ///
    /// # Errors
    /// [`MlError::FeatureDimTooLarge`] or empty design.
    pub fn zero_beta(num_features: usize, fit_intercept: bool) -> Result<Vec<f64>> {
        if num_features > MAX_FEATURES {
            return Err(MlError::FeatureDimTooLarge {
                actual: num_features,
                limit: MAX_FEATURES,
            });
        }
        let design_width = num_features + usize::from(fit_intercept);
        if num_features == 0 && !fit_intercept {
            return Err(MlError::EmptyDesign(
                "logistic: num_features=0 and fit_intercept=false".into(),
            ));
        }
        Ok(vec![0.0; design_width])
    }

    /// =======================================================================================
    /// Observe one training row for this IRLS pass.
    ///
    /// # Errors
    /// Width mismatch, non-finite values, or label outside `[0, 1]`.
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
        // Labels in {0, 1} preferred; still allow soft targets in (0,1).
        if !(0.0..=1.0).contains(&label) {
            return Err(MlError::IllegalArgument(format!(
                "logistic label must be in [0, 1], got {label} at row {}",
                self.row_offset
            )));
        }
        for &value in features {
            if !value.is_finite() {
                return Err(MlError::NonFinite {
                    what: "feature",
                    row_offset: self.row_offset,
                });
            }
        }

        let intercept_offset = usize::from(self.fit_intercept);
        let eta = {
            let mut sum = 0.0;
            for index in 0..self.design_width {
                let x = design_entry(features, index, self.fit_intercept, intercept_offset);
                sum += self.beta[index] * x;
            }
            sum
        };
        let probability = sigmoid(eta);
        // Weight floor avoids exact 0/1 probabilities killing the weight matrix.
        let mut weight = probability * (1.0 - probability);
        if weight < 1e-12 {
            weight = 1e-12;
        }
        let working = eta + (label - probability) / weight;

        let width = self.design_width;
        for row in 0..width {
            let x_row = design_entry(features, row, self.fit_intercept, intercept_offset);
            self.xtwz[row] += weight * working * x_row;
            for col in 0..=row {
                let x_col = design_entry(features, col, self.fit_intercept, intercept_offset);
                let add = weight * x_row * x_col;
                self.xtwx[row * width + col] += add;
                if row != col {
                    self.xtwx[col * width + row] += add;
                }
            }
        }

        self.num_rows += 1;
        self.row_offset += 1;
        Ok(())
    }

    /// =======================================================================================
    /// Finish this IRLS pass → new beta (design-ordered).
    ///
    /// # Errors
    /// Empty pass or singular weighted normal equations.
    /// =======================================================================================
    pub fn finish_pass(self) -> Result<Vec<f64>> {
        if self.num_rows == 0 {
            return Err(MlError::EmptyDesign(
                "logistic IRLS pass saw zero training rows".into(),
            ));
        }
        let mut xtwx = self.xtwx;
        cholesky_factor_and_solve(&mut xtwx, self.design_width, &self.xtwz)
    }
}

/// ===========================================================================================
/// Run IRLS with a caller-provided `stream_pass` that feeds one [`LogisticIrlsAccumulator`].
///
/// The closure is invoked once per iteration; it must re-stream all training rows into `acc`.
/// This is the multi-pass Rust fit seam (docs/design/python-facade.md §4 Q3).
///
/// # Errors
/// Propagates stream / Cholesky failures from each IRLS pass.
/// ===========================================================================================
pub fn fit_logistic_irls<F>(
    num_features: usize,
    fit_intercept: bool,
    max_iter: usize,
    tol: f64,
    mut stream_pass: F,
) -> Result<LogisticRegressionSolution>
where
    F: FnMut(&mut LogisticIrlsAccumulator) -> Result<()>,
{
    if !tol.is_finite() || tol < 0.0 {
        return Err(MlError::IllegalArgument(format!(
            "logistic tol must be finite and ≥ 0, got {tol}"
        )));
    }

    let mut beta = LogisticIrlsAccumulator::zero_beta(num_features, fit_intercept)?;
    let mut iterations = 0;
    let mut converged = false;
    let mut num_rows = 0_u64;

    // max_iter == 0 → cold-start zeros only (Spark: no optimization steps). No silent clamp.
    for _ in 0..max_iter {
        let mut acc = LogisticIrlsAccumulator::new_pass(num_features, fit_intercept, beta.clone())?;
        stream_pass(&mut acc)?;
        num_rows = acc.num_rows;
        let new_beta = acc.finish_pass()?;
        iterations += 1;

        let mut max_delta = 0.0_f64;
        for (old, new) in beta.iter().zip(new_beta.iter()) {
            max_delta = max_delta.max((old - new).abs());
        }
        beta = new_beta;
        if max_delta < tol {
            converged = true;
            break;
        }
    }

    // max_iter == 0 never streamed; still report 0 rows (params are zeros).
    let (intercept, coefficients) = if fit_intercept {
        (beta[0], beta[1..].to_vec())
    } else {
        (0.0, beta)
    };

    Ok(LogisticRegressionSolution {
        coefficients,
        intercept,
        fit_intercept,
        num_features,
        num_rows,
        iterations,
        converged,
    })
}

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

#[inline]
fn sigmoid(eta: f64) -> f64 {
    // Stable sigmoid.
    if eta >= 0.0 {
        let z = (-eta).exp();
        1.0 / (1.0 + z)
    } else {
        let z = eta.exp();
        z / (1.0 + z)
    }
}

/// Probability prediction from fitted params (for tests / plan builders).
#[must_use]
pub fn predict_probability(coefficients: &[f64], intercept: f64, features: &[f64]) -> f64 {
    let mut eta = intercept;
    for (coef, feature) in coefficients.iter().zip(features.iter()) {
        eta += coef * feature;
    }
    sigmoid(eta)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separable_1d_learns_positive_slope() {
        // labels: 0 for x<0-ish, 1 for x>0-ish
        let rows: Vec<(f64, f64)> = (-5..=5)
            .map(|x| {
                let xf = f64::from(x);
                let y = if xf > 0.0 { 1.0 } else { 0.0 };
                (xf, y)
            })
            .collect();

        let sol = fit_logistic_irls(1, true, 50, 1e-8, |acc| {
            for &(x, y) in &rows {
                acc.observe_row(&[x], y)?;
            }
            Ok(())
        })
        .expect("irls");

        assert!(sol.coefficients[0] > 0.0, "coef {}", sol.coefficients[0]);
        // At large +x probability high; at large -x low.
        let p_pos = predict_probability(&sol.coefficients, sol.intercept, &[5.0]);
        let p_neg = predict_probability(&sol.coefficients, sol.intercept, &[-5.0]);
        assert!(p_pos > 0.8, "p_pos={p_pos}");
        assert!(p_neg < 0.2, "p_neg={p_neg}");
        assert!(sol.converged || sol.iterations >= 1);
    }

    #[test]
    fn max_iter_zero_returns_cold_start() {
        let sol = fit_logistic_irls(1, true, 0, 1e-6, |_acc| {
            panic!("stream must not run when max_iter=0");
        })
        .expect("cold");
        assert_eq!(sol.iterations, 0);
        assert!(!sol.converged);
        assert_eq!(sol.coefficients, vec![0.0]);
        assert!((sol.intercept - 0.0).abs() < 1e-15);
        assert_eq!(sol.num_rows, 0);
    }

    #[test]
    fn label_outside_unit_interval_loud() {
        let mut acc = LogisticIrlsAccumulator::new_pass(1, true, vec![0.0, 0.0]).unwrap();
        let err = acc.observe_row(&[1.0], 2.0).unwrap_err();
        assert!(matches!(err, MlError::IllegalArgument(_)));
    }

    #[test]
    fn non_finite_feature_loud() {
        let mut acc = LogisticIrlsAccumulator::new_pass(1, true, vec![0.0, 0.0]).unwrap();
        let err = acc.observe_row(&[f64::NAN], 0.0).unwrap_err();
        assert!(matches!(
            err,
            MlError::NonFinite {
                what: "feature",
                ..
            }
        ));
    }

    #[test]
    fn width_mismatch_loud() {
        let mut acc = LogisticIrlsAccumulator::new_pass(2, false, vec![0.0, 0.0]).unwrap();
        let err = acc.observe_row(&[1.0], 0.0).unwrap_err();
        assert!(matches!(err, MlError::FeatureWidthMismatch { .. }));
    }

    #[test]
    fn negative_tol_loud() {
        let err = fit_logistic_irls(1, true, 1, -1.0, |_| Ok(())).unwrap_err();
        assert!(matches!(err, MlError::IllegalArgument(_)));
    }
}
