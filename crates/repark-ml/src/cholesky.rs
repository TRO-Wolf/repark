//! Hand-rolled Cholesky factorization and triangular solves (no ndarray / nalgebra).
//!
//! Factor `A = L Lᵀ` for symmetric positive-definite `A` stored in **row-major** dense layout.
//! Used by OLS (normal equations) and IRLS logistic steps. Ill-conditioned / singular matrices
//! fail loud at the first non-positive pivot — we never form a pseudoinverse.

use crate::error::{MlError, Result};

/// Relative pivot threshold vs the running scale of the matrix (refuse near-singular).
const PIVOT_REL_EPS: f64 = 1e-12;

/// Absolute floor for a pivot (also refuse exact / tiny zeros).
const PIVOT_ABS_EPS: f64 = 1e-14;

/// ===========================================================================================
/// In-place Cholesky factorization: overwrite the lower triangle of `matrix` (n×n row-major)
/// with `L` such that `A = L Lᵀ`. The strict upper triangle is left unspecified / untouched.
///
/// # Errors
/// [`MlError::Singular`] when a pivot is non-positive or below the relative threshold.
/// ===========================================================================================
pub fn cholesky_decompose_inplace(matrix: &mut [f64], dimension: usize) -> Result<()> {
    if dimension == 0 {
        return Err(MlError::EmptyDesign(
            "Cholesky dimension is 0 (empty design matrix)".into(),
        ));
    }
    // SAF-006: refuse wrapping `dimension * dimension` (would pass a too-short buffer).
    let expected_len = dimension.checked_mul(dimension).ok_or_else(|| {
        MlError::IllegalArgument(format!(
            "Cholesky dimension {dimension} overflows dimension² (checked_mul)"
        ))
    })?;
    if matrix.len() != expected_len {
        return Err(MlError::IllegalArgument(format!(
            "Cholesky buffer length {} != dimension² {expected_len}",
            matrix.len(),
        )));
    }

    // Scale for relative pivot check: max |A_ii| on entry.
    let mut scale = 0.0_f64;
    for index in 0..dimension {
        scale = scale.max(matrix[index * dimension + index].abs());
    }
    if !scale.is_finite() || scale == 0.0 {
        scale = 1.0;
    }
    let pivot_floor = (PIVOT_REL_EPS * scale).max(PIVOT_ABS_EPS);

    for column in 0..dimension {
        // Diagonal: L_cc = sqrt(A_cc - sum_{k<c} L_ck²)
        let mut sum = matrix[column * dimension + column];
        for k in 0..column {
            let l_ck = matrix[column * dimension + k];
            sum -= l_ck * l_ck;
        }
        if !sum.is_finite() || sum <= pivot_floor {
            return Err(MlError::Singular {
                pivot: column,
                detail: format!("pivot²={sum} (floor={pivot_floor}, scale={scale})"),
            });
        }
        let diag = sum.sqrt();
        matrix[column * dimension + column] = diag;
        let inv_diag = 1.0 / diag;

        // Below diagonal in this column: L_ic = (A_ic - sum_k L_ik L_ck) / L_cc
        for row in (column + 1)..dimension {
            let mut sum_off = matrix[row * dimension + column];
            for k in 0..column {
                sum_off -= matrix[row * dimension + k] * matrix[column * dimension + k];
            }
            matrix[row * dimension + column] = sum_off * inv_diag;
        }
    }
    Ok(())
}

/// ===========================================================================================
/// Validate buffer lengths for triangular solves (no panics in production).
/// ===========================================================================================
fn validate_solve_buffers(
    lower: &[f64],
    dimension: usize,
    right_hand_side: &[f64],
    out: &[f64],
    out_name: &str,
) -> Result<()> {
    if dimension == 0 {
        return Err(MlError::EmptyDesign("Cholesky solve dimension is 0".into()));
    }
    // SAF-006: checked dimension² (same overflow class as decompose).
    let expected_len = dimension.checked_mul(dimension).ok_or_else(|| {
        MlError::IllegalArgument(format!(
            "Cholesky dimension {dimension} overflows dimension² (checked_mul)"
        ))
    })?;
    if lower.len() != expected_len {
        return Err(MlError::IllegalArgument(format!(
            "Cholesky lower length {} != dimension² {expected_len}",
            lower.len(),
        )));
    }
    if right_hand_side.len() != dimension {
        return Err(MlError::IllegalArgument(format!(
            "Cholesky RHS length {} != dimension {dimension}",
            right_hand_side.len()
        )));
    }
    if out.len() != dimension {
        return Err(MlError::IllegalArgument(format!(
            "Cholesky {out_name} length {} != dimension {dimension}",
            out.len()
        )));
    }
    Ok(())
}

/// ===========================================================================================
/// Solve `L y = b` (forward substitution). `lower` is the lower-triangular factor from
/// [`cholesky_decompose_inplace`] (row-major, diagonal = `L_ii`).
///
/// # Errors
/// [`MlError::IllegalArgument`] / [`MlError::EmptyDesign`] on length mismatch.
/// ===========================================================================================
pub fn forward_solve(
    lower: &[f64],
    dimension: usize,
    right_hand_side: &[f64],
    y: &mut [f64],
) -> Result<()> {
    validate_solve_buffers(lower, dimension, right_hand_side, y, "y")?;
    for row in 0..dimension {
        let mut sum = right_hand_side[row];
        for column in 0..row {
            sum -= lower[row * dimension + column] * y[column];
        }
        y[row] = sum / lower[row * dimension + row];
    }
    Ok(())
}

/// ===========================================================================================
/// Solve `Lᵀ x = y` (back substitution).
///
/// # Errors
/// [`MlError::IllegalArgument`] / [`MlError::EmptyDesign`] on length mismatch.
/// ===========================================================================================
pub fn backward_solve(lower: &[f64], dimension: usize, y: &[f64], x: &mut [f64]) -> Result<()> {
    validate_solve_buffers(lower, dimension, y, x, "x")?;
    for row in (0..dimension).rev() {
        let mut sum = y[row];
        for column in (row + 1)..dimension {
            // Lᵀ[row, column] = L[column, row]
            sum -= lower[column * dimension + row] * x[column];
        }
        x[row] = sum / lower[row * dimension + row];
    }
    Ok(())
}

/// ===========================================================================================
/// Solve `A x = b` given the Cholesky factor of `A` already in `lower` (from
/// [`cholesky_decompose_inplace`]).
///
/// # Errors
/// Length mismatch on `lower` / `right_hand_side`.
/// ===========================================================================================
pub fn cholesky_solve(
    lower: &[f64],
    dimension: usize,
    right_hand_side: &[f64],
) -> Result<Vec<f64>> {
    let mut y = vec![0.0; dimension];
    let mut x = vec![0.0; dimension];
    forward_solve(lower, dimension, right_hand_side, &mut y)?;
    backward_solve(lower, dimension, &y, &mut x)?;
    Ok(x)
}

/// ===========================================================================================
/// Factor and solve `A x = b` in one shot. `matrix` is destroyed (overwritten by `L`).
///
/// # Errors
/// [`MlError::Singular`] / [`MlError::EmptyDesign`] / [`MlError::IllegalArgument`] from
/// factorization or solve buffer checks.
/// ===========================================================================================
pub fn cholesky_factor_and_solve(
    matrix: &mut [f64],
    dimension: usize,
    right_hand_side: &[f64],
) -> Result<Vec<f64>> {
    if right_hand_side.len() != dimension {
        return Err(MlError::IllegalArgument(format!(
            "Cholesky RHS length {} != dimension {dimension}",
            right_hand_side.len()
        )));
    }
    cholesky_decompose_inplace(matrix, dimension)?;
    cholesky_solve(matrix, dimension, right_hand_side)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cholesky_solves_well_conditioned_2x2() {
        // A = [[4, 2], [2, 3]] = L Lᵀ with L = [[2, 0], [1, sqrt(2)]]
        let mut a = vec![4.0, 2.0, 2.0, 3.0];
        let b = vec![1.0, 0.0];
        let x = cholesky_factor_and_solve(&mut a, 2, &b).expect("spd");
        // A^{-1} b: det=8, adj → x = [3/8, -1/4] = [0.375, -0.25]
        assert!((x[0] - 0.375).abs() < 1e-12);
        assert!((x[1] + 0.25).abs() < 1e-12);
    }

    #[test]
    fn cholesky_refuses_singular() {
        let mut a = vec![1.0, 1.0, 1.0, 1.0];
        let err = cholesky_decompose_inplace(&mut a, 2).expect_err("singular");
        assert!(matches!(err, MlError::Singular { .. }));
    }

    #[test]
    fn cholesky_solves_identity() {
        let n = 4;
        let mut a = vec![0.0; n * n];
        for i in 0..n {
            a[i * n + i] = 1.0;
        }
        let b = vec![1.0, 2.0, 3.0, 4.0];
        let x = cholesky_factor_and_solve(&mut a, n, &b).expect("I");
        assert_eq!(x, b);
    }

    #[test]
    fn cholesky_refuses_short_rhs() {
        let mut a = vec![1.0, 0.0, 0.0, 1.0];
        let err = cholesky_factor_and_solve(&mut a, 2, &[1.0]).expect_err("short rhs");
        assert!(matches!(err, MlError::IllegalArgument(_)));
    }

    #[test]
    fn cholesky_refuses_buffer_len_mismatch() {
        let mut a = vec![1.0, 0.0, 0.0];
        let err = cholesky_decompose_inplace(&mut a, 2).expect_err("len");
        assert!(matches!(err, MlError::IllegalArgument(_)));
    }

    /// SAF-006: a dimension whose square overflows `usize` must fail loud (not wrap and
    /// accept a short buffer). On 64-bit hosts `usize::MAX/2+1` overflows when squared.
    #[test]
    fn cholesky_refuses_dimension_square_overflow() {
        // Pick a dimension that overflows on multiply for any practical pointer width.
        let dimension = (usize::MAX / 2).saturating_add(1);
        let mut empty: Vec<f64> = vec![];
        let err = cholesky_decompose_inplace(&mut empty, dimension).expect_err("overflow");
        let text = err.to_string();
        assert!(
            matches!(err, MlError::IllegalArgument(_)),
            "expected IllegalArgument, got {err:?}"
        );
        assert!(
            text.contains("overflows") || text.contains("checked_mul"),
            "message must name overflow/checked_mul, got {text}"
        );
    }

    #[test]
    fn cholesky_refuses_zero_dimension() {
        let mut a: Vec<f64> = vec![];
        let err = cholesky_decompose_inplace(&mut a, 0).expect_err("dim0");
        assert!(matches!(err, MlError::EmptyDesign(_)));
    }

    #[test]
    fn forward_solve_refuses_short_y() {
        let lower = vec![1.0, 0.0, 0.0, 1.0];
        let mut y = vec![0.0];
        let err = forward_solve(&lower, 2, &[1.0, 2.0], &mut y).expect_err("short y");
        assert!(matches!(err, MlError::IllegalArgument(_)));
    }
}
