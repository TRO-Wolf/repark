//! Hand-rolled Cholesky factorization and triangular solves for row-major dense matrices.

use crate::error::{MlError, Result};

const PIVOT_REL_EPS: f64 = 1e-12;

const PIVOT_ABS_EPS: f64 = 1e-14;

/// Factor row-major `matrix` in place as `A = L Lᵀ`; only the lower triangle is defined.
/// # Errors
/// Returns [`MlError::EmptyDesign`], [`MlError::IllegalArgument`], or [`MlError::Singular`].
pub fn cholesky_decompose_inplace(matrix: &mut [f64], dimension: usize) -> Result<()> {
    if dimension == 0 {
        return Err(MlError::EmptyDesign(
            "Cholesky dimension is 0 (empty design matrix)".into(),
        ));
    }
    // Reject dimension-square overflow before sizing the buffer.
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

    // Scale the relative pivot threshold from the input diagonal.
    let mut scale = 0.0_f64;
    for index in 0..dimension {
        scale = scale.max(matrix[index * dimension + index].abs());
    }
    if !scale.is_finite() || scale == 0.0 {
        scale = 1.0;
    }
    let pivot_floor = (PIVOT_REL_EPS * scale).max(PIVOT_ABS_EPS);

    for column in 0..dimension {
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

/// Validate triangular-solve buffers without panics.
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
    // Apply the same checked dimension-square guard as factorization.
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

/// Solve `L y = b` using the row-major factor from [`cholesky_decompose_inplace`].
/// # Errors
/// Returns [`MlError::EmptyDesign`] or [`MlError::IllegalArgument`].
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

/// Solve `Lᵀ x = y` using the row-major factor.
/// # Errors
/// Returns [`MlError::EmptyDesign`] or [`MlError::IllegalArgument`].
pub fn backward_solve(lower: &[f64], dimension: usize, y: &[f64], x: &mut [f64]) -> Result<()> {
    validate_solve_buffers(lower, dimension, y, x, "x")?;
    for row in (0..dimension).rev() {
        let mut sum = y[row];
        for column in (row + 1)..dimension {
            sum -= lower[column * dimension + row] * x[column];
        }
        x[row] = sum / lower[row * dimension + row];
    }
    Ok(())
}

/// Solve `A x = b` from a factor produced by [`cholesky_decompose_inplace`].
/// # Errors
/// Returns [`MlError::EmptyDesign`] or [`MlError::IllegalArgument`].
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

/// Factor and solve `A x = b`; overwrite `matrix` with `L`.
/// # Errors
/// Propagates [`MlError::EmptyDesign`], [`MlError::IllegalArgument`], or [`MlError::Singular`].
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
        let mut a = vec![4.0, 2.0, 2.0, 3.0];
        let b = vec![1.0, 0.0];
        let x = cholesky_factor_and_solve(&mut a, 2, &b).expect("spd");
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

    /// A dimension whose square overflows `usize` must fail before buffer access.
    #[test]
    fn cholesky_refuses_dimension_square_overflow() {
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
