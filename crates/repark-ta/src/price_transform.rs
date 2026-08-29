//! Price transforms from TA-Lib C 0.4.0. Each has lookback 0 and emits one value per bar.
//! Addition order and divisors are bit-exactness contracts.
//!
//! **Oracle note.** The recorded `polars_talib` 0.1.5 bits define the association. `TYPPRICE`
//! uses `high + (low + close)`, and all four transforms are golden-pinned bit-exactly. Keep that
//! wrapper when re-recording, or re-verify all four series against C 0.4.0 first.

use crate::{Result, check_lengths};

/// ===========================================================================================
/// `AVGPRICE` — average price, `(open + high + low + close) / 4` (`ta_AVGPRICE.c`).
///
/// Sums in C order `high + low + close + open`, then divides by `4`.
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::LengthMismatch`] if the series differ in length.
pub fn avgprice(open: &[f64], high: &[f64], low: &[f64], close: &[f64]) -> Result<Vec<f64>> {
    check_lengths(open.len(), &[high.len(), low.len(), close.len()])?;
    Ok((0..open.len())
        .map(|i| (high[i] + low[i] + close[i] + open[i]) / 4.0)
        .collect())
}

/// ===========================================================================================
/// `MEDPRICE` — median price, `(high + low) / 2` (`ta_MEDPRICE.c`). Lookback 0.
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::LengthMismatch`] if the series differ in length.
pub fn medprice(high: &[f64], low: &[f64]) -> Result<Vec<f64>> {
    check_lengths(high.len(), &[low.len()])?;
    // `f64::midpoint` rounds differently from the oracle's plain `(high + low) / 2`.
    #[allow(clippy::manual_midpoint)]
    Ok((0..high.len()).map(|i| (high[i] + low[i]) / 2.0).collect())
}

/// ===========================================================================================
/// `TYPPRICE` — typical price, `(high + low + close) / 3` (`ta_TYPPRICE.c`). Lookback 0.
///
/// The oracle folds `low + close` first. Preserve `high + (low + close)` for its rounding.
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::LengthMismatch`] if the series differ in length.
pub fn typprice(high: &[f64], low: &[f64], close: &[f64]) -> Result<Vec<f64>> {
    check_lengths(high.len(), &[low.len(), close.len()])?;
    Ok((0..high.len())
        .map(|i| (high[i] + (low[i] + close[i])) / 3.0)
        .collect())
}

/// ===========================================================================================
/// `WCLPRICE` — weighted close price, `(high + low + close·2) / 4` (`ta_WCLPRICE.c`). Lookback 0.
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::LengthMismatch`] if the series differ in length.
pub fn wclprice(high: &[f64], low: &[f64], close: &[f64]) -> Result<Vec<f64>> {
    check_lengths(high.len(), &[low.len(), close.len()])?;
    Ok((0..high.len())
        .map(|i| (high[i] + low[i] + close[i] * 2.0) / 4.0)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn avgprice_averages_the_four_series() {
        let out = avgprice(&[10.0], &[12.0], &[8.0], &[11.0]).expect("valid");
        assert!((out[0] - 10.25).abs() < 1e-12);
    }

    #[test]
    fn medprice_is_the_high_low_midpoint() {
        let out = medprice(&[12.0, 20.0], &[8.0, 10.0]).expect("valid");
        assert!((out[0] - 10.0).abs() < 1e-12);
        assert!((out[1] - 15.0).abs() < 1e-12);
    }

    #[test]
    fn typprice_averages_high_low_close() {
        let out = typprice(&[12.0], &[9.0], &[15.0]).expect("valid");
        assert!((out[0] - 12.0).abs() < 1e-12);
    }

    #[test]
    fn wclprice_double_weights_the_close() {
        let out = wclprice(&[12.0], &[8.0], &[11.0]).expect("valid");
        assert!((out[0] - 10.5).abs() < 1e-12);
    }

    #[test]
    fn empty_input_yields_empty_output() {
        assert!(avgprice(&[], &[], &[], &[]).expect("empty").is_empty());
        assert!(medprice(&[], &[]).expect("empty").is_empty());
        assert!(typprice(&[], &[], &[]).expect("empty").is_empty());
        assert!(wclprice(&[], &[], &[]).expect("empty").is_empty());
    }

    #[test]
    fn length_mismatch_errors() {
        use crate::TaError;
        assert_eq!(
            avgprice(&[1.0, 2.0], &[1.0], &[1.0, 2.0], &[1.0, 2.0]),
            Err(TaError::LengthMismatch { left: 2, right: 1 })
        );
        assert_eq!(
            medprice(&[1.0, 2.0], &[1.0]),
            Err(TaError::LengthMismatch { left: 2, right: 1 })
        );
    }
}
