//! Price transforms — `AVGPRICE`, `MEDPRICE`, `TYPPRICE`, `WCLPRICE` (TA-Lib C 0.4.0 ports; see
//! the crate docs for the numerics contract).
//!
//! Each is a per-bar O/H/L/C combination with **no period parameter and lookback 0** — every input
//! bar produces one output (the [`crate::volatility::trange`]-shaped no-lookback family). The only
//! bit-exactness concern is the addition order and the divisor.
//!
//! **Oracle note.** Unlike every other kernel here, the `polars_talib` oracle implements these four
//! transforms in its *own* native Rust plugin (`_polars_talib`), not by calling C TA-Lib. For
//! `AVGPRICE`/`MEDPRICE`/`WCLPRICE` the oracle's association is bit-identical to the C source, but
//! its `TYPPRICE` folds `low + close` first and then adds `high` (`high + (low + close)`), which
//! rounds differently from the C source's left fold `(high + low) + close`. Since the pipeline's
//! models were trained on the `polars_talib` values, the oracle wins: `TYPPRICE` matches
//! `high + (low + close)`. The goldens pin all four bit-exactly.

use crate::{Result, check_lengths};

/// ===========================================================================================
/// `AVGPRICE` — average price, `(open + high + low + close) / 4` (`ta_AVGPRICE.c`).
///
/// C sums in the order `high + low + close + open` then divides by `4`; the addition order is
/// preserved for bit-exactness. Lookback 0 — every bar produces a value.
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
    // NOT `f64::midpoint`: it rounds differently (overflow-avoiding), which would break the
    // bit-exact match against the oracle's plain `(high + low) / 2` (the golden pins the latter).
    #[allow(clippy::manual_midpoint)]
    Ok((0..high.len()).map(|i| (high[i] + low[i]) / 2.0).collect())
}

/// ===========================================================================================
/// `TYPPRICE` — typical price, `(high + low + close) / 3` (`ta_TYPPRICE.c`). Lookback 0.
///
/// The `polars_talib` oracle folds `low + close` first, so the summation is `high + (low + close)`
/// (not the C source's `(high + low) + close`) — a bit-different rounding. The oracle wins (see the
/// module note); the golden pins this association exactly.
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
        // (high 12 + low 8 + close 11 + open 10) / 4 = 41 / 4 = 10.25
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
        // (12 + 9 + 15) / 3 = 12
        assert!((out[0] - 12.0).abs() < 1e-12);
    }

    #[test]
    fn wclprice_double_weights_the_close() {
        let out = wclprice(&[12.0], &[8.0], &[11.0]).expect("valid");
        // (12 + 8 + 11*2) / 4 = 42 / 4 = 10.5
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
