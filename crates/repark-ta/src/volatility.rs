//! Volatility — `TRANGE`, `ATR` (TA-Lib C 0.4.0 ports; see the crate docs for the numerics
//! contract).

use crate::{Result, as_f64, check_lengths, check_period, is_zero, true_range};

/// ===========================================================================================
/// `TRANGE` — true range (`ta_TRANGE.c`).
///
/// Return the greatest high-low or previous-close range; the first bar is NaN.
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::LengthMismatch`] if the series differ in length.
pub fn trange(high: &[f64], low: &[f64], close: &[f64]) -> Result<Vec<f64>> {
    check_lengths(high.len(), &[low.len(), close.len()])?;
    let len = high.len();
    // Preserve the measured single-write construction without changing arithmetic order.
    let mut out = Vec::with_capacity(len);
    if len == 0 {
        return Ok(out);
    }
    out.resize(1, f64::NAN);
    out.extend(high[1..].iter().zip(&low[1..]).zip(&close[..len - 1]).map(
        |((today_high, today_low), yesterday_close)| {
            true_range(*today_high, *today_low, *yesterday_close)
        },
    ));
    Ok(out)
}

/// ===========================================================================================
/// `ATR` — average true range (`ta_ATR.c`, unstable period 0).
///
/// Seed with a true-range SMA, then apply Wilder's three-statement recurrence.
/// Period one delegates to [`trange`].
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 1`;
/// [`crate::TaError::LengthMismatch`] if the series differ in length.
pub fn atr(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 1)?;
    if period == 1 {
        return trange(high, low, close);
    }
    let tr = trange(high, low, close)?;
    let len = tr.len();
    // Keep Wilder's statement order; only output construction uses the measured form.
    let mut out = Vec::with_capacity(len);
    if len < period + 1 {
        out.resize(len, f64::NAN);
        return Ok(out);
    }
    // Seed: TA_INT_SMA over the first `period` true ranges (tr[1..=period]).
    let mut period_total = 0.0_f64;
    for value in &tr[1..=period] {
        period_total += *value;
    }
    let mut prev_atr = period_total / as_f64(period);
    out.resize(period, f64::NAN);
    out.push(prev_atr);
    out.extend(tr[(period + 1)..].iter().map(|value| {
        prev_atr *= as_f64(period - 1);
        prev_atr += *value;
        prev_atr /= as_f64(period);
        prev_atr
    }));
    Ok(out)
}

/// ===========================================================================================
/// `NATR` — normalized average true range (`ta_NATR.c`, unstable period 0).
///
/// Normalize [`atr`] values by close with C's zero guard.
/// Period one returns raw [`trange`] values, as C does.
///
/// For a zero close, this port writes `0.0` at the current index; upstream C writes index zero.
/// Current goldens do not exercise this divergence.
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 1`;
/// [`crate::TaError::LengthMismatch`] if the series differ in length.
pub fn natr(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 1)?;
    let mut out = atr(high, low, close, period)?;
    if period == 1 {
        // C's period-one branch returns raw TRANGE.
        return Ok(out);
    }
    for (i, slot) in out.iter_mut().enumerate() {
        if !slot.is_nan() {
            let close_value = close[i];
            *slot = if is_zero(close_value) {
                0.0
            } else {
                (*slot / close_value) * 100.0
            };
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trange_matches_hand_computation() {
        let out =
            trange(&[10.0, 12.0, 11.0], &[9.0, 10.5, 9.5], &[9.5, 11.0, 10.0]).expect("valid");
        assert!(out[0].is_nan());
        assert!((out[1] - 2.5).abs() < 1e-12);
        assert!((out[2] - 1.5).abs() < 1e-12);
    }

    #[test]
    fn atr_period_one_is_trange() {
        let high = [10.0, 12.0, 11.0];
        let low = [9.0, 10.5, 9.5];
        let close = [9.5, 11.0, 10.0];
        let via_atr = atr(&high, &low, &close, 1).expect("valid");
        let via_trange = trange(&high, &low, &close).expect("valid");
        assert_eq!(via_atr.len(), via_trange.len());
        for (a, t) in via_atr.iter().zip(&via_trange) {
            assert!(a.to_bits() == t.to_bits() || (a.is_nan() && t.is_nan()));
        }
    }

    #[test]
    fn atr_seed_is_mean_of_first_true_ranges() {
        let high = [10.0, 11.0, 12.0, 13.0];
        let low = [9.0, 10.0, 11.0, 12.0];
        let close = [9.5, 10.5, 11.5, 12.5];
        let out = atr(&high, &low, &close, 2).expect("valid");
        assert!(out[1].is_nan());
        assert!((out[2] - 1.5).abs() < 1e-12);
        assert!((out[3] - 1.5).abs() < 1e-12);
    }

    #[test]
    fn natr_normalizes_atr_by_close_times_100() {
        let high = [10.0, 11.0, 12.0, 13.0];
        let low = [9.0, 10.0, 11.0, 12.0];
        let close = [9.5, 10.5, 11.5, 12.5];
        let atr_out = atr(&high, &low, &close, 2).expect("atr");
        let natr_out = natr(&high, &low, &close, 2).expect("natr");
        assert_eq!(natr_out.len(), atr_out.len());
        assert!(natr_out[0].is_nan());
        assert!(natr_out[1].is_nan());
        for i in 2..4 {
            let expected = (atr_out[i] / close[i]) * 100.0;
            assert!((natr_out[i] - expected).abs() < 1e-12);
        }
    }

    #[test]
    fn natr_period_one_is_raw_trange_unnormalized() {
        let high = [10.0, 12.0, 11.0];
        let low = [9.0, 10.5, 9.5];
        let close = [9.5, 11.0, 10.0];
        let via_natr = natr(&high, &low, &close, 1).expect("natr");
        let via_trange = trange(&high, &low, &close).expect("trange");
        for (a, t) in via_natr.iter().zip(&via_trange) {
            assert!(a.to_bits() == t.to_bits() || (a.is_nan() && t.is_nan()));
        }
    }
}
