//! Volatility — `TRANGE`, `ATR` (TA-Lib C 0.4.0 ports; see the crate docs for the numerics
//! contract).

use crate::{Result, as_f64, check_lengths, check_period, is_zero, true_range};

/// ===========================================================================================
/// `TRANGE` — true range (`ta_TRANGE.c`).
///
/// Greatest of `high − low`, `|prevClose − high|`, `|prevClose − low|`; the first bar has no
/// previous close, so lookback = 1.
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::LengthMismatch`] if the series differ in length.
pub fn trange(high: &[f64], low: &[f64], close: &[f64]) -> Result<Vec<f64>> {
    check_lengths(high.len(), &[low.len(), close.len()])?;
    let len = high.len();
    // Single-write construction (the `overlap::ema` pattern): one NaN via resize, then the
    // per-bar TRUE_RANGE streamed through a TrustedLen extend — one write per slot, never a
    // push loop. Arithmetic and bar order are unchanged.
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
/// Seed = SMA of the first `period` true ranges; then Wilder smoothing in C's statement order
/// (`prev *= period − 1`, `prev += tr`, `prev /= period`). `period == 1` delegates to
/// [`trange`], exactly as C does. Lookback = `period`.
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
    // Single-write construction (the `overlap::ema` pattern): NaN lookback prefix via resize,
    // then the Wilder recursion streamed through a TrustedLen extend. The three-statement
    // Wilder order (`*=`, `+=`, `/=`) is unchanged — construction only.
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
/// C computes the ATR internally exactly as [`atr`] does (TRANGE → SMA seed → Wilder in statement
/// order), then normalizes each value: `NATR[i] = (ATR[i] / close[i]) · 100`, with a
/// `TA_IS_ZERO(close)` guard that yields `0.0` instead of dividing. So this reuses [`atr`] and
/// applies the normalization to its non-NaN tail. `period == 1` returns the raw [`trange`] (C's
/// "no smoothing needed" trap), *unnormalized* — matching C. Lookback = `period`.
///
/// C's zero-close `else` branch has a known upstream quirk (it writes `outReal[0]` rather than the
/// current index); it is unreachable for any realistic positive-price close series (`|close| < 1e-8`
/// never holds around real prices), so this port writes `0.0` at the current index and the goldens
/// never exercise the branch.
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 1`;
/// [`crate::TaError::LengthMismatch`] if the series differ in length.
pub fn natr(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 1)?;
    let mut out = atr(high, low, close, period)?;
    if period == 1 {
        // C's `optInTimePeriod <= 1` trap returns raw TRANGE, not normalized.
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
        // Bar 1: max(12−10.5, |9.5−12|, |9.5−10.5|) = 2.5
        assert!((out[1] - 2.5).abs() < 1e-12);
        // Bar 2: max(11−9.5, |11−11|, |11−9.5|) = 1.5
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
        // TR[1] = 1.5, TR[2] = 1.5 → seed 1.5; then Wilder: (1.5*1 + 1.5)/2 = 1.5.
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
