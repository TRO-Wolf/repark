//! Volume indicators from TA-Lib C 0.4.0. See the crate docs for numeric contracts.
//!
//! Incremental accumulators preserve C order. AD/ADOSC use strict `tmp > 0.0`; MFI uses the
//! hard `pos+neg < 1.0` guard. None use FMA.

use crate::{Result, as_f64, check_lengths, check_period, nan_vec};

/// ===========================================================================================
/// C's `CALCULATE_AD` increment (`ta_AD.c` / `ta_ADOSC.c`).
///
/// Add only when `tmp = high − low > 0.0`. Preserve the CLV order
/// `(((close−low)−(high−close))/tmp)*volume`; algebraic rewrites round differently.
/// ===========================================================================================
fn calculate_ad(ad: &mut f64, high: f64, low: f64, close: f64, volume: f64) {
    let tmp = high - low;
    if tmp > 0.0 {
        *ad += (((close - low) - (high - close)) / tmp) * volume;
    }
}

/// ===========================================================================================
/// `AD` — Chaikin A/D Line (`ta_AD.c`). Lookback 0.
///
/// Emits the incremental cumulative CLV·volume. Flat bars re-emit the previous total.
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::LengthMismatch`] if the series differ in length.
pub fn ad(high: &[f64], low: &[f64], close: &[f64], volume: &[f64]) -> Result<Vec<f64>> {
    check_lengths(high.len(), &[low.len(), close.len(), volume.len()])?;
    let mut out = Vec::with_capacity(high.len());
    let mut ad = 0.0_f64;
    for i in 0..high.len() {
        calculate_ad(&mut ad, high[i], low[i], close[i], volume[i]);
        out.push(ad);
    }
    Ok(out)
}

/// ===========================================================================================
/// `ADOSC` — Chaikin A/D Oscillator (`ta_ADOSC.c`, unstable period 0).
///
/// Lookback is `slowest − 1`. C seeds both EMAs with the first AD value and updates them as
/// `(k*ad)+(one_minus_k*ema)` with `k = 2/(period+1)`; do not call standalone [`crate::ema`].
/// `fast` and `slow` are slots, so reversing them negates the result.
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `fast` or `slow` is outside `2..=MAX_PERIOD`;
/// [`crate::TaError::LengthMismatch`] if the series differ in length.
pub fn adosc(
    high: &[f64],
    low: &[f64],
    close: &[f64],
    volume: &[f64],
    fast: usize,
    slow: usize,
) -> Result<Vec<f64>> {
    check_period("optInFastPeriod", fast, 2)?;
    check_period("optInSlowPeriod", slow, 2)?;
    check_lengths(high.len(), &[low.len(), close.len(), volume.len()])?;
    let len = high.len();
    let mut out = nan_vec(len);
    let slowest = if fast < slow { slow } else { fast };
    let lookback = slowest - 1;
    if len == 0 || lookback >= len {
        return Ok(out);
    }

    let fastk = 2.0 / as_f64(fast + 1);
    let one_minus_fastk = 1.0 - fastk;
    let slowk = 2.0 / as_f64(slow + 1);
    let one_minus_slowk = 1.0 - slowk;

    let mut ad = 0.0_f64;
    let mut today = 0;
    calculate_ad(
        &mut ad,
        high[today],
        low[today],
        close[today],
        volume[today],
    );
    today += 1;
    let mut fast_ema = ad;
    let mut slow_ema = ad;

    while today < lookback {
        calculate_ad(
            &mut ad,
            high[today],
            low[today],
            close[today],
            volume[today],
        );
        today += 1;
        fast_ema = (fastk * ad) + (one_minus_fastk * fast_ema);
        slow_ema = (slowk * ad) + (one_minus_slowk * slow_ema);
    }

    while today < len {
        calculate_ad(
            &mut ad,
            high[today],
            low[today],
            close[today],
            volume[today],
        );
        today += 1;
        fast_ema = (fastk * ad) + (one_minus_fastk * fast_ema);
        slow_ema = (slowk * ad) + (one_minus_slowk * slow_ema);
        out[today - 1] = fast_ema - slow_ema;
    }
    Ok(out)
}

/// ===========================================================================================
/// `OBV` — On Balance Volume (`ta_OBV.c`). Lookback 0.
///
/// Seeds `prevOBV = volume[0]`, so the first output is the first volume. Later bars add, subtract,
/// or hold volume according to the close comparison.
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::LengthMismatch`] if the series differ in length.
pub fn obv(close: &[f64], volume: &[f64]) -> Result<Vec<f64>> {
    check_lengths(close.len(), &[volume.len()])?;
    let len = close.len();
    if len == 0 {
        return Ok(Vec::new());
    }
    let mut out = Vec::with_capacity(len);
    let mut prev_obv = volume[0];
    let mut prev_real = close[0];
    for i in 0..len {
        let temp_real = close[i];
        if temp_real > prev_real {
            prev_obv += volume[i];
        } else if temp_real < prev_real {
            prev_obv -= volume[i];
        }
        out.push(prev_obv);
        prev_real = temp_real;
    }
    Ok(out)
}

/// Classify money flow in C's statement order: negative delta updates only the negative sum,
/// positive delta only the positive sum, and zero updates neither.
fn classify_money_flow(
    delta: f64,
    money_flow: f64,
    pos_sum_mf: &mut f64,
    neg_sum_mf: &mut f64,
    pos_slot: &mut f64,
    neg_slot: &mut f64,
) {
    if delta < 0.0 {
        *neg_slot = money_flow;
        *neg_sum_mf += money_flow;
        *pos_slot = 0.0;
    } else if delta > 0.0 {
        *pos_slot = money_flow;
        *pos_sum_mf += money_flow;
        *neg_slot = 0.0;
    } else {
        *pos_slot = 0.0;
        *neg_slot = 0.0;
    }
}

/// ===========================================================================================
/// `MFI` — Money Flow Index (`ta_MFI.c`, unstable period 0). Lookback = `period`.
///
/// Uses a rolling positive/negative money-flow buffer, not Wilder smoothing. Subtract the
/// trailing slot before writing the new typical price. The output uses the hard `pos+neg < 1.0`
/// guard and is not clamped; incremental drift can make `pos_sum_mf` slightly negative.
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`;
/// [`crate::TaError::LengthMismatch`] if the series differ in length.
pub fn mfi(
    high: &[f64],
    low: &[f64],
    close: &[f64],
    volume: &[f64],
    period: usize,
) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    check_lengths(high.len(), &[low.len(), close.len(), volume.len()])?;
    let len = high.len();
    let mut out = nan_vec(len);
    if len <= period {
        return Ok(out);
    }

    let mut mflow_positive = vec![0.0_f64; period];
    let mut mflow_negative = vec![0.0_f64; period];
    let mut mflow_idx = 0;

    let mut today = 0;
    let mut prev_value = (high[today] + low[today] + close[today]) / 3.0;
    let mut pos_sum_mf = 0.0_f64;
    let mut neg_sum_mf = 0.0_f64;
    today += 1;

    for _ in 0..period {
        let mut temp_value1 = (high[today] + low[today] + close[today]) / 3.0;
        let temp_value2 = temp_value1 - prev_value;
        prev_value = temp_value1;
        temp_value1 *= volume[today];
        today += 1;
        classify_money_flow(
            temp_value2,
            temp_value1,
            &mut pos_sum_mf,
            &mut neg_sum_mf,
            &mut mflow_positive[mflow_idx],
            &mut mflow_negative[mflow_idx],
        );
        mflow_idx += 1;
        if mflow_idx == period {
            mflow_idx = 0;
        }
    }

    let temp_value1 = pos_sum_mf + neg_sum_mf;
    out[period] = if temp_value1 < 1.0 {
        0.0
    } else {
        100.0 * (pos_sum_mf / temp_value1)
    };

    while today < len {
        pos_sum_mf -= mflow_positive[mflow_idx];
        neg_sum_mf -= mflow_negative[mflow_idx];

        let mut temp_value1 = (high[today] + low[today] + close[today]) / 3.0;
        let temp_value2 = temp_value1 - prev_value;
        prev_value = temp_value1;
        temp_value1 *= volume[today];
        today += 1;
        classify_money_flow(
            temp_value2,
            temp_value1,
            &mut pos_sum_mf,
            &mut neg_sum_mf,
            &mut mflow_positive[mflow_idx],
            &mut mflow_negative[mflow_idx],
        );

        let temp_value1 = pos_sum_mf + neg_sum_mf;
        out[today - 1] = if temp_value1 < 1.0 {
            0.0
        } else {
            100.0 * (pos_sum_mf / temp_value1)
        };

        mflow_idx += 1;
        if mflow_idx == period {
            mflow_idx = 0;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaError;

    #[test]
    fn ad_clv_order_and_flat_bar_hold() {
        let out = ad(&[12.0, 10.0], &[8.0, 10.0], &[11.0, 10.0], &[100.0, 50.0]).expect("ad");
        assert_eq!(out[0].to_bits(), 50.0_f64.to_bits());
        assert_eq!(out[1].to_bits(), 50.0_f64.to_bits());
    }

    #[test]
    fn ad_does_not_rewrite_as_two_c_minus_h_minus_l() {
        let high = [1.000_000_03e-8];
        let low = [1.0e-8];
        let close = [0.250_000_01];
        let volume = [1.0];
        let out = ad(&high, &low, &close, &volume).expect("ad");
        let tmp = high[0] - low[0];
        let c_order = (((close[0] - low[0]) - (high[0] - close[0])) / tmp) * volume[0];
        let rewritten = ((2.0 * close[0] - high[0] - low[0]) / tmp) * volume[0];
        assert_eq!(out[0].to_bits(), c_order.to_bits());
        assert_ne!(
            c_order.to_bits(),
            rewritten.to_bits(),
            "fixture must distinguish CLV order from the (2c-h-l) rewrite"
        );
    }

    #[test]
    fn obv_first_output_is_first_volume() {
        let out = obv(&[10.0, 11.0, 10.0], &[5.0, 3.0, 2.0]).expect("obv");
        assert_eq!(out[0].to_bits(), 5.0_f64.to_bits());
        assert_eq!(out[1].to_bits(), 8.0_f64.to_bits());
        assert_eq!(out[2].to_bits(), 6.0_f64.to_bits());
    }

    #[test]
    fn obv_equal_close_holds() {
        let out = obv(&[10.0, 10.0], &[4.0, 9.0]).expect("obv");
        assert_eq!(out[0].to_bits(), 4.0_f64.to_bits());
        assert_eq!(out[1].to_bits(), 4.0_f64.to_bits());
    }

    #[test]
    fn adosc_lookback_is_slowest_minus_one() {
        let high = [12.0, 13.0, 14.0, 15.0];
        let low = [8.0, 9.0, 10.0, 11.0];
        let close = [11.0, 12.0, 13.0, 14.0];
        let volume = [100.0, 110.0, 120.0, 130.0];
        let short = adosc(&high, &low, &close, &volume, 3, 10).expect("short");
        assert!(short.iter().all(|v| v.is_nan()));
        let out = adosc(&high, &low, &close, &volume, 2, 2).expect("adosc 2/2");
        assert!(out[0].is_nan());
        assert!(out[1].is_finite());
    }

    #[test]
    fn adosc_inverted_periods_negate() {
        let high: Vec<f64> = (0..20).map(|i| 12.0 + f64::from(i)).collect();
        let low: Vec<f64> = (0..20).map(|i| 8.0 + f64::from(i)).collect();
        let close: Vec<f64> = (0..20).map(|i| 11.0 + f64::from(i)).collect();
        let volume: Vec<f64> = (0..20).map(|i| 100.0 + f64::from(i)).collect();
        let a = adosc(&high, &low, &close, &volume, 3, 10).expect("3,10");
        let b = adosc(&high, &low, &close, &volume, 10, 3).expect("10,3");
        assert!(a[..9].iter().all(|v| v.is_nan()));
        assert!(b[..9].iter().all(|v| v.is_nan()));
        for i in 9..20 {
            assert_eq!(a[i].to_bits(), (-b[i]).to_bits(), "row {i}");
        }
    }

    #[test]
    fn mfi_lookback_is_period_and_classifies_neg_first() {
        let high = [10.0, 13.0, 10.0];
        let low = [10.0, 10.0, 7.0];
        let close = [10.0, 13.0, 7.0];
        let volume = [1.0, 2.0, 3.0];
        let out = mfi(&high, &low, &close, &volume, 2).expect("mfi");
        assert!(out[0].is_nan());
        assert!(out[1].is_nan());
        assert!((out[2] - 50.0).abs() < 1e-12);
    }

    #[test]
    fn mfi_hard_lt_one_yields_zero_not_nan() {
        let high = [1.0, 1.0 + 1e-12, 1.0];
        let low = [1.0, 1.0, 1.0];
        let close = [1.0, 1.0 + 1e-12, 1.0];
        let volume = [1e-6, 1e-6, 1e-6];
        let out = mfi(&high, &low, &close, &volume, 2).expect("mfi tiny");
        assert_eq!(out[2].to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn empty_input_yields_empty_output() {
        assert!(ad(&[], &[], &[], &[]).expect("ad empty").is_empty());
        assert!(
            adosc(&[], &[], &[], &[], 3, 10)
                .expect("adosc empty")
                .is_empty()
        );
        assert!(obv(&[], &[]).expect("obv empty").is_empty());
        assert!(mfi(&[], &[], &[], &[], 14).expect("mfi empty").is_empty());
    }

    #[test]
    fn length_mismatch_errors() {
        let a = [1.0, 2.0, 3.0];
        let b = [1.0, 2.0];
        assert_eq!(
            ad(&a, &b, &a, &a),
            Err(TaError::LengthMismatch { left: 3, right: 2 })
        );
        assert_eq!(
            adosc(&a, &a, &a, &b, 3, 10),
            Err(TaError::LengthMismatch { left: 3, right: 2 })
        );
        assert_eq!(
            obv(&a, &b),
            Err(TaError::LengthMismatch { left: 3, right: 2 })
        );
        assert_eq!(
            mfi(&a, &a, &b, &a, 2),
            Err(TaError::LengthMismatch { left: 3, right: 2 })
        );
    }
}
