//! TA-Lib C 0.4.0 ports for variance, regression, forecast, and correlation.

use crate::{Result, as_f64, check_lengths, check_period, is_zero, is_zero_or_neg, nan_vec};

/// `VAR` — rolling population variance (`ta_VAR.c`, `TA_INT_VAR`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 1`.
pub fn var(input: &[f64], period: usize, nbdev: f64) -> Result<Vec<f64>> {
    let _ = nbdev;
    check_period("optInTimePeriod", period, 1)?;
    let len = input.len();
    // The measured extend form is slower with two mutable accumulators.
    let mut out = nan_vec(len);
    if len < period {
        return Ok(out);
    }
    let lookback = period - 1;
    let mut period_total1 = 0.0_f64;
    let mut period_total2 = 0.0_f64;
    if period > 1 {
        for value in &input[..lookback] {
            let mut temp_real = *value;
            period_total1 += temp_real;
            temp_real *= temp_real;
            period_total2 += temp_real;
        }
    }
    for (trailing, i) in (lookback..len).enumerate() {
        let mut temp_real = input[i];
        period_total1 += temp_real;
        temp_real *= temp_real;
        period_total2 += temp_real;
        let mean_value1 = period_total1 / as_f64(period);
        let mean_value2 = period_total2 / as_f64(period);
        let mut temp_real = input[trailing];
        period_total1 -= temp_real;
        temp_real *= temp_real;
        period_total2 -= temp_real;
        out[i] = mean_value2 - mean_value1 * mean_value1;
    }
    Ok(out)
}

/// `STDDEV` — rolling population standard deviation (`ta_STDDEV.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn stddev(input: &[f64], period: usize, nbdev: f64) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let mut out = var(input, period, nbdev)?;
    for slot in &mut out {
        let temp_real = *slot;
        if !temp_real.is_nan() {
            *slot = if is_zero_or_neg(temp_real) {
                0.0
            } else {
                temp_real.sqrt() * nbdev
            };
        }
    }
    Ok(out)
}

/// Compute C's per-window least-squares fit and emit `(m, b)` for each row.
#[allow(clippy::similar_names)] // sum_x/sum_y/sum_xy deliberately mirror C's SumX/SumY/SumXY.
fn linearreg_core(
    input: &[f64],
    period: usize,
    mut emit: impl FnMut(f64, f64) -> f64,
) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    // Preserve C's oldest-first accumulation; only output construction uses the measured form.
    let mut out = Vec::with_capacity(len);
    if len < period {
        out.resize(len, f64::NAN);
        return Ok(out);
    }
    let lookback = period - 1;
    let sum_x = as_f64(period * (period - 1)) * 0.5;
    let sum_x_sqr = as_f64(period * (period - 1) * (2 * period - 1) / 6);
    let divisor = sum_x * sum_x - as_f64(period) * sum_x_sqr;
    out.resize(lookback, f64::NAN);
    out.extend((lookback..len).map(|today| {
        let mut sum_xy = 0.0_f64;
        let mut sum_y = 0.0_f64;
        let mut i = period;
        while i != 0 {
            i -= 1;
            let temp_value1 = input[today - i];
            sum_y += temp_value1;
            sum_xy += as_f64(i) * temp_value1;
        }
        let m = (as_f64(period) * sum_xy - sum_x * sum_y) / divisor;
        let b = (sum_y - m * sum_x) / as_f64(period);
        emit(m, b)
    }));
    Ok(out)
}

/// `LINEARREG` — rolling linear-regression value at the window's last bar (`ta_LINEARREG.c`): `b + m * (period − 1)`
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn linearreg(input: &[f64], period: usize) -> Result<Vec<f64>> {
    linearreg_core(input, period, |m, b| b + m * as_f64(period - 1))
}

/// `LINEARREG_SLOPE` — the rolling regression slope `m` (`ta_LINEARREG_SLOPE.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn linearreg_slope(input: &[f64], period: usize) -> Result<Vec<f64>> {
    linearreg_core(input, period, |m, _b| m)
}

/// `LINEARREG_INTERCEPT` — the rolling regression intercept `b` (`ta_LINEARREG_INTERCEPT.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn linearreg_intercept(input: &[f64], period: usize) -> Result<Vec<f64>> {
    linearreg_core(input, period, |_m, b| b)
}

/// `LINEARREG_ANGLE` — the slope as degrees (`ta_LINEARREG_ANGLE.c`): `atan(m) * (180 / π)`.
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn linearreg_angle(input: &[f64], period: usize) -> Result<Vec<f64>> {
    let rad_to_deg = 180.0 / std::f64::consts::PI;
    linearreg_core(input, period, |m, _b| m.atan() * rad_to_deg)
}

/// `TSF` — time-series forecast, the regression projected one bar ahead (`ta_TSF.c`): `b + m * period`
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn tsf(input: &[f64], period: usize) -> Result<Vec<f64>> {
    let next_x = as_f64(period);
    linearreg_core(input, period, |m, b| b + m * next_x)
}

/// `CORREL` — rolling Pearson correlation (`ta_CORREL.c`).
/// # Errors
/// `InvalidPeriod` if `period < 1`; `LengthMismatch` if the input series lengths differ.
#[allow(clippy::similar_names)] // sum_x/sum_y/sum_x2/… deliberately mirror C's sumX/sumY/sumX2.
pub fn correl(input0: &[f64], input1: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 1)?;
    check_lengths(input0.len(), &[input1.len()])?;
    let len = input0.len();
    let mut out = nan_vec(len);
    if len < period {
        return Ok(out);
    }
    let lookback = period - 1;
    let mut sum_x = 0.0_f64;
    let mut sum_y = 0.0_f64;
    let mut sum_x2 = 0.0_f64;
    let mut sum_y2 = 0.0_f64;
    let mut sum_xy = 0.0_f64;
    for today in 0..period {
        let x = input0[today];
        sum_x += x;
        sum_x2 += x * x;
        let y = input1[today];
        sum_xy += x * y;
        sum_y += y;
        sum_y2 += y * y;
    }
    let mut trailing = 0;
    let mut trailing_x = input0[trailing];
    let mut trailing_y = input1[trailing];
    trailing += 1;
    let temp_real = (sum_x2 - ((sum_x * sum_x) / as_f64(period)))
        * (sum_y2 - ((sum_y * sum_y) / as_f64(period)));
    out[lookback] = if is_zero_or_neg(temp_real) {
        0.0
    } else {
        (sum_xy - ((sum_x * sum_y) / as_f64(period))) / temp_real.sqrt()
    };
    for today in period..len {
        sum_x -= trailing_x;
        sum_x2 -= trailing_x * trailing_x;
        sum_xy -= trailing_x * trailing_y;
        sum_y -= trailing_y;
        sum_y2 -= trailing_y * trailing_y;
        let x = input0[today];
        sum_x += x;
        sum_x2 += x * x;
        let y = input1[today];
        sum_xy += x * y;
        sum_y += y;
        sum_y2 += y * y;
        trailing_x = input0[trailing];
        trailing_y = input1[trailing];
        trailing += 1;
        let temp_real = (sum_x2 - ((sum_x * sum_x) / as_f64(period)))
            * (sum_y2 - ((sum_y * sum_y) / as_f64(period)));
        out[today] = if is_zero_or_neg(temp_real) {
            0.0
        } else {
            (sum_xy - ((sum_x * sum_y) / as_f64(period))) / temp_real.sqrt()
        };
    }
    Ok(out)
}

/// `BETA` — rolling beta of `price0` vs `price1` (`ta_BETA.c`).
/// # Errors
/// `InvalidPeriod` if `period < 1`; `LengthMismatch` if the input series lengths differ.
#[allow(clippy::similar_names)] // s_xx/s_xy/s_x/s_y mirror C's S_xx/S_xy/S_x/S_y.
pub fn beta(price0: &[f64], price1: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 1)?;
    check_lengths(price0.len(), &[price1.len()])?;
    let len = price0.len();
    let mut out = nan_vec(len);
    if len <= period {
        return Ok(out);
    }
    let n = as_f64(period);
    let mut s_xx = 0.0_f64;
    let mut s_xy = 0.0_f64;
    let mut s_x = 0.0_f64;
    let mut s_y = 0.0_f64;
    let mut last_price_x = price0[0];
    let mut last_price_y = price1[0];
    let mut trailing_last_price_x = price0[0];
    let mut trailing_last_price_y = price1[0];
    let mut trailing_idx = 1_usize;
    let mut i = 1_usize;
    while i < period {
        let tmp = price0[i];
        let x = if is_zero(last_price_x) {
            0.0
        } else {
            (tmp - last_price_x) / last_price_x
        };
        last_price_x = tmp;
        let tmp = price1[i];
        let y = if is_zero(last_price_y) {
            0.0
        } else {
            (tmp - last_price_y) / last_price_y
        };
        last_price_y = tmp;
        i += 1;
        s_xx += x * x;
        s_xy += x * y;
        s_x += x;
        s_y += y;
    }
    let mut out_idx = period;
    loop {
        let tmp = price0[i];
        let x = if is_zero(last_price_x) {
            0.0
        } else {
            (tmp - last_price_x) / last_price_x
        };
        last_price_x = tmp;
        let tmp = price1[i];
        let y = if is_zero(last_price_y) {
            0.0
        } else {
            (tmp - last_price_y) / last_price_y
        };
        last_price_y = tmp;
        i += 1;
        s_xx += x * x;
        s_xy += x * y;
        s_x += x;
        s_y += y;
        let tmp = price0[trailing_idx];
        let trail_x = if is_zero(trailing_last_price_x) {
            0.0
        } else {
            (tmp - trailing_last_price_x) / trailing_last_price_x
        };
        trailing_last_price_x = tmp;
        let tmp = price1[trailing_idx];
        let trail_y = if is_zero(trailing_last_price_y) {
            0.0
        } else {
            (tmp - trailing_last_price_y) / trailing_last_price_y
        };
        trailing_last_price_y = tmp;
        trailing_idx += 1;
        let tmp_real = (n * s_xx) - (s_x * s_x);
        out[out_idx] = if is_zero(tmp_real) {
            0.0
        } else {
            ((n * s_xy) - (s_x * s_y)) / tmp_real
        };
        out_idx += 1;
        s_xx -= trail_x * trail_x;
        s_xy -= trail_x * trail_y;
        s_x -= trail_x;
        s_y -= trail_y;
        if i >= len {
            break;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn var_of_constant_window_is_zero() {
        let out = var(&[3.0; 6], 3, 1.0).expect("valid");
        assert!(out[1].is_nan());
        assert!((out[2] - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn stddev_applies_nbdev_multiplier() {
        let input = [1.0, 2.0, 3.0, 4.0, 5.0];
        let one = stddev(&input, 3, 1.0).expect("valid");
        let two = stddev(&input, 3, 2.0).expect("valid");
        assert!((two[3] - 2.0 * one[3]).abs() < 1e-12);
    }

    #[test]
    fn linearreg_family_agrees_on_perfect_line() {
        let input: Vec<f64> = (0..8).map(|i| 2.0 * f64::from(i) + 1.0).collect();
        let value = linearreg(&input, 4).expect("valid");
        let slope = linearreg_slope(&input, 4).expect("valid");
        let intercept = linearreg_intercept(&input, 4).expect("valid");
        let forecast = tsf(&input, 4).expect("valid");
        assert!(value[2].is_nan());
        assert!((slope[5] - 2.0).abs() < 1e-9);
        assert!((intercept[5] - 5.0).abs() < 1e-9);
        assert!((value[5] - 11.0).abs() < 1e-9);
        assert!((forecast[5] - 13.0).abs() < 1e-9);
    }

    #[test]
    fn correl_perfectly_correlated_is_one() {
        let x: Vec<f64> = (0..10).map(f64::from).collect();
        let y: Vec<f64> = x.iter().map(|v| 3.0 * v + 2.0).collect();
        let out = correl(&x, &y, 5).expect("valid");
        assert!(out[3].is_nan());
        assert!((out[4] - 1.0).abs() < 1e-9);
        assert!((out[9] - 1.0).abs() < 1e-9);
    }

    #[test]
    fn correl_flat_series_guard_yields_zero() {
        let out = correl(&[1.0; 6], &[2.0; 6], 3).expect("valid");
        assert!((out[2] - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn beta_of_identical_series_is_one() {
        let price = [100.0, 105.0, 103.0, 108.0, 110.0, 107.0, 112.0];
        let out = beta(&price, &price, 3).expect("valid");
        assert!(out[0].is_nan());
        assert!(out[1].is_nan());
        assert!(out[2].is_nan());
        for value in &out[3..] {
            assert!(
                (value - 1.0).abs() < 1e-9,
                "beta of identical series = 1, got {value}"
            );
        }
    }

    #[test]
    fn beta_constant_returns_denominator_guard_is_zero() {
        let geometric: Vec<f64> = (0..8).map(|i| 100.0 * 1.1_f64.powi(i)).collect();
        let out = beta(&geometric, &geometric, 4).expect("valid");
        for value in &out[4..] {
            assert!(
                value.abs() < f64::EPSILON,
                "zero-variance beta guard → 0.0, got {value}"
            );
        }
    }
}
