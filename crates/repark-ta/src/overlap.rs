//! TA-Lib C 0.4.0 ports for overlap studies.

use crate::{
    Result, TaError, as_f64, check_lengths, check_period, check_real_param, is_zero,
    is_zero_or_neg, nan_vec,
};

/// `SMA` — simple moving average (`ta_SMA.c`, `TA_INT_SMA`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn sma(input: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    let mut out = nan_vec(len);
    if len < period {
        return Ok(out);
    }
    let lookback = period - 1;
    let mut period_total = 0.0_f64;
    for value in &input[..lookback] {
        period_total += *value;
    }
    let incoming = &input[lookback..len];
    let trailing = &input[..len - lookback];
    for ((output_slot, value), trailing_value) in out[lookback..len]
        .iter_mut()
        .zip(incoming.iter())
        .zip(trailing.iter())
    {
        period_total += *value;
        let temp = period_total;
        period_total -= *trailing_value;
        *output_slot = temp / as_f64(period);
    }
    Ok(out)
}

/// `EMA` — exponential moving average (`ta_EMA.c`, `TA_INT_EMA`, Classic compatibility).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn ema(input: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    // Single-write construction; the push-per-element form is +61% slower.
    let mut out = Vec::with_capacity(len);
    if len < period {
        out.resize(len, f64::NAN);
        return Ok(out);
    }
    let k = 2.0 / (as_f64(period) + 1.0);
    let lookback = period - 1;
    let mut temp = 0.0_f64;
    for value in &input[..period] {
        temp += *value;
    }
    let mut prev_ma = temp / as_f64(period);
    out.resize(lookback, f64::NAN);
    out.push(prev_ma);
    out.extend(input[period..].iter().map(|value| {
        prev_ma = ((value - prev_ma) * k) + prev_ma;
        prev_ma
    }));
    Ok(out)
}

/// `BBANDS` — Bollinger Bands, SMA flavor (`ta_BBANDS.c` + `TA_INT_stddev_using_precalc_ma`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
#[allow(clippy::float_cmp)] // C selects band branches with exact `==` on nbdev; so do we.
pub fn bbands(
    input: &[f64],
    period: usize,
    nbdev_up: f64,
    nbdev_dn: f64,
) -> Result<(Vec<f64>, Vec<f64>, Vec<f64>)> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    let middle = sma(input, period)?;
    let mut upper = nan_vec(len);
    let mut lower = nan_vec(len);
    if len < period {
        return Ok((upper, middle, lower));
    }
    let lookback = period - 1;
    let deviation = stddev_using_precalc_ma(input, &middle, period);

    if nbdev_up == nbdev_dn {
        if nbdev_up == 1.0 {
            for i in lookback..len {
                let dev = deviation[i - lookback];
                upper[i] = middle[i] + dev;
                lower[i] = middle[i] - dev;
            }
        } else {
            for i in lookback..len {
                let dev = deviation[i - lookback] * nbdev_up;
                upper[i] = middle[i] + dev;
                lower[i] = middle[i] - dev;
            }
        }
    } else if nbdev_up == 1.0 {
        for i in lookback..len {
            let dev = deviation[i - lookback];
            upper[i] = middle[i] + dev;
            lower[i] = middle[i] - (dev * nbdev_dn);
        }
    } else if nbdev_dn == 1.0 {
        for i in lookback..len {
            let dev = deviation[i - lookback];
            lower[i] = middle[i] - dev;
            upper[i] = middle[i] + (dev * nbdev_up);
        }
    } else {
        for i in lookback..len {
            let dev = deviation[i - lookback];
            upper[i] = middle[i] + (dev * nbdev_up);
            lower[i] = middle[i] - (dev * nbdev_dn);
        }
    }
    Ok((upper, middle, lower))
}

/// Compute C's standard deviation over a precomputed SMA.
fn stddev_using_precalc_ma(input: &[f64], ma: &[f64], period: usize) -> Vec<f64> {
    let len = input.len();
    let lookback = period - 1;
    let nb = len - lookback;
    let mut out = Vec::with_capacity(nb);
    let mut start_sum = 0;
    let mut end_sum = lookback;
    let mut period_total2 = 0.0_f64;
    for value in &input[start_sum..end_sum] {
        let mut temp = *value;
        temp *= temp;
        period_total2 += temp;
    }
    for out_idx in 0..nb {
        let mut temp = input[end_sum];
        temp *= temp;
        period_total2 += temp;
        let mut mean_value2 = period_total2 / as_f64(period);
        let mut temp = input[start_sum];
        temp *= temp;
        period_total2 -= temp;
        let mut temp = ma[out_idx + lookback];
        temp *= temp;
        mean_value2 -= temp;
        out.push(if is_zero_or_neg(mean_value2) {
            0.0
        } else {
            mean_value2.sqrt()
        });
        start_sum += 1;
        end_sum += 1;
    }
    out
}

/// `WMA` — weighted moving average (`ta_WMA.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
#[allow(clippy::similar_names)]
pub fn wma(input: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    let mut out = nan_vec(len);
    let lookback = period - 1;
    if len <= lookback {
        return Ok(out);
    }
    let divider = (period * (period + 1)) / 2;
    let mut trailing_idx = 0_usize;
    let mut period_sum = 0.0_f64;
    let mut period_sub = 0.0_f64;
    let mut in_idx = 0_usize;
    let mut weight = 1_usize;
    while in_idx < lookback {
        let temp = input[in_idx];
        in_idx += 1;
        period_sub += temp;
        period_sum += temp * as_f64(weight);
        weight += 1;
    }
    let mut trailing_value = 0.0_f64;
    let mut out_idx = lookback;
    while in_idx < len {
        let temp = input[in_idx];
        in_idx += 1;
        period_sub += temp;
        period_sub -= trailing_value;
        period_sum += temp * as_f64(period);
        trailing_value = input[trailing_idx];
        trailing_idx += 1;
        out[out_idx] = period_sum / as_f64(divider);
        out_idx += 1;
        period_sum -= period_sub;
    }
    Ok(out)
}

/// `DEMA` — double exponential moving average (`ta_DEMA.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn dema(input: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    let mut out = nan_vec(len);
    let lookback = period - 1;
    if len <= 2 * lookback {
        return Ok(out);
    }
    let first = ema(input, period)?;
    let first_dense = &first[lookback..];
    let second = ema(first_dense, period)?;
    for out_idx in 0..(len - 2 * lookback) {
        let first_ema = first_dense[lookback + out_idx];
        let second_ema = second[lookback + out_idx];
        out[2 * lookback + out_idx] = (2.0 * first_ema) - second_ema;
    }
    Ok(out)
}

/// `TEMA` — triple exponential moving average (`ta_TEMA.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn tema(input: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    let mut out = nan_vec(len);
    let lookback = period - 1;
    if len <= 3 * lookback {
        return Ok(out);
    }
    let first = ema(input, period)?;
    let first_dense = &first[lookback..];
    let second = ema(first_dense, period)?;
    let second_dense = &second[lookback..];
    let third = ema(second_dense, period)?;
    for out_idx in 0..(len - 3 * lookback) {
        let e1 = first_dense[2 * lookback + out_idx];
        let e2 = second_dense[lookback + out_idx];
        let e3 = third[lookback + out_idx];
        out[3 * lookback + out_idx] = e3 + ((3.0 * e1) - (3.0 * e2));
    }
    Ok(out)
}

/// `TRIMA` — triangular moving average (`ta_TRIMA.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn trima(input: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    let mut out = nan_vec(len);
    let lookback = period - 1;
    if len <= lookback {
        return Ok(out);
    }
    let half = period / 2;
    if period % 2 == 1 {
        let mut factor = as_f64((half + 1) * (half + 1));
        factor = 1.0 / factor;
        let mut trailing_idx = 0_usize;
        let mut middle_idx = trailing_idx + half;
        let mut today_idx = middle_idx + half;
        let mut numerator = 0.0_f64;
        let mut numerator_sub = 0.0_f64;
        for idx in (trailing_idx..=middle_idx).rev() {
            let temp = input[idx];
            numerator_sub += temp;
            numerator += numerator_sub;
        }
        let mut numerator_add = 0.0_f64;
        middle_idx += 1;
        for &temp in &input[middle_idx..=today_idx] {
            numerator_add += temp;
            numerator += numerator_add;
        }
        let mut out_pos = lookback;
        let mut temp_real = input[trailing_idx];
        trailing_idx += 1;
        out[out_pos] = numerator * factor;
        out_pos += 1;
        today_idx += 1;
        while today_idx < len {
            numerator -= numerator_sub;
            numerator_sub -= temp_real;
            temp_real = input[middle_idx];
            middle_idx += 1;
            numerator_sub += temp_real;
            numerator += numerator_add;
            numerator_add -= temp_real;
            temp_real = input[today_idx];
            today_idx += 1;
            numerator_add += temp_real;
            numerator += temp_real;
            temp_real = input[trailing_idx];
            trailing_idx += 1;
            out[out_pos] = numerator * factor;
            out_pos += 1;
        }
    } else {
        let mut factor = as_f64(half * (half + 1));
        factor = 1.0 / factor;
        let mut trailing_idx = 0_usize;
        let mut middle_idx = trailing_idx + half - 1;
        let mut today_idx = middle_idx + half;
        let mut numerator = 0.0_f64;
        let mut numerator_sub = 0.0_f64;
        for idx in (trailing_idx..=middle_idx).rev() {
            let temp = input[idx];
            numerator_sub += temp;
            numerator += numerator_sub;
        }
        let mut numerator_add = 0.0_f64;
        middle_idx += 1;
        for &temp in &input[middle_idx..=today_idx] {
            numerator_add += temp;
            numerator += numerator_add;
        }
        let mut out_pos = lookback;
        let mut temp_real = input[trailing_idx];
        trailing_idx += 1;
        out[out_pos] = numerator * factor;
        out_pos += 1;
        today_idx += 1;
        while today_idx < len {
            numerator -= numerator_sub;
            numerator_sub -= temp_real;
            temp_real = input[middle_idx];
            middle_idx += 1;
            numerator_sub += temp_real;
            numerator_add -= temp_real;
            numerator += numerator_add;
            temp_real = input[today_idx];
            today_idx += 1;
            numerator_add += temp_real;
            numerator += temp_real;
            temp_real = input[trailing_idx];
            trailing_idx += 1;
            out[out_pos] = numerator * factor;
            out_pos += 1;
        }
    }
    Ok(out)
}

/// `KAMA` — Kaufman adaptive moving average (`ta_KAMA.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn kama(input: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    let mut out = nan_vec(len);
    let lookback = period; // lookbackTotal = period (unstable period is fixed at 0)
    if len <= lookback {
        return Ok(out);
    }
    let const_max = 2.0 / (30.0 + 1.0);
    let const_diff = 2.0 / (2.0 + 1.0) - const_max;

    let mut sum_roc1 = 0.0_f64;
    let mut today = 0_usize;
    let mut trailing_idx = today;
    for _ in 0..period {
        let mut temp = input[today];
        today += 1;
        temp -= input[today];
        sum_roc1 += temp.abs();
    }

    let mut prev_kama = input[today - 1];
    let temp = input[today];
    let temp2 = input[trailing_idx];
    trailing_idx += 1;
    let period_roc = temp - temp2;
    let mut trailing_value = temp2;
    let er = if sum_roc1 <= period_roc || is_zero(sum_roc1) {
        1.0
    } else {
        (period_roc / sum_roc1).abs()
    };
    let mut sc = (er * const_diff) + const_max;
    sc *= sc;
    prev_kama = ((input[today] - prev_kama) * sc) + prev_kama;
    today += 1;

    let mut out_pos = lookback; // outBegIdx = today − 1 = period
    out[out_pos] = prev_kama;
    out_pos += 1;
    while today < len {
        let temp = input[today];
        let temp2 = input[trailing_idx];
        trailing_idx += 1;
        let period_roc = temp - temp2;
        sum_roc1 -= (trailing_value - temp2).abs();
        sum_roc1 += (temp - input[today - 1]).abs();
        trailing_value = temp2;
        let er = if sum_roc1 <= period_roc || is_zero(sum_roc1) {
            1.0
        } else {
            (period_roc / sum_roc1).abs()
        };
        let mut sc = (er * const_diff) + const_max;
        sc *= sc;
        prev_kama = ((input[today] - prev_kama) * sc) + prev_kama;
        today += 1;
        out[out_pos] = prev_kama;
        out_pos += 1;
    }
    Ok(out)
}

/// `T3` — Tillson T3 moving average (`ta_T3.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn t3(input: &[f64], period: usize, vfactor: f64) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    let mut out = nan_vec(len);
    let lookback = 6 * (period - 1);
    if len <= lookback {
        return Ok(out);
    }
    let k = 2.0 / (as_f64(period) + 1.0);
    let one_minus_k = 1.0 - k;
    let mut today = 0_usize;

    let mut temp = input[today];
    today += 1;
    for _ in 0..(period - 1) {
        temp += input[today];
        today += 1;
    }
    let mut e1 = temp / as_f64(period);

    let mut temp = e1;
    for _ in 0..(period - 1) {
        e1 = (k * input[today]) + (one_minus_k * e1);
        today += 1;
        temp += e1;
    }
    let mut e2 = temp / as_f64(period);

    let mut temp = e2;
    for _ in 0..(period - 1) {
        e1 = (k * input[today]) + (one_minus_k * e1);
        today += 1;
        e2 = (k * e1) + (one_minus_k * e2);
        temp += e2;
    }
    let mut e3 = temp / as_f64(period);

    let mut temp = e3;
    for _ in 0..(period - 1) {
        e1 = (k * input[today]) + (one_minus_k * e1);
        today += 1;
        e2 = (k * e1) + (one_minus_k * e2);
        e3 = (k * e2) + (one_minus_k * e3);
        temp += e3;
    }
    let mut e4 = temp / as_f64(period);

    let mut temp = e4;
    for _ in 0..(period - 1) {
        e1 = (k * input[today]) + (one_minus_k * e1);
        today += 1;
        e2 = (k * e1) + (one_minus_k * e2);
        e3 = (k * e2) + (one_minus_k * e3);
        e4 = (k * e3) + (one_minus_k * e4);
        temp += e4;
    }
    let mut e5 = temp / as_f64(period);

    let mut temp = e5;
    for _ in 0..(period - 1) {
        e1 = (k * input[today]) + (one_minus_k * e1);
        today += 1;
        e2 = (k * e1) + (one_minus_k * e2);
        e3 = (k * e2) + (one_minus_k * e3);
        e4 = (k * e3) + (one_minus_k * e4);
        e5 = (k * e4) + (one_minus_k * e5);
        temp += e5;
    }
    let mut e6 = temp / as_f64(period);

    let temp = vfactor * vfactor;
    let c1 = -(temp * vfactor);
    let c2 = 3.0 * (temp - c1);
    let c3 = -6.0 * temp - 3.0 * (vfactor - c1);
    let c4 = 1.0 + 3.0 * vfactor - c1 + 3.0 * temp;

    let mut out_pos = lookback;
    out[out_pos] = c1 * e6 + c2 * e5 + c3 * e4 + c4 * e3;
    out_pos += 1;
    while today < len {
        e1 = (k * input[today]) + (one_minus_k * e1);
        today += 1;
        e2 = (k * e1) + (one_minus_k * e2);
        e3 = (k * e2) + (one_minus_k * e3);
        e4 = (k * e3) + (one_minus_k * e4);
        e5 = (k * e4) + (one_minus_k * e5);
        e6 = (k * e5) + (one_minus_k * e6);
        out[out_pos] = c1 * e6 + c2 * e5 + c3 * e4 + c4 * e3;
        out_pos += 1;
    }
    Ok(out)
}

/// `MIDPOINT` — `(highest + lowest) / 2` over the trailing window of one series (`ta_MIDPOINT.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
// Preserve C's literal addition and division; `f64::midpoint` rounds differently.
#[allow(clippy::manual_midpoint)]
pub fn midpoint(input: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    let mut out = nan_vec(len);
    let lookback = period - 1;
    if len <= lookback {
        return Ok(out);
    }
    let mut today = lookback;
    let mut trailing_idx = 0_usize;
    while today < len {
        let mut lowest = input[trailing_idx];
        trailing_idx += 1;
        let mut highest = lowest;
        for &tmp in &input[trailing_idx..=today] {
            if tmp < lowest {
                lowest = tmp;
            } else if tmp > highest {
                highest = tmp;
            }
        }
        out[today] = (highest + lowest) / 2.0;
        today += 1;
    }
    Ok(out)
}

/// `MIDPRICE` — `(highest high + lowest low) / 2` over the trailing window (`ta_MIDPRICE.c`).
/// # Errors
/// `InvalidPeriod` if `period < 2`; `LengthMismatch` if the input series lengths differ.
// Preserve C's literal addition and division.
#[allow(clippy::manual_midpoint)]
pub fn midprice(high: &[f64], low: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    check_lengths(high.len(), &[low.len()])?;
    let len = high.len();
    let mut out = nan_vec(len);
    let lookback = period - 1;
    if len <= lookback {
        return Ok(out);
    }
    let mut today = lookback;
    let mut trailing_idx = 0_usize;
    while today < len {
        let mut lowest = low[trailing_idx];
        let mut highest = high[trailing_idx];
        trailing_idx += 1;
        for idx in trailing_idx..=today {
            let tmp = low[idx];
            if tmp < lowest {
                lowest = tmp;
            }
            let tmp = high[idx];
            if tmp > highest {
                highest = tmp;
            }
        }
        out[today] = (highest + lowest) / 2.0;
        today += 1;
    }
    Ok(out)
}

/// `MA` — the moving-average selector (`ta_MA.c`).
/// # Errors
/// `InvalidPeriod` if `period < 1`; `UnsupportedMaType` for an unknown MA type.
pub fn ma(input: &[f64], period: usize, matype: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 1)?;
    crate::momentum::ma_dispatch(input, period, matype)
}

/// Return MAMA's fixed 32-bar lookback.
const MAMA_LOOKBACK: usize = 32;

/// Hilbert-transform FIR coefficients used by TA-Lib.
const HILBERT_A: f64 = 0.0962;
const HILBERT_B: f64 = 0.5769;

/// Hold one Hilbert-transform variable for each bar parity.
struct HilbertVar {
    buf: [[f64; 3]; 2],
    prev: [f64; 2],
    prev_input: [f64; 2],
}

impl HilbertVar {
    fn new() -> Self {
        Self {
            buf: [[0.0; 3]; 2],
            prev: [0.0; 2],
            prev_input: [0.0; 2],
        }
    }

    /// Apply C's Hilbert-transform recurrence for one bar parity.
    fn transform(
        &mut self,
        input: f64,
        hilbert_idx: usize,
        adjusted_prev_period: f64,
        parity: usize,
    ) -> f64 {
        let hilbert_temp_real = HILBERT_A * input;
        let mut var = -self.buf[parity][hilbert_idx];
        self.buf[parity][hilbert_idx] = hilbert_temp_real;
        var += hilbert_temp_real;
        var -= self.prev[parity];
        self.prev[parity] = HILBERT_B * self.prev_input[parity];
        var += self.prev[parity];
        self.prev_input[parity] = input;
        var *= adjusted_prev_period;
        var
    }
}

/// Advance MAMA's four-period WMA price smoother.
#[allow(clippy::similar_names)] // period_wma_sub / period_wma_sum are C's periodWMASub / periodWMASum.
fn do_price_wma(
    new_price: f64,
    period_wma_sub: &mut f64,
    period_wma_sum: &mut f64,
    trailing_wma_value: &mut f64,
    trailing_wma_idx: &mut usize,
    input: &[f64],
) -> f64 {
    *period_wma_sub += new_price;
    *period_wma_sub -= *trailing_wma_value;
    *period_wma_sum += new_price * 4.0;
    *trailing_wma_value = input[*trailing_wma_idx];
    *trailing_wma_idx += 1;
    let smoothed = *period_wma_sum * 0.1;
    *period_wma_sum -= *period_wma_sub;
    smoothed
}

/// `MAMA` — MESA Adaptive Moving Average, with FAMA (`ta_MAMA.c`).
/// # Errors
/// [`crate::TaError::InvalidRealParam`] if either limit is outside `[0.01, 0.99]`.
#[allow(clippy::similar_names)] // prev_i2/prev_q2, i1_for_odd/even mirror ta_MAMA.c's names.
#[allow(clippy::float_cmp)] // C guards the atan/period divisors with an exact `!= 0.0`; so do we.
#[allow(clippy::if_not_else)] // C's `if(I1 != 0.0) atan else 0.0` order — kept for a faithful port.
#[allow(clippy::many_single_char_names)] // re/im/q2/i2/ji/jq mirror the Ehlers/TA-Lib variables.
#[allow(clippy::manual_clamp)] // C's two-statement `if period<6…else if period>50…` period clamp.
#[allow(clippy::too_many_lines)] // one op-for-op port of ta_MAMA.c's single function.
pub fn mama(input: &[f64], fast_limit: f64, slow_limit: f64) -> Result<(Vec<f64>, Vec<f64>)> {
    check_real_param("optInFastLimit", fast_limit, 0.01, 0.99, "0.01..=0.99")?;
    check_real_param("optInSlowLimit", slow_limit, 0.01, 0.99, "0.01..=0.99")?;
    let len = input.len();
    let mut out_mama = nan_vec(len);
    let mut out_fama = nan_vec(len);
    let lookback = MAMA_LOOKBACK;
    if len <= lookback {
        return Ok((out_mama, out_fama));
    }
    let rad2deg = 180.0 / (4.0 * (1.0_f64).atan());

    let mut trailing_wma_idx = 0_usize; // startIdx − lookbackTotal = 0 for the full array
    let mut today = 0_usize;
    let mut temp_real = input[today];
    today += 1;
    let mut period_wma_sub = temp_real;
    let mut period_wma_sum = temp_real;
    temp_real = input[today];
    today += 1;
    period_wma_sub += temp_real;
    period_wma_sum += temp_real * 2.0;
    temp_real = input[today];
    today += 1;
    period_wma_sub += temp_real;
    period_wma_sum += temp_real * 3.0;
    let mut trailing_wma_value = 0.0_f64;

    for _ in 0..9 {
        temp_real = input[today];
        today += 1;
        do_price_wma(
            temp_real,
            &mut period_wma_sub,
            &mut period_wma_sum,
            &mut trailing_wma_value,
            &mut trailing_wma_idx,
            input,
        );
    }

    let mut hilbert_idx = 0_usize;
    let mut detrender_h = HilbertVar::new();
    let mut q1_h = HilbertVar::new();
    let mut ji_h = HilbertVar::new();
    let mut jq_h = HilbertVar::new();
    let mut period = 0.0_f64;
    let mut prev_i2 = 0.0_f64;
    let mut prev_q2 = 0.0_f64;
    let mut re = 0.0_f64;
    let mut im = 0.0_f64;
    let mut mama_value = 0.0_f64;
    let mut fama_value = 0.0_f64;
    let mut i1_for_odd_prev3 = 0.0_f64;
    let mut i1_for_even_prev3 = 0.0_f64;
    let mut i1_for_odd_prev2 = 0.0_f64;
    let mut i1_for_even_prev2 = 0.0_f64;
    let mut prev_phase = 0.0_f64;

    while today < len {
        let adjusted_prev_period = (0.075 * period) + 0.54;
        let today_value = input[today];
        let smoothed_value = do_price_wma(
            today_value,
            &mut period_wma_sub,
            &mut period_wma_sum,
            &mut trailing_wma_value,
            &mut trailing_wma_idx,
            input,
        );

        let q2;
        let i2;
        let alpha_deg; // tempReal2 — the phase, in degrees
        if today.is_multiple_of(2) {
            let detrender =
                detrender_h.transform(smoothed_value, hilbert_idx, adjusted_prev_period, 0);
            let q1 = q1_h.transform(detrender, hilbert_idx, adjusted_prev_period, 0);
            let ji = ji_h.transform(i1_for_even_prev3, hilbert_idx, adjusted_prev_period, 0);
            let jq = jq_h.transform(q1, hilbert_idx, adjusted_prev_period, 0);
            hilbert_idx += 1;
            if hilbert_idx == 3 {
                hilbert_idx = 0;
            }
            q2 = (0.2 * (q1 + ji)) + (0.8 * prev_q2);
            i2 = (0.2 * (i1_for_even_prev3 - jq)) + (0.8 * prev_i2);
            i1_for_odd_prev3 = i1_for_odd_prev2;
            i1_for_odd_prev2 = detrender;
            alpha_deg = if i1_for_even_prev3 != 0.0 {
                (q1 / i1_for_even_prev3).atan() * rad2deg
            } else {
                0.0
            };
        } else {
            let detrender =
                detrender_h.transform(smoothed_value, hilbert_idx, adjusted_prev_period, 1);
            let q1 = q1_h.transform(detrender, hilbert_idx, adjusted_prev_period, 1);
            let ji = ji_h.transform(i1_for_odd_prev3, hilbert_idx, adjusted_prev_period, 1);
            let jq = jq_h.transform(q1, hilbert_idx, adjusted_prev_period, 1);
            q2 = (0.2 * (q1 + ji)) + (0.8 * prev_q2);
            i2 = (0.2 * (i1_for_odd_prev3 - jq)) + (0.8 * prev_i2);
            i1_for_even_prev3 = i1_for_even_prev2;
            i1_for_even_prev2 = detrender;
            alpha_deg = if i1_for_odd_prev3 != 0.0 {
                (q1 / i1_for_odd_prev3).atan() * rad2deg
            } else {
                0.0
            };
        }

        let mut delta_phase = prev_phase - alpha_deg;
        prev_phase = alpha_deg;
        if delta_phase < 1.0 {
            delta_phase = 1.0;
        }
        let alpha = if delta_phase > 1.0 {
            let scaled = fast_limit / delta_phase;
            if scaled < slow_limit {
                slow_limit
            } else {
                scaled
            }
        } else {
            fast_limit
        };

        mama_value = (alpha * today_value) + ((1.0 - alpha) * mama_value);
        let half_alpha = alpha * 0.5;
        fama_value = (half_alpha * mama_value) + ((1.0 - half_alpha) * fama_value);
        if today >= lookback {
            out_mama[today] = mama_value;
            out_fama[today] = fama_value;
        }

        re = (0.2 * ((i2 * prev_i2) + (q2 * prev_q2))) + (0.8 * re);
        im = (0.2 * ((i2 * prev_q2) - (q2 * prev_i2))) + (0.8 * im);
        prev_q2 = q2;
        prev_i2 = i2;
        let prev_period = period;
        if im != 0.0 && re != 0.0 {
            period = 360.0 / ((im / re).atan() * rad2deg);
        }
        let mut bound = 1.5 * prev_period;
        if period > bound {
            period = bound;
        }
        bound = 0.67 * prev_period;
        if period < bound {
            period = bound;
        }
        if period < 6.0 {
            period = 6.0;
        } else if period > 50.0 {
            period = 50.0;
        }
        period = (0.2 * period) + (0.8 * prev_period);
        today += 1;
    }
    Ok((out_mama, out_fama))
}

/// `SAR` — Parabolic SAR (`ta_SAR.c`; `SAR_ROUNDING` is a no-op in the default build — TA-Lib does
/// # Errors
/// `InvalidRealParam` if `acceleration` or `maximum` is outside `[0, 3e37]`.
#[allow(clippy::too_many_lines)] // one op-for-op port of ta_SAR.c's single function.
pub fn sar(high: &[f64], low: &[f64], acceleration: f64, maximum: f64) -> Result<Vec<f64>> {
    check_real_param(
        "optInAcceleration",
        acceleration,
        0.0,
        crate::TA_REAL_MAX,
        "0..=3e37",
    )?;
    check_real_param("optInMaximum", maximum, 0.0, crate::TA_REAL_MAX, "0..=3e37")?;
    check_lengths(high.len(), &[low.len()])?;
    let len = high.len();
    let mut out = nan_vec(len);
    if len < 2 {
        return Ok(out); // startIdx (1) > endIdx (< 1) → no output
    }
    let mut acceleration = acceleration;
    let mut af = acceleration;
    if af > maximum {
        af = maximum;
        acceleration = maximum;
    }
    let minus_dm1 = crate::minus_dm(&high[..2], &low[..2], 1)?[1];
    let mut is_long = minus_dm1 <= 0.0;
    let (mut ep, mut sar_value) = if is_long {
        (high[1], low[0])
    } else {
        (low[1], high[0])
    };
    let mut new_low = low[1];
    let mut new_high = high[1];
    let mut today_idx = 1_usize;
    let mut out_pos = 1_usize; // outBegIdx = startIdx = 1
    while today_idx < len {
        let prev_low = new_low;
        let prev_high = new_high;
        new_low = low[today_idx];
        new_high = high[today_idx];
        today_idx += 1;
        if is_long {
            if new_low <= sar_value {
                is_long = false;
                sar_value = ep;
                if sar_value < prev_high {
                    sar_value = prev_high;
                }
                if sar_value < new_high {
                    sar_value = new_high;
                }
                out[out_pos] = sar_value;
                out_pos += 1;
                af = acceleration;
                ep = new_low;
                sar_value += af * (ep - sar_value);
                if sar_value < prev_high {
                    sar_value = prev_high;
                }
                if sar_value < new_high {
                    sar_value = new_high;
                }
            } else {
                out[out_pos] = sar_value;
                out_pos += 1;
                if new_high > ep {
                    ep = new_high;
                    af += acceleration;
                    if af > maximum {
                        af = maximum;
                    }
                }
                sar_value += af * (ep - sar_value);
                if sar_value > prev_low {
                    sar_value = prev_low;
                }
                if sar_value > new_low {
                    sar_value = new_low;
                }
            }
        } else if new_high >= sar_value {
            is_long = true;
            sar_value = ep;
            if sar_value > prev_low {
                sar_value = prev_low;
            }
            if sar_value > new_low {
                sar_value = new_low;
            }
            out[out_pos] = sar_value;
            out_pos += 1;
            af = acceleration;
            ep = new_high;
            sar_value += af * (ep - sar_value);
            if sar_value > prev_low {
                sar_value = prev_low;
            }
            if sar_value > new_low {
                sar_value = new_low;
            }
        } else {
            out[out_pos] = sar_value;
            out_pos += 1;
            if new_low < ep {
                ep = new_low;
                af += acceleration;
                if af > maximum {
                    af = maximum;
                }
            }
            sar_value += af * (ep - sar_value);
            if sar_value < prev_high {
                sar_value = prev_high;
            }
            if sar_value < new_high {
                sar_value = new_high;
            }
        }
    }
    Ok(out)
}

/// `SAREXT` — Parabolic SAR, extended (`ta_SAREXT.c`).
/// # Errors
/// `InvalidRealParam` if any parameter is outside its documented range.
#[allow(clippy::too_many_arguments)] // eight optional parameters, exactly TA-Lib's SAREXT signature.
#[allow(clippy::float_cmp)] // C compares start_value / offset with exact `== 0.0` / `!= 0.0`.
#[allow(clippy::too_many_lines)] // one op-for-op port of ta_SAREXT.c's single function.
pub fn sarext(
    high: &[f64],
    low: &[f64],
    start_value: f64,
    offset_on_reverse: f64,
    accel_init_long: f64,
    accel_long: f64,
    accel_max_long: f64,
    accel_init_short: f64,
    accel_short: f64,
    accel_max_short: f64,
) -> Result<Vec<f64>> {
    check_real_param(
        "optInStartValue",
        start_value,
        -crate::TA_REAL_MAX,
        crate::TA_REAL_MAX,
        "-3e37..=3e37",
    )?;
    check_real_param(
        "optInOffsetOnReverse",
        offset_on_reverse,
        0.0,
        crate::TA_REAL_MAX,
        "0..=3e37",
    )?;
    check_real_param(
        "optInAccelerationInitLong",
        accel_init_long,
        0.0,
        crate::TA_REAL_MAX,
        "0..=3e37",
    )?;
    check_real_param(
        "optInAccelerationLong",
        accel_long,
        0.0,
        crate::TA_REAL_MAX,
        "0..=3e37",
    )?;
    check_real_param(
        "optInAccelerationMaxLong",
        accel_max_long,
        0.0,
        crate::TA_REAL_MAX,
        "0..=3e37",
    )?;
    check_real_param(
        "optInAccelerationInitShort",
        accel_init_short,
        0.0,
        crate::TA_REAL_MAX,
        "0..=3e37",
    )?;
    check_real_param(
        "optInAccelerationShort",
        accel_short,
        0.0,
        crate::TA_REAL_MAX,
        "0..=3e37",
    )?;
    check_real_param(
        "optInAccelerationMaxShort",
        accel_max_short,
        0.0,
        crate::TA_REAL_MAX,
        "0..=3e37",
    )?;
    check_lengths(high.len(), &[low.len()])?;
    let len = high.len();
    let mut out = nan_vec(len);
    if len < 2 {
        return Ok(out);
    }
    let mut accel_init_long = accel_init_long;
    let mut accel_init_short = accel_init_short;
    let mut accel_long = accel_long;
    let mut accel_short = accel_short;
    let mut af_long = accel_init_long;
    let mut af_short = accel_init_short;
    if af_long > accel_max_long {
        af_long = accel_max_long;
        accel_init_long = accel_max_long;
    }
    if accel_long > accel_max_long {
        accel_long = accel_max_long;
    }
    if af_short > accel_max_short {
        af_short = accel_max_short;
        accel_init_short = accel_max_short;
    }
    if accel_short > accel_max_short {
        accel_short = accel_max_short;
    }
    let mut is_long = if start_value == 0.0 {
        crate::minus_dm(&high[..2], &low[..2], 1)?[1] <= 0.0
    } else {
        start_value > 0.0
    };
    let (mut ep, mut sar_value) = if start_value == 0.0 {
        if is_long {
            (high[1], low[0])
        } else {
            (low[1], high[0])
        }
    } else if start_value > 0.0 {
        (high[1], start_value)
    } else {
        (low[1], start_value.abs())
    };
    let mut new_low = low[1];
    let mut new_high = high[1];
    let mut today_idx = 1_usize;
    let mut out_pos = 1_usize;
    while today_idx < len {
        let prev_low = new_low;
        let prev_high = new_high;
        new_low = low[today_idx];
        new_high = high[today_idx];
        today_idx += 1;
        if is_long {
            if new_low <= sar_value {
                is_long = false;
                sar_value = ep;
                if sar_value < prev_high {
                    sar_value = prev_high;
                }
                if sar_value < new_high {
                    sar_value = new_high;
                }
                if offset_on_reverse != 0.0 {
                    sar_value += sar_value * offset_on_reverse;
                }
                out[out_pos] = -sar_value;
                out_pos += 1;
                af_short = accel_init_short;
                ep = new_low;
                sar_value += af_short * (ep - sar_value);
                if sar_value < prev_high {
                    sar_value = prev_high;
                }
                if sar_value < new_high {
                    sar_value = new_high;
                }
            } else {
                out[out_pos] = sar_value;
                out_pos += 1;
                if new_high > ep {
                    ep = new_high;
                    af_long += accel_long;
                    if af_long > accel_max_long {
                        af_long = accel_max_long;
                    }
                }
                sar_value += af_long * (ep - sar_value);
                if sar_value > prev_low {
                    sar_value = prev_low;
                }
                if sar_value > new_low {
                    sar_value = new_low;
                }
            }
        } else if new_high >= sar_value {
            is_long = true;
            sar_value = ep;
            if sar_value > prev_low {
                sar_value = prev_low;
            }
            if sar_value > new_low {
                sar_value = new_low;
            }
            if offset_on_reverse != 0.0 {
                sar_value -= sar_value * offset_on_reverse;
            }
            out[out_pos] = sar_value;
            out_pos += 1;
            af_long = accel_init_long;
            ep = new_high;
            sar_value += af_long * (ep - sar_value);
            if sar_value > prev_low {
                sar_value = prev_low;
            }
            if sar_value > new_low {
                sar_value = new_low;
            }
        } else {
            out[out_pos] = -sar_value;
            out_pos += 1;
            if new_low < ep {
                ep = new_low;
                af_short += accel_short;
                if af_short > accel_max_short {
                    af_short = accel_max_short;
                }
            }
            sar_value += af_short * (ep - sar_value);
            if sar_value < prev_high {
                sar_value = prev_high;
            }
            if sar_value < new_high {
                sar_value = new_high;
            }
        }
    }
    Ok(out)
}

/// Truncate and clamp one MAVP period using C's cast semantics.
#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_possible_wrap
)]
#[allow(clippy::manual_clamp)]
fn clamp_period(raw: f64, min_period: usize, max_period: usize) -> usize {
    let truncated = raw as i64;
    if truncated < min_period as i64 {
        min_period
    } else if truncated > max_period as i64 {
        max_period
    } else {
        truncated as usize
    }
}

/// `MAVP` — Moving Average with Variable Period (`ta_MAVP.c`).
/// # Errors
/// `InvalidPeriod` if `min_period`/`max_period` is outside `2..=MAX_PERIOD`.
pub fn mavp(
    input: &[f64],
    periods: &[f64],
    min_period: usize,
    max_period: usize,
    matype: usize,
) -> Result<Vec<f64>> {
    check_period("optInMinPeriod", min_period, 2)?;
    check_period("optInMaxPeriod", max_period, 2)?;
    if matype > 8 {
        return Err(TaError::UnsupportedMaType {
            matype,
            reason: "not a TA-Lib MA type (expected 0..=8)",
        });
    }
    check_lengths(input.len(), &[periods.len()])?;
    let len = input.len();
    let mut out = nan_vec(len);
    if matype == 7 {
        return Ok(mama(input, 0.5, 0.05)?.0);
    }
    let lookback = crate::momentum::ma_lookback(max_period, matype)?;
    if len <= lookback {
        return Ok(out);
    }
    let out_len = len - lookback;
    let mut clamped: Vec<usize> = Vec::with_capacity(out_len);
    for &raw in &periods[lookback..] {
        clamped.push(clamp_period(raw, min_period, max_period));
    }
    for i in 0..out_len {
        let cur = clamped[i];
        if cur != 0 {
            let ma_dense = crate::momentum::ma_range(input, cur, matype, lookback, len - 1)?;
            out[lookback + i] = ma_dense[i];
            for j in (i + 1)..out_len {
                if clamped[j] == cur {
                    clamped[j] = 0;
                    out[lookback + j] = ma_dense[j];
                }
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaError;

    #[test]
    fn ma_selector_dispatches_and_identity() {
        fn bit_eq(a: &[f64], b: &[f64]) -> bool {
            a.len() == b.len()
                && a.iter()
                    .zip(b)
                    .all(|(x, y)| x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan()))
        }
        let data: Vec<f64> = (1..=10).map(f64::from).collect();
        assert!(bit_eq(
            &ma(&data, 3, 0).expect("ma sma"),
            &sma(&data, 3).expect("sma")
        ));
        assert!(bit_eq(
            &ma(&data, 3, 1).expect("ma ema"),
            &ema(&data, 3).expect("ema")
        ));
        assert_eq!(ma(&data, 1, 0).expect("ma id"), data);
        assert_eq!(ma(&data, 1, 7).expect("ma id mama"), data);
    }

    #[test]
    fn ma_selector_matype7_dispatches_to_mama_and_rejects_out_of_range() {
        let data = [1.0, 2.0, 3.0, 4.0];
        let out = ma(&data, 3, 7).expect("ma matype 7 short");
        assert_eq!(out.len(), data.len());
        assert!(out.iter().all(|v| v.is_nan()));
        let long: Vec<f64> = (1..=40).map(f64::from).collect();
        let (mama_out, _) = mama(&long, 0.5, 0.05).expect("mama");
        for period in [30, 100] {
            let via_ma = ma(&long, period, 7).expect("ma matype 7 long");
            assert_eq!(via_ma.len(), mama_out.len());
            assert!(
                via_ma
                    .iter()
                    .zip(&mama_out)
                    .all(|(x, y)| x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())),
                "ma(period={period}, matype=7) must equal MAMA regardless of period"
            );
        }
        assert!(matches!(
            ma(&data, 3, 9),
            Err(TaError::UnsupportedMaType { matype: 9, .. })
        ));
        assert!(matches!(
            ma(&data, 0, 0),
            Err(TaError::InvalidPeriod { .. })
        ));
    }

    #[test]
    fn sma_rejects_period_below_two() {
        assert_eq!(
            sma(&[1.0, 2.0], 1),
            Err(TaError::InvalidPeriod {
                name: "optInTimePeriod",
                value: 1,
                min: 2
            })
        );
    }

    #[test]
    fn sma_nan_prefix_then_means() {
        let out = sma(&[1.0, 2.0, 3.0, 4.0], 2).expect("valid");
        assert!(out[0].is_nan());
        assert!((out[1] - 1.5).abs() < 1e-12);
        assert!((out[3] - 3.5).abs() < 1e-12);
    }

    #[test]
    fn short_input_is_all_nan_not_error() {
        let out = ema(&[1.0, 2.0], 3).expect("valid");
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|v| v.is_nan()));
        assert!(ema(&[], 3).expect("valid").is_empty());
    }

    #[test]
    fn ema_seed_is_sma_of_first_period() {
        let out = ema(&[2.0, 4.0, 6.0, 8.0], 3).expect("valid");
        assert!(out[1].is_nan());
        assert!((out[2] - 4.0).abs() < 1e-12);
    }

    #[test]
    fn bbands_flat_series_has_zero_width() {
        let (upper, middle, lower) = bbands(&[5.0; 10], 4, 2.0, 2.0).expect("valid");
        assert!(upper[2].is_nan());
        assert!((upper[5] - 5.0).abs() < 1e-12);
        assert!((middle[5] - 5.0).abs() < 1e-12);
        assert!((lower[5] - 5.0).abs() < 1e-12);
    }

    #[test]
    fn wma_weights_the_window_by_recency() {
        let out = wma(&[1.0, 2.0, 3.0, 4.0], 3).expect("valid");
        assert!(out[0].is_nan());
        assert!(out[1].is_nan());
        assert!((out[2] - 14.0 / 6.0).abs() < 1e-12);
        assert!((out[3] - 20.0 / 6.0).abs() < 1e-12);
    }

    #[test]
    fn trima_odd_period_matches_hand_weights() {
        let out = trima(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0], 5).expect("valid");
        assert!(out[3].is_nan());
        assert!((out[4] - 3.0).abs() < 1e-12);
        assert!((out[5] - 4.0).abs() < 1e-12);
    }

    #[test]
    fn trima_even_period_matches_hand_weights() {
        let out = trima(&[1.0, 2.0, 3.0, 4.0, 5.0], 4).expect("valid");
        assert!(out[2].is_nan());
        assert!((out[3] - 2.5).abs() < 1e-12);
        assert!((out[4] - 3.5).abs() < 1e-12);
    }

    #[test]
    fn midpoint_is_window_high_low_mean() {
        let out = midpoint(&[5.0, 3.0, 8.0, 1.0, 9.0], 3).expect("valid");
        assert!(out[1].is_nan());
        assert!((out[2] - 5.5).abs() < 1e-12);
        assert!((out[3] - 4.5).abs() < 1e-12);
        assert!((out[4] - 5.0).abs() < 1e-12);
    }

    #[test]
    fn midprice_uses_high_max_and_low_min() {
        let high = [5.0, 6.0, 7.0];
        let low = [1.0, 2.0, 3.0];
        let out = midprice(&high, &low, 2).expect("valid");
        assert!(out[0].is_nan());
        assert!((out[1] - 3.5).abs() < 1e-12);
        assert!((out[2] - 4.5).abs() < 1e-12);
    }

    #[test]
    fn dema_nan_prefix_is_twice_the_ema_lookback() {
        let input: Vec<f64> = (1..=20).map(f64::from).collect();
        let out = dema(&input, 4).expect("valid");
        for value in &out[..6] {
            assert!(value.is_nan());
        }
        assert!(out[6].is_finite());
    }

    #[test]
    fn tema_nan_prefix_is_thrice_the_ema_lookback() {
        let input: Vec<f64> = (1..=30).map(f64::from).collect();
        let out = tema(&input, 4).expect("valid");
        for value in &out[..9] {
            assert!(value.is_nan());
        }
        assert!(out[9].is_finite());
    }

    #[test]
    fn kama_nan_prefix_is_period_then_finite() {
        let input: Vec<f64> = (1..=30).map(f64::from).collect();
        let out = kama(&input, 5).expect("valid");
        for value in &out[..5] {
            assert!(value.is_nan());
        }
        assert!(out[5].is_finite());
    }

    #[test]
    fn kama_flat_series_holds_the_seed() {
        let out = kama(&[7.0; 12], 4).expect("valid");
        assert!(out[3].is_nan());
        assert!((out[4] - 7.0).abs() < 1e-12);
        assert!((out[11] - 7.0).abs() < 1e-12);
    }

    #[test]
    fn t3_nan_prefix_is_six_ema_lookbacks_and_vfactor_threads() {
        let input: Vec<f64> = (1..=40).map(f64::from).collect();
        let out = t3(&input, 3, 0.7).expect("valid");
        for value in &out[..12] {
            assert!(value.is_nan());
        }
        assert!(out[12].is_finite());
        let other = t3(&input, 3, 0.3).expect("valid");
        assert!(
            (out[20] - other[20]).abs() > 1e-9,
            "vfactor must change the curve"
        );
    }

    #[test]
    fn new_overlap_kernels_reject_period_below_two() {
        assert!(matches!(
            wma(&[1.0, 2.0], 1),
            Err(TaError::InvalidPeriod { .. })
        ));
        assert!(matches!(
            dema(&[1.0, 2.0], 1),
            Err(TaError::InvalidPeriod { .. })
        ));
        assert!(matches!(
            tema(&[1.0, 2.0], 1),
            Err(TaError::InvalidPeriod { .. })
        ));
        assert!(matches!(
            trima(&[1.0, 2.0], 1),
            Err(TaError::InvalidPeriod { .. })
        ));
        assert!(matches!(
            kama(&[1.0, 2.0], 1),
            Err(TaError::InvalidPeriod { .. })
        ));
        assert!(matches!(
            t3(&[1.0, 2.0], 1, 0.7),
            Err(TaError::InvalidPeriod { .. })
        ));
        assert!(matches!(
            midpoint(&[1.0, 2.0], 1),
            Err(TaError::InvalidPeriod { .. })
        ));
        assert!(matches!(
            midprice(&[1.0, 2.0], &[1.0, 2.0], 1),
            Err(TaError::InvalidPeriod { .. })
        ));
    }

    #[test]
    fn midprice_length_mismatch_errors() {
        assert_eq!(
            midprice(&[1.0, 2.0, 3.0], &[1.0, 2.0], 2),
            Err(TaError::LengthMismatch { left: 3, right: 2 })
        );
    }

    #[test]
    fn new_overlap_kernels_short_input_is_all_nan() {
        let short = [1.0, 2.0, 3.0];
        assert!(dema(&short, 8).expect("ok").iter().all(|v| v.is_nan()));
        assert!(tema(&short, 8).expect("ok").iter().all(|v| v.is_nan()));
        assert!(wma(&short, 8).expect("ok").iter().all(|v| v.is_nan()));
        assert!(trima(&short, 8).expect("ok").iter().all(|v| v.is_nan()));
        assert!(kama(&short, 8).expect("ok").iter().all(|v| v.is_nan()));
        assert!(t3(&short, 8, 0.7).expect("ok").iter().all(|v| v.is_nan()));
        assert!(midpoint(&short, 8).expect("ok").iter().all(|v| v.is_nan()));
        assert!(
            midprice(&short, &short, 8)
                .expect("ok")
                .iter()
                .all(|v| v.is_nan())
        );
        assert!(dema(&[], 8).expect("ok").is_empty());
    }

    #[test]
    fn mama_lookback_is_32_and_rejects_out_of_range_limits() {
        let input: Vec<f64> = (1..=33).map(f64::from).collect();
        let (mama_out, fama_out) = mama(&input, 0.5, 0.05).expect("mama");
        assert_eq!(mama_out.len(), 33);
        for value in &mama_out[..32] {
            assert!(value.is_nan());
        }
        assert!(mama_out[32].is_finite() && fama_out[32].is_finite());
        let short: Vec<f64> = (1..=32).map(f64::from).collect();
        let (mama_out, _) = mama(&short, 0.5, 0.05).expect("mama short");
        assert!(mama_out.iter().all(|v| v.is_nan()));
        assert!(matches!(
            mama(&input, 0.009, 0.05),
            Err(TaError::InvalidRealParam { .. })
        ));
        assert!(matches!(
            mama(&input, 0.5, 0.991),
            Err(TaError::InvalidRealParam { .. })
        ));
    }

    #[test]
    fn sar_rising_series_starts_long_and_seeds_at_first_low() {
        let high = [10.0, 11.0, 12.0, 13.0];
        let low = [9.0, 10.0, 11.0, 12.0];
        let out = sar(&high, &low, 0.02, 0.2).expect("sar");
        assert_eq!(out.len(), 4);
        assert!(out[0].is_nan());
        assert!((out[1] - 9.0).abs() < 1e-12);
        assert!(out[2].is_finite() && out[3].is_finite());
        assert!(out[1..].iter().all(|v| *v > 0.0));
    }

    #[test]
    fn sarext_short_side_output_is_negative() {
        let high = [13.0, 12.0, 11.0, 10.0];
        let low = [12.0, 11.0, 10.0, 9.0];
        let out = sarext(&high, &low, 0.0, 0.0, 0.02, 0.02, 0.2, 0.02, 0.02, 0.2).expect("sarext");
        assert_eq!(out.len(), 4);
        assert!(out[0].is_nan());
        assert!(
            (out[1] + 13.0).abs() < 1e-12,
            "short-side SAR is −sar, got {}",
            out[1]
        );
    }

    #[test]
    fn sarext_start_value_forces_direction_and_seed() {
        let high = [10.0, 11.0, 12.0, 13.0];
        let low = [9.0, 10.0, 11.0, 12.0];
        let long = sarext(&high, &low, 8.0, 0.0, 0.02, 0.02, 0.2, 0.02, 0.02, 0.2).expect("long");
        assert!((long[1] - 8.0).abs() < 1e-12);
        let short =
            sarext(&high, &low, -15.0, 0.0, 0.02, 0.02, 0.2, 0.02, 0.02, 0.2).expect("short");
        assert!((short[1] + 15.0).abs() < 1e-12);
    }

    #[test]
    fn mavp_clamps_period_and_lookback_uses_max_period() {
        let input = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let periods_low = [1.0; 8];
        let out = mavp(&input, &periods_low, 2, 3, 0).expect("mavp low");
        assert_eq!(out.len(), 8);
        assert!(out[0].is_nan() && out[1].is_nan());
        assert!((out[2] - 2.5).abs() < 1e-12);
        assert!((out[3] - 3.5).abs() < 1e-12);
        let periods_high = [9.0; 8];
        let out = mavp(&input, &periods_high, 2, 3, 0).expect("mavp high");
        assert!((out[2] - 2.0).abs() < 1e-12);
        assert!((out[3] - 3.0).abs() < 1e-12);
    }

    #[test]
    fn mavp_uses_shifted_ma_seeding_not_full_array() {
        let input: Vec<f64> = (1..=30)
            .map(|i| f64::from(i) + (f64::from(i) * 0.3).sin())
            .collect();
        let periods = vec![6.0; 30];
        let out = mavp(&input, &periods, 2, 10, 1).expect("mavp ema");
        let lookback = 9; // ma_lookback(10, EMA)
        let dense =
            crate::momentum::ma_range(&input, 6, 1, lookback, input.len() - 1).expect("ma_range");
        for (i, value) in out.iter().enumerate().skip(lookback) {
            assert!(
                value.to_bits() == dense[i - lookback].to_bits(),
                "mavp must match the shifted ma_range at {i}"
            );
        }
        let full = ema(&input, 6).expect("ema");
        assert!(
            (lookback..input.len()).any(|i| out[i].to_bits() != full[i].to_bits()),
            "shifted seeding must diverge from the full-array EMA somewhere"
        );
    }
}
