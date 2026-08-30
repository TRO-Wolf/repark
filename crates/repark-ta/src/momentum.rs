//! TA-Lib C 0.4.0 ports for momentum indicators and stochastics.

use crate::{
    Result, TaError, as_f64, check_lengths, check_period, dema, ema, is_zero, is_zero_or_neg, kama,
    mama, nan_vec, sma, t3, tema, trima, true_range, wma,
};

/// `RSI` — relative strength index (`ta_RSI.c`, Classic compatibility, unstable period 0).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn rsi(input: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    let mut out = nan_vec(len);
    if len < period + 1 {
        return Ok(out);
    }
    let mut prev_value = input[0];
    let mut prev_gain = 0.0_f64;
    let mut prev_loss = 0.0_f64;
    for value in &input[1..=period] {
        let temp_value1 = *value;
        let temp_value2 = temp_value1 - prev_value;
        prev_value = temp_value1;
        if temp_value2 < 0.0 {
            prev_loss -= temp_value2;
        } else {
            prev_gain += temp_value2;
        }
    }
    prev_loss /= as_f64(period);
    prev_gain /= as_f64(period);
    let temp_value1 = prev_gain + prev_loss;
    out[period] = if is_zero(temp_value1) {
        0.0
    } else {
        100.0 * (prev_gain / temp_value1)
    };
    for (output_slot, incoming) in out[period + 1..len]
        .iter_mut()
        .zip(input[period + 1..len].iter())
    {
        let temp_value1 = *incoming;
        let temp_value2 = temp_value1 - prev_value;
        prev_value = temp_value1;
        prev_loss *= as_f64(period - 1);
        prev_gain *= as_f64(period - 1);
        if temp_value2 < 0.0 {
            prev_loss -= temp_value2;
        } else {
            prev_gain += temp_value2;
        }
        prev_loss /= as_f64(period);
        prev_gain /= as_f64(period);
        let temp_value1 = prev_gain + prev_loss;
        *output_slot = if is_zero(temp_value1) {
            0.0
        } else {
            100.0 * (prev_gain / temp_value1)
        };
    }
    Ok(out)
}

/// Shared per-bar ±DM and true-range bookkeeping for all ADX phases.
#[allow(clippy::struct_field_names)] // the `prev` prefix mirrors C's prevMinusDM/prevTR/… names.
struct DirectionalState {
    prev_minus_dm: f64,
    prev_plus_dm: f64,
    prev_tr: f64,
    prev_high: f64,
    prev_low: f64,
    prev_close: f64,
}

impl DirectionalState {
    fn new(high: f64, low: f64, close: f64) -> Self {
        Self {
            prev_minus_dm: 0.0,
            prev_plus_dm: 0.0,
            prev_tr: 0.0,
            prev_high: high,
            prev_low: low,
            prev_close: close,
        }
    }

    /// Advance one bar using raw or Wilder-decayed accumulation.
    fn step(&mut self, high: f64, low: f64, close: f64, period: f64, decay: bool) {
        let diff_p = high - self.prev_high;
        self.prev_high = high;
        let diff_m = self.prev_low - low;
        self.prev_low = low;
        if decay {
            self.prev_minus_dm -= self.prev_minus_dm / period;
            self.prev_plus_dm -= self.prev_plus_dm / period;
        }
        if diff_m > 0.0 && diff_p < diff_m {
            self.prev_minus_dm += diff_m;
        } else if diff_p > 0.0 && diff_p > diff_m {
            self.prev_plus_dm += diff_p;
        }
        let temp_real = true_range(self.prev_high, self.prev_low, self.prev_close);
        if decay {
            self.prev_tr = self.prev_tr - (self.prev_tr / period) + temp_real;
        } else {
            self.prev_tr += temp_real;
        }
        self.prev_close = close;
    }

    /// Return DX, or `None` when a C zero guard fires.
    fn dx(&self) -> Option<f64> {
        if is_zero(self.prev_tr) {
            return None;
        }
        let minus_di = 100.0 * (self.prev_minus_dm / self.prev_tr);
        let plus_di = 100.0 * (self.prev_plus_dm / self.prev_tr);
        let temp_real = minus_di + plus_di;
        if is_zero(temp_real) {
            return None;
        }
        Some(100.0 * ((minus_di - plus_di).abs() / temp_real))
    }

    /// Return smoothed `+DI`, applying C's zero guard.
    fn plus_di(&self) -> f64 {
        if is_zero(self.prev_tr) {
            0.0
        } else {
            100.0 * (self.prev_plus_dm / self.prev_tr)
        }
    }

    /// Return smoothed `−DI`, applying C's zero guard.
    fn minus_di(&self) -> f64 {
        if is_zero(self.prev_tr) {
            0.0
        } else {
            100.0 * (self.prev_minus_dm / self.prev_tr)
        }
    }
}

/// Seed directional state and consume the first `period − 1` raw bars.
fn directional_prime(
    high: &[f64],
    low: &[f64],
    close: &[f64],
    period: usize,
) -> (DirectionalState, usize) {
    let period_f = as_f64(period);
    let mut state = DirectionalState::new(high[0], low[0], close[0]);
    let mut today = 0;
    for _ in 0..(period - 1) {
        today += 1;
        state.step(high[today], low[today], close[today], period_f, false);
    }
    (state, today)
}

/// `DX` — directional movement index (`ta_DX.c`, unstable period 0, `round_pos` disabled).
/// # Errors
/// `InvalidPeriod` if `period < 2`; `LengthMismatch` if the input series lengths differ.
pub fn dx(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    check_lengths(high.len(), &[low.len(), close.len()])?;
    let len = high.len();
    let mut out = nan_vec(len);
    let lookback = period;
    if len <= lookback {
        return Ok(out);
    }
    let period_f = as_f64(period);
    let (mut state, mut today) = directional_prime(high, low, close, period);
    today += 1;
    state.step(high[today], low[today], close[today], period_f, true);
    out[today] = state.dx().unwrap_or(0.0);
    while today < len - 1 {
        today += 1;
        state.step(high[today], low[today], close[today], period_f, true);
        let prev = out[today - 1];
        out[today] = state.dx().unwrap_or(prev);
    }
    Ok(out)
}

/// `ADXR` — average directional movement rating (`ta_ADXR.c`).
/// # Errors
/// `InvalidPeriod` if `period < 2`; `LengthMismatch` if the input series lengths differ.
// Preserve C's literal addition and division; `f64::midpoint` rounds differently.
#[allow(clippy::manual_midpoint)]
pub fn adxr(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    check_lengths(high.len(), &[low.len(), close.len()])?;
    let len = high.len();
    let mut out = nan_vec(len);
    let lookback = 3 * period - 2;
    if len <= lookback {
        return Ok(out);
    }
    let adx_series = adx(high, low, close, period)?;
    let back = period - 1;
    for today in lookback..len {
        out[today] = (adx_series[today] + adx_series[today - back]) / 2.0;
    }
    Ok(out)
}

/// Compute the period-one ±DM or DI fast path without Wilder smoothing.
#[allow(clippy::if_not_else)] // ordered to mirror C's `if(case) … else 0.0`.
fn directional_period_one(
    high: &[f64],
    low: &[f64],
    close: &[f64],
    plus: bool,
    di: bool,
) -> Vec<f64> {
    let len = high.len();
    let mut out = nan_vec(len);
    let mut prev_high = high[0];
    let mut prev_low = low[0];
    let mut prev_close = close[0];
    let mut today = 0;
    while today < len - 1 {
        today += 1;
        let diff_p = high[today] - prev_high;
        prev_high = high[today];
        let diff_m = prev_low - low[today];
        prev_low = low[today];
        let (fired, value) = if plus {
            (diff_p > 0.0 && diff_p > diff_m, diff_p)
        } else {
            (diff_m > 0.0 && diff_p < diff_m, diff_m)
        };
        out[today] = if fired {
            if di {
                let tr = true_range(prev_high, prev_low, prev_close);
                if is_zero(tr) { 0.0 } else { value / tr }
            } else {
                value
            }
        } else {
            0.0
        };
        prev_close = close[today];
    }
    out
}

/// `PLUS_DI` — plus directional indicator (`ta_PLUS_DI.c`, `round_pos` disabled).
/// # Errors
/// `InvalidPeriod` if `period < 1`; `LengthMismatch` if the input series lengths differ.
pub fn plus_di(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Result<Vec<f64>> {
    directional_di(high, low, close, period, true)
}

/// `MINUS_DI` — minus directional indicator (`ta_MINUS_DI.c`).
/// # Errors
/// `InvalidPeriod` if `period < 1`; `LengthMismatch` if the input series lengths differ.
pub fn minus_di(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Result<Vec<f64>> {
    directional_di(high, low, close, period, false)
}

/// Compute either directional indicator from shared state.
fn directional_di(
    high: &[f64],
    low: &[f64],
    close: &[f64],
    period: usize,
    plus: bool,
) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 1)?;
    check_lengths(high.len(), &[low.len(), close.len()])?;
    let len = high.len();
    let lookback = if period > 1 { period } else { 1 };
    if len <= lookback {
        return Ok(nan_vec(len));
    }
    if period == 1 {
        return Ok(directional_period_one(high, low, close, plus, true));
    }
    let mut out = nan_vec(len);
    let period_f = as_f64(period);
    let (mut state, mut today) = directional_prime(high, low, close, period);
    let di = |s: &DirectionalState| if plus { s.plus_di() } else { s.minus_di() };
    today += 1;
    state.step(high[today], low[today], close[today], period_f, true);
    out[today] = di(&state);
    while today < len - 1 {
        today += 1;
        state.step(high[today], low[today], close[today], period_f, true);
        out[today] = di(&state);
    }
    Ok(out)
}

/// `PLUS_DM` — plus directional movement (`ta_PLUS_DM.c`).
/// # Errors
/// `InvalidPeriod` if `period < 1`; `LengthMismatch` if the input series lengths differ.
pub fn plus_dm(high: &[f64], low: &[f64], period: usize) -> Result<Vec<f64>> {
    directional_dm(high, low, period, true)
}

/// `MINUS_DM` — minus directional movement (`ta_MINUS_DM.c`).
/// # Errors
/// `InvalidPeriod` if `period < 1`; `LengthMismatch` if the input series lengths differ.
pub fn minus_dm(high: &[f64], low: &[f64], period: usize) -> Result<Vec<f64>> {
    directional_dm(high, low, period, false)
}

/// Compute either smoothed DM from shared directional state.
fn directional_dm(high: &[f64], low: &[f64], period: usize, plus: bool) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 1)?;
    check_lengths(high.len(), &[low.len()])?;
    let len = high.len();
    let lookback = if period > 1 { period - 1 } else { 1 };
    if len <= lookback {
        return Ok(nan_vec(len));
    }
    let close = vec![0.0_f64; len];
    if period == 1 {
        return Ok(directional_period_one(high, low, &close, plus, false));
    }
    let mut out = nan_vec(len);
    let period_f = as_f64(period);
    let (mut state, mut today) = directional_prime(high, low, &close, period);
    let dm = |s: &DirectionalState| {
        if plus {
            s.prev_plus_dm
        } else {
            s.prev_minus_dm
        }
    };
    out[today] = dm(&state);
    while today < len - 1 {
        today += 1;
        state.step(high[today], low[today], close[today], period_f, true);
        out[today] = dm(&state);
    }
    Ok(out)
}

/// `ADX` — average directional movement index (`ta_ADX.c`, unstable period 0, `round_pos` disabled
/// # Errors
/// `InvalidPeriod` if `period < 2`; `LengthMismatch` if the input series lengths differ.
pub fn adx(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    check_lengths(high.len(), &[low.len(), close.len()])?;
    let len = high.len();
    // Keeps `nan_vec`: the `ema` single-write extend form measured +42% at n=1e6.
    let mut out = nan_vec(len);
    let lookback = 2 * period - 1;
    if len < lookback + 1 {
        return Ok(out);
    }
    let period_f = as_f64(period);
    let mut state = DirectionalState::new(high[0], low[0], close[0]);
    let mut today = 0;

    for _ in 0..(period - 1) {
        today += 1;
        state.step(high[today], low[today], close[today], period_f, false);
    }

    let mut sum_dx = 0.0_f64;
    for _ in 0..period {
        today += 1;
        state.step(high[today], low[today], close[today], period_f, true);
        if let Some(dx) = state.dx() {
            sum_dx += dx;
        }
    }
    let mut prev_adx = sum_dx / period_f;
    out[today] = prev_adx;

    while today < len - 1 {
        today += 1;
        state.step(high[today], low[today], close[today], period_f, true);
        if let Some(dx) = state.dx() {
            prev_adx = ((prev_adx * as_f64(period - 1)) + dx) / period_f;
        }
        out[today] = prev_adx;
    }
    Ok(out)
}

/// `MOM` — momentum, `price − prevPrice` (`ta_MOM.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 1`.
pub fn mom(input: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 1)?;
    let len = input.len();
    let mut out = nan_vec(len);
    if len <= period {
        return Ok(out);
    }
    let mut today = period;
    let mut trailing_idx = 0_usize;
    while today < len {
        out[today] = input[today] - input[trailing_idx];
        today += 1;
        trailing_idx += 1;
    }
    Ok(out)
}

/// Run the shared C-compatible trailing-ratio loop for the ROC family.
#[allow(clippy::float_cmp, clippy::if_not_else)] // C guards with exact `prevPrice != 0.0`.
fn roc_family(input: &[f64], period: usize, f: impl Fn(f64, f64) -> f64) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 1)?;
    let len = input.len();
    let mut out = nan_vec(len);
    if len <= period {
        return Ok(out);
    }
    let mut today = period;
    let mut trailing_idx = 0_usize;
    while today < len {
        let prev = input[trailing_idx];
        out[today] = if prev != 0.0 {
            f(input[today], prev)
        } else {
            0.0
        };
        today += 1;
        trailing_idx += 1;
    }
    Ok(out)
}

/// `ROC` — rate of change, `((price / prevPrice) − 1) · 100` (`ta_ROC.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 1`.
pub fn roc(input: &[f64], period: usize) -> Result<Vec<f64>> {
    roc_family(input, period, |price, prev| ((price / prev) - 1.0) * 100.0)
}

/// `ROCP` — rate of change percentage, `(price − prevPrice) / prevPrice` (`ta_ROCP.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 1`.
pub fn rocp(input: &[f64], period: usize) -> Result<Vec<f64>> {
    roc_family(input, period, |price, prev| (price - prev) / prev)
}

/// `ROCR` — rate of change ratio, `price / prevPrice` (`ta_ROCR.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 1`.
pub fn rocr(input: &[f64], period: usize) -> Result<Vec<f64>> {
    roc_family(input, period, |price, prev| price / prev)
}

/// `ROCR100` — rate of change ratio ×100, `(price / prevPrice) · 100` (`ta_ROCR100.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 1`.
pub fn rocr100(input: &[f64], period: usize) -> Result<Vec<f64>> {
    roc_family(input, period, |price, prev| (price / prev) * 100.0)
}

/// `WILLR` — Williams %R (`ta_WILLR.c`).
/// # Errors
/// `InvalidPeriod` if `period < 2`; `LengthMismatch` if the input series lengths differ.
#[allow(clippy::float_cmp, clippy::if_not_else)] // C's output guard is an exact `diff != 0.0`.
pub fn willr(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    check_lengths(high.len(), &[low.len(), close.len()])?;
    let len = high.len();
    let mut out = nan_vec(len);
    let lookback = period - 1;
    if len <= lookback {
        return Ok(out);
    }
    let mut today = lookback;
    let mut trailing_idx = 0_usize;
    let mut lowest_idx: Option<usize> = None;
    let mut highest_idx: Option<usize> = None;
    let mut lowest = 0.0_f64;
    let mut highest = 0.0_f64;
    let mut diff = 0.0_f64;
    while today < len {
        let tmp = low[today];
        if lowest_idx.is_none_or(|idx| idx < trailing_idx) {
            lowest_idx = Some(trailing_idx);
            lowest = low[trailing_idx];
            #[allow(clippy::needless_range_loop)] // index rescan, op-for-op with C's `while(++i)`.
            for i in (trailing_idx + 1)..=today {
                if low[i] < lowest {
                    lowest_idx = Some(i);
                    lowest = low[i];
                }
            }
            diff = (highest - lowest) / (-100.0);
        } else if tmp <= lowest {
            lowest_idx = Some(today);
            lowest = tmp;
            diff = (highest - lowest) / (-100.0);
        }
        let tmp = high[today];
        if highest_idx.is_none_or(|idx| idx < trailing_idx) {
            highest_idx = Some(trailing_idx);
            highest = high[trailing_idx];
            #[allow(clippy::needless_range_loop)] // index rescan, op-for-op with C.
            for i in (trailing_idx + 1)..=today {
                if high[i] > highest {
                    highest_idx = Some(i);
                    highest = high[i];
                }
            }
            diff = (highest - lowest) / (-100.0);
        } else if tmp >= highest {
            highest_idx = Some(today);
            highest = tmp;
            diff = (highest - lowest) / (-100.0);
        }
        out[today] = if diff != 0.0 {
            (highest - close[today]) / diff
        } else {
            0.0
        };
        trailing_idx += 1;
        today += 1;
    }
    Ok(out)
}

/// `CCI` — commodity channel index (`ta_CCI.c`).
/// # Errors
/// `InvalidPeriod` if `period < 2`; `LengthMismatch` if the input series lengths differ.
#[allow(clippy::float_cmp)] // C guards with exact `!= 0.0` on both the deviation and the MAD.
pub fn cci(high: &[f64], low: &[f64], close: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    check_lengths(high.len(), &[low.len(), close.len()])?;
    let len = high.len();
    let mut out = nan_vec(len);
    let lookback = period - 1;
    if len <= lookback {
        return Ok(out);
    }
    let period_f = as_f64(period);
    let mut circ = vec![0.0_f64; period];
    let mut circ_idx = 0_usize;
    let mut today = 0_usize;
    while today < lookback {
        circ[circ_idx] = (high[today] + low[today] + close[today]) / 3.0;
        today += 1;
        circ_idx += 1;
        if circ_idx >= period {
            circ_idx = 0;
        }
    }
    while today < len {
        let last_value = (high[today] + low[today] + close[today]) / 3.0;
        circ[circ_idx] = last_value;
        // Physical buffer order is load-bearing for bit parity.
        let mut the_average = 0.0_f64;
        for value in &circ {
            the_average += *value;
        }
        the_average /= period_f;
        let mut mad = 0.0_f64;
        for value in &circ {
            mad += (*value - the_average).abs();
        }
        let temp_real = last_value - the_average;
        out[today] = if temp_real != 0.0 && mad != 0.0 {
            temp_real / (0.015 * (mad / period_f))
        } else {
            0.0
        };
        circ_idx += 1;
        if circ_idx >= period {
            circ_idx = 0;
        }
        today += 1;
    }
    Ok(out)
}

/// `CMO` — Chande momentum oscillator (`ta_CMO.c`, Classic compatibility, unstable period 0).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn cmo(input: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    let mut out = nan_vec(len);
    if len < period + 1 {
        return Ok(out);
    }
    let mut prev_value = input[0];
    let mut prev_gain = 0.0_f64;
    let mut prev_loss = 0.0_f64;
    for value in &input[1..=period] {
        let delta = *value - prev_value;
        prev_value = *value;
        if delta < 0.0 {
            prev_loss -= delta;
        } else {
            prev_gain += delta;
        }
    }
    prev_loss /= as_f64(period);
    prev_gain /= as_f64(period);
    let temp = prev_gain + prev_loss;
    out[period] = if is_zero(temp) {
        0.0
    } else {
        100.0 * ((prev_gain - prev_loss) / temp)
    };
    for i in (period + 1)..len {
        let delta = input[i] - prev_value;
        prev_value = input[i];
        prev_loss *= as_f64(period - 1);
        prev_gain *= as_f64(period - 1);
        if delta < 0.0 {
            prev_loss -= delta;
        } else {
            prev_gain += delta;
        }
        prev_loss /= as_f64(period);
        prev_gain /= as_f64(period);
        let temp = prev_gain + prev_loss;
        out[i] = if is_zero(temp) {
            0.0
        } else {
            100.0 * ((prev_gain - prev_loss) / temp)
        };
    }
    Ok(out)
}

/// `BOP` — balance of power, `(close − open) / (high − low)` (`ta_BOP.c`).
/// # Errors
/// [`crate::TaError::LengthMismatch`] if the series differ in length.
pub fn bop(open: &[f64], high: &[f64], low: &[f64], close: &[f64]) -> Result<Vec<f64>> {
    check_lengths(open.len(), &[high.len(), low.len(), close.len()])?;
    let len = open.len();
    let mut out = nan_vec(len);
    for i in 0..len {
        let range = high[i] - low[i];
        out[i] = if is_zero_or_neg(range) {
            0.0
        } else {
            (close[i] - open[i]) / range
        };
    }
    Ok(out)
}

/// Select TA-Lib's moving-average type `0..=8`; period one is identity.
pub(crate) fn ma_dispatch(input: &[f64], period: usize, matype: usize) -> Result<Vec<f64>> {
    if period <= 1 {
        if matype > 8 {
            return Err(TaError::UnsupportedMaType {
                matype,
                reason: "not a TA-Lib MA type (expected 0..=8)",
            });
        }
        return Ok(input.to_vec());
    }
    match matype {
        0 => sma(input, period),
        1 => ema(input, period),
        2 => wma(input, period),
        3 => dema(input, period),
        4 => tema(input, period),
        5 => trima(input, period),
        6 => kama(input, period),
        7 => Ok(mama(input, 0.5, 0.05)?.0),
        8 => t3(input, period, 0.7),
        _ => Err(TaError::UnsupportedMaType {
            matype,
            reason: "not a TA-Lib MA type (expected 0..=8)",
        }),
    }
}

/// Return the first non-NaN index for a selected moving average.
pub(crate) fn ma_lookback(period: usize, matype: usize) -> Result<usize> {
    if period <= 1 {
        if matype > 8 {
            return Err(TaError::UnsupportedMaType {
                matype,
                reason: "not a TA-Lib MA type (expected 0..=8)",
            });
        }
        return Ok(0);
    }
    let lookback = match matype {
        0 | 1 | 2 | 5 => period - 1,
        3 => 2 * (period - 1),
        4 => 3 * (period - 1),
        6 => period,
        7 => 32,
        8 => 6 * (period - 1),
        _ => {
            return Err(TaError::UnsupportedMaType {
                matype,
                reason: "not a TA-Lib MA type (expected 0..=8)",
            });
        }
    };
    Ok(lookback)
}

/// Compute a shifted MA range with C's buffer indexing.
pub(crate) fn ma_range(
    input: &[f64],
    period: usize,
    matype: usize,
    start_idx: usize,
    end_idx: usize,
) -> Result<Vec<f64>> {
    let lookback = ma_lookback(period, matype)?;
    let eff_start = start_idx.max(lookback);
    if end_idx < eff_start {
        return Ok(Vec::new());
    }
    if matype == 7 && period > 1 {
        let dense = mama(input, 0.5, 0.05)?.0;
        return Ok(dense[eff_start..=end_idx].to_vec());
    }
    let dense = ma_dispatch(&input[(eff_start - lookback)..=end_idx], period, matype)?;
    Ok(dense[lookback..].to_vec())
}

/// Return C's EMA smoothing constant.
fn per_to_k(period: usize) -> f64 {
    2.0 / (as_f64(period) + 1.0)
}

/// Compute C's shifted EMA with an explicit smoothing constant.
fn int_ema_dense(
    input: &[f64],
    period: usize,
    k: f64,
    start_idx: usize,
    end_idx: usize,
) -> Vec<f64> {
    let lookback = period - 1;
    let eff_start = start_idx.max(lookback);
    let mut today = eff_start - lookback;
    let mut temp = 0.0_f64;
    for _ in 0..period {
        temp += input[today];
        today += 1;
    }
    let mut prev = temp / as_f64(period);
    let mut out = Vec::with_capacity(end_idx - eff_start + 1);
    out.push(prev);
    out.extend(input[today..=end_idx].iter().map(|value| {
        prev = ((value - prev) * k) + prev;
        prev
    }));
    out
}

/// Compute MACD, signal, and histogram with C's seed and trim ordering.
fn int_macd(
    input: &[f64],
    fast_period: usize,
    slow_period: usize,
    signal_period: usize,
    fix: bool,
) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
    let len = input.len();
    let (fast_period, slow_period) = if slow_period < fast_period {
        (slow_period, fast_period)
    } else {
        (fast_period, slow_period)
    };
    let (k_slow, k_fast) = if fix {
        (0.075, 0.15)
    } else {
        (per_to_k(slow_period), per_to_k(fast_period))
    };
    let lookback_signal = signal_period - 1;
    let lookback_total = lookback_signal + (slow_period - 1);
    let mut macd_out = nan_vec(len);
    let mut signal_out = nan_vec(len);
    let mut hist_out = nan_vec(len);
    if len <= lookback_total {
        return (macd_out, signal_out, hist_out);
    }
    let start_idx = lookback_total;
    let end_idx = len - 1;
    let temp_start = start_idx - lookback_signal;
    let slow_ema = int_ema_dense(input, slow_period, k_slow, temp_start, end_idx);
    let fast_ema = int_ema_dense(input, fast_period, k_fast, temp_start, end_idx);
    let macd_buf: Vec<f64> = fast_ema
        .iter()
        .zip(&slow_ema)
        .map(|(fast, slow)| fast - slow)
        .collect();
    let signal_dense = int_ema_dense(
        &macd_buf,
        signal_period,
        per_to_k(signal_period),
        0,
        macd_buf.len() - 1,
    );
    for i in 0..(len - lookback_total) {
        let macd_value = macd_buf[lookback_signal + i];
        let signal_value = signal_dense[i];
        macd_out[start_idx + i] = macd_value;
        signal_out[start_idx + i] = signal_value;
        hist_out[start_idx + i] = macd_value - signal_value;
    }
    (macd_out, signal_out, hist_out)
}

/// `MACD` — moving-average convergence/divergence (`ta_MACD.c`, split into three outputs).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `fast < 2`, `slow < 2`, or `signal < 1`.
pub fn macd(
    input: &[f64],
    fast_period: usize,
    slow_period: usize,
    signal_period: usize,
) -> Result<(Vec<f64>, Vec<f64>, Vec<f64>)> {
    check_period("optInFastPeriod", fast_period, 2)?;
    check_period("optInSlowPeriod", slow_period, 2)?;
    check_period("optInSignalPeriod", signal_period, 1)?;
    Ok(int_macd(
        input,
        fast_period,
        slow_period,
        signal_period,
        false,
    ))
}

/// `MACDFIX` — MACD with the periods pinned at 12/26 (`ta_MACDFIX.c`, split into three outputs).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `signal < 1`.
pub fn macdfix(input: &[f64], signal_period: usize) -> Result<(Vec<f64>, Vec<f64>, Vec<f64>)> {
    check_period("optInSignalPeriod", signal_period, 1)?;
    Ok(int_macd(input, 12, 26, signal_period, true))
}

/// `MACDEXT` — MACD with configurable MA types (`ta_MACDEXT.c`, split into three outputs).
/// # Errors
/// `InvalidPeriod` if `fast < 2`, `slow < 2`, or `signal < 1`.
pub fn macdext(
    input: &[f64],
    fast_period: usize,
    fast_matype: usize,
    slow_period: usize,
    slow_matype: usize,
    signal_period: usize,
    signal_matype: usize,
) -> Result<(Vec<f64>, Vec<f64>, Vec<f64>)> {
    check_period("optInFastPeriod", fast_period, 2)?;
    check_period("optInSlowPeriod", slow_period, 2)?;
    check_period("optInSignalPeriod", signal_period, 1)?;
    let len = input.len();
    let (fast_period, fast_matype, slow_period, slow_matype) = if slow_period < fast_period {
        (slow_period, slow_matype, fast_period, fast_matype)
    } else {
        (fast_period, fast_matype, slow_period, slow_matype)
    };
    let lookback_largest =
        ma_lookback(fast_period, fast_matype)?.max(ma_lookback(slow_period, slow_matype)?);
    let lookback_signal = ma_lookback(signal_period, signal_matype)?;
    let lookback_total = lookback_signal + lookback_largest;
    let mut macd_out = nan_vec(len);
    let mut signal_out = nan_vec(len);
    let mut hist_out = nan_vec(len);
    if len <= lookback_total {
        return Ok((macd_out, signal_out, hist_out));
    }
    let start_idx = lookback_total;
    let end_idx = len - 1;
    let temp_start = start_idx - lookback_signal; // = lookback_largest
    let slow_ma = ma_range(input, slow_period, slow_matype, temp_start, end_idx)?;
    let fast_ma = ma_range(input, fast_period, fast_matype, temp_start, end_idx)?;
    let macd_buf: Vec<f64> = fast_ma
        .iter()
        .zip(&slow_ma)
        .map(|(fast, slow)| fast - slow)
        .collect();
    let signal_dense = ma_range(
        &macd_buf,
        signal_period,
        signal_matype,
        0,
        macd_buf.len() - 1,
    )?;
    for i in 0..(len - lookback_total) {
        let macd_value = macd_buf[lookback_signal + i];
        let signal_value = signal_dense[i];
        macd_out[start_idx + i] = macd_value;
        signal_out[start_idx + i] = signal_value;
        hist_out[start_idx + i] = macd_value - signal_value;
    }
    Ok((macd_out, signal_out, hist_out))
}

/// `APO` — absolute price oscillator, `MA(fast) − MA(slow)` (`ta_APO.c` / `TA_INT_PO`).
/// # Errors
/// Fails if a period is `< 2` or `matype` is out of range (`> 8`); MAMA (7) is supported.
pub fn apo(
    input: &[f64],
    fast_period: usize,
    slow_period: usize,
    matype: usize,
) -> Result<Vec<f64>> {
    check_period("optInFastPeriod", fast_period, 2)?;
    check_period("optInSlowPeriod", slow_period, 2)?;
    let (fast_p, slow_p) = if slow_period < fast_period {
        (slow_period, fast_period)
    } else {
        (fast_period, slow_period)
    };
    let fast = ma_dispatch(input, fast_p, matype)?;
    let slow = ma_dispatch(input, slow_p, matype)?;
    let mut out = nan_vec(input.len());
    for (k, (&fast_ma, &slow_ma)) in fast.iter().zip(&slow).enumerate() {
        if !slow_ma.is_nan() {
            out[k] = fast_ma - slow_ma;
        }
    }
    Ok(out)
}

/// `PPO` — percentage price oscillator, `(MA(fast) − MA(slow)) / MA(slow) · 100` (`ta_PPO.c` /
/// # Errors
/// Fails if a period is `< 2` or `matype` is out of range (`> 8`); MAMA (7) is supported.
pub fn ppo(
    input: &[f64],
    fast_period: usize,
    slow_period: usize,
    matype: usize,
) -> Result<Vec<f64>> {
    check_period("optInFastPeriod", fast_period, 2)?;
    check_period("optInSlowPeriod", slow_period, 2)?;
    let (fast_p, slow_p) = if slow_period < fast_period {
        (slow_period, fast_period)
    } else {
        (fast_period, slow_period)
    };
    let fast = ma_dispatch(input, fast_p, matype)?;
    let slow = ma_dispatch(input, slow_p, matype)?;
    let mut out = nan_vec(input.len());
    for (k, (&fast_ma, &slow_ma)) in fast.iter().zip(&slow).enumerate() {
        if !slow_ma.is_nan() {
            out[k] = if is_zero(slow_ma) {
                0.0
            } else {
                ((fast_ma - slow_ma) / slow_ma) * 100.0
            };
        }
    }
    Ok(out)
}

/// `AROON` — Aroon up / down (`ta_AROON.c`).
/// # Errors
/// `InvalidPeriod` if `period < 2`; `LengthMismatch` if the input series lengths differ.
// Checked signed-index conversions prevent wraparound in C-style sentinel bookkeeping.
#[allow(clippy::cast_precision_loss)]
pub fn aroon(high: &[f64], low: &[f64], period: usize) -> Result<(Vec<f64>, Vec<f64>)> {
    check_period("optInTimePeriod", period, 2)?;
    check_lengths(high.len(), &[low.len()])?;
    let len = high.len();
    let mut down = nan_vec(len);
    let mut up = nan_vec(len);
    if len <= period {
        return Ok((down, up));
    }
    let end_idx = aroon_end_idx(len)?;
    let period_i = i64::try_from(period).map_err(|_| TaError::InputTooLong {
        name: "AROON period",
        len: period,
    })?;
    let factor = 100.0 / as_f64(period);
    let mut today = period_i;
    let mut trailing_idx = 0_i64;
    let mut lowest_idx = -1_i64;
    let mut highest_idx = -1_i64;
    let mut lowest = 0.0_f64;
    let mut highest = 0.0_f64;
    while today <= end_idx {
        let today_u = aroon_usize(today, len, "AROON today")?;
        let tmp = low[today_u];
        if lowest_idx < trailing_idx {
            lowest_idx = trailing_idx;
            lowest = low[aroon_usize(lowest_idx, len, "AROON lowest")?];
            let mut i = lowest_idx + 1;
            while i <= today {
                let value = low[aroon_usize(i, len, "AROON low scan")?];
                if value <= lowest {
                    lowest_idx = i;
                    lowest = value;
                }
                i += 1;
            }
        } else if tmp <= lowest {
            lowest_idx = today;
            lowest = tmp;
        }
        let tmp = high[today_u];
        if highest_idx < trailing_idx {
            highest_idx = trailing_idx;
            highest = high[aroon_usize(highest_idx, len, "AROON highest")?];
            let mut i = highest_idx + 1;
            while i <= today {
                let value = high[aroon_usize(i, len, "AROON high scan")?];
                if value >= highest {
                    highest_idx = i;
                    highest = value;
                }
                i += 1;
            }
        } else if tmp >= highest {
            highest_idx = today;
            highest = tmp;
        }
        let up_bars = period_i - (today - highest_idx);
        let down_bars = period_i - (today - lowest_idx);
        up[today_u] = factor * (up_bars as f64);
        down[today_u] = factor * (down_bars as f64);
        trailing_idx += 1;
        today += 1;
    }
    Ok((down, up))
}

/// Convert the final series index to checked `i64`.
fn aroon_end_idx(len: usize) -> Result<i64> {
    let len_i = i64::try_from(len).map_err(|_| TaError::InputTooLong { name: "AROON", len })?;
    len_i
        .checked_sub(1)
        .ok_or(TaError::InputTooLong { name: "AROON", len })
}

/// Convert a checked `i64` index and reject out-of-range values.
fn aroon_usize(index: i64, len: usize, name: &'static str) -> Result<usize> {
    let index = usize::try_from(index).map_err(|_| TaError::InputTooLong { name, len })?;
    if index >= len {
        return Err(TaError::InputTooLong { name, len });
    }
    Ok(index)
}

/// `AROONOSC` — Aroon oscillator, `AroonUp − AroonDown` (`ta_AROONOSC.c`).
/// # Errors
/// `InvalidPeriod` if `period < 2`; `LengthMismatch` if the input series lengths differ.
#[allow(clippy::cast_precision_loss)]
pub fn aroonosc(high: &[f64], low: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    check_lengths(high.len(), &[low.len()])?;
    let len = high.len();
    let mut out = nan_vec(len);
    if len <= period {
        return Ok(out);
    }
    let end_idx = aroon_end_idx(len)?;
    let period_i = i64::try_from(period).map_err(|_| TaError::InputTooLong {
        name: "AROONOSC period",
        len: period,
    })?;
    let factor = 100.0 / as_f64(period);
    let mut today = period_i;
    let mut trailing_idx = 0_i64;
    let mut lowest_idx = -1_i64;
    let mut highest_idx = -1_i64;
    let mut lowest = 0.0_f64;
    let mut highest = 0.0_f64;
    while today <= end_idx {
        let today_u = aroon_usize(today, len, "AROONOSC today")?;
        let tmp = low[today_u];
        if lowest_idx < trailing_idx {
            lowest_idx = trailing_idx;
            lowest = low[aroon_usize(lowest_idx, len, "AROONOSC lowest")?];
            let mut i = lowest_idx + 1;
            while i <= today {
                let value = low[aroon_usize(i, len, "AROONOSC low scan")?];
                if value <= lowest {
                    lowest_idx = i;
                    lowest = value;
                }
                i += 1;
            }
        } else if tmp <= lowest {
            lowest_idx = today;
            lowest = tmp;
        }
        let tmp = high[today_u];
        if highest_idx < trailing_idx {
            highest_idx = trailing_idx;
            highest = high[aroon_usize(highest_idx, len, "AROONOSC highest")?];
            let mut i = highest_idx + 1;
            while i <= today {
                let value = high[aroon_usize(i, len, "AROONOSC high scan")?];
                if value >= highest {
                    highest_idx = i;
                    highest = value;
                }
                i += 1;
            }
        } else if tmp >= highest {
            highest_idx = today;
            highest = tmp;
        }
        out[today_u] = factor * ((highest_idx - lowest_idx) as f64);
        trailing_idx += 1;
        today += 1;
    }
    Ok(out)
}

/// `TRIX` — 1-day rate of change of a triple-smoothed EMA (`ta_TRIX.c`).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
#[allow(clippy::float_cmp, clippy::if_not_else)] // the trailing ROC guard is C's exact `!= 0.0`.
pub fn trix(input: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    let mut out = nan_vec(len);
    let ema_lookback = period - 1;
    let total_lookback = ema_lookback * 3 + 1;
    if len <= total_lookback {
        return Ok(out);
    }
    let first = ema(input, period)?;
    let first_dense = &first[ema_lookback..];
    let second = ema(first_dense, period)?;
    let second_dense = &second[ema_lookback..];
    let third = ema(second_dense, period)?;
    let third_dense = &third[ema_lookback..];
    for m in 0..(third_dense.len() - 1) {
        let prev = third_dense[m];
        let cur = third_dense[m + 1];
        out[total_lookback + m] = if prev != 0.0 {
            ((cur / prev) - 1.0) * 100.0
        } else {
            0.0
        };
    }
    Ok(out)
}

/// Compute C's per-bar buying-pressure and true-range terms.
#[allow(clippy::similar_names)] // temp_lt/temp_ht/temp_cy mirror C's tempLT/tempHT/tempCY.
fn ultosc_terms(high: &[f64], low: &[f64], close: &[f64], day: usize) -> (f64, f64) {
    let temp_lt = low[day];
    let temp_ht = high[day];
    let temp_cy = close[day - 1];
    let true_low = if temp_lt < temp_cy { temp_lt } else { temp_cy };
    let close_minus_true_low = close[day] - true_low;
    let mut true_range = temp_ht - temp_lt;
    let temp = (temp_cy - temp_ht).abs();
    if temp > true_range {
        true_range = temp;
    }
    let temp = (temp_cy - temp_lt).abs();
    if temp > true_range {
        true_range = temp;
    }
    (close_minus_true_low, true_range)
}

/// `ULTOSC` — ultimate oscillator (`ta_ULTOSC.c`).
/// # Errors
/// Fails if any period `< 1`, series lengths differ, or `matype` is outside the supported range.
pub fn ultosc(
    high: &[f64],
    low: &[f64],
    close: &[f64],
    period1: usize,
    period2: usize,
    period3: usize,
) -> Result<Vec<f64>> {
    check_period("optInTimePeriod1", period1, 1)?;
    check_period("optInTimePeriod2", period2, 1)?;
    check_period("optInTimePeriod3", period3, 1)?;
    check_lengths(high.len(), &[low.len(), close.len()])?;
    let len = high.len();
    let mut out = nan_vec(len);
    let mut sorted = [period1, period2, period3];
    sorted.sort_unstable();
    let [p1, p2, p3] = sorted; // p1 shortest (weight 4) … p3 longest (weight 1)
    let lookback = p3; // SMA_Lookback(max) + 1 = (max − 1) + 1
    if len <= lookback {
        return Ok(out);
    }
    let prime = |period: usize| -> (f64, f64) {
        let mut a_total = 0.0_f64;
        let mut b_total = 0.0_f64;
        for i in (lookback - period + 1)..lookback {
            let (cmtl, tr) = ultosc_terms(high, low, close, i);
            a_total += cmtl;
            b_total += tr;
        }
        (a_total, b_total)
    };
    let (mut a1, mut b1) = prime(p1);
    let (mut a2, mut b2) = prime(p2);
    let (mut a3, mut b3) = prime(p3);

    let mut today = lookback;
    let mut trailing1 = today - p1 + 1;
    let mut trailing2 = today - p2 + 1;
    let mut trailing3 = today - p3 + 1;
    while today < len {
        let (cmtl, tr) = ultosc_terms(high, low, close, today);
        a1 += cmtl;
        a2 += cmtl;
        a3 += cmtl;
        b1 += tr;
        b2 += tr;
        b3 += tr;

        let mut output = 0.0_f64;
        if !is_zero(b1) {
            output += 4.0 * (a1 / b1);
        }
        if !is_zero(b2) {
            output += 2.0 * (a2 / b2);
        }
        if !is_zero(b3) {
            output += a3 / b3;
        }

        let (cmtl, tr) = ultosc_terms(high, low, close, trailing1);
        a1 -= cmtl;
        b1 -= tr;
        let (cmtl, tr) = ultosc_terms(high, low, close, trailing2);
        a2 -= cmtl;
        b2 -= tr;
        let (cmtl, tr) = ultosc_terms(high, low, close, trailing3);
        a3 -= cmtl;
        b3 -= tr;

        out[today] = 100.0 * (output / 7.0);
        today += 1;
        trailing1 += 1;
        trailing2 += 1;
        trailing3 += 1;
    }
    Ok(out)
}

/// Return the moving-average lookback for stochastic smoothing.
fn ma_selector_lookback(period: usize, matype: usize) -> Result<usize> {
    if period == 1 {
        if matype > 8 {
            return Err(TaError::UnsupportedMaType {
                matype,
                reason: "not a TA-Lib MA type (expected 0..=8)",
            });
        }
        return Ok(0);
    }
    ma_lookback(period, matype)
}

/// Compute C's dense raw stochastic %K buffer with trailing extrema and a flat-window guard.
// C guards the divisor with an exact `diff != 0.0` (float_cmp) in that branch order (if_not_else).
#[allow(clippy::float_cmp, clippy::if_not_else)]
fn raw_stoch_k(high: &[f64], low: &[f64], close: &[f64], fastk_period: usize) -> Vec<f64> {
    let len = high.len();
    let lookback_k = fastk_period - 1;
    if len <= lookback_k {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(len - lookback_k);
    let mut today = lookback_k;
    let mut trailing_idx = 0_usize;
    let mut lowest_idx: Option<usize> = None;
    let mut highest_idx: Option<usize> = None;
    let mut lowest = 0.0_f64;
    let mut highest = 0.0_f64;
    let mut diff = 0.0_f64;
    while today < len {
        let tmp = low[today];
        if lowest_idx.is_none_or(|idx| idx < trailing_idx) {
            lowest_idx = Some(trailing_idx);
            lowest = low[trailing_idx];
            #[allow(clippy::needless_range_loop)] // index rescan, op-for-op with C's `while(++i)`.
            for i in (trailing_idx + 1)..=today {
                if low[i] < lowest {
                    lowest_idx = Some(i);
                    lowest = low[i];
                }
            }
            diff = (highest - lowest) / 100.0;
        } else if tmp <= lowest {
            lowest_idx = Some(today);
            lowest = tmp;
            diff = (highest - lowest) / 100.0;
        }
        let tmp = high[today];
        if highest_idx.is_none_or(|idx| idx < trailing_idx) {
            highest_idx = Some(trailing_idx);
            highest = high[trailing_idx];
            #[allow(clippy::needless_range_loop)] // index rescan, op-for-op with C.
            for i in (trailing_idx + 1)..=today {
                if high[i] > highest {
                    highest_idx = Some(i);
                    highest = high[i];
                }
            }
            diff = (highest - lowest) / 100.0;
        } else if tmp >= highest {
            highest_idx = Some(today);
            highest = tmp;
            diff = (highest - lowest) / 100.0;
        }
        out.push(if diff != 0.0 {
            (close[today] - lowest) / diff
        } else {
            0.0
        });
        trailing_idx += 1;
        today += 1;
    }
    out
}

/// `STOCHF` — fast stochastic (`ta_STOCHF.c`, split into fast-%K / fast-%D).
/// # Errors
/// `InvalidPeriod` if `fastk_period < 1` or `fastd_period < 1`.
#[allow(clippy::similar_names)] // fastk/fastd mirror TA-Lib's own output names.
pub fn stochf(
    high: &[f64],
    low: &[f64],
    close: &[f64],
    fastk_period: usize,
    fastd_period: usize,
    fastd_matype: usize,
) -> Result<(Vec<f64>, Vec<f64>)> {
    check_period("optInFastK_Period", fastk_period, 1)?;
    check_period("optInFastD_Period", fastd_period, 1)?;
    check_lengths(high.len(), &[low.len(), close.len()])?;
    let len = high.len();
    let mut fastk = nan_vec(len);
    let mut fastd = nan_vec(len);
    let lookback_k = fastk_period - 1;
    let lookback_fastd = ma_selector_lookback(fastd_period, fastd_matype)?;
    let lookback_total = lookback_k + lookback_fastd;
    if len <= lookback_total {
        return Ok((fastk, fastd));
    }
    let raw_k = raw_stoch_k(high, low, close, fastk_period);
    let fastd_full = crate::ma(&raw_k, fastd_period, fastd_matype)?;
    let n = len - lookback_total;
    fastk[lookback_total..lookback_total + n]
        .copy_from_slice(&raw_k[lookback_fastd..lookback_fastd + n]);
    fastd[lookback_total..lookback_total + n]
        .copy_from_slice(&fastd_full[lookback_fastd..lookback_fastd + n]);
    Ok((fastk, fastd))
}

/// `STOCH` — slow stochastic (`ta_STOCH.c`, split into slow-%K / slow-%D).
/// # Errors
/// Fails if any period `< 1`, series lengths differ, or `matype` is outside the supported range.
#[allow(clippy::similar_names, clippy::too_many_arguments)]
pub fn stoch(
    high: &[f64],
    low: &[f64],
    close: &[f64],
    fastk_period: usize,
    slowk_period: usize,
    slowk_matype: usize,
    slowd_period: usize,
    slowd_matype: usize,
) -> Result<(Vec<f64>, Vec<f64>)> {
    check_period("optInFastK_Period", fastk_period, 1)?;
    check_period("optInSlowK_Period", slowk_period, 1)?;
    check_period("optInSlowD_Period", slowd_period, 1)?;
    check_lengths(high.len(), &[low.len(), close.len()])?;
    let len = high.len();
    let mut slowk = nan_vec(len);
    let mut slowd = nan_vec(len);
    let lookback_k = fastk_period - 1;
    let lookback_kslow = ma_selector_lookback(slowk_period, slowk_matype)?;
    let lookback_dslow = ma_selector_lookback(slowd_period, slowd_matype)?;
    let lookback_total = lookback_k + lookback_kslow + lookback_dslow;
    if len <= lookback_total {
        return Ok((slowk, slowd));
    }
    let raw_k = raw_stoch_k(high, low, close, fastk_period);
    let slowk_full = crate::ma(&raw_k, slowk_period, slowk_matype)?;
    let slowk_dense = &slowk_full[lookback_kslow..];
    let slowd_full = crate::ma(slowk_dense, slowd_period, slowd_matype)?;
    let n = len - lookback_total;
    slowk[lookback_total..lookback_total + n]
        .copy_from_slice(&slowk_dense[lookback_dslow..lookback_dslow + n]);
    slowd[lookback_total..lookback_total + n]
        .copy_from_slice(&slowd_full[lookback_dslow..lookback_dslow + n]);
    Ok((slowk, slowd))
}

/// `STOCHRSI` — stochastic RSI (`ta_STOCHRSI.c`, split into fast-%K / fast-%D).
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `timeperiod < 2`, `fastk_period < 1`, or `fastd_period < 1`
#[allow(clippy::similar_names)] // fastk/fastd mirror TA-Lib's own output names.
pub fn stochrsi(
    input: &[f64],
    timeperiod: usize,
    fastk_period: usize,
    fastd_period: usize,
    fastd_matype: usize,
) -> Result<(Vec<f64>, Vec<f64>)> {
    check_period("optInTimePeriod", timeperiod, 2)?;
    check_period("optInFastK_Period", fastk_period, 1)?;
    check_period("optInFastD_Period", fastd_period, 1)?;
    let len = input.len();
    let mut fastk = nan_vec(len);
    let mut fastd = nan_vec(len);
    let lookback_stochf = (fastk_period - 1) + ma_selector_lookback(fastd_period, fastd_matype)?;
    let lookback_total = timeperiod + lookback_stochf;
    if len <= lookback_total {
        return Ok((fastk, fastd));
    }
    let rsi_full = rsi(input, timeperiod)?;
    let rsi_dense = &rsi_full[timeperiod..];
    let (fk, fd) = stochf(
        rsi_dense,
        rsi_dense,
        rsi_dense,
        fastk_period,
        fastd_period,
        fastd_matype,
    )?;
    let n = len - lookback_total;
    fastk[lookback_total..lookback_total + n]
        .copy_from_slice(&fk[lookback_stochf..lookback_stochf + n]);
    fastd[lookback_total..lookback_total + n]
        .copy_from_slice(&fd[lookback_stochf..lookback_stochf + n]);
    Ok((fastk, fastd))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TaError, mama};

    #[test]
    fn rsi_all_gains_is_100() {
        let input: Vec<f64> = (1..=10).map(f64::from).collect();
        let out = rsi(&input, 3).expect("valid");
        assert!(out[2].is_nan());
        assert!((out[3] - 100.0).abs() < 1e-12);
        assert!((out[9] - 100.0).abs() < 1e-12);
    }

    #[test]
    fn rsi_flat_series_guard_yields_zero() {
        let out = rsi(&[5.0; 8], 3).expect("valid");
        assert!((out[3] - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn adx_lookback_and_range() {
        let high: Vec<f64> = (0..30).map(|i| 10.0 + 0.5 * f64::from(i) + 1.0).collect();
        let low: Vec<f64> = (0..30).map(|i| 10.0 + 0.5 * f64::from(i) - 1.0).collect();
        let close: Vec<f64> = (0..30).map(|i| 10.0 + 0.5 * f64::from(i)).collect();
        let out = adx(&high, &low, &close, 5).expect("valid");
        assert!(out[8].is_nan());
        assert!(!out[9].is_nan());
        assert!(out[9..].iter().all(|v| (0.0..=100.0).contains(v)));
    }

    #[test]
    fn adx_flat_market_holds_previous_value() {
        let flat = [10.0; 20];
        let out = adx(&flat, &flat, &flat, 4).expect("valid");
        assert!(out[6].is_nan());
        assert!(out[7..].iter().all(|v| (*v - 0.0).abs() < f64::EPSILON));
    }

    #[test]
    fn adx_rejects_length_mismatch() {
        assert_eq!(
            adx(&[1.0, 2.0], &[1.0], &[1.0, 2.0], 2),
            Err(TaError::LengthMismatch { left: 2, right: 1 })
        );
    }

    #[test]
    fn mom_is_the_period_difference() {
        let out = mom(&[1.0, 3.0, 6.0, 10.0, 15.0], 2).expect("valid");
        assert!(out[0].is_nan());
        assert!(out[1].is_nan());
        assert!((out[2] - 5.0).abs() < 1e-12);
        assert!((out[3] - 7.0).abs() < 1e-12);
        assert!((out[4] - 9.0).abs() < 1e-12);
    }

    #[test]
    fn roc_family_ratios_and_zero_guard() {
        let data = [10.0, 20.0, 40.0];
        assert!((roc(&data, 1).expect("roc")[1] - 100.0).abs() < 1e-12);
        assert!((rocp(&data, 1).expect("rocp")[2] - 1.0).abs() < 1e-12);
        assert!((rocr(&data, 1).expect("rocr")[1] - 2.0).abs() < 1e-12);
        assert!((rocr100(&data, 1).expect("rocr100")[2] - 200.0).abs() < 1e-12);
        assert!(roc(&[0.0, 5.0], 1).expect("guard")[1].abs() < f64::EPSILON);
        assert!(rocr(&[0.0, 5.0], 1).expect("guard")[1].abs() < f64::EPSILON);
    }

    #[test]
    fn willr_sits_in_the_negative_hundred_band() {
        let high = [10.0, 12.0, 11.0, 13.0];
        let low = [8.0, 9.0, 7.0, 10.0];
        let close = [9.0, 11.0, 8.0, 12.0];
        let out = willr(&high, &low, &close, 3).expect("valid");
        assert!(out[1].is_nan());
        assert!((out[2] - (-80.0)).abs() < 1e-12);
        assert!((out[3] - (-100.0 / 6.0)).abs() < 1e-12);
        assert!(out[2..].iter().all(|v| (-100.0..=0.0).contains(v)));
    }

    #[test]
    fn cci_typical_price_window() {
        let series = [2.0, 4.0, 6.0];
        let out = cci(&series, &series, &series, 2).expect("valid");
        assert!(out[0].is_nan());
        assert!((out[1] - 1.0 / 0.015).abs() < 1e-9);
        assert!((out[2] - 1.0 / 0.015).abs() < 1e-9);
    }

    #[test]
    fn cmo_spans_plus_minus_hundred() {
        let up: Vec<f64> = (1..=6).map(f64::from).collect();
        let out = cmo(&up, 3).expect("valid");
        assert!(out[2].is_nan());
        assert!((out[3] - 100.0).abs() < 1e-12);
        let down: Vec<f64> = (1..=6).rev().map(f64::from).collect();
        assert!((cmo(&down, 3).expect("valid")[3] - (-100.0)).abs() < 1e-12);
    }

    #[test]
    fn cmo_flat_series_guard_yields_zero() {
        assert!(cmo(&[5.0; 8], 3).expect("valid")[3].abs() < f64::EPSILON);
    }

    #[test]
    fn bop_ratio_and_zero_range_guard() {
        let open = [1.0, 2.0];
        let high = [4.0, 5.0];
        let low = [0.0, 1.0];
        let close = [3.0, 4.0];
        let out = bop(&open, &high, &low, &close).expect("valid");
        assert!((out[0] - 0.5).abs() < 1e-12);
        assert!((out[1] - 0.5).abs() < 1e-12);
        let flat = bop(&[1.0], &[5.0], &[5.0], &[3.0]).expect("valid");
        assert!(flat[0].abs() < f64::EPSILON);
    }

    #[test]
    fn apo_and_ppo_are_fast_minus_slow_sma() {
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        let apo_out = apo(&data, 2, 3, 0).expect("apo");
        assert!(apo_out[1].is_nan()); // slow SMA3 lookback = 2
        assert!((apo_out[2] - 0.5).abs() < 1e-12);
        assert!((apo_out[4] - 0.5).abs() < 1e-12);
        let ppo_out = ppo(&data, 2, 3, 0).expect("ppo");
        assert!((ppo_out[2] - 25.0).abs() < 1e-12);
        assert!((ppo_out[4] - 12.5).abs() < 1e-12);
    }

    #[test]
    fn apo_swaps_fast_and_slow_periods() {
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        let straight = apo(&data, 2, 3, 0).expect("valid");
        let swapped = apo(&data, 3, 2, 0).expect("valid");
        for (a, b) in straight.iter().zip(&swapped) {
            assert!(a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan()));
        }
    }

    #[test]
    fn apo_mama_matype_dispatches_and_rejects_unknown() {
        let data = [1.0, 2.0, 3.0, 4.0, 5.0];
        let apo_out = apo(&data, 2, 3, 7).expect("apo mama short");
        assert_eq!(apo_out.len(), data.len());
        assert!(apo_out.iter().all(|v| v.is_nan()));
        let ppo_out = ppo(&data, 2, 3, 7).expect("ppo mama short");
        assert!(ppo_out.iter().all(|v| v.is_nan()));
        assert!(matches!(
            ppo(&data, 2, 3, 9),
            Err(TaError::UnsupportedMaType { matype: 9, .. })
        ));
    }

    #[test]
    fn apo_mama_equals_mama_minus_mama_on_long_series() {
        let close: Vec<f64> = (0..80).map(|i| 50.0 + (f64::from(i) * 0.1).sin()).collect();
        let out = apo(&close, 12, 26, 7).expect("apo mama");
        let (mama_line, _) = mama(&close, 0.5, 0.05).expect("mama");
        for (index, value) in out.iter().enumerate() {
            if mama_line[index].is_nan() {
                assert!(value.is_nan(), "index {index}");
            } else {
                assert_eq!(value.to_bits(), 0.0_f64.to_bits(), "index {index}");
            }
        }
    }

    #[test]
    fn aroon_tracks_bars_since_extreme() {
        let high = [10.0, 11.0, 12.0, 9.0, 8.0];
        let low = [5.0, 6.0, 7.0, 4.0, 3.0];
        let (down, up) = aroon(&high, &low, 3).expect("valid");
        assert!(down[2].is_nan());
        assert!((up[3] - 100.0 / 3.0 * 2.0).abs() < 1e-12);
        assert!((down[3] - 100.0).abs() < 1e-12);
        assert!((up[4] - 100.0 / 3.0).abs() < 1e-12);
        assert!((down[4] - 100.0).abs() < 1e-12);
    }

    #[test]
    fn aroonosc_is_up_minus_down() {
        let high = [10.0, 11.0, 12.0, 9.0, 8.0];
        let low = [5.0, 6.0, 7.0, 4.0, 3.0];
        let (down, up) = aroon(&high, &low, 3).expect("valid");
        let osc = aroonosc(&high, &low, 3).expect("valid");
        for i in 3..5 {
            assert!((osc[i] - (up[i] - down[i])).abs() < 1e-12);
        }
    }

    #[test]
    fn aroon_checked_casts_refuse_adversarial_length() {
        let err = aroon_end_idx(usize::MAX).expect_err("usize::MAX must not cast to i64");
        assert!(matches!(err, TaError::InputTooLong { .. }), "got {err:?}");
        let err = aroon_usize(-1, 10, "probe").expect_err("negative index");
        assert!(matches!(err, TaError::InputTooLong { .. }));
        let err = aroon_usize(10, 10, "probe").expect_err("index == len is OOB");
        assert!(matches!(err, TaError::InputTooLong { .. }));
        assert_eq!(aroon_usize(0, 10, "probe").unwrap(), 0);
        assert_eq!(aroon_usize(9, 10, "probe").unwrap(), 9);
        let high = [1.0, 2.0, 3.0, 4.0, 5.0];
        let low = [0.5, 1.5, 2.5, 3.5, 4.5];
        assert!(aroon(&high, &low, 2).is_ok());
        assert!(aroonosc(&high, &low, 2).is_ok());
    }

    #[test]
    fn trix_nan_prefix_then_finite() {
        let input: Vec<f64> = (1..=40).map(f64::from).collect();
        let out = trix(&input, 4).expect("valid");
        for value in &out[..10] {
            assert!(value.is_nan());
        }
        assert!(out[10].is_finite());
    }

    #[test]
    fn ultosc_flat_market_is_zero_after_lookback() {
        let flat = [50.0; 40];
        let out = ultosc(&flat, &flat, &flat, 7, 14, 28).expect("valid");
        assert!(out[27].is_nan());
        assert!(out[28..].iter().all(|v| (*v - 0.0).abs() < f64::EPSILON));
    }

    #[test]
    fn ultosc_stays_in_the_zero_hundred_band() {
        let close: Vec<f64> = (0..60).map(|i| 50.0 + f64::from(i)).collect();
        let high: Vec<f64> = close.iter().map(|c| c + 1.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 1.0).collect();
        let out = ultosc(&high, &low, &close, 7, 14, 28).expect("valid");
        assert!(out[28..].iter().all(|v| (0.0..=100.0).contains(v)));
    }

    fn ohlc(n: i32) -> (Vec<f64>, Vec<f64>, Vec<f64>) {
        let close: Vec<f64> = (0..n)
            .map(|i| 50.0 + 3.0 * (f64::from(i) * 0.4).sin() + 0.2 * f64::from(i))
            .collect();
        let high: Vec<f64> = close.iter().map(|c| c + 1.5).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 1.5).collect();
        (high, low, close)
    }

    #[test]
    fn dx_lookback_is_period_and_range_bounded() {
        let (high, low, close) = ohlc(40);
        let out = dx(&high, &low, &close, 5).expect("valid");
        assert!(out[4].is_nan());
        assert!(!out[5].is_nan());
        assert!(out[5..].iter().all(|v| (0.0..=100.0).contains(v)));
    }

    #[test]
    fn dx_flat_market_holds_previous_value() {
        let flat = [10.0; 20];
        let out = dx(&flat, &flat, &flat, 4).expect("valid");
        assert!(out[3].is_nan());
        assert!(out[4..].iter().all(|v| v.abs() < f64::EPSILON));
    }

    #[test]
    #[allow(clippy::similar_names, clippy::manual_midpoint)] // adx_out/adxr_out mirror the fns; /2.0 is the port.
    fn adxr_is_the_mean_of_adx_ends() {
        let (high, low, close) = ohlc(80);
        let period = 6;
        let adxr_out = adxr(&high, &low, &close, period).expect("adxr");
        let adx_out = adx(&high, &low, &close, period).expect("adx");
        let lookback = 3 * period - 2;
        assert!(adxr_out[lookback - 1].is_nan());
        for t in lookback..high.len() {
            let expect = (adx_out[t] + adx_out[t - (period - 1)]) / 2.0;
            assert_eq!(adxr_out[t].to_bits(), expect.to_bits());
        }
    }

    #[test]
    fn di_lookback_is_period_and_range_bounded() {
        let (high, low, close) = ohlc(40);
        for plus in [true, false] {
            let out = directional_di(&high, &low, &close, 7, plus).expect("di");
            assert!(out[6].is_nan());
            assert!(!out[7].is_nan());
            assert!(out[7..].iter().all(|v| (0.0..=100.0).contains(v)));
        }
    }

    #[test]
    fn dm_lookback_is_period_minus_one() {
        let (high, low, _close) = ohlc(30);
        let out = plus_dm(&high, &low, 8).expect("plus_dm");
        assert!(out[6].is_nan());
        assert!(!out[7].is_nan()); // lookback = period − 1 = 7
        assert!(out[7..].iter().all(|v| *v >= 0.0));
    }

    #[test]
    fn dm_period_one_is_the_raw_one_bar_move() {
        let high = [1.0, 3.0, 2.0];
        let low = [0.0, 1.0, 0.0];
        let plus = plus_dm(&high, &low, 1).expect("plus_dm p1");
        assert!(plus[0].is_nan());
        assert!((plus[1] - 2.0).abs() < 1e-12); // diffP=2, diffM=−1 → +DM=2
        assert!(plus[2].abs() < f64::EPSILON); // diffP=−1 → 0
        let minus = minus_dm(&high, &low, 1).expect("minus_dm p1");
        assert!(minus[1].abs() < f64::EPSILON); // diffM=−1 → 0
        assert!((minus[2] - 1.0).abs() < 1e-12); // diffM=1, diffP=−1 → −DM=1
    }

    #[test]
    fn di_period_one_divides_dm_by_true_range() {
        let high = [1.0, 3.0, 2.0];
        let low = [0.0, 1.0, 0.0];
        let close = [0.5, 2.0, 1.0];
        let plus = plus_di(&high, &low, &close, 1).expect("plus_di p1");
        assert!(plus[0].is_nan());
        assert!((plus[1] - 0.8).abs() < 1e-12);
    }

    #[test]
    fn macd_lookback_and_hist_relation() {
        let close: Vec<f64> = (0..80_i32)
            .map(|i| 50.0 + 5.0 * (f64::from(i) * 0.3).sin())
            .collect();
        let (macd_line, signal, hist) = macd(&close, 12, 26, 9).expect("macd");
        let lookback = (26 - 1) + (9 - 1); // 33
        assert!(macd_line[lookback - 1].is_nan());
        assert!(!macd_line[lookback].is_nan());
        for i in lookback..close.len() {
            assert_eq!(hist[i].to_bits(), (macd_line[i] - signal[i]).to_bits());
        }
    }

    #[test]
    fn macdfix_differs_from_macd_12_26() {
        let close: Vec<f64> = (0..80_i32)
            .map(|i| 50.0 + 5.0 * (f64::from(i) * 0.3).sin())
            .collect();
        let (fix_macd, _, _) = macdfix(&close, 9).expect("macdfix");
        let (proper_macd, _, _) = macd(&close, 12, 26, 9).expect("macd");
        assert!(
            (fix_macd[40] - proper_macd[40]).abs() > 1e-9,
            "MACDFIX must differ from PER_TO_K MACD"
        );
    }

    #[test]
    fn macdext_all_sma_lookback_and_hist_relation() {
        let close: Vec<f64> = (0..80_i32)
            .map(|i| 50.0 + 5.0 * (f64::from(i) * 0.3).sin())
            .collect();
        let (macd_line, signal, hist) = macdext(&close, 12, 0, 26, 0, 9, 0).expect("macdext");
        let lookback = 25 + 8;
        assert!(macd_line[lookback - 1].is_nan());
        assert!(!macd_line[lookback].is_nan());
        for i in lookback..close.len() {
            assert_eq!(hist[i].to_bits(), (macd_line[i] - signal[i]).to_bits());
        }
    }

    #[test]
    fn macdext_signal_period_one_is_identity_ma() {
        let close: Vec<f64> = (0..80_i32)
            .map(|i| 50.0 + 5.0 * (f64::from(i) * 0.3).sin())
            .collect();
        for signal_matype in [0_usize, 6, 7, 8] {
            let (macd_line, signal, hist) =
                macdext(&close, 12, 0, 26, 0, 1, signal_matype).expect("macdext signal=1");
            let lookback = 25; // max(11, 25) + 0 — period≤1 lookback is 0 for every matype
            assert!(
                macd_line[lookback - 1].is_nan(),
                "signal_matype={signal_matype}"
            );
            assert!(
                !macd_line[lookback].is_nan(),
                "signal_matype={signal_matype}"
            );
            for i in lookback..close.len() {
                assert_eq!(
                    signal[i].to_bits(),
                    macd_line[i].to_bits(),
                    "signal period 1 is identity at {i} matype={signal_matype}"
                );
                assert_eq!(
                    hist[i].to_bits(),
                    0.0_f64.to_bits(),
                    "hist is zero at {i} matype={signal_matype}"
                );
            }
        }
    }

    #[test]
    fn macdext_mama_with_longer_other_leg_matches_full_array_mama() {
        let close: Vec<f64> = (0..120_i32)
            .map(|i| 50.0 + 5.0 * (f64::from(i) * 0.3).sin())
            .collect();
        let (macd_line, signal, hist) =
            macdext(&close, 12, 7, 40, 0, 9, 0).expect("macdext mama+sma40");
        let mama_full = mama(&close, 0.5, 0.05).expect("mama").0;
        let slow = sma(&close, 40).expect("sma40");
        let lookback = 39 + 8; // max(32, 39) + signal SMA9
        assert!(macd_line[lookback - 1].is_nan());
        assert!(!macd_line[lookback].is_nan());
        for i in lookback..close.len() {
            let expected_macd = mama_full[i] - slow[i];
            assert_eq!(
                macd_line[i].to_bits(),
                expected_macd.to_bits(),
                "macd at {i} must use full-prefix MAMA, not slice-reseed"
            );
            assert_eq!(hist[i].to_bits(), (macd_line[i] - signal[i]).to_bits());
        }
        let reseeded = mama(&close[(39 - 32)..=119], 0.5, 0.05)
            .expect("reseed mama")
            .0;
        assert_ne!(
            mama_full[39].to_bits(),
            reseeded[32].to_bits(),
            "fixture must make full-prefix vs reseed diverge at index 39"
        );
    }

    #[test]
    fn macdext_mama_matype_lookback_and_unknown_still_rejects() {
        let short = [1.0, 2.0, 3.0, 4.0, 5.0];
        let (macd_line, signal, hist) =
            macdext(&short, 12, 7, 26, 7, 9, 7).expect("macdext mama short");
        assert!(
            macd_line
                .iter()
                .chain(&signal)
                .chain(&hist)
                .all(|v| v.is_nan())
        );
        let close: Vec<f64> = (0..100)
            .map(|i| 50.0 + 5.0 * (f64::from(i) * 0.3).sin())
            .collect();
        let (macd_line, signal, hist) = macdext(&close, 12, 7, 26, 7, 9, 7).expect("macdext mama");
        let lookback_total = 32 + 32;
        assert!(macd_line[lookback_total - 1].is_nan());
        assert!(!macd_line[lookback_total].is_nan());
        for i in lookback_total..close.len() {
            assert_eq!(hist[i].to_bits(), (macd_line[i] - signal[i]).to_bits());
        }
        assert!(matches!(
            macdext(&close, 12, 9, 26, 0, 9, 0),
            Err(TaError::UnsupportedMaType { matype: 9, .. })
        ));
    }

    #[test]
    fn raw_stoch_k_is_hand_checkable() {
        let high = [10.0, 12.0, 11.0, 13.0];
        let low = [8.0, 9.0, 7.0, 10.0];
        let close = [9.0, 11.0, 8.0, 12.0];
        let raw = raw_stoch_k(&high, &low, &close, 3);
        assert_eq!(raw.len(), 2);
        assert!((raw[0] - 20.0).abs() < 1e-12);
        assert!((raw[1] - 5.0 / 0.06).abs() < 1e-12);
        assert!(raw.iter().all(|v| (0.0..=100.0).contains(v)));
    }

    #[test]
    #[allow(clippy::similar_names)] // fastk/fastd mirror TA-Lib's output names.
    fn stochf_lookback_and_identity_smoothing() {
        let high = [10.0, 12.0, 11.0, 13.0];
        let low = [8.0, 9.0, 7.0, 10.0];
        let close = [9.0, 11.0, 8.0, 12.0];
        let (fastk, fastd) = stochf(&high, &low, &close, 3, 1, 0).expect("stochf");
        assert!(fastk[1].is_nan());
        assert!((fastk[2] - 20.0).abs() < 1e-12);
        for i in 2..4 {
            assert_eq!(fastk[i].to_bits(), fastd[i].to_bits());
        }
    }

    #[test]
    #[allow(clippy::similar_names)] // slowk/slowd/fastk/fastd mirror TA-Lib's output names.
    fn stoch_path_period1_matype7_is_identity_via_ma_selector_lookback() {
        let close: Vec<f64> = (0..20_i32)
            .map(|i| 50.0 + 5.0 * (f64::from(i) * 0.3).sin())
            .collect();
        let high: Vec<f64> = close.iter().map(|c| c + 1.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 1.0).collect();

        let (fk7, fd7) = stochf(&high, &low, &close, 5, 1, 7).expect("stochf period1 mama");
        let (fk0, fd0) = stochf(&high, &low, &close, 5, 1, 0).expect("stochf period1 sma");
        let stochf_lb = 4; // (5 − 1) + ma_selector_lookback(1, 7) = 4 + 0
        assert!(fk7[stochf_lb - 1].is_nan());
        assert!(!fk7[stochf_lb].is_nan());
        assert!(fd7[stochf_lb - 1].is_nan());
        assert!(!fd7[stochf_lb].is_nan());
        for i in stochf_lb..close.len() {
            assert_eq!(
                fk7[i].to_bits(),
                fd7[i].to_bits(),
                "fastd period 1 + matype 7 is identity: fastk == fastd at {i}"
            );
            assert_eq!(
                fk7[i].to_bits(),
                fk0[i].to_bits(),
                "period-1 MAMA and SMA identity MA must agree at {i}"
            );
            assert_eq!(fd7[i].to_bits(), fd0[i].to_bits());
        }

        let (sk7, sd7) = stoch(&high, &low, &close, 5, 1, 7, 1, 7).expect("stoch period1 mama");
        let stoch_lb = 4;
        assert!(sk7[stoch_lb - 1].is_nan());
        assert!(!sk7[stoch_lb].is_nan());
        assert!(sd7[stoch_lb - 1].is_nan());
        assert!(!sd7[stoch_lb].is_nan());
        for i in stoch_lb..close.len() {
            assert_eq!(
                sk7[i].to_bits(),
                sd7[i].to_bits(),
                "both period-1 MAMA legs are identity: slowk == slowd at {i}"
            );
            assert_eq!(sk7[i].to_bits(), fk7[i].to_bits());
        }

        let longer: Vec<f64> = (0..40_i32)
            .map(|i| 50.0 + 5.0 * (f64::from(i) * 0.2).sin())
            .collect();
        let (rk7, rd7) = stochrsi(&longer, 14, 5, 1, 7).expect("stochrsi period1 mama");
        let stochrsi_lb = 18;
        assert!(rk7[stochrsi_lb - 1].is_nan());
        assert!(!rk7[stochrsi_lb].is_nan());
        assert!(rd7[stochrsi_lb - 1].is_nan());
        assert!(!rd7[stochrsi_lb].is_nan());
        for i in stochrsi_lb..longer.len() {
            assert_eq!(
                rk7[i].to_bits(),
                rd7[i].to_bits(),
                "stochrsi fastd period 1 + matype 7 is identity at {i}"
            );
        }
    }

    #[test]
    #[allow(clippy::similar_names, clippy::manual_midpoint)] // fastk/fastd names; /2.0 is the check.
    fn stochf_fastd_is_the_sma_of_raw_k() {
        let high = [10.0, 12.0, 11.0, 13.0];
        let low = [8.0, 9.0, 7.0, 10.0];
        let close = [9.0, 11.0, 8.0, 12.0];
        let raw = raw_stoch_k(&high, &low, &close, 3); // len 2
        let (fastk, fastd) = stochf(&high, &low, &close, 3, 2, 0).expect("stochf");
        assert!(fastk[2].is_nan());
        assert!((fastk[3] - raw[1]).abs() < 1e-12);
        assert!((fastd[3] - (raw[0] + raw[1]) / 2.0).abs() < 1e-12);
    }

    #[test]
    #[allow(clippy::similar_names)] // slowk/slowd/fastk/fastd mirror TA-Lib's output names.
    fn stoch_default_lookback_and_slowk_equals_stochf_fastd() {
        let close: Vec<f64> = (0..40_i32)
            .map(|i| 50.0 + 5.0 * (f64::from(i) * 0.3).sin())
            .collect();
        let high: Vec<f64> = close.iter().map(|c| c + 1.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 1.0).collect();
        let (slowk, _slowd) = stoch(&high, &low, &close, 5, 3, 0, 3, 0).expect("stoch");
        assert!(slowk[7].is_nan());
        assert!(!slowk[8].is_nan());
        let (_fastk, fastd) = stochf(&high, &low, &close, 5, 3, 0).expect("stochf");
        for i in 8..close.len() {
            assert_eq!(slowk[i].to_bits(), fastd[i].to_bits());
        }
    }

    #[test]
    #[allow(clippy::similar_names)] // fastk/fastd mirror TA-Lib's output names.
    fn stochrsi_lookback_and_range() {
        let close: Vec<f64> = (0..60_i32)
            .map(|i| 50.0 + 5.0 * (f64::from(i) * 0.2).sin())
            .collect();
        let (fastk, fastd) = stochrsi(&close, 14, 5, 3, 0).expect("stochrsi");
        assert!(fastk[19].is_nan());
        assert!(!fastk[20].is_nan());
        assert!(
            fastk[20..]
                .iter()
                .all(|v| (-1e-9..=100.0 + 1e-9).contains(v))
        );
        assert!(
            fastd[20..]
                .iter()
                .all(|v| (-1e-9..=100.0 + 1e-9).contains(v))
        );
    }

    #[test]
    #[allow(clippy::similar_names)] // fastk/fastd mirror TA-Lib's output names.
    fn stochf_flat_window_yields_zero() {
        let flat = [7.0; 10];
        let (fastk, fastd) = stochf(&flat, &flat, &flat, 3, 3, 0).expect("stochf flat");
        assert!(fastk[4..].iter().all(|v| v.abs() < f64::EPSILON));
        assert!(fastd[4..].iter().all(|v| v.abs() < f64::EPSILON));
    }

    #[test]
    fn stochastics_accept_mama_matype_and_reject_out_of_range() {
        let close = [1.0, 2.0, 3.0, 4.0, 5.0];
        let (fk, fd) = stochf(&close, &close, &close, 5, 3, 7).expect("stochf matype 7 short");
        assert_eq!(fk.len(), close.len());
        assert!(fk.iter().chain(&fd).all(|v| v.is_nan()));
        let (sk, sd) = stoch(&close, &close, &close, 5, 3, 7, 3, 0).expect("stoch mixed 7/0 short");
        assert!(sk.iter().chain(&sd).all(|v| v.is_nan()));
        let (rk, rd) = stochrsi(&close, 14, 5, 3, 7).expect("stochrsi matype 7 short");
        assert!(rk.iter().chain(&rd).all(|v| v.is_nan()));
        assert!(matches!(
            stochf(&close, &close, &close, 5, 3, 9),
            Err(TaError::UnsupportedMaType { matype: 9, .. })
        ));
        assert!(matches!(
            stoch(&close, &close, &close, 5, 3, 0, 3, 9),
            Err(TaError::UnsupportedMaType { matype: 9, .. })
        ));
        assert!(matches!(
            stochrsi(&close, 14, 5, 3, 9),
            Err(TaError::UnsupportedMaType { matype: 9, .. })
        ));
    }

    #[test]
    #[allow(clippy::similar_names)] // slowk/fastk/fastd mirror TA-Lib's output names.
    fn stochastics_matype7_first_non_nan_lookback_pins() {
        let close: Vec<f64> = (0..120_i32)
            .map(|i| 50.0 + 5.0 * (f64::from(i) * 0.3).sin())
            .collect();
        let high: Vec<f64> = close.iter().map(|c| c + 1.0).collect();
        let low: Vec<f64> = close.iter().map(|c| c - 1.0).collect();

        let (fastk, fastd) = stochf(&high, &low, &close, 5, 3, 7).expect("stochf type7");
        let stochf_lb = 36;
        assert!(fastk[stochf_lb - 1].is_nan());
        assert!(!fastk[stochf_lb].is_nan());
        assert!(fastd[stochf_lb - 1].is_nan());
        assert!(!fastd[stochf_lb].is_nan());

        let (mixed_k, mixed_d) =
            stoch(&high, &low, &close, 5, 3, 7, 3, 0).expect("stoch mixed 7/0");
        let mixed_lb = 38;
        assert!(mixed_k[mixed_lb - 1].is_nan());
        assert!(!mixed_k[mixed_lb].is_nan());
        assert!(mixed_d[mixed_lb - 1].is_nan());
        assert!(!mixed_d[mixed_lb].is_nan());

        let (type7_k, type7_d) = stoch(&high, &low, &close, 5, 3, 7, 3, 7).expect("stoch all-MAMA");
        let all_mama_lb = 68;
        assert!(type7_k[all_mama_lb - 1].is_nan());
        assert!(!type7_k[all_mama_lb].is_nan());
        assert!(type7_d[all_mama_lb - 1].is_nan());
        assert!(!type7_d[all_mama_lb].is_nan());
        assert!(mixed_lb < all_mama_lb);
        assert!(!mixed_k[mixed_lb].is_nan());
        assert!(type7_k[mixed_lb].is_nan());

        let (rsi_k, rsi_d) = stochrsi(&close, 14, 5, 3, 7).expect("stochrsi type7");
        let stochrsi_lb = 50;
        assert!(rsi_k[stochrsi_lb - 1].is_nan());
        assert!(!rsi_k[stochrsi_lb].is_nan());
        assert!(rsi_d[stochrsi_lb - 1].is_nan());
        assert!(!rsi_d[stochrsi_lb].is_nan());
    }
}
