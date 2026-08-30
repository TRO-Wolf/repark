//! Copyright (c) 1999-2007 Mario Fortier et al. See `NOTICE` in this crate.
//! Pure-Rust, bit-exact ports of TA-Lib C 0.4.0 technical-analysis kernels.

use thiserror::Error;

#[cfg(feature = "datafusion")]
pub mod extension;
pub mod math_operator;
pub mod momentum;
pub mod overlap;
pub mod price_transform;
pub mod statistic;
#[cfg(feature = "datafusion")]
pub mod udf;
pub mod volatility;
pub mod volume;

#[cfg(feature = "datafusion")]
pub use extension::TaExtension;
pub use math_operator::{max, min, sum};
pub use momentum::{
    adx, adxr, apo, aroon, aroonosc, bop, cci, cmo, dx, macd, macdext, macdfix, minus_di, minus_dm,
    mom, plus_di, plus_dm, ppo, roc, rocp, rocr, rocr100, rsi, stoch, stochf, stochrsi, trix,
    ultosc, willr,
};
pub use overlap::{
    bbands, dema, ema, kama, ma, mama, mavp, midpoint, midprice, sar, sarext, sma, t3, tema, trima,
    wma,
};
pub use price_transform::{avgprice, medprice, typprice, wclprice};
pub use statistic::{
    beta, correl, linearreg, linearreg_angle, linearreg_intercept, linearreg_slope, stddev, tsf,
    var,
};
pub use volatility::{atr, natr, trange};
pub use volume::{ad, adosc, mfi, obv};

/// Kernel-argument errors.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum TaError {
    /// The period parameter is outside the function's TA-Lib documented range.
    #[error("invalid {name}: {value} (TA-Lib requires {min}..={MAX_PERIOD})")]
    InvalidPeriod {
        /// Parameter name as documented by TA-Lib (e.g.
        name: &'static str,
        /// The rejected value.
        value: usize,
        /// The TA-Lib documented minimum (the maximum is always [`MAX_PERIOD`]).
        min: usize,
    },
    /// A window-UDF period argument was not a whole number.
    #[error("invalid {name}: {value} is not a whole number")]
    NonIntegralPeriod {
        /// Parameter name (e.g.
        name: &'static str,
        /// The rejected floating value, rendered for the error message.
        value: String,
    },
    /// Multi-series inputs must be equally long.
    #[error("input length mismatch: {left} vs {right}")]
    LengthMismatch {
        /// First input's length.
        left: usize,
        /// Mismatching input's length.
        right: usize,
    },
    /// Series length exceeds the signed-index range used by C-port bookkeeping.
    #[error("input too long for {name}: length {len} exceeds i64 index range")]
    InputTooLong {
        /// Kernel / parameter name for the message.
        name: &'static str,
        /// Rejected length.
        len: usize,
    },
    /// A moving-average type code outside TA-Lib's implemented `0..=8` range.
    #[error("unsupported optInMAType {matype}: {reason}")]
    UnsupportedMaType {
        /// The rejected MA-type code.
        matype: usize,
        /// Why it is rejected (codes outside 0..=8 are undefined).
        reason: &'static str,
    },
    /// A real-valued parameter is outside its TA-Lib range; NaN is rejected.
    #[error("invalid {name}: {value} (TA-Lib requires {range})")]
    InvalidRealParam {
        /// Parameter name as documented by TA-Lib (e.g.
        name: &'static str,
        /// The rejected value, rendered for the message (`f64` is not `Eq`).
        value: String,
        /// The documented valid range, as text (e.g.
        range: &'static str,
    },
}

/// Result type returned by kernels.
pub type Result<T> = std::result::Result<T, TaError>;

/// TA-Lib's documented period ceiling, which also bounds period arithmetic.
pub const MAX_PERIOD: usize = 100_000;

/// C TA-Lib's upper bound for real-valued optional parameters.
pub const TA_REAL_MAX: f64 = 3.0e37;

/// C TA-Lib's epsilon for zero guards.
const TA_EPSILON: f64 = 0.000_000_01;

/// Implement C TA-Lib's `TA_IS_ZERO` predicate.
pub(crate) fn is_zero(v: f64) -> bool {
    (-TA_EPSILON < v) && (v < TA_EPSILON)
}

/// Implement C TA-Lib's `TA_IS_ZERO_OR_NEG` predicate.
pub(crate) fn is_zero_or_neg(v: f64) -> bool {
    v < TA_EPSILON
}

/// Convert a bounded period to `f64` without precision loss.
#[allow(clippy::cast_precision_loss)]
pub(crate) fn as_f64(n: usize) -> f64 {
    n as f64
}

/// Allocate a full-length all-NaN output buffer.
pub(crate) fn nan_vec(len: usize) -> Vec<f64> {
    vec![f64::NAN; len]
}

/// Validate a period against its TA-Lib range.
pub(crate) fn check_period(name: &'static str, value: usize, min: usize) -> Result<()> {
    if value < min || value > MAX_PERIOD {
        return Err(TaError::InvalidPeriod { name, value, min });
    }
    Ok(())
}

/// Validate a real-valued parameter, rejecting NaN and values outside `[min, max]`.
pub(crate) fn check_real_param(
    name: &'static str,
    value: f64,
    min: f64,
    max: f64,
    range: &'static str,
) -> Result<()> {
    if !(value >= min && value <= max) {
        return Err(TaError::InvalidRealParam {
            name,
            value: value.to_string(),
            range,
        });
    }
    Ok(())
}

/// Validate that every co-series input matches the first length.
pub(crate) fn check_lengths(first: usize, rest: &[usize]) -> Result<()> {
    for &len in rest {
        if len != first {
            return Err(TaError::LengthMismatch {
                left: first,
                right: len,
            });
        }
    }
    Ok(())
}

/// Return C TA-Lib's greatest high-low or previous-close range.
pub(crate) fn true_range(today_high: f64, today_low: f64, yesterday_close: f64) -> f64 {
    let mut greatest = today_high - today_low;
    let val2 = (yesterday_close - today_high).abs();
    if val2 > greatest {
        greatest = val2;
    }
    let val3 = (yesterday_close - today_low).abs();
    if val3 > greatest {
        greatest = val3;
    }
    greatest
}

#[cfg(test)]
mod tests;
