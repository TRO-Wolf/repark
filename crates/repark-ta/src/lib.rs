//! `RePark`'s own pure-Rust technical-analysis kernels — bit-exact TA-Lib C 0.4.0 ports.
//!
//! The pipeline's models were trained on features computed by C TA-Lib 0.4.0 (via
//! `polars_talib`); serving them indicator values with *different* numerics is a P&L bug no
//! unit test can see. So every kernel here is a hand-port of the corresponding TA-Lib C 0.4.0
//! function (`ta-lib/src/ta_func/ta_<NAME>.c`) preserving its exact operation order, and every
//! kernel is gated by golden fixtures recorded from that C library: `tests/goldens.rs` asserts
//! **strict `f64::to_bits` equality** over two fixtures — a 5000-row random walk and a 600-row
//! flat-plateau series that drives the epsilon-guard branches. No C is compiled, linked, or
//! vendored — the C source is a read-only porting reference.
//!
//! ## The numerics contract (violating any of these breaks bit-exactness)
//!
//! - **No `mul_add` / FMA, ever.** C TA-Lib rounds the multiply and the add separately; a fused
//!   op rounds once and drifts (this exact bug is why the `talib-rs` crate failed evaluation —
//!   task/todo.md T0).
//! - **Replicate TA-Lib's *incremental* window accumulators, drift and all.** SMA/VAR/CORREL/
//!   BBANDS keep running totals updated add-one/subtract-one per row; recomputing each window
//!   from scratch is mathematically identical but bit-different after thousands of rows.
//! - **Wilder smoothing in C's expression order** (`prev *= period - 1; prev += x; prev /=
//!   period` — three statements, three roundings) for RSI/ATR/ADX.
//! - **The `TA_IS_ZERO` epsilon guards** (±1e-8) short-circuit divisions exactly where C does,
//!   producing `0.0` (not NaN/Inf) there.
//! - **NaN lookback prefixes**: outputs are input-length with the first `lookback` positions
//!   NaN, matching what `polars_talib` surfaces as nulls. Inputs too short for one output
//!   return an all-NaN vector (C reports zero output elements — success, not an error).
//!
//! Global TA-Lib settings are fixed at their defaults: compatibility = Classic, unstable
//! period = 0 for every function. These are deliberately not configurable.
//!
//! ## Known, deliberate divergences from C
//!
//! - **Huge `LINEARREG` periods.** C computes `SumX`/`SumXSqr` in 32-bit `int`, which silently
//!   overflows past period ≈ 46 340 (undefined behavior in C). We compute in 64-bit and stay
//!   correct through [`MAX_PERIOD`]; goldens never exercise that range.
//! - **`linearreg_angle` and libm.** `atan` is a transcendental libm call on BOTH sides and
//!   IEEE-754 does not require it to be correctly rounded — the goldens are recorded and tested
//!   on glibc/x86-64, and a different libm (musl, macOS) can legally differ by ~1 ulp. A
//!   `linearreg_angle` golden failure on a new platform is a libm delta, not a port bug —
//!   check the other kernels first.
//!
//! ## Attribution
//!
//! The algorithms are ported from TA-Lib (<https://ta-lib.org>), BSD-3-Clause,
//! Copyright (c) 1999-2007 Mario Fortier et al. See `NOTICE` in this crate.
//!
//! **Style exception:** kernel locals keep C-mirrored names (`end_idx`, `trailing_idx`, …) so
//! ports stay reviewable against the TA-Lib C reference. This is a deliberate exception to the
//! workspace "spell things out (`index` not `idx`)" house rule — do not mass-rename.

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

/// ===========================================================================================
/// Kernel-argument errors.
///
/// Mirrors TA-Lib's `TA_BAD_PARAM`: an out-of-range period or mismatched input lengths is a
/// caller bug and errors loudly. A *short* input is NOT an error — it yields an all-NaN output,
/// exactly as C TA-Lib reports zero output elements with success.
/// ===========================================================================================
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum TaError {
    /// The period parameter is outside the function's TA-Lib documented range.
    #[error("invalid {name}: {value} (TA-Lib requires {min}..={MAX_PERIOD})")]
    InvalidPeriod {
        /// Parameter name as documented by TA-Lib (e.g. `optInTimePeriod`).
        name: &'static str,
        /// The rejected value.
        value: usize,
        /// The TA-Lib documented minimum (the maximum is always [`MAX_PERIOD`]).
        min: usize,
    },
    /// A window-UDF period argument was not a whole number (silent `f64 as usize` truncation
    /// is forbidden — e.g. `21.9` must fail, not become `21`).
    #[error("invalid {name}: {value} is not a whole number")]
    NonIntegralPeriod {
        /// Parameter name (e.g. `optInTimePeriod`).
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
    /// Series length exceeds the signed-index range the C-port bookkeeping can address
    /// without truncating/wrapping (audit SAF-005 — AROON and similar trailing-index kernels).
    #[error("input too long for {name}: length {len} exceeds i64 index range")]
    InputTooLong {
        /// Kernel / parameter name for the message.
        name: &'static str,
        /// Rejected length.
        len: usize,
    },
    /// A moving-average type code (`optInMAType`) this batch does not implement. TA-Lib's `MA`
    /// selector routes `matype` 0..=8 to SMA/EMA/WMA/DEMA/TEMA/TRIMA/KAMA/MAMA/T3; `MA`/`MAVP`/
    /// `APO`/`PPO`/`MACDEXT` and the stochastic smoothing legs (`STOCH`/`STOCHF`/`STOCHRSI`) all
    /// support the full set (MAMA via [`mama`]). Codes outside 0..=8 are undefined everywhere.
    #[error("unsupported optInMAType {matype}: {reason}")]
    UnsupportedMaType {
        /// The rejected MA-type code.
        matype: usize,
        /// Why it is rejected (codes outside 0..=8 are undefined).
        reason: &'static str,
    },
    /// A real-valued (non-period) parameter is outside its TA-Lib documented range — e.g. `SAR`'s
    /// `optInAcceleration` / `optInMaximum` (`[0, 3e37]`), `SAREXT`'s eight acceleration/offset/
    /// start parameters, or `MAMA`'s `optInFastLimit` / `optInSlowLimit` (`[0.01, 0.99]`). Mirrors
    /// TA-Lib's `TA_BAD_PARAM`. `NaN` is rejected here (C's `(<min)||(>max)` quirk lets `NaN`
    /// through; failing loud is safer and no golden exercises a `NaN` parameter).
    #[error("invalid {name}: {value} (TA-Lib requires {range})")]
    InvalidRealParam {
        /// Parameter name as documented by TA-Lib (e.g. `optInAcceleration`).
        name: &'static str,
        /// The rejected value, rendered for the message (`f64` is not `Eq`).
        value: String,
        /// The documented valid range, as text (e.g. `0..=3e37`).
        range: &'static str,
    },
}

/// Convenient alias for kernel results.
pub type Result<T> = std::result::Result<T, TaError>;

/// TA-Lib's documented period ceiling (`TA_INTEGER_MAX`-adjacent bound: every `optInTimePeriod`
/// is specified as `From N to 100000`). Enforcing it mirrors C's `TA_BAD_PARAM` AND removes the
/// entire usize-overflow class from period arithmetic (`period + 1`, `2 * period`,
/// `period * (period − 1) * (2 * period − 1)` are all safely below 2^63 at this cap).
pub const MAX_PERIOD: usize = 100_000;

/// C TA-Lib's `TA_REAL_MAX` (`ta_defs.h`): the upper bound on real-valued optional parameters
/// (`3.0e37`). `SAR`/`SAREXT` accelerations and `SAREXT`'s start value are range-checked against it.
pub const TA_REAL_MAX: f64 = 3.0e37;

/// C TA-Lib's epsilon for the zero guards (`ta_utility.h`): 0.00000001.
const TA_EPSILON: f64 = 0.000_000_01;

/// C TA-Lib's `TA_IS_ZERO(v)`: `((-0.00000001) < v) && (v < 0.00000001)`.
pub(crate) fn is_zero(v: f64) -> bool {
    (-TA_EPSILON < v) && (v < TA_EPSILON)
}

/// C TA-Lib's `TA_IS_ZERO_OR_NEG(v)`: `v < 0.00000001`.
pub(crate) fn is_zero_or_neg(v: f64) -> bool {
    v < TA_EPSILON
}

/// Lossless `usize → f64` for period parameters (periods are capped at [`MAX_PERIOD`] ≪ 2^52).
#[allow(clippy::cast_precision_loss)]
pub(crate) fn as_f64(n: usize) -> f64 {
    n as f64
}

/// Full-length all-NaN output buffer — the lookback prefix and the short-input result.
pub(crate) fn nan_vec(len: usize) -> Vec<f64> {
    vec![f64::NAN; len]
}

/// Validate a period against its TA-Lib documented range (`min..=MAX_PERIOD`).
pub(crate) fn check_period(name: &'static str, value: usize, min: usize) -> Result<()> {
    if value < min || value > MAX_PERIOD {
        return Err(TaError::InvalidPeriod { name, value, min });
    }
    Ok(())
}

/// Validate a real-valued parameter against its TA-Lib documented `[min, max]` range. Rejects
/// `NaN` (which C's `(v<min)||(v>max)` check lets through — failing loud is safer). `range` is the
/// human-readable range text carried into the error.
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

/// Validate that every co-series input matches the first input's length.
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

/// C TA-Lib's `TRUE_RANGE(TH, TL, YC, OUT)` macro: greatest of `TH−TL`, `|TH−YC|`, `|TL−YC|`.
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
