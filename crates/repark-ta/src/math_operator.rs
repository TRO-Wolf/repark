//! Math Operators — `MIN`, `MAX`, `SUM` (TA-Lib C 0.4.0 ports; see the crate docs for the
//! numerics contract).
//!
//! `MIN`/`MAX` are the rolling extrema over the trailing `period` window. They are ported
//! op-for-op from TA-Lib's trailing-index rescan algorithm (`ta_MIN.c` / `ta_MAX.c`): the running
//! extreme is kept together with the index it sits at, and the full window is rescanned only when
//! that index falls out of the trailing edge — otherwise a single comparison against the new bar
//! extends it. The tie-break (`<=` for `MIN`, `>=` for `MAX`) prefers the *most recent* equal
//! value, exactly as C does. `SUM` is the incremental running total (`ta_SUM.c`), the same
//! add-one/subtract-one accumulator as [`crate::sma`] without the final divide.

use crate::{Result, check_period, nan_vec};

/// ===========================================================================================
/// `MIN` — lowest value over the trailing `period` window (`ta_MIN.c`, `TA_MIN`).
///
/// TA-Lib's trailing-index rescan: `lowest`/`lowest_idx` track the current window minimum and
/// its position. When that position drops below the trailing edge it is stale, so the window
/// `(trailing..=today)` is rescanned to find the new minimum; otherwise the incoming bar is
/// compared once (`tmp <= lowest`, `<=` so an equal value adopts the more recent index — this
/// is what makes the rescans fire at C's exact cadence, hence bit-identical output).
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn min(input: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    let mut out = nan_vec(len);
    if len < period {
        return Ok(out);
    }
    let lookback = period - 1;
    let end_idx = len - 1;
    let mut today = lookback;
    let mut trailing_idx = 0_usize;
    let mut lowest_idx: Option<usize> = None;
    let mut lowest = 0.0_f64;
    while today <= end_idx {
        let need_rescan = match lowest_idx {
            Some(idx) => idx < trailing_idx,
            None => true,
        };
        if need_rescan {
            lowest_idx = Some(trailing_idx);
            lowest = input[trailing_idx];
            // Index-based rescan, op-for-op with C's `while(++i<=today)` — the index is the
            // running argmin, so an iterator would only obscure the port.
            #[allow(clippy::needless_range_loop)]
            for i in (trailing_idx + 1)..=today {
                if input[i] < lowest {
                    lowest_idx = Some(i);
                    lowest = input[i];
                }
            }
        } else if input[today] <= lowest {
            lowest_idx = Some(today);
            lowest = input[today];
        }
        out[today] = lowest;
        trailing_idx += 1;
        today += 1;
    }
    Ok(out)
}

/// ===========================================================================================
/// `MAX` — highest value over the trailing `period` window (`ta_MAX.c`, `TA_MAX`).
///
/// The mirror of [`min`]: same trailing-index rescan, with `>` inside the rescan and `>=` for
/// the single-bar extension (an equal value adopts the more recent index).
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn max(input: &[f64], period: usize) -> Result<Vec<f64>> {
    check_period("optInTimePeriod", period, 2)?;
    let len = input.len();
    let mut out = nan_vec(len);
    if len < period {
        return Ok(out);
    }
    let lookback = period - 1;
    let end_idx = len - 1;
    let mut today = lookback;
    let mut trailing_idx = 0_usize;
    let mut highest_idx: Option<usize> = None;
    let mut highest = 0.0_f64;
    while today <= end_idx {
        let need_rescan = match highest_idx {
            Some(idx) => idx < trailing_idx,
            None => true,
        };
        if need_rescan {
            highest_idx = Some(trailing_idx);
            highest = input[trailing_idx];
            // Index-based rescan, op-for-op with C's `while(++i<=today)` (see [`min`]).
            #[allow(clippy::needless_range_loop)]
            for i in (trailing_idx + 1)..=today {
                if input[i] > highest {
                    highest_idx = Some(i);
                    highest = input[i];
                }
            }
        } else if input[today] >= highest {
            highest_idx = Some(today);
            highest = input[today];
        }
        out[today] = highest;
        trailing_idx += 1;
        today += 1;
    }
    Ok(out)
}

/// ===========================================================================================
/// `SUM` — rolling sum over the trailing `period` window (`ta_SUM.c`, `TA_SUM`).
///
/// Incremental running total: add the incoming value, snapshot the total, subtract the trailing
/// value. The subtract-BEFORE-snapshot-consume order is C's and is load-bearing for
/// bit-exactness — this is [`crate::sma`]'s accumulator without the divide.
/// ===========================================================================================
///
/// # Errors
/// [`crate::TaError::InvalidPeriod`] if `period < 2`.
pub fn sum(input: &[f64], period: usize) -> Result<Vec<f64>> {
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
    // `enumerate` gives (trailing_idx, today): (0, lookback), (1, lookback+1), … exactly the two
    // C indices (`trailingIdx` starts at `startIdx − lookbackTotal = 0`, `i` at `startIdx`).
    for (trailing_idx, today) in (lookback..len).enumerate() {
        period_total += input[today];
        let temp = period_total;
        period_total -= input[trailing_idx];
        out[today] = temp;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaError;

    /// A short series with a clear rolling extremum / sum, hand-checked against the C algorithm
    /// (and cross-checked against `polars_talib` when the goldens were recorded).
    const SERIES: [f64; 10] = [5.0, 3.0, 8.0, 1.0, 9.0, 2.0, 7.0, 4.0, 6.0, 10.0];

    #[test]
    fn min_max_sum_reject_period_below_two() {
        for name_result in [min(&SERIES, 1), max(&SERIES, 1), sum(&SERIES, 1)] {
            assert_eq!(
                name_result,
                Err(TaError::InvalidPeriod {
                    name: "optInTimePeriod",
                    value: 1,
                    min: 2,
                })
            );
        }
    }

    #[test]
    fn short_input_is_all_nan_not_error() {
        let out = min(&[1.0, 2.0], 3).expect("valid");
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|v| v.is_nan()));
        assert!(sum(&[], 3).expect("valid").is_empty());
    }

    #[test]
    fn min_rolling_window_matches_c() {
        let out = min(&SERIES, 3).expect("valid");
        assert!(out[0].is_nan());
        assert!(out[1].is_nan());
        let want = [3.0, 1.0, 1.0, 1.0, 2.0, 2.0, 4.0, 4.0];
        for (got, expected) in out[2..].iter().zip(&want) {
            assert!((got - expected).abs() < 1e-12);
        }
    }

    #[test]
    fn max_rolling_window_matches_c() {
        let out = max(&SERIES, 3).expect("valid");
        assert!(out[1].is_nan());
        let want = [8.0, 8.0, 9.0, 9.0, 9.0, 7.0, 7.0, 10.0];
        for (got, expected) in out[2..].iter().zip(&want) {
            assert!((got - expected).abs() < 1e-12);
        }
    }

    #[test]
    fn sum_rolling_window_matches_c() {
        let out = sum(&SERIES, 3).expect("valid");
        assert!(out[1].is_nan());
        let want = [16.0, 12.0, 18.0, 12.0, 18.0, 13.0, 17.0, 20.0];
        for (got, expected) in out[2..].iter().zip(&want) {
            assert!((got - expected).abs() < 1e-12);
        }
    }

    #[test]
    fn max_prefers_the_more_recent_equal_value() {
        // Two equal maxima; `>=` keeps the later index, so the window slides without a rescan
        // producing a different value — the branch that makes MIN/MAX bit-exact.
        let out = max(&[1.0, 5.0, 5.0, 1.0], 2).expect("valid");
        assert!((out[1] - 5.0).abs() < 1e-12);
        assert!((out[2] - 5.0).abs() < 1e-12);
        assert!((out[3] - 5.0).abs() < 1e-12);
    }
}
