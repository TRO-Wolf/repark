//! Momentum-family window-UDF dispatch (RSI/ADX, ROC, MACD*, stochastics, AROON*).
//!
//! Called only from [`TaFn::compute`](super::TaFn::compute) /
//! [`TaFn::compute_all`](super::TaFn::compute_all). Kernel math stays in `crate::momentum`.

use crate::{
    adx, adxr, apo, aroon, aroonosc, bop, cci, cmo, dx, macd, macdext, macdfix, minus_di, minus_dm,
    mom, plus_di, plus_dm, ppo, roc, rocp, rocr, rocr100, rsi, stoch, stochf, stochrsi, trix,
    ultosc, willr,
};

use super::{MultiFamily, TaFn, family_dispatch_miss, family_dispatch_miss_multi, period};

/// ===========================================================================================
/// Momentum-family single-output dispatch.
/// ===========================================================================================
#[allow(clippy::too_many_lines)] // one flat match arm per UDF — splitting it would obscure the table.
pub(super) fn compute(func: TaFn, series: &[&[f64]], params: &[f64]) -> crate::Result<Vec<f64>> {
    match func {
        TaFn::Rsi => rsi(series[0], period(params[0])?),
        TaFn::Adx => adx(series[0], series[1], series[2], period(params[0])?),
        TaFn::Mom => mom(series[0], period(params[0])?),
        TaFn::Roc => roc(series[0], period(params[0])?),
        TaFn::Rocp => rocp(series[0], period(params[0])?),
        TaFn::Rocr => rocr(series[0], period(params[0])?),
        TaFn::Rocr100 => rocr100(series[0], period(params[0])?),
        TaFn::Willr => willr(series[0], series[1], series[2], period(params[0])?),
        TaFn::Cci => cci(series[0], series[1], series[2], period(params[0])?),
        TaFn::Cmo => cmo(series[0], period(params[0])?),
        TaFn::Bop => bop(series[0], series[1], series[2], series[3]),
        // params: [fastPeriod, slowPeriod, matype]; `matype` is a small non-negative code
        // coerced via [`period`] (kernel `ma_dispatch` range-validates it).
        TaFn::Apo => apo(
            series[0],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
        ),
        TaFn::Ppo => ppo(
            series[0],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
        ),
        TaFn::AroonDown => aroon(series[0], series[1], period(params[0])?).map(|(down, _)| down),
        TaFn::AroonUp => aroon(series[0], series[1], period(params[0])?).map(|(_, up)| up),
        TaFn::Aroonosc => aroonosc(series[0], series[1], period(params[0])?),
        TaFn::Trix => trix(series[0], period(params[0])?),
        TaFn::Ultosc => ultosc(
            series[0],
            series[1],
            series[2],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
        ),
        TaFn::Dx => dx(series[0], series[1], series[2], period(params[0])?),
        TaFn::Adxr => adxr(series[0], series[1], series[2], period(params[0])?),
        TaFn::PlusDi => plus_di(series[0], series[1], series[2], period(params[0])?),
        TaFn::MinusDi => minus_di(series[0], series[1], series[2], period(params[0])?),
        TaFn::PlusDm => plus_dm(series[0], series[1], period(params[0])?),
        TaFn::MinusDm => minus_dm(series[0], series[1], period(params[0])?),
        // MACD splits: params [fast, slow, signal]; each output picks one band.
        TaFn::Macd => macd(
            series[0],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
        )
        .map(|(m, _, _)| m),
        TaFn::MacdSignal => macd(
            series[0],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
        )
        .map(|(_, s, _)| s),
        TaFn::MacdHist => macd(
            series[0],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
        )
        .map(|(_, _, h)| h),
        // MACDFIX splits: params [signal] (12/26 pinned).
        TaFn::Macdfix => macdfix(series[0], period(params[0])?).map(|(m, _, _)| m),
        TaFn::MacdfixSignal => macdfix(series[0], period(params[0])?).map(|(_, s, _)| s),
        TaFn::MacdfixHist => macdfix(series[0], period(params[0])?).map(|(_, _, h)| h),
        // MACDEXT splits: params [fastPeriod, fastMAType, slowPeriod, slowMAType, signalPeriod,
        // signalMAType], `matype`s coerced via [`period`] then range-validated by the kernel.
        TaFn::Macdext => macdext(
            series[0],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
            period(params[3])?,
            period(params[4])?,
            period(params[5])?,
        )
        .map(|(m, _, _)| m),
        TaFn::MacdextSignal => macdext(
            series[0],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
            period(params[3])?,
            period(params[4])?,
            period(params[5])?,
        )
        .map(|(_, s, _)| s),
        TaFn::MacdextHist => macdext(
            series[0],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
            period(params[3])?,
            period(params[4])?,
            period(params[5])?,
        )
        .map(|(_, _, h)| h),
        // STOCH splits: params [fastkPeriod, slowkPeriod, slowkMAType, slowdPeriod,
        // slowdMAType]; each output picks one line.
        TaFn::StochSlowk => stoch(
            series[0],
            series[1],
            series[2],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
            period(params[3])?,
            period(params[4])?,
        )
        .map(|(k, _)| k),
        TaFn::StochSlowd => stoch(
            series[0],
            series[1],
            series[2],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
            period(params[3])?,
            period(params[4])?,
        )
        .map(|(_, d)| d),
        // STOCHF splits: params [fastkPeriod, fastdPeriod, fastdMAType].
        TaFn::StochfFastk => stochf(
            series[0],
            series[1],
            series[2],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
        )
        .map(|(k, _)| k),
        TaFn::StochfFastd => stochf(
            series[0],
            series[1],
            series[2],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
        )
        .map(|(_, d)| d),
        // STOCHRSI splits: params [timeperiod, fastkPeriod, fastdPeriod, fastdMAType].
        TaFn::StochrsiFastk => stochrsi(
            series[0],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
            period(params[3])?,
        )
        .map(|(k, _)| k),
        TaFn::StochrsiFastd => stochrsi(
            series[0],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
            period(params[3])?,
        )
        .map(|(_, d)| d),
        other => Err(family_dispatch_miss(other)),
    }
}

/// ===========================================================================================
/// Momentum multi-output families (MACD* / STOCH* / AROON) — one kernel run, every band.
/// ===========================================================================================
pub(super) fn compute_all(
    family: MultiFamily,
    series: &[&[f64]],
    params: &[f64],
) -> crate::Result<Vec<Vec<f64>>> {
    match family {
        MultiFamily::Macd => {
            let (macd_line, signal, hist) = macd(
                series[0],
                period(params[0])?,
                period(params[1])?,
                period(params[2])?,
            )?;
            Ok(vec![macd_line, signal, hist])
        }
        MultiFamily::Macdfix => {
            let (macd_line, signal, hist) = macdfix(series[0], period(params[0])?)?;
            Ok(vec![macd_line, signal, hist])
        }
        MultiFamily::Macdext => {
            let (macd_line, signal, hist) = macdext(
                series[0],
                period(params[0])?,
                period(params[1])?,
                period(params[2])?,
                period(params[3])?,
                period(params[4])?,
                period(params[5])?,
            )?;
            Ok(vec![macd_line, signal, hist])
        }
        MultiFamily::Stoch => {
            let (slow_k, slow_d) = stoch(
                series[0],
                series[1],
                series[2],
                period(params[0])?,
                period(params[1])?,
                period(params[2])?,
                period(params[3])?,
                period(params[4])?,
            )?;
            Ok(vec![slow_k, slow_d])
        }
        MultiFamily::Stochf => {
            let (fast_k, fast_d) = stochf(
                series[0],
                series[1],
                series[2],
                period(params[0])?,
                period(params[1])?,
                period(params[2])?,
            )?;
            Ok(vec![fast_k, fast_d])
        }
        MultiFamily::Stochrsi => {
            let (fast_k, fast_d) = stochrsi(
                series[0],
                period(params[0])?,
                period(params[1])?,
                period(params[2])?,
                period(params[3])?,
            )?;
            Ok(vec![fast_k, fast_d])
        }
        MultiFamily::Aroon => {
            let (down, up) = aroon(series[0], series[1], period(params[0])?)?;
            Ok(vec![down, up])
        }
        other => Err(family_dispatch_miss_multi(other)),
    }
}
