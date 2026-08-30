//! Overlap-family window-UDF dispatch.

use crate::{
    bbands, dema, ema, kama, ma, mama, mavp, midpoint, midprice, sar, sarext, sma, t3, tema, trima,
    wma,
};

use super::{MultiFamily, TaFn, family_dispatch_miss, family_dispatch_miss_multi, period};

/// Overlap-family single-output dispatch.
pub(super) fn compute(func: TaFn, series: &[&[f64]], params: &[f64]) -> crate::Result<Vec<f64>> {
    match func {
        TaFn::Sma => sma(series[0], period(params[0])?),
        TaFn::Ema => ema(series[0], period(params[0])?),
        TaFn::Wma => wma(series[0], period(params[0])?),
        TaFn::Dema => dema(series[0], period(params[0])?),
        TaFn::Tema => tema(series[0], period(params[0])?),
        TaFn::Trima => trima(series[0], period(params[0])?),
        TaFn::Kama => kama(series[0], period(params[0])?),
        TaFn::T3 => t3(series[0], period(params[0])?, params[1]),
        TaFn::Midpoint => midpoint(series[0], period(params[0])?),
        TaFn::Midprice => midprice(series[0], series[1], period(params[0])?),
        TaFn::BbandsUpper => {
            bbands(series[0], period(params[0])?, params[1], params[2]).map(|(u, _, _)| u)
        }
        TaFn::BbandsMiddle => {
            bbands(series[0], period(params[0])?, params[1], params[2]).map(|(_, m, _)| m)
        }
        TaFn::BbandsLower => {
            bbands(series[0], period(params[0])?, params[1], params[2]).map(|(_, _, l)| l)
        }
        TaFn::Ma => ma(series[0], period(params[0])?, period(params[1])?),
        // MAMA/SAR/SAREXT parameters are real-valued; MAVP parameters are integral periods.
        TaFn::Mama => mama(series[0], params[0], params[1]).map(|(mama_out, _)| mama_out),
        TaFn::Fama => mama(series[0], params[0], params[1]).map(|(_, fama_out)| fama_out),
        TaFn::Sar => sar(series[0], series[1], params[0], params[1]),
        TaFn::Sarext => sarext(
            series[0], series[1], params[0], params[1], params[2], params[3], params[4], params[5],
            params[6], params[7],
        ),
        TaFn::Mavp => mavp(
            series[0],
            series[1],
            period(params[0])?,
            period(params[1])?,
            period(params[2])?,
        ),
        other => Err(family_dispatch_miss(other)),
    }
}

/// Compute all bands for an overlap multi-output family in one kernel run.
pub(super) fn compute_all(
    family: MultiFamily,
    series: &[&[f64]],
    params: &[f64],
) -> crate::Result<Vec<Vec<f64>>> {
    match family {
        MultiFamily::Bbands => {
            let (upper, middle, lower) =
                bbands(series[0], period(params[0])?, params[1], params[2])?;
            Ok(vec![upper, middle, lower])
        }
        MultiFamily::Mama => {
            let (mama_out, fama_out) = mama(series[0], params[0], params[1])?;
            Ok(vec![mama_out, fama_out])
        }
        other => Err(family_dispatch_miss_multi(other)),
    }
}
