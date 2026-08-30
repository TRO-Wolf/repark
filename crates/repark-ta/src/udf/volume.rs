//! Volume-family window-UDF dispatch.

use crate::{ad, adosc, mfi, obv};

use super::{TaFn, family_dispatch_miss, period};

/// Dispatch the volume kernels.
pub(super) fn compute(func: TaFn, series: &[&[f64]], params: &[f64]) -> crate::Result<Vec<f64>> {
    match func {
        TaFn::Ad => ad(series[0], series[1], series[2], series[3]),
        TaFn::Adosc => adosc(
            series[0],
            series[1],
            series[2],
            series[3],
            period(params[0])?,
            period(params[1])?,
        ),
        TaFn::Obv => obv(series[0], series[1]),
        TaFn::Mfi => mfi(
            series[0],
            series[1],
            series[2],
            series[3],
            period(params[0])?,
        ),
        other => Err(family_dispatch_miss(other)),
    }
}
