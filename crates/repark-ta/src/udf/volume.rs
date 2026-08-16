//! Volume-family window-UDF dispatch (`ad`/`adosc`/`obv`/`mfi`).
//!
//! Called only from [`TaFn::compute`](super::TaFn::compute). Kernel math stays in
//! `crate::volume`.

use crate::{ad, adosc, mfi, obv};

use super::{TaFn, family_dispatch_miss, period};

/// ===========================================================================================
/// Volume-family single-output dispatch (TA-4).
/// ===========================================================================================
pub(super) fn compute(func: TaFn, series: &[&[f64]], params: &[f64]) -> crate::Result<Vec<f64>> {
    match func {
        // TA-4 volume: AD/ADOSC/MFI are H/L/C/V; OBV is close+volume.
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
