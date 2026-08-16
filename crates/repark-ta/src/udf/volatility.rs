//! Volatility-family window-UDF dispatch (`trange`/`atr`/`natr`).
//!
//! Called only from [`TaFn::compute`](super::TaFn::compute). Kernel math stays in
//! `crate::volatility`.

use crate::{atr, natr, trange};

use super::{TaFn, family_dispatch_miss, period};

/// ===========================================================================================
/// Volatility-family single-output dispatch.
/// ===========================================================================================
pub(super) fn compute(func: TaFn, series: &[&[f64]], params: &[f64]) -> crate::Result<Vec<f64>> {
    match func {
        TaFn::Atr => atr(series[0], series[1], series[2], period(params[0])?),
        TaFn::Trange => trange(series[0], series[1], series[2]),
        TaFn::Natr => natr(series[0], series[1], series[2], period(params[0])?),
        other => Err(family_dispatch_miss(other)),
    }
}
