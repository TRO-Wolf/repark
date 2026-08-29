//! Price-transform-family window-UDF dispatch. Kernel math stays in `crate::price_transform`.

use crate::{avgprice, medprice, typprice, wclprice};

use super::{TaFn, family_dispatch_miss};

/// ===========================================================================================
/// Dispatch the no-period O/H/L/C price transforms.
/// ===========================================================================================
pub(super) fn compute(func: TaFn, series: &[&[f64]], _params: &[f64]) -> crate::Result<Vec<f64>> {
    match func {
        TaFn::Avgprice => avgprice(series[0], series[1], series[2], series[3]),
        TaFn::Medprice => medprice(series[0], series[1]),
        TaFn::Typprice => typprice(series[0], series[1], series[2]),
        TaFn::Wclprice => wclprice(series[0], series[1], series[2]),
        other => Err(family_dispatch_miss(other)),
    }
}
