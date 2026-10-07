use std::ffi::CString;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Expr, SortExpr};
use pyo3::exceptions::{PyUserWarning, PyValueError};
use pyo3::prelude::*;
use repark_core::series_order::{
    SeriesOrderSource, claim_series_order_notice, resolve_series_order,
};

use super::PyColumn;
use super::window::{OverSpec, build_over_expression};

pub(crate) fn bind_series(frame: &DataFrame, bound: Expr) -> PyResult<Expr> {
    if !bound
        .exists(|node| Ok(is_bare_ta_window(node)))
        .unwrap_or(false)
    {
        return Ok(bound);
    }
    let order = resolve_series_order(frame.logical_plan(), frame.schema());
    if order.source != SeriesOrderSource::Declared
        && claim_series_order_notice(frame.task_ctx().session_config().options())
    {
        warn_series_order(&order.source)?;
    }
    if order.keys.is_empty() {
        return Ok(bound);
    }
    let mut failure = None;
    let rewritten = bound
        .transform_up(|node| {
            if failure.is_some() || !is_bare_ta_window(&node) {
                return Ok(Transformed::no(node));
            }
            match build_over_expression(&node, series_spec(&order.keys)) {
                Ok(ordered) => Ok(Transformed::yes(ordered)),
                Err(error) => {
                    failure = Some(error);
                    Ok(Transformed::no(node))
                }
            }
        })
        .map_err(crate::datafusion_to_py_err)?
        .data;
    match failure {
        Some(error) => Err(error),
        None => Ok(rewritten),
    }
}

fn is_bare_ta_window(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::WindowFunction(function)
            if function.params.partition_by.is_empty()
                && function.params.order_by.is_empty()
                && repark_ta::udf::is_ta_window(function)
    )
}

fn series_spec(keys: &[SortExpr]) -> OverSpec {
    OverSpec {
        partition_by: Vec::new(),
        order_by: keys
            .iter()
            .map(|key| PyColumn::from_expr(crate::deep_stack::grown_clone_expr(&key.expr, 1, 0)))
            .collect(),
        order_ascending: keys.iter().map(|key| key.asc).collect(),
        order_nulls_first: keys.iter().map(|key| key.nulls_first).collect(),
        frame_units: None,
        frame_start: None,
        frame_end: None,
    }
}

fn series_order_notice(source: &SeriesOrderSource) -> Option<String> {
    match source {
        SeriesOrderSource::Declared => None,
        SeriesOrderSource::FirstTemporal(name) => Some(format!(
            "ta.* series ordered by '{name}' (the first date/timestamp column) because the frame \
             has no declared order; sort the frame first or use .over(Window.orderBy(...))"
        )),
        SeriesOrderSource::CurrentRowOrder => Some(String::from(
            "ta.* series computed over the frame's current row order because the frame has no \
             declared order and no date/timestamp column; sort the frame first or use \
             .over(Window.orderBy(...))",
        )),
    }
}

fn warn_series_order(source: &SeriesOrderSource) -> PyResult<()> {
    let Some(text) = series_order_notice(source) else {
        return Ok(());
    };
    let message = CString::new(text).map_err(|error| {
        PyValueError::new_err(crate::exceptions::mask_user_visible(error.to_string()))
    })?;
    Python::attach(|py| PyErr::warn(py, py.get_type::<PyUserWarning>().as_any(), &message, 1))
}
