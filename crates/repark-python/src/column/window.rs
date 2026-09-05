//! Window-UDF constructors and Spark `rowsBetween` / `rangeBetween` frame translation.

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::{
    Cast, Expr, ExprFunctionExt, WindowFrame, WindowFrameBound, WindowFrameUnits,
    WindowFunctionDefinition,
};
use datafusion::scalar::ScalarValue;
use pyo3::PyResult;
use pyo3::exceptions::PyValueError;

use super::PyColumn;
use super::expr_build::{
    inner_null_treatment, replace_wrapped_aggregate, single_wrapped_aggregate,
    window_from_aggregate,
};

pub(super) struct OverSpec {
    pub(super) partition_by: Vec<PyColumn>,
    pub(super) order_by: Vec<PyColumn>,
    pub(super) order_ascending: Vec<bool>,
    pub(super) order_nulls_first: Vec<bool>,
    pub(super) frame_units: Option<String>,
    pub(super) frame_start: Option<i64>,
    pub(super) frame_end: Option<i64>,
}

pub(super) fn build_over_expression(expr: &Expr, spec: OverSpec) -> PyResult<Expr> {
    let OverSpec {
        partition_by,
        order_by,
        order_ascending,
        order_nulls_first,
        frame_units,
        frame_start,
        frame_end,
    } = spec;
    if order_by.len() != order_ascending.len() || order_by.len() != order_nulls_first.len() {
        return Err(PyValueError::new_err(
            "over expects order_by, order_ascending, and order_nulls_first of equal length",
        ));
    }
    let (inner, cast_type) = match expr {
        Expr::Cast(cast) => (&*cast.expr, Some(cast.field.data_type().clone())),
        other => (other, None),
    };
    let wrapped = match inner {
        Expr::WindowFunction(_) | Expr::AggregateFunction(_) => None,
        other => single_wrapped_aggregate(other),
    };
    let target = wrapped.as_ref().unwrap_or(inner);
    let window_expr = match target {
        Expr::WindowFunction(_) => target.clone(),
        Expr::AggregateFunction(agg) => window_from_aggregate(agg),
        _ => {
            return Err(PyValueError::new_err(
                "over() applies only to a window or aggregate function column \
                 (e.g. row_number(), sum(...))",
            ));
        }
    };
    let partitions: Vec<Expr> = partition_by.iter().map(PyColumn::expr).collect();
    let orderings: Vec<_> = order_by
        .iter()
        .zip(order_ascending)
        .zip(order_nulls_first)
        .map(|((column, is_ascending), nulls_first)| column.expr().sort(is_ascending, nulls_first))
        .collect();
    let unordered_frame = order_by
        .is_empty()
        .then(|| unordered_window_frame(&window_expr))
        .transpose()
        .map_err(crate::AnalysisException::new_err)?;
    let mut builder = window_expr
        .partition_by(partitions)
        .order_by(orderings)
        .null_treatment(inner_null_treatment(target));
    if let Some(units_text) = frame_units.as_deref() {
        let start = frame_start
            .ok_or_else(|| PyValueError::new_err("over frame_units requires frame_start"))?;
        let end = frame_end
            .ok_or_else(|| PyValueError::new_err("over frame_units requires frame_end"))?;
        let frame = spark_window_frame(units_text, start, end).map_err(PyValueError::new_err)?;
        builder = builder.window_frame(frame);
    } else if let Some(frame) = unordered_frame {
        builder = builder.window_frame(frame);
    }
    let built = builder.build().map_err(|err| {
        PyValueError::new_err(format!("could not build window expression: {err}"))
    })?;
    let windowed = match wrapped {
        Some(_) => {
            replace_wrapped_aggregate(inner.clone(), &built).map_err(PyValueError::new_err)?
        }
        None => built,
    };
    Ok(match cast_type {
        Some(data_type) => Expr::Cast(Cast::new(Box::new(windowed), data_type)),
        None => windowed,
    })
}

impl PyColumn {
    /// Window UDF with Spark `IntegerType` cast (`row_number` / `rank` / `dense_rank` / `ntile`).
    pub(super) fn window_udwf_i32(
        udwf: std::sync::Arc<datafusion::logical_expr::WindowUDF>,
        args: Vec<Expr>,
    ) -> Self {
        let window = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(udwf),
            args,
        ));
        Self::from_expr(Expr::Cast(Cast::new(Box::new(window), DataType::Int32)))
    }

    /// Window UDF with no `IntegerType` cast.
    pub(super) fn window_udwf(
        udwf: std::sync::Arc<datafusion::logical_expr::WindowUDF>,
        args: &[PyColumn],
    ) -> Self {
        Self::from_expr(Expr::from(WindowFunction::new(
            WindowFunctionDefinition::WindowUDF(udwf),
            args.iter().map(PyColumn::expr).collect(),
        )))
    }
}

/// Build a DataFusion [`WindowFrame`] from Spark-relative offsets.
pub(super) fn spark_window_frame(
    units_text: &str,
    start: i64,
    end: i64,
) -> Result<WindowFrame, String> {
    let units = match units_text {
        "rows" => WindowFrameUnits::Rows,
        "range" => WindowFrameUnits::Range,
        other => {
            return Err(format!(
                "window frame units must be 'rows' or 'range', got {other:?}"
            ));
        }
    };
    let start_bound = spark_offset_to_bound(start, units)?;
    let end_bound = spark_offset_to_bound(end, units)?;
    Ok(WindowFrame::new_bounds(units, start_bound, end_bound))
}

/// Return Spark's unordered-window frame or refuse an unordered window UDF.
/// # Errors
/// The refusal names the function and requests `ORDER BY`.
pub(super) fn unordered_window_frame(window_expr: &Expr) -> Result<WindowFrame, String> {
    if let Expr::WindowFunction(function) = window_expr
        && matches!(function.fun, WindowFunctionDefinition::WindowUDF(_))
    {
        let name = function.fun.name();
        return Err(format!(
            "Window function {name}() requires window to be ordered, please add ORDER BY clause. \
             For example SELECT {name}() OVER (PARTITION BY ... ORDER BY ...)"
        ));
    }
    Ok(WindowFrame::new_bounds(
        WindowFrameUnits::Rows,
        WindowFrameBound::Preceding(unbounded_scalar(WindowFrameUnits::Rows)),
        WindowFrameBound::Following(unbounded_scalar(WindowFrameUnits::Rows)),
    ))
}

fn spark_offset_to_bound(offset: i64, units: WindowFrameUnits) -> Result<WindowFrameBound, String> {
    if offset == i64::MIN {
        return Ok(WindowFrameBound::Preceding(unbounded_scalar(units)));
    }
    if offset == i64::MAX {
        return Ok(WindowFrameBound::Following(unbounded_scalar(units)));
    }
    if offset == 0 {
        return Ok(WindowFrameBound::CurrentRow);
    }
    if offset < 0 {
        let n = u64::try_from(-offset)
            .map_err(|_| format!("window frame offset {offset} cannot be converted to a bound"))?;
        return Ok(WindowFrameBound::Preceding(offset_scalar(units, n)));
    }
    let n = u64::try_from(offset)
        .map_err(|_| format!("window frame offset {offset} cannot be converted to a bound"))?;
    Ok(WindowFrameBound::Following(offset_scalar(units, n)))
}

fn unbounded_scalar(units: WindowFrameUnits) -> ScalarValue {
    match units {
        WindowFrameUnits::Rows | WindowFrameUnits::Groups => ScalarValue::UInt64(None),
        WindowFrameUnits::Range => ScalarValue::Int64(None),
    }
}

fn offset_scalar(units: WindowFrameUnits, n: u64) -> ScalarValue {
    match units {
        WindowFrameUnits::Rows | WindowFrameUnits::Groups => ScalarValue::UInt64(Some(n)),
        WindowFrameUnits::Range => ScalarValue::Utf8(Some(n.to_string())),
    }
}
