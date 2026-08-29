//! Window-UDF constructors and Spark `rowsBetween` / `rangeBetween` frame translation.
//!
//! Crate-internal helpers for [`super::PyColumn`]; these are not `#[pymethods]`.

use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::{
    Cast, Expr, WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::scalar::ScalarValue;

use super::PyColumn;

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

/// ===========================================================================================
/// Build a DataFusion [`WindowFrame`] from Spark-relative offsets.
/// `i64::MIN`/`MAX` are unbounded; zero is the current row; signs select preceding/following.
/// ===========================================================================================
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
/// Aggregates use an unbounded `ROWS` frame; ranking and offset UDFs require `ORDER BY`.
///
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
        WindowFrameUnits::Range => ScalarValue::Int64(Some(i64::try_from(n).unwrap_or(i64::MAX))),
    }
}
