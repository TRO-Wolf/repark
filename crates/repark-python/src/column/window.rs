//! Window-UDF constructors and Spark `rowsBetween` / `rangeBetween` frame translation.
//!
//! Crate-internal helpers for [`super::PyColumn`]'s `#[pymethods]` arms. These are not
//! themselves `#[pymethods]` — PyO3 `multiple-pymethods` stays off.

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
/// Build a DataFusion [`WindowFrame`] from Spark-relative `rowsBetween` / `rangeBetween` offsets.
///
/// Spark offsets: `i64::MIN` → unbounded preceding; `i64::MAX` → unbounded following;
/// `0` → current row; negative → N preceding; positive → N following.
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

/// The frame a window with **no** `ORDER BY` gets — or the error Spark gives instead.
///
/// Spark documents two defaults: an ordered window frames `RANGE BETWEEN UNBOUNDED PRECEDING AND
/// CURRENT ROW`, an unordered one frames `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED
/// FOLLOWING`. DataFusion supplies the first and has no answer for the second — an unordered
/// window reaches physical planning and fails with `Internal error: ORDER BY column cannot be
/// empty. This issue was likely caused by a bug in DataFusion's code`, for
/// `count(v).over(Window.partitionBy("k"))`, which is ordinary PySpark.
///
/// The default applies to **aggregates only**. Spark requires an ordering for every ranking and
/// offset function, and refuses `row_number`, `rank`, `dense_rank`, `percent_rank`, `cume_dist`,
/// `ntile`, `lag`, `lead` and `nth_value` over an unordered window (measured, LRS-7). That set is
/// exactly the window UDFs — `first` / `last`, which Spark allows, arrive as aggregates — so the
/// split is read off the function's kind rather than a name list that would drift.
///
/// # Errors
/// The message Spark and DataFusion both give, so the four DataFusion already catches keep the
/// wording they had and the five it does not are refused the same way.
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
    // Mirror PySpark `Window` JVM-long clamping (facade already clamps to `i64` extremes).
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
        // RANGE unbounded uses null of a numeric type; Int64 is the common Spark long path.
        WindowFrameUnits::Range => ScalarValue::Int64(None),
    }
}

fn offset_scalar(units: WindowFrameUnits, n: u64) -> ScalarValue {
    match units {
        WindowFrameUnits::Rows | WindowFrameUnits::Groups => ScalarValue::UInt64(Some(n)),
        // Frame offsets from Spark are non-negative magnitudes; i64::MAX is the only
        // out-of-range input and is handled as unbounded before this path.
        WindowFrameUnits::Range => ScalarValue::Int64(Some(i64::try_from(n).unwrap_or(i64::MAX))),
    }
}
