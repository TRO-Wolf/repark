use arrow::datatypes::DataType as ArrowDataType;
use datafusion::logical_expr::{Cast, Expr};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::column::PyColumn;
use crate::column::display::wrap_cast;
use crate::dataframe::PyDataFrame;
use crate::fence::fenced;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(fill_expr_for_column, module)?)?;
    Ok(())
}

fn fill_cast_token(data_type: &ArrowDataType) -> Option<&'static str> {
    match data_type {
        ArrowDataType::Int8 => Some("TINYINT"),
        ArrowDataType::Int16 => Some("SMALLINT"),
        ArrowDataType::Int32 => Some("INT"),
        ArrowDataType::Int64 => Some("BIGINT"),
        ArrowDataType::Float32 => Some("FLOAT"),
        ArrowDataType::Float64 => Some("DOUBLE"),
        _ => None,
    }
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#[allow(clippy::too_many_arguments)]
#[pyfunction]
fn fill_expr_for_column(
    frame: &PyDataFrame,
    bound: PyColumn,
    field_name: String,
    fallback_name: String,
    literal: PyColumn,
    lit_parts: (String, String, String),
) -> PyResult<(PyColumn, Option<(PyColumn, String, String, String)>)> {
    fenced!("dataframe_fill.fill_expr_for_column", {
        let lit_expr = literal.expr();
        let scalar = match &lit_expr {
            Expr::Literal(value, _) => value.clone(),
            other => {
                return Err(PyValueError::new_err(format!(
                    "fill value must be a scalar literal, got {other:?}"
                )));
            }
        };
        let schema = frame.analyzed_arrow_schema_native()?;
        let build = repark_core::na_fill_expr(
            &schema,
            bound.expr(),
            field_name.as_str(),
            fallback_name.as_str(),
            scalar,
        );
        let filled = PyColumn::from_expr(build.expr);
        let cast = match build.cast_to {
            None => None,
            Some(target) => {
                let token = fill_cast_token(&target).ok_or_else(|| {
                    PyValueError::new_err(format!("fill cast target has no CAST token: {target:?}"))
                })?;
                let cast_inner =
                    PyColumn::from_expr(Expr::Cast(Cast::new(Box::new(lit_expr), target)));
                let (display, sql, join) = lit_parts;
                Some((
                    cast_inner,
                    wrap_cast("CAST", display.as_str(), token),
                    wrap_cast("CAST", sql.as_str(), token),
                    wrap_cast("CAST", join.as_str(), token),
                ))
            }
        };
        Ok((filled, cast))
    })
}
