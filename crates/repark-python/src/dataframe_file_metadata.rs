use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
use datafusion::logical_expr::Expr;
use datafusion::prelude::DataFrame;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::column::PyColumn;
use crate::column::expr_build::metadata_field_expr;
use crate::dataframe::{PyDataFrame, arrow_type_key};
use crate::exceptions::AnalysisException;
use crate::fence::fenced;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(metadata_field, module)?)?;
    Ok(())
}

#[pyfunction]
fn metadata_field(hidden: &str, field: &str) -> PyResult<PyColumn> {
    fenced!("metadata_field", {
        Ok(PyColumn::from_expr(metadata_field_expr(hidden, field)))
    })
}

pub(crate) fn metadata_error(error: repark_core::FileMetadataError) -> PyErr {
    Python::attach(|py| {
        let raised = AnalysisException::new_err(error.message);
        if let Some(class) = error.error_class
            && let Err(failure) = raised.value(py).setattr("_spark_error_class", class)
        {
            tracing::warn!(error = %failure, "metadata error class setattr failed");
        }
        if let Some(state) = error.sql_state
            && let Err(failure) = raised.value(py).setattr("_spark_sql_state", state)
        {
            tracing::warn!(error = %failure, "metadata error state setattr failed");
        }
        if !error.message_parameters.is_empty() {
            let params = pyo3::types::PyDict::new(py);
            for (key, value) in error.message_parameters {
                if let Err(failure) = params.set_item(key, value) {
                    tracing::warn!(error = %failure, "metadata error param set failed");
                }
            }
            if let Err(failure) = raised
                .value(py)
                .setattr("_spark_message_parameters", params)
            {
                tracing::warn!(error = %failure, "metadata error params setattr failed");
            }
        }
        raised
    })
}

pub(crate) fn ensure_planned(
    frame: &PyDataFrame,
    exprs: Vec<Expr>,
) -> PyResult<(DataFrame, Vec<Expr>)> {
    if !exprs.iter().any(repark_core::expr_mentions_file_metadata) {
        return Ok((frame.df.clone(), exprs));
    }
    let (state, plan) = frame.df.clone().into_parts();
    let (plan, exprs) = Python::attach(|py| {
        py.detach(|| {
            frame
                .runtime
                .block_on(repark_core::ensure_file_metadata(&state, plan, exprs))
        })
    })
    .map_err(metadata_error)?;
    Ok((DataFrame::new(state, plan), exprs))
}

pub(crate) fn ensure_single(
    frame: &PyDataFrame,
    expr: Expr,
    what: &str,
) -> PyResult<(DataFrame, Expr)> {
    let (planned, mut exprs) = ensure_planned(frame, vec![expr])?;
    let Some(single) = exprs.pop() else {
        return Err(crate::to_py_err(repark_core::Error::Analysis(format!(
            "metadata planning lost its {what} expression"
        ))));
    };
    Ok((planned, single))
}

pub(crate) fn ensure_bound(
    frame: &PyDataFrame,
    column: &crate::column::PyColumn,
    what: &str,
) -> PyResult<(DataFrame, Expr)> {
    let bound = frame.bound(column)?;
    ensure_single(frame, bound, what)
}

pub(crate) fn hidden_name(frame: &PyDataFrame) -> PyResult<Option<String>> {
    fenced!("PyDataFrame.file_metadata_hidden_name", {
        let (_, plan) = frame.df.clone().into_parts();
        Ok(match repark_core::file_metadata_status(&plan) {
            repark_core::FileMetadataStatus::Available => {
                Some(repark_core::METADATA_COLUMN_NAME.to_string())
            }
            repark_core::FileMetadataStatus::Shadowed => {
                Some(repark_core::SHADOW_METADATA_COLUMN_NAME.to_string())
            }
            repark_core::FileMetadataStatus::Absent => None,
        })
    })
}

pub(crate) fn metadata_struct_type_key(field: &ArrowField) -> Option<String> {
    if !field
        .metadata()
        .contains_key(repark_core::FILE_SOURCE_METADATA_KEY)
    {
        return None;
    }
    Some(annotated_type_key(field.data_type()))
}

pub(crate) fn ensure_sorts(
    frame: &PyDataFrame,
    sorts: Vec<datafusion::logical_expr::expr::Sort>,
) -> PyResult<(DataFrame, Vec<datafusion::logical_expr::SortExpr>)> {
    let keys = sorts
        .iter()
        .map(|sort| sort.expr.clone())
        .collect::<Vec<_>>();
    let (planned, keys) = ensure_planned(frame, keys)?;
    let rebuilt = sorts
        .into_iter()
        .zip(keys)
        .map(|(sort, expr)| datafusion::logical_expr::SortExpr {
            expr,
            asc: sort.asc,
            nulls_first: sort.nulls_first,
        })
        .collect::<Vec<_>>();
    Ok((planned, rebuilt))
}

fn annotated_type_key(data_type: &ArrowDataType) -> String {
    match data_type {
        ArrowDataType::Struct(children) => {
            let parts = children
                .iter()
                .map(|child| {
                    let inner = annotated_type_key(child.data_type());
                    if child.is_nullable() {
                        format!("{}:{inner}", child.name())
                    } else {
                        format!("{}:{inner} NOT NULL", child.name())
                    }
                })
                .collect::<Vec<_>>()
                .join(",");
            format!("struct<{parts}>")
        }
        other => arrow_type_key(other),
    }
}
