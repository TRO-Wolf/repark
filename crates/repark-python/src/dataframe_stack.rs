use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::dataframe::PyDataFrame;
use crate::fence::fenced;
use crate::to_py_err;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(stack_dataframe, module)?)?;
    Ok(())
}

#[pyfunction]
#[pyo3(signature = (frame, n, passthrough_count, output_names, row_labels=None, cell_indices=None))]
fn stack_dataframe(
    frame: &PyDataFrame,
    n: i64,
    passthrough_count: usize,
    output_names: Option<Vec<String>>,
    row_labels: Option<Vec<String>>,
    cell_indices: Option<Vec<usize>>,
) -> PyResult<PyDataFrame> {
    fenced!("stack_dataframe", {
        let depths = frame.depths();
        let need = crate::deep_stack::clone_need_bytes(depths.plan, depths.expression);
        let df = match (row_labels, cell_indices) {
            (None, None) => crate::deep_stack::grown_sync(need, || {
                repark_core::apply_stack(
                    crate::deep_stack::grown_clone_frame(frame.inner(), &depths),
                    n,
                    passthrough_count,
                    output_names.as_deref(),
                )
            }),
            (Some(names), Some(cells)) => {
                if i64::try_from(names.len()).unwrap_or(-1) != n {
                    return Err(to_py_err(repark_core::Error::Analysis(
                        "stack_dataframe row_labels count must equal n".to_string(),
                    )));
                }
                crate::deep_stack::grown_sync(need, || {
                    repark_core::apply_labeled_stack(
                        crate::deep_stack::grown_clone_frame(frame.inner(), &depths),
                        repark_core::StackLabels { names, cells },
                        output_names.as_deref(),
                    )
                })
            }
            _ => {
                return Err(to_py_err(repark_core::Error::Analysis(
                    "stack_dataframe requires row_labels and cell_indices together".to_string(),
                )));
            }
        }
        .map_err(to_py_err)?;
        Ok(PyDataFrame::new(df, Arc::clone(&frame.runtime)))
    })
}
