use std::collections::HashMap;

use datafusion::logical_expr::LogicalPlan;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::dataframe::PyDataFrame;
use crate::fence::{fenced, fenced_span};
use crate::{datafusion_to_py_err, to_py_err};

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(input_files, module)?)?;
    module.add_function(wrap_pyfunction!(semantic_hash, module)?)?;
    module.add_function(wrap_pyfunction!(same_semantics, module)?)?;
    Ok(())
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#[pyfunction]
fn input_files(py: Python<'_>, frame: &PyDataFrame) -> PyResult<Vec<String>> {
    fenced_span!("py.action", "plan_introspect.input_files", {
        let plan = py
            .detach(|| {
                frame
                    .runtime
                    .block_on(frame.df.clone().create_physical_plan())
            })
            .map_err(datafusion_to_py_err)?;
        Ok(repark_core::input_files(&plan))
    })
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#[pyfunction]
fn semantic_hash(
    py: Python<'_>,
    frame: &PyDataFrame,
    lineages: HashMap<String, Bound<'_, PyDataFrame>>,
) -> PyResult<i64> {
    fenced!("plan_introspect.semantic_hash", {
        let (state, plan) = frame.df.clone().into_parts();
        let mut definitions = HashMap::with_capacity(lineages.len());
        for (name, lineage) in &lineages {
            let definition: LogicalPlan = lineage.borrow().df.clone().into_parts().1;
            definitions.insert(name.clone(), definition);
        }
        py.detach(|| repark_core::semantic_hash(&state, &plan, &definitions).map_err(to_py_err))
    })
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#[pyfunction]
fn same_semantics(
    py: Python<'_>,
    left: &PyDataFrame,
    right: &PyDataFrame,
    lineages: HashMap<String, Bound<'_, PyDataFrame>>,
) -> PyResult<bool> {
    fenced!("plan_introspect.same_semantics", {
        let (state_left, plan_left) = left.df.clone().into_parts();
        let (state_right, plan_right) = right.df.clone().into_parts();
        let mut definitions = HashMap::with_capacity(lineages.len());
        for (name, lineage) in &lineages {
            let definition: LogicalPlan = lineage.borrow().df.clone().into_parts().1;
            definitions.insert(name.clone(), definition);
        }
        py.detach(|| {
            repark_core::same_semantics(
                &state_left,
                &plan_left,
                &state_right,
                &plan_right,
                &definitions,
            )
            .map_err(to_py_err)
        })
    })
}
