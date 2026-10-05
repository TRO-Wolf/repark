use std::collections::HashMap;

use datafusion::logical_expr::LogicalPlan;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::dataframe::PyDataFrame;
use crate::deep_stack::{block_on_grown_sized, frame_drive_segment_cached};
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
        let segment = frame_drive_segment_cached(&frame.depths())?;
        let df = crate::deep_stack::grown_clone_frame(&frame.df, &frame.depths());
        py.detach(|| {
            block_on_grown_sized(
                &frame.runtime,
                async {
                    let plan = df.create_physical_plan().await?;
                    Ok::<_, datafusion::error::DataFusionError>(repark_core::input_files(&plan))
                },
                segment,
            )
        })
        .map_err(datafusion_to_py_err)
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
        let mut depths = frame.depths();
        for lineage in lineages.values() {
            depths = crate::deep_stack::max_depths(&depths, &lineage.borrow().depths());
        }
        let need = crate::deep_stack::clone_need_bytes(depths.plan, depths.expression);
        crate::deep_stack::grown_sync(need, || {
            let (state, plan) =
                crate::deep_stack::grown_clone_frame(frame.inner(), &frame.depths()).into_parts();
            let mut definitions = HashMap::with_capacity(lineages.len());
            for (name, lineage) in &lineages {
                let lineage_depths = lineage.borrow().depths();
                let definition: LogicalPlan =
                    crate::deep_stack::grown_clone_frame(lineage.borrow().inner(), &lineage_depths)
                        .into_parts()
                        .1;
                definitions.insert(name.clone(), definition);
            }
            py.detach(|| repark_core::semantic_hash(&state, &plan, &definitions).map_err(to_py_err))
        })
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
        let mut depths = crate::deep_stack::max_depths(&left.depths(), &right.depths());
        for lineage in lineages.values() {
            depths = crate::deep_stack::max_depths(&depths, &lineage.borrow().depths());
        }
        let need = crate::deep_stack::clone_need_bytes(depths.plan, depths.expression);
        crate::deep_stack::grown_sync(need, || {
            let (state_left, plan_left) =
                crate::deep_stack::grown_clone_frame(left.inner(), &left.depths()).into_parts();
            let (state_right, plan_right) =
                crate::deep_stack::grown_clone_frame(right.inner(), &right.depths()).into_parts();
            let mut definitions = HashMap::with_capacity(lineages.len());
            for (name, lineage) in &lineages {
                let lineage_depths = lineage.borrow().depths();
                let definition: LogicalPlan =
                    crate::deep_stack::grown_clone_frame(lineage.borrow().inner(), &lineage_depths)
                        .into_parts()
                        .1;
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
    })
}
