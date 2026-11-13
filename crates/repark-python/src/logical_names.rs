use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::Column;
use datafusion::common::metadata::FieldMetadata;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::expr::Alias;
use pyo3::prelude::*;

use crate::dataframe::PyDataFrame;
use crate::datafusion_to_py_err;
use crate::deep_stack::{
    PlanDepths, clone_need_bytes, expression_depth, grown_clone_frame, grown_sync,
};
use crate::fence::fenced;

#[pyfunction]
pub fn logical_column_names(frame: PyRef<'_, PyDataFrame>) -> PyResult<Vec<String>> {
    fenced!("logical_names.logical_column_names", {
        let depths = frame.depths();
        let need = crate::deep_stack::clone_need_bytes(depths.plan, depths.expression);
        Ok(crate::deep_stack::grown_sync(need, || {
            frame
                .inner()
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect()
        }))
    })
}

#[pyfunction]
pub fn logical_column_qualifiers(frame: PyRef<'_, PyDataFrame>) -> PyResult<Vec<Option<String>>> {
    fenced!("logical_names.logical_column_qualifiers", {
        Ok(frame
            .inner()
            .schema()
            .iter()
            .map(|(qualifier, _)| qualifier.map(std::string::ToString::to_string))
            .collect())
    })
}

fn float_key(positions: &[usize], position: usize, field: &Field) -> bool {
    matches!(field.data_type(), DataType::Float32 | DataType::Float64)
        && positions.contains(&position)
}

#[allow(clippy::needless_pass_by_value)]
#[pyfunction]
pub fn normalized_float_keys(
    frame: PyRef<'_, PyDataFrame>,
    positions: Vec<usize>,
) -> PyResult<Option<PyDataFrame>> {
    fenced!("logical_names.normalized_float_keys", {
        if !frame
            .inner()
            .schema()
            .fields()
            .iter()
            .enumerate()
            .any(|(position, field)| float_key(&positions, position, field))
        {
            return Ok(None);
        }
        let depths = frame.depths();
        let need = clone_need_bytes(depths.plan, depths.expression);
        let (projected, bound) = grown_sync(need, || {
            let source = grown_clone_frame(frame.inner(), &depths);
            let projection = source
                .schema()
                .iter()
                .enumerate()
                .map(|(position, (qualifier, field))| {
                    let column = Expr::Column(Column::from((qualifier, field.as_ref())));
                    if !float_key(&positions, position, field) {
                        return column;
                    }
                    let metadata = FieldMetadata::from(field.as_ref());
                    Expr::Alias(
                        Alias::new(
                            repark_core::extension::normalize_zero_call(column),
                            qualifier.cloned(),
                            field.name(),
                        )
                        .with_metadata((!metadata.is_empty()).then_some(metadata)),
                    )
                })
                .collect::<Vec<_>>();
            let bound = projection.iter().map(expression_depth).max().unwrap_or(0);
            source
                .select(projection)
                .map(|projected| (projected, bound))
        })
        .map_err(datafusion_to_py_err)?;
        Ok(Some(frame.derived(
            projected,
            PlanDepths {
                plan: depths.plan + 1,
                limited: depths.limited + 1,
                expression: depths.expression.max(bound),
            },
        )))
    })
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(logical_column_names, module)?)?;
    module.add_function(wrap_pyfunction!(logical_column_qualifiers, module)?)?;
    module.add_function(wrap_pyfunction!(normalized_float_keys, module)?)?;
    Ok(())
}
