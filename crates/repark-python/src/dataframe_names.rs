use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Expr, JoinType};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::column::PyColumn;
use crate::column::expr_build::{parse_canonical_predicate, parse_canonical_predicate_exact};
use crate::dataframe::PyDataFrame;
use crate::datafusion_to_py_err;
use crate::fence::fenced;
use repark_core::frame_names::NameRule;
use repark_functions::case_sensitive::spark_case_sensitive_from_options;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(attribute_column, module)?)?;
    module.add_function(wrap_pyfunction!(attribute_copies, module)?)?;
    module.add_function(wrap_pyfunction!(attribute_copy_name, module)?)?;
    module.add_function(wrap_pyfunction!(drop_frame_columns, module)?)?;
    module.add_function(wrap_pyfunction!(frame_case_sensitive, module)?)?;
    module.add_function(wrap_pyfunction!(refuse_ambiguous_join_condition, module)?)?;
    module.add_function(wrap_pyfunction!(requalify_join_sides, module)?)?;
    module.add_function(wrap_pyfunction!(resolve_frame_names, module)?)?;
    Ok(())
}

pub(crate) fn frame_rule(frame: &DataFrame) -> NameRule {
    NameRule::from_case_sensitive(spark_case_sensitive_from_options(
        frame.task_ctx().session_config().options(),
    ))
}

pub(crate) fn bound_column(frame: &DataFrame, column: &PyColumn) -> PyResult<(Expr, usize)> {
    let bound = column
        .expr()
        .resolve_lambda_variables(frame.schema())
        .and_then(|expr| {
            repark_core::frame_names::resolve_bound_expr_with(
                expr.data,
                frame.schema(),
                frame_rule(frame),
            )
        })
        .map_err(datafusion_to_py_err)?;
    let bound = crate::column::series::bind_series(frame, bound)?;
    let depth = crate::deep_stack::expression_depth(&bound);
    Ok((bound, depth))
}

pub(crate) fn bound_projection(frame: &DataFrame, column: &PyColumn) -> PyResult<(Expr, usize)> {
    let bound = column
        .expr()
        .resolve_lambda_variables(frame.schema())
        .and_then(|expr| {
            repark_core::frame_names::bind_projection_expr(
                expr.data,
                frame.schema(),
                frame_rule(frame),
            )
        })
        .map_err(datafusion_to_py_err)?;
    let bound = crate::column::series::bind_series(frame, bound)?;
    let depth = crate::deep_stack::expression_depth(&bound);
    Ok((bound, depth))
}

pub(crate) fn filter_frame_with_sql(
    frame: &PyDataFrame,
    predicate: &str,
) -> PyResult<(DataFrame, usize, bool)> {
    repark_spark::refuse_sql_fragment(predicate).map_err(datafusion_to_py_err)?;
    let parsed = match frame_rule(frame.inner()) {
        NameRule::Exact => parse_canonical_predicate_exact(frame, predicate),
        NameRule::IgnoreCase => parse_canonical_predicate(frame, predicate),
    }
    .map_err(|error| crate::unknown_routine_to_py_err(predicate, error))?;
    let (depth, subquery_plan) = crate::deep_stack::survey_expression(&parsed);
    let filtered = crate::deep_stack::grown_clone_frame(frame.inner(), &frame.depths())
        .filter(parsed)
        .map_err(datafusion_to_py_err)?;
    Ok((filtered, depth, subquery_plan > 0))
}

pub(crate) fn join_on_keys(
    left: &PyDataFrame,
    right: &PyDataFrame,
    on: &[String],
    join_type: JoinType,
) -> PyResult<DataFrame> {
    let rule = frame_rule(left.inner());
    repark_core::frame_names::join_on_named_keys(
        crate::deep_stack::grown_clone_frame(left.inner(), &left.depths()),
        crate::deep_stack::grown_clone_frame(right.inner(), &right.depths()),
        on,
        join_type,
        rule,
    )
    .map_err(datafusion_to_py_err)
}

pub(crate) fn union_frames(
    left: &PyDataFrame,
    right: &PyDataFrame,
    allow_missing: bool,
) -> PyResult<DataFrame> {
    let rule = frame_rule(left.inner());
    repark_core::frame_names::union_by_folded_name(
        crate::deep_stack::grown_clone_frame(left.inner(), &left.depths()),
        crate::deep_stack::grown_clone_frame(right.inner(), &right.depths()),
        allow_missing,
        rule,
    )
    .map_err(datafusion_to_py_err)
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
fn attribute_column(name: &str) -> PyResult<PyColumn> {
    fenced!("dataframe_names.attribute_column", {
        Ok(PyColumn::from_expr(
            repark_core::frame_names::attribute_reference(name),
        ))
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
fn attribute_copies(frame: &PyDataFrame) -> PyResult<PyDataFrame> {
    fenced!("dataframe_names.attribute_copies", {
        let depths = frame.depths();
        let need = crate::deep_stack::clone_need_bytes(depths.plan, depths.expression);
        let df = crate::deep_stack::grown_sync(need, || {
            repark_core::frame_names::with_attribute_copies(crate::deep_stack::grown_clone_frame(
                frame.inner(),
                &frame.depths(),
            ))
        })
        .map_err(datafusion_to_py_err)?;
        Ok(PyDataFrame::new_with_depths(
            df,
            frame.runtime_handle(),
            crate::deep_stack::PlanDepths {
                plan: depths.plan + 1,
                limited: depths.limited + 1,
                expression: depths.expression.max(2),
            },
        ))
    })
}

#[pyfunction]
fn attribute_copy_name(frame: &PyDataFrame, name: &str) -> String {
    let depths = frame.depths();
    let need = crate::deep_stack::clone_need_bytes(depths.plan, depths.expression);
    crate::deep_stack::grown_sync(need, || {
        repark_core::frame_names::attribute_copy_name_in(frame.inner().schema(), name)
    })
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
fn drop_frame_columns(
    frame: &PyDataFrame,
    names: Vec<String>,
    references: Vec<String>,
    attributes: Vec<String>,
) -> PyResult<PyDataFrame> {
    fenced!("dataframe_names.drop_frame_columns", {
        let depths = frame.depths();
        let need = crate::deep_stack::clone_need_bytes(depths.plan, depths.expression);
        let df = crate::deep_stack::grown_sync(need, || {
            repark_core::frame_names::drop_named_columns(
                crate::deep_stack::grown_clone_frame(frame.inner(), &frame.depths()),
                &names,
                &references,
                &attributes,
                frame_rule(frame.inner()),
            )
        })
        .map_err(datafusion_to_py_err)?;
        Ok(PyDataFrame::new(df, frame.runtime_handle()))
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
fn refuse_ambiguous_join_condition(
    left: &PyDataFrame,
    right: &PyDataFrame,
    condition_sql: &str,
) -> PyResult<()> {
    fenced!("dataframe_names.refuse_ambiguous_join_condition", {
        let frames = crate::deep_stack::max_depths(&left.depths(), &right.depths());
        let need = crate::deep_stack::clone_need_bytes(frames.plan, frames.expression);
        crate::deep_stack::grown_sync(need, || {
            repark_core::frame_names::refuse_ambiguous_condition(
                condition_sql,
                &[left.inner().schema(), right.inner().schema()],
            )
        })
        .map_err(datafusion_to_py_err)
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
fn requalify_join_sides(
    joined: &PyDataFrame,
    left: &PyDataFrame,
    right: Option<&PyDataFrame>,
) -> PyResult<PyDataFrame> {
    fenced!("dataframe_names.requalify_join_sides", {
        let mut depths = crate::deep_stack::max_depths(&joined.depths(), &left.depths());
        if let Some(frame) = right {
            depths = crate::deep_stack::max_depths(&depths, &frame.depths());
        }
        let need = crate::deep_stack::clone_need_bytes(depths.plan, depths.expression);
        let df = crate::deep_stack::grown_sync(need, || {
            let mut sides = vec![left.inner().schema()];
            sides.extend(right.map(|frame| frame.inner().schema()));
            repark_core::frame_names::requalify_join_sides(
                crate::deep_stack::grown_clone_frame(joined.inner(), &joined.depths()),
                &sides,
            )
        })
        .map_err(datafusion_to_py_err)?;
        Ok(PyDataFrame::new(df, joined.runtime_handle()))
    })
}

#[pyfunction]
fn frame_case_sensitive(frame: &PyDataFrame) -> bool {
    matches!(frame_rule(frame.inner()), NameRule::Exact)
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
fn resolve_frame_names(frame: &PyDataFrame, names: Vec<String>) -> PyResult<Vec<(String, String)>> {
    fenced!("dataframe_names.resolve_frame_names", {
        let depths = frame.depths();
        let need = crate::deep_stack::clone_need_bytes(depths.plan, depths.expression);
        crate::deep_stack::grown_sync(need, || {
            repark_core::frame_names::resolve_written_names(
                frame.inner().schema(),
                &names,
                frame_rule(frame.inner()),
            )
        })
        .map_err(datafusion_to_py_err)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    #[test]
    fn attribute_copies_levels_match_a_fresh_survey() {
        let runtime: std::sync::Arc<tokio::runtime::Runtime> =
            std::sync::Arc::new(tokio::runtime::Runtime::new().expect("a runtime builds"));
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as _],
        )
        .expect("a probe batch builds");
        let context = SessionContext::new();
        context.register_batch("t", batch).expect("register");
        let df = runtime.block_on(context.table("t")).expect("a table scans");
        let frame = PyDataFrame::new(df, runtime);
        let copied = attribute_copies(&frame).expect("copies build");
        let walked = crate::deep_stack::plan_depths(copied.inner().logical_plan());
        let cached = copied.depths();
        assert_eq!(
            (cached.plan, cached.limited, cached.expression),
            (walked.plan, walked.limited, walked.expression),
        );
    }
}
