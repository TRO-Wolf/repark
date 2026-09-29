use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Expr, JoinType};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::column::PyColumn;
use crate::column::expr_build::{parse_canonical_predicate, parse_canonical_predicate_exact};
use crate::dataframe::PyDataFrame;
use crate::datafusion_to_py_err;
use crate::fence::fenced;
use repark_core::frame_names::{Disposition, NameRule};
use repark_functions::case_sensitive::spark_case_sensitive_from_options;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(attribute_column, module)?)?;
    module.add_function(wrap_pyfunction!(attribute_copies, module)?)?;
    module.add_function(wrap_pyfunction!(attribute_copy_name, module)?)?;
    module.add_function(wrap_pyfunction!(drop_frame_columns, module)?)?;
    module.add_function(wrap_pyfunction!(frame_case_sensitive, module)?)?;
    module.add_function(wrap_pyfunction!(frame_is_exact, module)?)?;
    module.add_function(wrap_pyfunction!(match_display_names, module)?)?;
    module.add_function(wrap_pyfunction!(match_subset_names, module)?)?;
    module.add_function(wrap_pyfunction!(refuse_ambiguous_display_name, module)?)?;
    module.add_function(wrap_pyfunction!(refuse_ambiguous_join_condition, module)?)?;
    module.add_function(wrap_pyfunction!(refuse_folded_duplicate_keys, module)?)?;
    module.add_function(wrap_pyfunction!(requalify_join_sides, module)?)?;
    module.add_function(wrap_pyfunction!(resolve_df_names, module)?)?;
    module.add_function(wrap_pyfunction!(resolve_frame_names, module)?)?;
    module.add_function(wrap_pyfunction!(resolve_qualified_display_names, module)?)?;
    module.add_function(wrap_pyfunction!(rewrite_join_condition_aliases, module)?)?;
    Ok(())
}

pub(crate) fn frame_rule(frame: &DataFrame) -> NameRule {
    NameRule::from_case_sensitive(spark_case_sensitive_from_options(
        frame.task_ctx().session_config().options(),
    ))
}

pub(crate) fn bound_column(frame: &DataFrame, column: &PyColumn) -> PyResult<Expr> {
    column
        .expr()
        .resolve_lambda_variables(frame.schema())
        .and_then(|expr| {
            repark_core::frame_names::resolve_bound_expr_with(
                expr.data,
                frame.schema(),
                frame_rule(frame),
            )
        })
        .map_err(datafusion_to_py_err)
}

pub(crate) fn bound_projection(frame: &DataFrame, column: &PyColumn) -> PyResult<Expr> {
    column
        .expr()
        .resolve_lambda_variables(frame.schema())
        .and_then(|expr| {
            repark_core::frame_names::bind_projection_expr(
                expr.data,
                frame.schema(),
                frame_rule(frame),
            )
        })
        .map_err(datafusion_to_py_err)
}

pub(crate) fn filter_frame_with_sql(frame: &DataFrame, predicate: &str) -> PyResult<DataFrame> {
    repark_spark::refuse_sql_fragment(predicate).map_err(datafusion_to_py_err)?;
    let parsed = match frame_rule(frame) {
        NameRule::Exact => parse_canonical_predicate_exact(frame, predicate),
        NameRule::IgnoreCase => parse_canonical_predicate(frame, predicate),
    }
    .map_err(|error| crate::unknown_routine_to_py_err(predicate, error))?;
    frame.clone().filter(parsed).map_err(datafusion_to_py_err)
}

pub(crate) fn join_on_keys(
    left: &DataFrame,
    right: &DataFrame,
    on: &[String],
    join_type: JoinType,
) -> PyResult<DataFrame> {
    repark_core::frame_names::join_on_named_keys(
        left.clone(),
        right.clone(),
        on,
        join_type,
        frame_rule(left),
    )
    .map_err(datafusion_to_py_err)
}

pub(crate) fn union_frames(
    left: &DataFrame,
    right: &DataFrame,
    allow_missing: bool,
) -> PyResult<DataFrame> {
    repark_core::frame_names::union_by_folded_name(
        left.clone(),
        right.clone(),
        allow_missing,
        frame_rule(left),
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
        let df = repark_core::frame_names::with_attribute_copies(frame.inner().clone())
            .map_err(datafusion_to_py_err)?;
        Ok(PyDataFrame::new(df, frame.runtime_handle()))
    })
}

#[pyfunction]
fn attribute_copy_name(frame: &PyDataFrame, name: &str) -> String {
    repark_core::frame_names::attribute_copy_name_in(frame.inner().schema(), name)
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
        let df = repark_core::frame_names::drop_named_columns(
            frame.inner().clone(),
            &names,
            &references,
            &attributes,
            frame_rule(frame.inner()),
        )
        .map_err(datafusion_to_py_err)?;
        Ok(PyDataFrame::new(df, frame.runtime_handle()))
    })
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
fn refuse_ambiguous_display_name(
    frame: &PyDataFrame,
    written: String,
    held: Vec<String>,
) -> PyResult<()> {
    fenced!("dataframe_names.refuse_ambiguous_display_name", {
        repark_core::frame_names::refuse_ambiguous_display_name(
            frame.inner().schema(),
            &held,
            &written,
            frame_rule(frame.inner()),
        )
        .map_err(datafusion_to_py_err)
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
        repark_core::frame_names::refuse_ambiguous_condition(
            condition_sql,
            &[left.inner().schema(), right.inner().schema()],
        )
        .map_err(datafusion_to_py_err)
    })
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
fn refuse_folded_duplicate_keys(frame: &PyDataFrame, keys: Vec<String>) -> PyResult<()> {
    fenced!("dataframe_names.refuse_folded_duplicate_keys", {
        repark_core::frame_names::refuse_folded_duplicate_keys(&keys, frame_rule(frame.inner()))
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
        let mut sides = vec![left.inner().schema()];
        sides.extend(right.map(|frame| frame.inner().schema()));
        let df = repark_core::frame_names::requalify_join_sides(joined.inner().clone(), &sides)
            .map_err(datafusion_to_py_err)?;
        Ok(PyDataFrame::new(df, joined.runtime_handle()))
    })
}

#[pyfunction]
fn frame_case_sensitive(frame: &PyDataFrame) -> bool {
    frame_is_exact(frame)
}

#[pyfunction]
fn frame_is_exact(frame: &PyDataFrame) -> bool {
    matches!(frame_rule(frame.inner()), NameRule::Exact)
}

fn disposition_text(disposition: Disposition) -> String {
    match disposition {
        Disposition::Bound => "bound",
        Disposition::Ambiguous => "ambiguous",
        Disposition::Missing => "missing",
    }
    .to_string()
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
fn resolve_df_names(
    frame: &PyDataFrame,
    names: Vec<String>,
) -> PyResult<Vec<(String, String, String, String)>> {
    fenced!("dataframe_names.resolve_df_names", {
        repark_core::frame_names::resolve_df_names(
            frame.inner().schema(),
            &names,
            frame_rule(frame.inner()),
        )
        .map(|rows| {
            rows.into_iter()
                .map(|(written, qualifier, engine, disposition)| {
                    (written, qualifier, engine, disposition_text(disposition))
                })
                .collect()
        })
        .map_err(datafusion_to_py_err)
    })
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
fn match_display_names(
    frame: &PyDataFrame,
    written: Vec<String>,
    held: Vec<String>,
) -> PyResult<Vec<(String, Vec<String>, String)>> {
    fenced!("dataframe_names.match_display_names", {
        repark_core::frame_names::match_display_names(&written, &held, frame_rule(frame.inner()))
            .map(|rows| {
                rows.into_iter()
                    .map(|(name, hits, disposition)| (name, hits, disposition_text(disposition)))
                    .collect()
            })
            .map_err(datafusion_to_py_err)
    })
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
fn match_subset_names(
    frame: &PyDataFrame,
    written: Vec<String>,
    held: Vec<String>,
) -> PyResult<Vec<(String, Vec<String>)>> {
    fenced!("dataframe_names.match_subset_names", {
        repark_core::frame_names::match_subset_names(&written, &held, frame_rule(frame.inner()))
            .map_err(datafusion_to_py_err)
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
fn resolve_frame_names(frame: &PyDataFrame, names: Vec<String>) -> PyResult<Vec<(String, String)>> {
    fenced!("dataframe_names.resolve_frame_names", {
        repark_core::frame_names::resolve_written_names(
            frame.inner().schema(),
            &names,
            frame_rule(frame.inner()),
        )
        .map_err(datafusion_to_py_err)
    })
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
fn resolve_qualified_display_names(
    frame: &PyDataFrame,
    displays: Vec<String>,
    names: Vec<String>,
) -> PyResult<Vec<(String, String, String)>> {
    fenced!("dataframe_names.resolve_qualified_display_names", {
        repark_core::frame_names::resolve_qualified_display_names(
            frame.inner().schema(),
            &displays,
            &names,
            frame_rule(frame.inner()),
        )
        .map(|rows| {
            rows.into_iter()
                .map(|(written, engine, disposition)| {
                    (written, engine, disposition_text(disposition))
                })
                .collect()
        })
        .map_err(datafusion_to_py_err)
    })
}

#[pyfunction]
fn rewrite_join_condition_aliases(
    left: &PyDataFrame,
    right: &PyDataFrame,
    condition_sql: &str,
    left_view: &str,
    right_view: &str,
) -> String {
    repark_core::frame_names::rewrite_join_condition_aliases(
        left.inner().schema(),
        right.inner().schema(),
        condition_sql,
        left_view,
        right_view,
        frame_rule(left.inner()),
    )
}
