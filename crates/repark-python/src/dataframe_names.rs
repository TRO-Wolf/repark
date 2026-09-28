use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::common::Column;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Expr, JoinType};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::column::PyColumn;
use crate::column::expr_build::{
    ambiguous_column, parse_canonical_predicate, parse_canonical_predicate_exact,
};
use crate::dataframe::PyDataFrame;
use crate::datafusion_to_py_err;
use crate::fence::fenced;
use crate::frame_lineage::PyFrameNode;
use repark_core::frame_names::{AttrId, Disposition, FrameNode, NameRule, Resolution, SortShape};
use repark_functions::case_sensitive::spark_case_sensitive_from_options;

#[allow(clippy::missing_errors_doc)]
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(attribute_column, module)?)?;
    module.add_function(wrap_pyfunction!(attribute_ids, module)?)?;
    module.add_function(wrap_pyfunction!(attribute_copies, module)?)?;
    module.add_function(wrap_pyfunction!(attribute_copy_name, module)?)?;
    module.add_function(wrap_pyfunction!(bind_free_names, module)?)?;
    module.add_function(wrap_pyfunction!(bind_qualified_free_refs, module)?)?;
    module.add_function(wrap_pyfunction!(copy_attribute_ids, module)?)?;
    module.add_function(wrap_pyfunction!(drop_frame_columns, module)?)?;
    module.add_function(wrap_pyfunction!(engine_field_is_unique, module)?)?;
    module.add_function(wrap_pyfunction!(grandchild_qualified_key, module)?)?;
    module.add_function(wrap_pyfunction!(qualifier_star_positions, module)?)?;
    module.add_function(wrap_pyfunction!(join_dup_below_wrappers, module)?)?;
    module.add_function(wrap_pyfunction!(join_output_sources, module)?)?;
    module.add_function(wrap_pyfunction!(union_dup_below_wrappers, module)?)?;
    module.add_function(wrap_pyfunction!(frame_case_sensitive, module)?)?;
    module.add_function(wrap_pyfunction!(frame_is_relation, module)?)?;
    module.add_function(wrap_pyfunction!(grandchild_key_status, module)?)?;
    module.add_function(wrap_pyfunction!(projection_source_ids, module)?)?;
    module.add_function(wrap_pyfunction!(java_fold_hits, module)?)?;
    module.add_function(wrap_pyfunction!(frame_is_exact, module)?)?;
    module.add_function(wrap_pyfunction!(match_display_names, module)?)?;
    module.add_function(wrap_pyfunction!(refuse_ambiguous_join_condition, module)?)?;
    module.add_function(wrap_pyfunction!(refuse_ambiguous_free_names, module)?)?;
    module.add_function(wrap_pyfunction!(refuse_folded_duplicate_keys, module)?)?;
    module.add_function(wrap_pyfunction!(requalify_join_sides, module)?)?;
    module.add_function(wrap_pyfunction!(resolve_display_name, module)?)?;
    module.add_function(wrap_pyfunction!(resolve_df_names, module)?)?;
    module.add_function(wrap_pyfunction!(resolve_frame_names, module)?)?;
    module.add_function(wrap_pyfunction!(sort_child_shape, module)?)?;
    module.add_function(wrap_pyfunction!(stamp_attribute_ids, module)?)?;
    module.add_function(wrap_pyfunction!(strip_attribute_ids, module)?)?;
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
    frame.clone().filter(parsed).map_err(|error| {
        if let Some((relation, name)) = ambiguous_column(&error) {
            let probe = Expr::Column(Column::new(relation, name));
            if let Err(shaped) = repark_core::frame_names::resolve_bound_expr_with(
                probe,
                frame.schema(),
                frame_rule(frame),
            ) {
                return datafusion_to_py_err(shaped);
            }
        }
        datafusion_to_py_err(error)
    })
}

pub(crate) fn join_on_keys(
    left: &DataFrame,
    right: &DataFrame,
    on: &[String],
    join_type: JoinType,
    left_node: &PyFrameNode,
    right_node: &PyFrameNode,
) -> PyResult<(DataFrame, PyFrameNode)> {
    let (joined, node) = repark_core::frame_names::join_on_named_keys(
        left.clone(),
        right.clone(),
        on,
        join_type,
        frame_rule(left),
        Arc::clone(&left_node.node),
        Arc::clone(&right_node.node),
    )
    .map_err(datafusion_to_py_err)?;
    Ok((joined, PyFrameNode { node }))
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
#[pyo3(signature = (joined, left, right, left_node, right_node, remint, emits_right))]
pub(crate) fn requalify_join_sides(
    joined: &PyDataFrame,
    left: &PyDataFrame,
    right: Option<&PyDataFrame>,
    left_node: &PyFrameNode,
    right_node: &PyFrameNode,
    remint: HashMap<String, String>,
    emits_right: bool,
) -> PyResult<(PyDataFrame, PyFrameNode)> {
    fenced!("dataframe_names.requalify_join_sides", {
        let mut sides = vec![left.inner().schema()];
        sides.extend(right.map(|frame| frame.inner().schema()));
        let df = repark_core::frame_names::requalify_join_sides(joined.inner().clone(), &sides)
            .map_err(datafusion_to_py_err)?;
        let remint = remint
            .iter()
            .map(|(old, new)| (AttrId::from_token(old), AttrId::from_token(new)))
            .collect::<HashMap<_, _>>();
        let plan = match right {
            Some(_) => {
                let left_width = left.inner().schema().fields().len();
                let (state, plan) = df.into_parts();
                let plan = repark_core::frame_names::remint_with_map(plan, left_width, &remint)
                    .map_err(datafusion_to_py_err)?;
                DataFrame::new(state, plan)
            }
            None => df,
        };
        let schema = plan.schema().clone();
        let node = FrameNode::join(
            &schema,
            Arc::clone(&left_node.node),
            Arc::clone(&right_node.node),
            remint,
            emits_right,
        )
        .map_err(datafusion_to_py_err)?;
        Ok((
            PyDataFrame::new(plan, joined.runtime_handle()),
            PyFrameNode { node },
        ))
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

#[pyfunction]
fn frame_is_relation(frame: &PyDataFrame) -> bool {
    repark_core::frame_names::plan_is_relation(frame.inner().logical_plan())
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

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (frame, column, displays, exact, for_sort))]
pub(crate) fn bind_free_names(
    frame: &PyDataFrame,
    column: &PyColumn,
    displays: Vec<String>,
    exact: bool,
    for_sort: bool,
) -> PyResult<PyColumn> {
    fenced!("dataframe_names.bind_free_names", {
        let prepared = column
            .expr()
            .resolve_lambda_variables(frame.inner().schema())
            .map_err(datafusion_to_py_err)?
            .data;
        let bound = repark_core::frame_names::bind_free_names(
            prepared,
            frame.inner().logical_plan(),
            NameRule::from_case_sensitive(exact),
            &displays,
            for_sort,
        )
        .map_err(datafusion_to_py_err)?;
        Ok(PyColumn::from_expr(bound))
    })
}

#[allow(clippy::missing_errors_doc, clippy::too_many_arguments)]
#[pyfunction]
#[pyo3(signature = (frame, sql=None, column=None, names=None, *, select_item=false, qualified_only=false, displays, exact, frame_qualifiers=None))]
pub(crate) fn refuse_ambiguous_free_names(
    py: Python<'_>,
    frame: &PyDataFrame,
    sql: Option<String>,
    column: Option<&PyColumn>,
    names: Option<Vec<String>>,
    select_item: bool,
    qualified_only: bool,
    displays: Vec<String>,
    exact: bool,
    frame_qualifiers: Option<BTreeMap<String, Vec<String>>>,
) -> PyResult<()> {
    fenced!("dataframe_names.refuse_ambiguous_free_names", {
        let rule = NameRule::from_case_sensitive(exact);
        if !folded_duplicate_display(&displays, rule) {
            return Ok(());
        }
        let schema = frame.inner().schema();
        if displays.len() != schema.fields().len() {
            return Ok(());
        }
        if schema
            .fields()
            .iter()
            .any(|field| AttrId::of(field).is_none())
        {
            return Ok(());
        }
        let collected: Vec<(Vec<String>, String)> = if let Some(sql) = sql.as_deref() {
            if !repark_core::frame_names::sql_mentions_duplicate(sql, &displays, rule) {
                return Ok(());
            }
            repark_core::frame_names::free_sql_names(sql, select_item)
        } else if let Some(column) = column {
            let prepared = column
                .expr()
                .resolve_lambda_variables(schema)
                .map_err(datafusion_to_py_err)?
                .data;
            repark_core::frame_names::free_expr_names(&prepared, qualified_only)
                .map_err(datafusion_to_py_err)?
        } else if let Some(names) = names {
            names.into_iter().map(|name| (Vec::new(), name)).collect()
        } else {
            return Ok(());
        };
        let offense = repark_core::frame_names::refuse_free_names(
            schema,
            rule,
            &displays,
            frame_qualifiers.as_ref(),
            &collected,
        )
        .map_err(datafusion_to_py_err)?;
        if let Some(offense) = offense {
            return Err(crate::frame_lineage::ambiguous_reference_error(
                py,
                &offense.qualifier,
                &offense.written,
                &offense.hits,
                &displays,
            ));
        }
        Ok(())
    })
}

fn folded_duplicate_display(displays: &[String], rule: NameRule) -> bool {
    displays.iter().enumerate().any(|(index, left)| {
        displays[index + 1..]
            .iter()
            .any(|right| rule.matches(left, right))
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
#[pyo3(signature = (frame, written, exact))]
pub(crate) fn grandchild_key_status(
    frame: &PyDataFrame,
    written: &str,
    exact: bool,
) -> PyResult<String> {
    fenced!("dataframe_names.grandchild_key_status", {
        let status = repark_core::frame_names::grandchild_key(
            frame.inner().logical_plan(),
            written,
            NameRule::from_case_sensitive(exact),
        )
        .map_err(datafusion_to_py_err)?;
        Ok(match status {
            None => "not-applicable",
            Some(Resolution::Bound(_)) => "bound",
            Some(Resolution::Ambiguous(_)) => "ambiguous",
            Some(Resolution::Missing) => "missing",
        }
        .to_string())
    })
}

#[pyfunction]
pub(crate) fn sort_child_shape(frame: &PyDataFrame) -> String {
    match repark_core::frame_names::sort_shape(frame.inner().logical_plan()) {
        SortShape::Project => "project",
        SortShape::Aggregate => "aggregate",
        SortShape::Other => "other",
    }
    .to_string()
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
pub fn stamp_attribute_ids(frame: Py<PyDataFrame>) -> PyResult<Py<PyDataFrame>> {
    fenced!("dataframe_names.stamp_attribute_ids", {
        Python::attach(|py| {
            let bound = frame.bind(py);
            let borrowed = bound.borrow();
            if repark_core::frame_names::plan_is_stamped(borrowed.inner().logical_plan()) {
                return Ok(frame.clone_ref(py));
            }
            let (state, plan) = borrowed.inner().clone().into_parts();
            let plan = repark_core::frame_names::stamp(plan).map_err(datafusion_to_py_err)?;
            let runtime = borrowed.runtime_handle();
            Py::new(py, PyDataFrame::new(DataFrame::new(state, plan), runtime))
        })
    })
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
pub(crate) fn strip_attribute_ids(frame: &PyDataFrame) -> PyResult<PyDataFrame> {
    fenced!("dataframe_names.strip_attribute_ids", {
        let (state, plan) = frame.inner().clone().into_parts();
        let plan = repark_core::frame_names::strip(plan).map_err(datafusion_to_py_err)?;
        Ok(PyDataFrame::new(
            DataFrame::new(state, plan),
            frame.runtime_handle(),
        ))
    })
}

#[pyfunction]
pub(crate) fn engine_field_is_unique(frame: &PyDataFrame, name: &str) -> bool {
    repark_core::frame_names::engine_field_is_unique(frame.inner().schema(), name)
}

#[pyfunction]
pub(crate) fn join_dup_below_wrappers(frame: &PyDataFrame) -> bool {
    repark_core::frame_names::join_dup_below_wrappers(frame.inner().logical_plan())
}

#[allow(clippy::needless_pass_by_value)]
#[pyfunction]
#[pyo3(signature = (frame, positions))]
pub(crate) fn union_dup_below_wrappers(frame: &PyDataFrame, positions: Vec<usize>) -> bool {
    repark_core::frame_names::union_dup_below_wrappers(frame.inner().logical_plan(), &positions)
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
#[pyo3(signature = (written, displays, mode))]
pub(crate) fn java_fold_hits(
    written: &str,
    displays: Vec<String>,
    mode: &str,
) -> PyResult<Vec<usize>> {
    let fold = match mode {
        "a" => repark_core::string_lower_equal,
        "b" => repark_core::fold_b_equal,
        _ => {
            return Err(PyValueError::new_err(format!(
                "java_fold_hits mode must be \"a\" or \"b\", got {mode:?}"
            )));
        }
    };
    Ok(displays
        .iter()
        .enumerate()
        .filter(|(_, display)| display.as_str() != written && fold(display, written))
        .map(|(index, _)| index)
        .collect())
}

#[pyfunction]
pub(crate) fn attribute_ids(frame: &PyDataFrame) -> Vec<Option<String>> {
    repark_core::frame_names::attribute_ids(frame.inner().schema())
        .into_iter()
        .map(|id| id.map(|id| id.as_str().to_string()))
        .collect()
}

#[pyfunction]
pub(crate) fn projection_source_ids(frame: &PyDataFrame) -> Vec<Option<String>> {
    repark_core::frame_names::projection_source_ids(frame.inner().logical_plan())
        .into_iter()
        .map(|id| id.map(|id| id.as_str().to_string()))
        .collect()
}

#[allow(
    clippy::missing_errors_doc,
    clippy::needless_pass_by_value,
    clippy::type_complexity
)]
#[pyfunction]
#[pyo3(signature = (frame, written, qualifier, displays, exact, frame_qualifiers))]
pub(crate) fn resolve_display_name(
    frame: &PyDataFrame,
    written: &str,
    qualifier: Option<&str>,
    displays: Vec<String>,
    exact: bool,
    frame_qualifiers: Option<BTreeMap<String, Vec<String>>>,
) -> PyResult<(String, Vec<usize>, Vec<Option<Vec<String>>>)> {
    fenced!("dataframe_names.resolve_display_name", {
        let resolution = repark_core::frame_names::resolve(
            frame.inner().schema(),
            written,
            qualifier,
            NameRule::from_case_sensitive(exact),
            &displays,
            frame_qualifiers.as_ref(),
        )
        .map_err(datafusion_to_py_err)?;
        let held = frame
            .inner()
            .schema()
            .iter()
            .map(|(qualifier, _)| {
                qualifier.map(|held| {
                    [held.catalog(), held.schema(), Some(held.table())]
                        .into_iter()
                        .flatten()
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>();
        let plan_qualifiers = |hits: &Vec<usize>| {
            hits.iter()
                .map(|position| held.get(*position).and_then(Clone::clone))
                .collect::<Vec<_>>()
        };
        Ok(match resolution {
            Resolution::Bound(hits) => {
                let quals = plan_qualifiers(&hits);
                ("bound".to_string(), hits, quals)
            }
            Resolution::Ambiguous(hits) => {
                let quals = plan_qualifiers(&hits);
                ("ambiguous".to_string(), hits, quals)
            }
            Resolution::Missing => ("missing".to_string(), Vec::new(), Vec::new()),
        })
    })
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
#[pyo3(signature = (frame, column, displays, exact, for_sort, frame_qualifiers))]
pub(crate) fn bind_qualified_free_refs(
    frame: &PyDataFrame,
    column: &PyColumn,
    displays: Vec<String>,
    exact: bool,
    for_sort: bool,
    frame_qualifiers: Option<BTreeMap<String, Vec<String>>>,
) -> PyResult<PyColumn> {
    fenced!("dataframe_names.bind_qualified_free_refs", {
        let prepared = column
            .expr()
            .resolve_lambda_variables(frame.inner().schema())
            .map_err(datafusion_to_py_err)?
            .data;
        let bound = repark_core::frame_names::bind_qualified_free_refs(
            prepared,
            frame.inner().logical_plan(),
            NameRule::from_case_sensitive(exact),
            &displays,
            frame_qualifiers.as_ref(),
            for_sort,
        )
        .map_err(datafusion_to_py_err)?;
        Ok(PyColumn::from_expr(bound))
    })
}

#[allow(clippy::missing_errors_doc, clippy::needless_pass_by_value)]
#[pyfunction]
#[pyo3(signature = (frame, written, qualifier, exact, frame_qualifiers))]
pub(crate) fn grandchild_qualified_key(
    frame: &PyDataFrame,
    written: &str,
    qualifier: &str,
    exact: bool,
    frame_qualifiers: Option<BTreeMap<String, Vec<String>>>,
) -> PyResult<(String, Vec<String>, Option<String>)> {
    fenced!("dataframe_names.grandchild_qualified_key", {
        let found = repark_core::frame_names::grandchild_qualified_key(
            frame.inner().logical_plan(),
            written,
            qualifier,
            NameRule::from_case_sensitive(exact),
            frame_qualifiers.as_ref(),
        )
        .map_err(datafusion_to_py_err)?;
        Ok(match found {
            None => ("not-applicable".to_string(), Vec::new(), None),
            Some((Resolution::Bound(_), render)) => {
                let (parts, engine) = render.unwrap_or((Vec::new(), String::new()));
                ("bound".to_string(), parts, Some(engine))
            }
            Some((Resolution::Ambiguous(_), _)) => ("ambiguous".to_string(), Vec::new(), None),
            Some((Resolution::Missing, _)) => ("missing".to_string(), Vec::new(), None),
        })
    })
}

#[pyfunction]
pub(crate) fn join_output_sources(frame: &PyDataFrame) -> Vec<Vec<(bool, usize)>> {
    repark_core::frame_names::join_output_sources(frame.inner().logical_plan())
}

#[allow(clippy::needless_pass_by_value)]
#[pyfunction]
#[pyo3(signature = (frame, head, displays, exact, frame_qualifiers))]
pub(crate) fn qualifier_star_positions(
    frame: &PyDataFrame,
    head: &str,
    displays: Vec<String>,
    exact: bool,
    frame_qualifiers: Option<BTreeMap<String, Vec<String>>>,
) -> Option<Vec<(usize, Vec<String>)>> {
    repark_core::frame_names::qualifier_star_positions(
        frame.inner().logical_plan(),
        head,
        NameRule::from_case_sensitive(exact),
        &displays,
        frame_qualifiers.as_ref(),
    )
}

#[allow(clippy::missing_errors_doc)]
#[pyfunction]
pub(crate) fn copy_attribute_ids(
    frame: &PyDataFrame,
    source: &PyDataFrame,
) -> PyResult<PyDataFrame> {
    fenced!("dataframe_names.copy_attribute_ids", {
        let (state, plan) = frame.inner().clone().into_parts();
        let plan =
            repark_core::frame_names::copy_attribute_ids(plan, source.inner().logical_plan())
                .map_err(datafusion_to_py_err)?;
        Ok(PyDataFrame::new(
            DataFrame::new(state, plan),
            frame.runtime_handle(),
        ))
    })
}
