use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, JoinConstraint, NullEquality, Spans};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::expr::Exists;
use datafusion::logical_expr::{
    Expr, Join, JoinType, LogicalPlan, LogicalPlanBuilder, Subquery, SubqueryAlias,
};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use crate::column::PyColumn;
use crate::dataframe::PyDataFrame;
use crate::datafusion_to_py_err;
use crate::fence::fenced;

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(column_outer, module)?)?;
    module.add_function(wrap_pyfunction!(scalar_subquery, module)?)?;
    module.add_function(wrap_pyfunction!(exists_subquery, module)?)?;
    module.add_function(wrap_pyfunction!(subquery_alias, module)?)?;
    module.add_function(wrap_pyfunction!(lateral_join, module)?)?;
    Ok(())
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#[pyfunction]
fn column_outer(column: &PyColumn) -> PyResult<PyColumn> {
    fenced!("subquery.column_outer", {
        column
            .expr()
            .transform(|node| match node {
                Expr::Column(reference) => Ok(Transformed::yes(Expr::OuterReferenceColumn(
                    Arc::new(Field::new(reference.name(), DataType::Null, true)),
                    reference,
                ))),
                _ => Ok(Transformed::no(node)),
            })
            .map(|transformed| transformed.data)
            .map(PyColumn::from_expr)
            .map_err(datafusion_to_py_err)
    })
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#[pyfunction]
fn scalar_subquery(frame: &PyDataFrame) -> PyResult<PyColumn> {
    fenced!("subquery.scalar_subquery", {
        Ok(PyColumn::from_expr(Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(frame.df.logical_plan().clone()),
            outer_ref_columns: Vec::new(),
            spans: Spans::new(),
        })))
    })
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#[pyfunction]
fn exists_subquery(frame: &PyDataFrame) -> PyResult<PyColumn> {
    fenced!("subquery.exists_subquery", {
        Ok(PyColumn::from_expr(Expr::Exists(Exists::new(
            Subquery {
                subquery: Arc::new(frame.df.logical_plan().clone()),
                outer_ref_columns: Vec::new(),
                spans: Spans::new(),
            },
            false,
        ))))
    })
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#[pyfunction]
fn subquery_alias(frame: &PyDataFrame, alias: &str) -> PyResult<PyDataFrame> {
    fenced!("subquery.subquery_alias", {
        let (state, plan) = frame.df.clone().into_parts();
        let aliased = LogicalPlan::SubqueryAlias(
            SubqueryAlias::try_new(Arc::new(plan), alias).map_err(datafusion_to_py_err)?,
        );
        Ok(PyDataFrame::new(
            DataFrame::new(state, aliased),
            frame.runtime_handle(),
        ))
    })
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#[pyfunction]
fn lateral_join(
    left: &PyDataFrame,
    right: &PyDataFrame,
    join_type: &str,
    on: Option<PyColumn>,
    on_names: Option<Vec<String>>,
) -> PyResult<PyDataFrame> {
    fenced!("subquery.lateral_join", {
        let engine_type = match join_type {
            "inner" | "cross" => JoinType::Inner,
            "left" => JoinType::Left,
            other => {
                return Err(crate::exceptions::AnalysisException::new_err(format!(
                    "[UNSUPPORTED_JOIN_TYPE] Unsupported join type '{other}'. Supported join \
                     types include: 'inner', 'leftouter', 'left', 'left_outer', 'cross'. \
                     SQLSTATE: 0A000"
                )));
            }
        };
        let (state, left_plan) = left.df.clone().into_parts();
        let left_schema = Arc::clone(left_plan.schema());
        let resolved_right =
            repark_core::resolve_subquery_plan(right.df.logical_plan().clone(), &left_schema)
                .map_err(datafusion_to_py_err)?;
        let right_wrapped = LogicalPlan::Subquery(Subquery {
            outer_ref_columns: resolved_right.all_out_ref_exprs(),
            subquery: Arc::new(resolved_right),
            spans: Spans::new(),
        });
        let filter = match on {
            Some(condition) => Some(
                repark_core::resolve_scoped_expr(
                    condition.expr(),
                    &[left_schema, Arc::clone(right_wrapped.schema())],
                )
                .map_err(datafusion_to_py_err)?,
            ),
            None => None,
        };
        let on_pairs = on_names
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .map(|name| {
                (
                    Expr::Column(Column::from_name(name)),
                    Expr::Column(Column::from_name(name)),
                )
            })
            .collect();
        let join = Join::try_new(
            Arc::new(left_plan),
            Arc::new(right_wrapped),
            on_pairs,
            filter,
            engine_type,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
            false,
        )
        .map_err(datafusion_to_py_err)?;
        let join_plan = LogicalPlan::Join(join);
        let output = match on_names {
            Some(names) => {
                let dropped: std::collections::HashSet<&str> =
                    names.iter().map(String::as_str).collect();
                let right_count = join_plan.inputs()[1].schema().fields().len();
                let fields = join_plan.schema().fields();
                let keep: Vec<Expr> = fields
                    .iter()
                    .enumerate()
                    .filter(|(index, field)| {
                        *index < fields.len() - right_count
                            || !dropped.contains(field.name().as_str())
                    })
                    .map(|(index, _)| {
                        Expr::Column(Column::from(join_plan.schema().qualified_field(index)))
                    })
                    .collect();
                LogicalPlanBuilder::from(join_plan)
                    .project(keep)
                    .and_then(LogicalPlanBuilder::build)
                    .map_err(datafusion_to_py_err)?
            }
            None => join_plan,
        };
        Ok(PyDataFrame::new(
            DataFrame::new(state, output),
            left.runtime_handle(),
        ))
    })
}
