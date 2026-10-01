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
use crate::deep_stack::{
    DEEP_NESTING_DEPTH, drive_segment_bytes, frame_drive_segment_cached, grown_clone_frame,
    grown_clone_plan, grown_sync, refuse_expression_depth, run_grown_if, stack_is_small,
};
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
        let input = column.expr();
        let need =
            crate::deep_stack::clone_need_bytes(column.plan_depth(), column.expression_depth());
        let rewritten = grown_sync(need, || {
            input.transform(|node| match node {
                Expr::Column(reference) => Ok(Transformed::yes(Expr::OuterReferenceColumn(
                    Arc::new(Field::new(reference.name(), DataType::Null, true)),
                    reference,
                ))),
                _ => Ok(Transformed::no(node)),
            })
        })
        .map_err(datafusion_to_py_err)?;
        Ok(PyColumn::combine_surveyed(rewritten.data, [column]))
    })
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#[pyfunction]
fn scalar_subquery(frame: &PyDataFrame) -> PyResult<PyColumn> {
    fenced!("subquery.scalar_subquery", {
        let depths = frame.depths();
        let plan = grown_clone_plan(frame.df.logical_plan(), depths.plan, depths.expression);
        Ok(PyColumn::from_expr(Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(plan),
            outer_ref_columns: Vec::new(),
            spans: Spans::new(),
        })))
    })
}

#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#[pyfunction]
fn exists_subquery(frame: &PyDataFrame) -> PyResult<PyColumn> {
    fenced!("subquery.exists_subquery", {
        let depths = frame.depths();
        let plan = grown_clone_plan(frame.df.logical_plan(), depths.plan, depths.expression);
        Ok(PyColumn::from_expr(Expr::Exists(Exists::new(
            Subquery {
                subquery: Arc::new(plan),
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
        let depths = frame.depths();
        let need = crate::deep_stack::clone_need_bytes(depths.plan, depths.expression);
        let (state, aliased) = grown_sync(need, || {
            let (state, plan) = grown_clone_frame(frame.inner(), &frame.depths()).into_parts();
            let aliased = LogicalPlan::SubqueryAlias(
                SubqueryAlias::try_new(Arc::new(plan), alias).map_err(datafusion_to_py_err)?,
            );
            Ok::<_, PyErr>((state, aliased))
        })?;
        Ok(PyDataFrame::new_with_depths(
            DataFrame::new(state, aliased),
            frame.runtime_handle(),
            crate::deep_stack::PlanDepths {
                plan: depths.plan + 1,
                limited: depths.limited + 1,
                expression: depths.expression,
            },
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
        let deepest = on.as_ref().map_or(0, PyColumn::df_depth);
        let deepest_plan = on.as_ref().map_or(0, PyColumn::plan_depth);
        refuse_expression_depth(deepest)?;
        let grown = deepest > DEEP_NESTING_DEPTH
            || deepest_plan > DEEP_NESTING_DEPTH
            || frame_drive_segment_cached(&right.depths())?.is_some()
            || drive_segment_bytes(&left.depths()).is_some()
            || stack_is_small();
        let runtime = left.runtime_handle();
        run_grown_if(&runtime, grown, || {
            let (state, left_plan) = grown_clone_frame(left.inner(), &left.depths()).into_parts();
            let left_schema = Arc::clone(left_plan.schema());
            let owned_right = grown_clone_plan(
                right.df.logical_plan(),
                right.depths().plan,
                right.depths().expression,
            );
            let resolved_right = repark_core::resolve_subquery_plan(owned_right, &left_schema)
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
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;
    use tokio::runtime::Runtime;

    fn battery_frame() -> PyDataFrame {
        let runtime: Arc<Runtime> = Arc::new(Runtime::new().expect("a runtime builds"));
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = datafusion::arrow::array::RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])) as _],
        )
        .expect("a probe batch builds");
        let context = SessionContext::new();
        context.register_batch("t", batch).expect("register");
        let df = runtime.block_on(context.table("t")).expect("a table scans");
        PyDataFrame::new(df, runtime)
    }

    #[test]
    fn subquery_levels_match_a_fresh_survey() {
        let frame = battery_frame();
        let scalar = scalar_subquery(&frame).expect("a scalar subquery builds");
        let (expr, plan) = scalar.grown_read(crate::deep_stack::survey_expression);
        assert_eq!(scalar.expression_depth(), expr);
        assert_eq!(scalar.plan_depth(), plan);
        assert_eq!(plan, frame.depths().plan);
        assert_eq!(
            scalar.df_depth(),
            expr,
            "the wrapper counts, the plan does not"
        );
        let exists = exists_subquery(&frame).expect("an exists builds");
        let (expr, plan) = exists.grown_read(crate::deep_stack::survey_expression);
        assert_eq!(exists.expression_depth(), expr);
        assert_eq!(exists.plan_depth(), plan);
        assert_eq!(exists.df_depth(), expr);
        let id = PyColumn::column("id").expect("a column builds");
        let outer = column_outer(&id).expect("an outer ref builds");
        let (expr, _) = outer.grown_read(crate::deep_stack::survey_expression);
        assert_eq!(outer.expression_depth(), expr);
        assert_eq!(outer.df_depth(), id.df_depth());
        let aliased = subquery_alias(&frame, "s").expect("an alias builds");
        let walked = crate::deep_stack::plan_depths(aliased.inner().logical_plan());
        let cached = aliased.depths();
        assert_eq!(
            (cached.plan, cached.limited, cached.expression),
            (walked.plan, walked.limited, walked.expression),
        );
    }

    #[test]
    fn builders_carrying_subquery_columns_match_a_fresh_survey() {
        fn walked_depths(frame: &PyDataFrame) -> (usize, usize, usize) {
            let walked = crate::deep_stack::plan_depths(frame.inner().logical_plan());
            (walked.plan, walked.limited, walked.expression)
        }
        fn cached_depths(frame: &PyDataFrame) -> (usize, usize, usize) {
            let cached = frame.depths();
            (cached.plan, cached.limited, cached.expression)
        }
        let id = || PyColumn::column("id").expect("a column builds");
        let base = battery_frame();
        let mut chained = base
            .filter(id().is_not_null().expect("a predicate builds"))
            .expect("a filter builds");
        for _ in 0..30 {
            chained = chained
                .filter(id().is_not_null().expect("a predicate builds"))
                .expect("a chained filter builds");
        }
        assert_eq!(cached_depths(&chained), walked_depths(&chained));
        let aggregated = chained
            .aggregate(
                vec![],
                vec![id().aggregate("sum", false).expect("a sum builds")],
            )
            .expect("an aggregate builds");
        assert_eq!(cached_depths(&aggregated), walked_depths(&aggregated));
        let scalar = scalar_subquery(&aggregated).expect("a scalar subquery builds");
        assert_eq!(scalar.plan_depth(), aggregated.depths().plan);
        let selected = base
            .limit(1)
            .expect("a limit builds")
            .select(vec![scalar.alias("c").expect("an alias builds")])
            .expect("a select over a scalar builds");
        assert_eq!(
            cached_depths(&selected),
            walked_depths(&selected),
            "a select carrying a subquery plan re-surveys"
        );
        let exists = exists_subquery(&aggregated).expect("an exists builds");
        let filtered = base.filter(exists).expect("a filter over an exists builds");
        assert_eq!(
            cached_depths(&filtered),
            walked_depths(&filtered),
            "a filter carrying a subquery plan re-surveys"
        );
    }
}
