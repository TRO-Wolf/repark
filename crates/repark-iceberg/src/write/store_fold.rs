use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Schema as ArrowSchema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::ExecutionProps;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::scalar::ScalarValue;

use super::store_overflow::{CONVERTIBLE_EVAL_HEADS, StoreIssue, split_aliases};
use crate::write::store_cast::{
    cast_store_value, store_cast_udf_for_target, store_int_guard_udf, store_overflow_error,
};

pub(crate) fn wrap_store_expr(issue: &StoreIssue, value: Expr) -> Expr {
    let Some(udf) = store_cast_udf_for_target(&issue.target) else {
        return value;
    };
    Expr::ScalarFunction(ScalarFunction::new_udf(
        udf,
        vec![
            value,
            Expr::Literal(ScalarValue::Utf8(Some(issue.column.clone())), None),
        ],
    ))
}

pub(crate) fn store_guard_expr(divisor: Expr, issue: &StoreIssue) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        store_int_guard_udf(),
        vec![
            divisor,
            Expr::Literal(ScalarValue::Utf8(Some(issue.column.clone())), None),
            Expr::Literal(ScalarValue::Utf8(Some(issue.source_name.clone())), None),
            Expr::Literal(ScalarValue::Utf8(Some(issue.target_name.clone())), None),
        ],
    ))
}

pub(crate) fn check_folded_store_input(
    issue: &StoreIssue,
    value: &Expr,
    input: Option<&LogicalPlan>,
    in_values: bool,
) -> Result<()> {
    let resolved = match input {
        Some(plan) => resolve_store_input(plan, value)?,
        None => (!expr_has_columns(value)?).then(|| value.clone()),
    };
    let Some(folded) = resolved else {
        return Ok(());
    };
    let scalar = match fold_scalar(&folded) {
        Some(Ok(scalar)) => scalar,
        Some(Err(error)) => return convert_fold_error(issue, &error, in_values),
        None => return Ok(()),
    };
    if scalar.is_null() {
        return Ok(());
    }
    if !(scalar.data_type().is_floating()
        || matches!(
            scalar.data_type(),
            DataType::Decimal128(..) | DataType::Decimal256(..)
        ))
    {
        return Ok(());
    }
    match cast_store_value(
        &issue.column,
        &datafusion::logical_expr::ColumnarValue::Scalar(scalar),
        &issue.target,
    ) {
        Ok(_) => Ok(()),
        Err(error)
            if error
                .to_string()
                .contains("[CAST_OVERFLOW_IN_TABLE_INSERT]") =>
        {
            Err(store_overflow_error(
                &issue.column,
                &issue.source_name,
                &issue.target_name,
            ))
        }
        Err(error) => Err(error),
    }
}

fn convert_fold_error(issue: &StoreIssue, error: &DataFusionError, in_values: bool) -> Result<()> {
    if !in_values
        && CONVERTIBLE_EVAL_HEADS
            .iter()
            .any(|head| error.to_string().contains(head))
    {
        return Err(store_overflow_error(
            &issue.column,
            &issue.source_name,
            &issue.target_name,
        ));
    }
    Ok(())
}

fn expr_has_columns(expr: &Expr) -> Result<bool> {
    let mut found = false;
    expr.apply(|node| match node {
        Expr::Column(_) | Expr::OuterReferenceColumn(..) => {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
    Ok(found)
}

pub(crate) fn expr_references_column(expr: &Expr, column: &str) -> bool {
    let mut found = false;
    let _ = expr.apply(|node| match node {
        Expr::Column(candidate) if candidate.name == column => {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    });
    found
}

fn fold_scalar(expr: &Expr) -> Option<std::result::Result<ScalarValue, DataFusionError>> {
    let Ok(physical) = datafusion::physical_expr::create_physical_expr(
        expr,
        &DFSchema::empty(),
        &ExecutionProps::new(),
    ) else {
        return None;
    };
    let batch = RecordBatch::new_empty(Arc::new(ArrowSchema::empty()));
    let value = match physical.evaluate(&batch) {
        Ok(value) => value,
        Err(error) => return Some(Err(error)),
    };
    match value {
        datafusion::logical_expr::ColumnarValue::Scalar(scalar) => Some(Ok(scalar)),
        datafusion::logical_expr::ColumnarValue::Array(array) if array.len() == 1 => {
            match ScalarValue::try_from_array(array.as_ref(), 0) {
                Ok(scalar) => Some(Ok(scalar)),
                Err(_) => None,
            }
        }
        datafusion::logical_expr::ColumnarValue::Array(_) => None,
    }
}

fn resolve_store_input(input: &LogicalPlan, expr: &Expr) -> Result<Option<Expr>> {
    if !expr_has_columns(expr)? {
        return Ok(Some(expr.clone()));
    }
    let Expr::Column(column) = expr else {
        return resolve_store_substituted(input, expr);
    };
    lookup_store_column(input, column)
}

fn resolve_store_substituted(input: &LogicalPlan, expr: &Expr) -> Result<Option<Expr>> {
    let mut failed = false;
    let substituted = expr
        .clone()
        .transform_up(|node| {
            let Expr::Column(column) = &node else {
                return Ok(Transformed::no(node));
            };
            if let Some(defining) = lookup_store_column(input, column)? {
                Ok(Transformed::yes(defining))
            } else {
                failed = true;
                Ok(Transformed::no(node))
            }
        })?
        .data;
    if failed || expr_has_columns(&substituted)? {
        return Ok(None);
    }
    Ok(Some(substituted))
}

fn lookup_store_column(
    input: &LogicalPlan,
    column: &datafusion::common::Column,
) -> Result<Option<Expr>> {
    let mut column = column.clone();
    let mut plan = input;
    loop {
        match plan {
            LogicalPlan::Projection(projection) => {
                let matches: Vec<&Expr> = projection
                    .expr
                    .iter()
                    .filter(|candidate| match candidate {
                        Expr::Alias(alias) => alias.name == column.name,
                        Expr::Column(candidate) => candidate.name == column.name,
                        other => other.schema_name().to_string() == column.name,
                    })
                    .collect();
                if matches.len() != 1 {
                    return Ok(None);
                }
                let (core, _) = split_aliases(matches[0]);
                match core {
                    Expr::Column(next) => {
                        column = next.clone();
                        plan = &projection.input;
                    }
                    defining => {
                        if expr_has_columns(defining)? {
                            return Ok(None);
                        }
                        return Ok(Some((*defining).clone()));
                    }
                }
            }
            LogicalPlan::SubqueryAlias(alias) => {
                plan = &alias.input;
            }
            LogicalPlan::Filter(filter) => {
                plan = &filter.input;
            }
            LogicalPlan::Sort(sort) => {
                plan = &sort.input;
            }
            LogicalPlan::Limit(limit) => {
                plan = &limit.input;
            }
            _ => return Ok(None),
        }
    }
}
