use std::borrow::Cow;
use std::sync::Arc;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Result, ScalarValue};
use datafusion::logical_expr::{DmlStatement, EmptyRelation, LogicalPlan, Values, WriteOp, lit};

pub(super) fn plan_empty_dml_as_no_op(plan: &LogicalPlan) -> Result<Cow<'_, LogicalPlan>> {
    if !plan.exists(|node| Ok(is_empty_dml(node)))? {
        return Ok(Cow::Borrowed(plan));
    }
    let rewritten = plan.clone().transform_up(|node| match node {
        LogicalPlan::Dml(dml) if matches_nothing(&dml) => zero_count(&dml).map(Transformed::yes),
        other => Ok(Transformed::no(other)),
    })?;
    Ok(Cow::Owned(rewritten.data))
}

fn is_empty_dml(node: &LogicalPlan) -> bool {
    matches!(node, LogicalPlan::Dml(dml) if matches_nothing(dml))
}

fn matches_nothing(dml: &DmlStatement) -> bool {
    matches!(dml.op, WriteOp::Delete | WriteOp::Update) && reads_no_rows(&dml.input)
}

fn reads_no_rows(input: &LogicalPlan) -> bool {
    let mut current = input;
    while let LogicalPlan::Projection(projection) = current {
        current = projection.input.as_ref();
    }
    matches!(
        current,
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            ..
        })
    )
}

fn zero_count(dml: &DmlStatement) -> Result<LogicalPlan> {
    let row = dml
        .output_schema
        .fields()
        .iter()
        .map(|field| ScalarValue::new_zero(field.data_type()).map(lit))
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan::Values(Values {
        schema: Arc::clone(&dml.output_schema),
        values: vec![row],
    }))
}

#[cfg(test)]
mod tests;
