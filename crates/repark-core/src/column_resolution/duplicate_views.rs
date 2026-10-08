use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, TableReference};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, LogicalPlan};
use repark_common::spark_error;

use super::{WrittenRefs, quoted};
use crate::frame_names::display_name;

fn same_name(display: &str, written: &str, exact: bool) -> bool {
    if exact {
        display == written
    } else {
        display.eq_ignore_ascii_case(written)
    }
}

fn candidate(relation: Option<&TableReference>, written: &str) -> String {
    match relation {
        Some(relation) => quoted(&[relation.table().to_string()], written),
        None => quoted(&[], written),
    }
}

fn ambiguous(reference: &str, options: &[String]) -> DataFusionError {
    DataFusionError::Plan(spark_error::message(
        spark_error::AMBIGUOUS_REFERENCE,
        &[
            ("reference", reference),
            ("options", options.join(", ").as_str()),
        ],
    ))
}

pub(super) fn ambiguous_reference(
    field: &Column,
    valid: &[Column],
    exact: bool,
) -> Option<DataFusionError> {
    let written = field.name.as_str();
    let mut hits: Vec<&Column> = Vec::new();
    for column in valid {
        let Some(display) = display_name(&column.name) else {
            continue;
        };
        if !same_name(display, written, exact) {
            continue;
        }
        if let Some(asked) = field.relation.as_ref()
            && !column
                .relation
                .as_ref()
                .is_some_and(|owner| owner.table().eq_ignore_ascii_case(asked.table()))
        {
            continue;
        }
        if !hits.contains(&column) {
            hits.push(column);
        }
    }
    if hits.len() < 2 {
        return None;
    }
    let options = hits
        .iter()
        .map(|column| candidate(column.relation.as_ref(), written))
        .collect::<Vec<_>>();
    Some(ambiguous(
        &candidate(field.relation.as_ref(), written),
        &options,
    ))
}

pub(super) fn suggested_name(column: &Column) -> Option<&str> {
    match display_name(&column.name) {
        Some(display) => Some(display),
        None if crate::frame_names::is_scratch_relation(&column.name) => None,
        None => Some(column.name.as_str()),
    }
}

fn written_bare(written: &WrittenRefs, name: &str, exact: bool) -> bool {
    written
        .bare
        .iter()
        .chain(written.outer_bare.iter())
        .any(|held| same_name(held, name, exact))
}

fn audit_node(node: &LogicalPlan, written: &WrittenRefs, exact: bool) -> Result<()> {
    let mut hidden: Vec<(&str, Option<&TableReference>)> = Vec::new();
    for input in node.inputs() {
        for (qualifier, field) in input.schema().iter() {
            if let Some(display) = display_name(field.name()) {
                hidden.push((display, qualifier));
            }
        }
    }
    if hidden.is_empty() {
        return Ok(());
    }
    for expr in node.expressions() {
        expr.apply(|leaf| {
            let Expr::Column(column) = leaf else {
                return Ok(TreeNodeRecursion::Continue);
            };
            let name = column.name.as_str();
            if display_name(name).is_some() || !written_bare(written, name, exact) {
                return Ok(TreeNodeRecursion::Continue);
            }
            let twins = hidden
                .iter()
                .filter(|(display, _)| same_name(display, name, exact))
                .collect::<Vec<_>>();
            if twins.is_empty() {
                return Ok(TreeNodeRecursion::Continue);
            }
            let mut options = vec![candidate(column.relation.as_ref(), name)];
            options.extend(twins.iter().map(|(_, relation)| candidate(*relation, name)));
            Err(ambiguous(&candidate(None, name), &options))
        })?;
    }
    Ok(())
}

pub(super) fn refuse_shadowed_duplicates(
    plan: &LogicalPlan,
    written: &WrittenRefs,
    exact: bool,
) -> Result<()> {
    plan.apply_with_subqueries(|node| {
        audit_node(node, written, exact)?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}
