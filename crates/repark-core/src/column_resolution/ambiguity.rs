use std::collections::HashMap;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, TableReference};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::expr::{Exists, InSubquery, SetComparison};
use datafusion::logical_expr::{Expr, LogicalPlan};

use super::{WrittenRefs, ambiguous_message, has_upper_ascii};

type Twins<'a> = HashMap<String, Vec<(Option<&'a TableReference>, &'a str)>>;

pub(super) fn audit_plan_for_ambiguity(plan: &LogicalPlan, written: &WrittenRefs) -> Result<()> {
    audit_level(plan, written, false)
}

fn audit_level(node: &LogicalPlan, written: &WrittenRefs, inside: bool) -> Result<()> {
    if let LogicalPlan::SubqueryAlias(alias) = node
        && written
            .views
            .contains(&alias.alias.table().to_ascii_lowercase())
    {
        return Ok(());
    }
    audit_node(node, written, inside)?;
    node.apply_subqueries(|child| {
        audit_level(child, written, inside)?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    let child_inside = inside
        || matches!(
            node,
            LogicalPlan::SubqueryAlias(_) | LogicalPlan::Subquery(_)
        );
    node.apply_children(|child| {
        let nested = child_inside || is_bare_derived_body(node, child);
        audit_level(child, written, nested)?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

fn is_bare_derived_body(parent: &LogicalPlan, child: &LogicalPlan) -> bool {
    matches!(
        parent,
        LogicalPlan::Projection(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Window(_)
    ) && matches!(
        child,
        LogicalPlan::Projection(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Distinct(_)
    )
}

fn audit_node(node: &LogicalPlan, written: &WrittenRefs, inside: bool) -> Result<()> {
    let twins = input_twins(node);
    if twins.is_empty() {
        return Ok(());
    }
    for expr in node.expressions() {
        expr.apply(|leaf| {
            match leaf {
                Expr::Column(column) => audit_column(column, &twins, written, inside)?,
                Expr::Exists(Exists { subquery, .. })
                | Expr::InSubquery(InSubquery { subquery, .. })
                | Expr::SetComparison(SetComparison { subquery, .. })
                | Expr::ScalarSubquery(subquery) => {
                    for outer in &subquery.outer_ref_columns {
                        if let Expr::OuterReferenceColumn(_, column) = outer {
                            audit_column(column, &twins, written, inside)?;
                        }
                    }
                }
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    if written.has_star
        && let LogicalPlan::Projection(projection) = node
    {
        super::star_twins::refuse_star_twins(node, &projection.expr, &twins)?;
    }
    Ok(())
}

fn input_twins(node: &LogicalPlan) -> Twins<'_> {
    let inputs = node.inputs();
    let mut index: Twins<'_> = HashMap::new();
    if !inputs.iter().any(|input| {
        input
            .schema()
            .fields()
            .iter()
            .any(|field| has_upper_ascii(field.name()))
    }) {
        return index;
    }
    for input in inputs {
        for (qualifier, field) in input.schema().iter() {
            index
                .entry(field.name().to_ascii_lowercase())
                .or_default()
                .push((qualifier, field.name().as_str()));
        }
    }
    index.retain(|_, fields| fields.iter().any(|(_, name)| *name != fields[0].1));
    index
}

fn audit_column(
    column: &Column,
    twins: &Twins<'_>,
    written: &WrittenRefs,
    inside: bool,
) -> Result<()> {
    let Some(fields) = twins.get(&column.name.to_ascii_lowercase()) else {
        return Ok(());
    };
    let relation_hit = column
        .relation
        .as_ref()
        .and_then(|relation| qualified_hit(written, inside, relation, &column.name));
    let bare_hit = bare_hit(written, inside, &column.name);
    let (qualifier, requested) = match (relation_hit, bare_hit) {
        (Some((qualifier, name)), _) => (Some(qualifier.as_str()), name.as_str()),
        (None, Some(name)) => (None, name.as_str()),
        (None, None) => return Ok(()),
    };
    let matching = fields
        .iter()
        .filter(
            |(candidate, _)| match (qualifier, column.relation.as_ref()) {
                (Some(_), Some(relation)) => {
                    candidate.is_some_and(|candidate| qualifier_matches(relation, candidate))
                }
                _ => true,
            },
        )
        .collect::<Vec<_>>();
    if matching.len() < 2 || matching.iter().all(|(_, name)| *name == matching[0].1) {
        return Ok(());
    }
    let scopes = matching
        .iter()
        .map(|(candidate, _)| {
            candidate
                .filter(|candidate| !crate::frame_names::is_scratch_relation(candidate.table()))
                .map(|candidate| written.relation_parts(candidate))
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    Err(DataFusionError::Plan(ambiguous_message(
        qualifier, requested, &scopes,
    )))
}

fn qualified_hit<'a>(
    written: &'a WrittenRefs,
    inside: bool,
    relation: &TableReference,
    name: &str,
) -> Option<&'a (String, String)> {
    let refs = if inside {
        &written.qualified
    } else {
        &written.outer_qualified
    };
    refs.iter().find(|(qualifier, held)| {
        qualifier.eq_ignore_ascii_case(relation.table()) && held.eq_ignore_ascii_case(name)
    })
}

fn bare_hit<'a>(written: &'a WrittenRefs, inside: bool, name: &str) -> Option<&'a String> {
    let refs = if inside {
        &written.bare
    } else {
        &written.outer_bare
    };
    refs.iter().find(|held| held.eq_ignore_ascii_case(name))
}

fn qualifier_matches(written: &TableReference, candidate: &TableReference) -> bool {
    written.table().eq_ignore_ascii_case(candidate.table())
        && part_matches(written.schema(), candidate.schema())
        && part_matches(written.catalog(), candidate.catalog())
}

fn part_matches(first: Option<&str>, second: Option<&str>) -> bool {
    match (first, second) {
        (None, None) => true,
        (Some(first), Some(second)) => first.eq_ignore_ascii_case(second),
        _ => false,
    }
}
