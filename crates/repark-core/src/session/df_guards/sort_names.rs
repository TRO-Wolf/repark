use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, Result, internal_datafusion_err};
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{Distinct, Expr, LogicalPlan, Projection};
use repark_common::names::NameRule;

use super::attr_id::{AttrId, Resolution, resolve};
use super::case_bind::{
    ambiguous_reference, attribute_reference, is_scratch_relation, unresolved_column,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortShape {
    Project,
    Aggregate,
    Other,
}

#[must_use]
pub fn sort_shape(plan: &LogicalPlan) -> SortShape {
    let mut node = plan;
    loop {
        let below = below_transparent(node);
        if !std::ptr::eq(below, node) {
            node = below;
            continue;
        }
        match node {
            LogicalPlan::Projection(_) => return SortShape::Project,
            LogicalPlan::Aggregate(_) => return SortShape::Aggregate,
            _ => return SortShape::Other,
        }
    }
}

fn below_transparent(plan: &LogicalPlan) -> &LogicalPlan {
    match plan {
        LogicalPlan::Filter(filter) => filter.input.as_ref(),
        LogicalPlan::Sort(sort) => sort.input.as_ref(),
        LogicalPlan::Limit(limit) => limit.input.as_ref(),
        LogicalPlan::Repartition(repartition) => repartition.input.as_ref(),
        LogicalPlan::Distinct(Distinct::All(input)) => input.as_ref(),
        LogicalPlan::Distinct(Distinct::On(on)) => on.input.as_ref(),
        LogicalPlan::SubqueryAlias(alias) => alias.input.as_ref(),
        LogicalPlan::Projection(projection) if is_transparent(projection) => &projection.input,
        _ => plan,
    }
}

fn below_join_through(plan: &LogicalPlan) -> Option<&DFSchema> {
    let mut node = plan;
    loop {
        if matches!(node, LogicalPlan::Join(_)) {
            return Some(plan.schema());
        }
        let below = below_transparent(node);
        if std::ptr::eq(below, node) {
            return None;
        }
        node = below;
    }
}

fn is_transparent(projection: &Projection) -> bool {
    is_passthrough(projection) || is_join_select(projection)
}

fn is_join_select(projection: &Projection) -> bool {
    if !matches!(projection.input.as_ref(), LogicalPlan::Join(_)) {
        return false;
    }
    projection.expr.iter().all(|expr| {
        let column = match expr {
            Expr::Column(column) => column,
            Expr::Alias(Alias { expr, .. }) => match expr.as_ref() {
                Expr::Column(column) => column,
                _ => return false,
            },
            _ => return false,
        };
        column
            .relation
            .as_ref()
            .is_some_and(|relation| is_scratch_relation(relation.table()))
    })
}

fn is_passthrough(projection: &Projection) -> bool {
    let below = projection.input.schema();
    if projection.expr.len() != below.fields().len() {
        return false;
    }
    projection
        .expr
        .iter()
        .zip(below.iter())
        .all(|(expr, (qualifier, field))| {
            let column = match expr {
                Expr::Column(column) => column,
                Expr::Alias(Alias { expr, name, .. }) => match expr.as_ref() {
                    Expr::Column(column) if name == &column.name => column,
                    _ => return false,
                },
                _ => return false,
            };
            column.name.as_str() == field.name() && column.relation.as_ref() == qualifier
        })
}

#[allow(clippy::missing_errors_doc)]
pub fn grandchild_key(
    plan: &LogicalPlan,
    written: &str,
    rule: NameRule,
) -> Result<Option<Resolution>> {
    let LogicalPlan::Projection(projection) = plan else {
        return Ok(None);
    };
    let Some(schema) = below_join_through(projection.input.as_ref()) else {
        return Ok(None);
    };
    let displays = schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    resolve(schema, written, None, rule, &displays).map(Some)
}

#[allow(clippy::missing_errors_doc)]
pub fn bind_free_names(
    expr: Expr,
    plan: &LogicalPlan,
    rule: NameRule,
    displays: &[String],
    for_sort: bool,
) -> Result<Expr> {
    let schema = plan.schema();
    if displays.len() != schema.fields().len() {
        return Ok(expr);
    }
    let shape = if for_sort {
        sort_shape(plan)
    } else {
        SortShape::Other
    };
    expr.transform(|node| {
        Ok(match node {
            Expr::Column(column) if column.relation.is_none() => Transformed::yes(
                bind_free_column(column, schema, rule, displays, for_sort, shape)?,
            ),
            _ => Transformed::no(node),
        })
    })
    .map(|transformed| transformed.data)
}

fn bind_free_column(
    column: Column,
    schema: &DFSchema,
    rule: NameRule,
    displays: &[String],
    for_sort: bool,
    shape: SortShape,
) -> Result<Expr> {
    match resolve(schema, &column.name, None, rule, displays)? {
        Resolution::Bound(hits) => {
            let position = hits.first().ok_or_else(|| {
                internal_datafusion_err!("resolve bound no position for {}", column.name)
            })?;
            let field = schema.fields().get(*position).ok_or_else(|| {
                internal_datafusion_err!("resolve bound out of range for {}", column.name)
            })?;
            Ok(attribute_reference(field.name()))
        }
        Resolution::Ambiguous(hits) => {
            if for_sort {
                if shape == SortShape::Project {
                    return oldest_field(schema, &hits);
                }
                return Err(unresolved_column(&column, schema));
            }
            let options = hits
                .iter()
                .filter_map(|position| schema.iter().nth(*position))
                .map(|(qualifier, field)| (qualifier, field.as_ref()))
                .collect::<Vec<_>>();
            Err(ambiguous_reference(&column, &options))
        }
        Resolution::Missing => Ok(Expr::Column(column)),
    }
}

fn oldest_field(schema: &DFSchema, hits: &[usize]) -> Result<Expr> {
    let oldest = hits
        .iter()
        .filter_map(|position| {
            let field = schema.fields().get(*position)?;
            AttrId::of(field).map(|id| (id, field.name().clone()))
        })
        .min_by(|(left, _), (right, _)| left.cmp(right))
        .map(|(_, name)| name);
    oldest.map_or_else(
        || Err(internal_datafusion_err!("resolve bound an id-free hit")),
        |name| Ok(attribute_reference(&name)),
    )
}
