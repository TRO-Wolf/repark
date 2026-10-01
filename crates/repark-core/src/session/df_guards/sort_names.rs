use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, Result, internal_datafusion_err};
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{Distinct, Expr, LogicalPlan, Projection};
use repark_common::names::NameRule;

use super::attr_id::{AttrId, Resolution, resolve};
use super::case_bind::{
    ambiguous_reference, attribute_reference, is_scratch_relation, unresolved_column,
};

#[must_use]
pub fn engine_field_is_unique(schema: &DFSchema, name: &str) -> bool {
    schema
        .fields()
        .iter()
        .filter(|field| field.name() == name)
        .take(2)
        .count()
        == 1
}

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

fn projection_preserves_ids(projection: &Projection) -> bool {
    let above = projection
        .schema
        .fields()
        .iter()
        .map(|field| AttrId::of(field))
        .collect::<Vec<_>>();
    let below = projection
        .input
        .schema()
        .fields()
        .iter()
        .map(|field| AttrId::of(field))
        .collect::<Vec<_>>();
    above == below || (above.len() > below.len() && above[..below.len()] == below)
}

#[must_use]
pub fn join_dup_below_wrappers(plan: &LogicalPlan) -> bool {
    let mut node = plan;
    loop {
        if matches!(node, LogicalPlan::Join(_)) {
            return true;
        }
        if let LogicalPlan::Projection(projection) = node {
            if !projection_preserves_ids(projection) {
                return false;
            }
            node = projection.input.as_ref();
            continue;
        }
        let below = below_transparent(node);
        if std::ptr::eq(below, node) {
            return false;
        }
        node = below;
    }
}

fn projection_preserves_id_subset(projection: &Projection) -> bool {
    let below = projection
        .input
        .schema()
        .fields()
        .iter()
        .map(|field| AttrId::of(field))
        .collect::<Vec<_>>();
    let above = projection
        .schema
        .fields()
        .iter()
        .map(|field| AttrId::of(field))
        .collect::<Vec<_>>();
    !above.contains(&None) && !below.contains(&None) && above.iter().all(|id| below.contains(id))
}

fn projection_input_ordinal(projection: &Projection, position: usize) -> Option<usize> {
    let column = match projection.expr.get(position)? {
        Expr::Column(column) => column,
        Expr::Alias(Alias { expr, .. }) => match expr.as_ref() {
            Expr::Column(column) => column,
            _ => return None,
        },
        _ => return None,
    };
    projection
        .input
        .schema()
        .iter()
        .position(|(qualifier, field)| {
            field.name() == &column.name && qualifier == column.relation.as_ref()
        })
}

#[must_use]
pub fn union_dup_below_wrappers(plan: &LogicalPlan, positions: &[usize]) -> bool {
    let mut node = plan;
    let mapped = positions.to_vec();
    loop {
        if matches!(node, LogicalPlan::Union(_)) {
            let mut ordered = mapped.clone();
            ordered.sort_unstable();
            ordered.dedup();
            return ordered.len() >= 2 && ordered.len() == mapped.len();
        }
        if let LogicalPlan::Projection(projection) = node {
            if !projection_preserves_id_subset(projection) {
                return false;
            }
            let mut below = Vec::with_capacity(mapped.len());
            for position in &mapped {
                match projection_input_ordinal(projection, *position) {
                    Some(ordinal) => below.push(ordinal),
                    None => return false,
                }
            }
            if below != mapped {
                return false;
            }
            node = projection.input.as_ref();
            continue;
        }
        let below = below_transparent(node);
        if std::ptr::eq(below, node) {
            return false;
        }
        node = below;
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
    let join_dup = !for_sort && join_dup_below_wrappers(plan);
    expr.transform(|node| {
        Ok(match node {
            Expr::Column(column) if column.relation.is_none() => Transformed::yes(
                bind_free_column(column, schema, rule, displays, for_sort, shape, join_dup)?,
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
    join_dup: bool,
) -> Result<Expr> {
    match resolve(schema, &column.name, None, rule, displays)? {
        Resolution::Bound(hits) => {
            let position = hits.first().ok_or_else(|| {
                internal_datafusion_err!("resolve bound no position for {}", column.name)
            })?;
            let field = schema.fields().get(*position).ok_or_else(|| {
                internal_datafusion_err!("resolve bound out of range for {}", column.name)
            })?;
            if !for_sort && !engine_field_is_unique(schema, field.name()) {
                return Ok(Expr::Column(column));
            }
            if !for_sort && hits.len() > 1 && join_dup {
                return ambiguous_for_hits(&column, schema, &hits);
            }
            Ok(attribute_reference(field.name()))
        }
        Resolution::Ambiguous(hits) => {
            if for_sort {
                if shape == SortShape::Project {
                    return oldest_field(schema, &hits);
                }
                return Err(unresolved_column(&column, schema));
            }
            ambiguous_for_hits(&column, schema, &hits)
        }
        Resolution::Missing => Ok(Expr::Column(column)),
    }
}

fn ambiguous_for_hits(column: &Column, schema: &DFSchema, hits: &[usize]) -> Result<Expr> {
    let options = hits
        .iter()
        .filter_map(|position| schema.iter().nth(*position))
        .map(|(qualifier, field)| (qualifier, field.as_ref()))
        .collect::<Vec<_>>();
    Err(ambiguous_reference(column, &options))
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::{EmptyRelation, Filter, LogicalPlan, LogicalPlanBuilder, Union};

    use super::union_dup_below_wrappers;

    fn keyed(names: &[&str], ids: &[&str]) -> Arc<DFSchema> {
        let fields = names
            .iter()
            .zip(ids.iter())
            .map(|(name, id)| {
                Field::new(*name, DataType::Int64, true).with_metadata(HashMap::from([(
                    "repark.attr".to_string(),
                    (*id).to_string(),
                )]))
            })
            .collect::<Vec<_>>();
        Arc::new(DFSchema::try_from(Schema::new(fields)).unwrap())
    }

    fn empty(schema: Arc<DFSchema>) -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema,
        })
    }

    fn union_of(schema: Arc<DFSchema>) -> LogicalPlan {
        LogicalPlan::Union(Union {
            inputs: vec![
                Arc::new(empty(Arc::clone(&schema))),
                Arc::new(empty(Arc::clone(&schema))),
            ],
            schema,
        })
    }

    #[test]
    fn union_dup_below_wrappers_sees_through_filters() {
        let schema = keyed(&["id", "v"], &["a1", "a2"]);
        let union = union_of(schema);
        assert!(union_dup_below_wrappers(&union, &[0, 1]));
        let filtered = Filter::try_new(
            datafusion::logical_expr::col("v").gt(datafusion::logical_expr::lit(1i64)),
            Arc::new(union),
        )
        .map(LogicalPlan::Filter)
        .unwrap();
        assert!(union_dup_below_wrappers(&filtered, &[0, 1]));
    }

    #[test]
    fn union_dup_below_wrappers_misses_without_union() {
        let schema = keyed(&["id", "v"], &["a1", "a2"]);
        assert!(!union_dup_below_wrappers(&empty(schema), &[0, 1]));
    }

    #[test]
    fn union_dup_below_wrappers_stops_at_reorder() {
        let schema = keyed(&["id", "v", "w"], &["a1", "a2", "a2"]);
        let union = union_of(schema);
        let reordered = LogicalPlanBuilder::from(union)
            .project(vec![
                datafusion::logical_expr::col("w"),
                datafusion::logical_expr::col("v"),
            ])
            .unwrap()
            .build()
            .unwrap();
        assert!(!union_dup_below_wrappers(&reordered, &[0, 1]));
    }

    #[test]
    fn union_dup_below_wrappers_stops_at_new_expression() {
        let schema = keyed(&["id", "v"], &["a1", "a2"]);
        let union = union_of(schema);
        let computed = LogicalPlanBuilder::from(union)
            .project(vec![
                datafusion::logical_expr::col("v").gt(datafusion::logical_expr::lit(1i64)),
            ])
            .unwrap()
            .build()
            .unwrap();
        assert!(!union_dup_below_wrappers(&computed, &[0]));
    }

    #[test]
    fn union_dup_below_wrappers_ignores_dup_created_above_union() {
        let schema = keyed(&["id", "v"], &["a1", "a2"]);
        let union = union_of(schema);
        let doubled = LogicalPlanBuilder::from(union)
            .project(vec![
                datafusion::logical_expr::col("v").alias("w1"),
                datafusion::logical_expr::col("v").alias("w2"),
            ])
            .unwrap()
            .build()
            .unwrap();
        assert!(!union_dup_below_wrappers(&doubled, &[0, 1]));
    }

    #[test]
    fn union_dup_below_wrappers_stops_at_aliased_reorder() {
        let schema = keyed(&["id", "v", "w"], &["a1", "a2", "a2"]);
        let union = union_of(schema);
        let reordered = LogicalPlanBuilder::from(union)
            .project(vec![
                datafusion::logical_expr::col("w").alias("w1"),
                datafusion::logical_expr::col("v"),
            ])
            .unwrap()
            .build()
            .unwrap();
        assert!(!union_dup_below_wrappers(&reordered, &[0, 1]));
    }

    #[test]
    fn union_dup_below_wrappers_keeps_dup_present_at_union() {
        let schema = keyed(&["id", "v", "v"], &["a1", "a2", "a2"]);
        let union = union_of(schema);
        assert!(union_dup_below_wrappers(&union, &[1, 2]));
    }

    #[test]
    fn union_dup_below_wrappers_sees_through_identity_projection() {
        let schema = keyed(&["id", "v", "w"], &["a1", "a2", "a2"]);
        let union = union_of(schema);
        let starred = LogicalPlanBuilder::from(union)
            .project(vec![
                datafusion::logical_expr::col("id"),
                datafusion::logical_expr::col("v"),
                datafusion::logical_expr::col("w"),
            ])
            .unwrap()
            .build()
            .unwrap();
        assert!(union_dup_below_wrappers(&starred, &[1, 2]));
    }

    #[test]
    fn union_dup_below_wrappers_sees_through_rename() {
        let schema = keyed(&["id", "v", "w"], &["a1", "a2", "a2"]);
        let union = union_of(schema);
        let renamed = LogicalPlanBuilder::from(union)
            .project(vec![
                datafusion::logical_expr::col("id").alias("ID2"),
                datafusion::logical_expr::col("v"),
                datafusion::logical_expr::col("w"),
            ])
            .unwrap()
            .build()
            .unwrap();
        assert!(union_dup_below_wrappers(&renamed, &[1, 2]));
    }

    #[test]
    fn union_dup_below_wrappers_misses_single_position() {
        let schema = keyed(&["id", "v"], &["a1", "a2"]);
        let union = union_of(schema);
        assert!(!union_dup_below_wrappers(&union, &[1]));
    }
}
