use std::collections::BTreeMap;

use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{
    Column, DFSchema, DFSchemaRef, DataFusionError, Result, TableReference, internal_datafusion_err,
};
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{Distinct, Expr, Join, JoinType, LogicalPlan, Projection};
use repark_common::names::NameRule;

use super::attr_id::{AttrId, Resolution, qualifier_matches_position, resolve};
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
    resolve(schema, written, None, rule, &displays, None).map(Some)
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
    match resolve(schema, &column.name, None, rule, displays, None)? {
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

#[allow(clippy::missing_errors_doc)]
pub fn bind_qualified_free_refs(
    expr: Expr,
    plan: &LogicalPlan,
    rule: NameRule,
    displays: &[String],
    frame_qualifiers: Option<&BTreeMap<String, Vec<String>>>,
    for_sort: bool,
) -> Result<Expr> {
    let schema = plan.schema();
    if displays.len() != schema.fields().len() {
        return Ok(expr);
    }
    let join_dup = !for_sort && join_dup_below_wrappers(plan);
    expr.transform(|node| {
        Ok(match node {
            Expr::Column(column) if column.relation.is_some() => {
                Transformed::yes(bind_qualified_free_column(
                    column,
                    schema,
                    rule,
                    displays,
                    frame_qualifiers,
                    for_sort,
                    join_dup,
                )?)
            }
            _ => Transformed::no(node),
        })
    })
    .map(|transformed| transformed.data)
}

#[allow(clippy::missing_errors_doc)]
fn bind_qualified_free_column(
    column: Column,
    schema: &DFSchema,
    rule: NameRule,
    displays: &[String],
    frame_qualifiers: Option<&BTreeMap<String, Vec<String>>>,
    for_sort: bool,
    join_dup: bool,
) -> Result<Expr> {
    let qualifier = column.relation.as_ref().map(ToString::to_string);
    match resolve(
        schema,
        &column.name,
        qualifier.as_deref(),
        rule,
        displays,
        frame_qualifiers,
    )? {
        Resolution::Bound(hits) => {
            if hits.len() > 1 && join_dup {
                return Err(qualified_refusal(&column, schema, &hits, for_sort));
            }
            let position = hits.first().ok_or_else(|| {
                internal_datafusion_err!("resolve bound no position for {}", column.name)
            })?;
            let field = schema.fields().get(*position).ok_or_else(|| {
                internal_datafusion_err!("resolve bound out of range for {}", column.name)
            })?;
            let held = schema.iter().nth(*position).and_then(|(held, _)| held);
            Ok(Expr::Column(Column {
                relation: held.cloned(),
                name: field.name().clone(),
                ..column
            }))
        }
        Resolution::Ambiguous(hits) => Err(qualified_refusal(&column, schema, &hits, for_sort)),
        Resolution::Missing => Ok(Expr::Column(column)),
    }
}

fn qualified_refusal(
    column: &Column,
    schema: &DFSchema,
    hits: &[usize],
    for_sort: bool,
) -> DataFusionError {
    if for_sort {
        return unresolved_column(column, schema);
    }
    let options = hits
        .iter()
        .filter_map(|position| schema.iter().nth(*position))
        .map(|(_, field)| (column.relation.as_ref(), field.as_ref()))
        .collect::<Vec<_>>();
    ambiguous_reference(column, &options)
}

#[allow(clippy::missing_errors_doc, clippy::type_complexity)]
pub fn grandchild_qualified_key(
    plan: &LogicalPlan,
    written: &str,
    qualifier: &str,
    rule: NameRule,
    frame_qualifiers: Option<&BTreeMap<String, Vec<String>>>,
) -> Result<Option<(Resolution, Option<(Vec<String>, String)>)>> {
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
    let resolution = resolve(
        schema,
        written,
        Some(qualifier),
        rule,
        &displays,
        frame_qualifiers,
    )?;
    let Resolution::Bound(hits) = &resolution else {
        return Ok(Some((resolution, None)));
    };
    let render = hits.first().and_then(|position| {
        let (held, field) = schema.iter().nth(*position)?;
        let parts = held
            .map(|held| {
                [held.catalog(), held.schema(), Some(held.table())]
                    .into_iter()
                    .flatten()
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Some((parts, field.name().clone()))
    });
    Ok(Some((resolution, render)))
}

#[must_use]
pub fn join_output_sources(plan: &LogicalPlan) -> Vec<Vec<(bool, usize)>> {
    let width = plan.schema().fields().len();
    let mut mapped: Vec<Option<usize>> = (0..width).map(Some).collect();
    let mut node = plan;
    let joined = loop {
        if let LogicalPlan::Projection(projection) = node {
            mapped = mapped
                .into_iter()
                .map(|position| {
                    position.and_then(|index| projection_input_ordinal(projection, index))
                })
                .collect();
            node = projection.input.as_ref();
            continue;
        }
        if let LogicalPlan::Join(joined) = node {
            break joined;
        }
        let below = below_transparent(node);
        if std::ptr::eq(below, node) {
            return vec![Vec::new(); width];
        }
        node = below;
    };
    let left_len = joined.left.schema().fields().len();
    let pairs = if join_pairs_key_sides(joined, width) {
        join_key_pairs(joined)
    } else {
        Vec::new()
    };
    mapped
        .into_iter()
        .map(|position| match position {
            None => Vec::new(),
            Some(ordinal) if ordinal < left_len => {
                let mut sources = vec![(false, ordinal)];
                for (left_key, right_key) in &pairs {
                    if *left_key == ordinal {
                        sources.push((true, *right_key));
                    }
                }
                sources
            }
            Some(ordinal) => vec![(true, ordinal - left_len)],
        })
        .collect()
}

fn join_pairs_key_sides(joined: &Join, width: usize) -> bool {
    if matches!(joined.join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
        return false;
    }
    let joined_width = joined.left.schema().fields().len() + joined.right.schema().fields().len();
    width < joined_width
}

fn join_key_pairs(joined: &Join) -> Vec<(usize, usize)> {
    let left_schema = joined.left.schema();
    let right_schema = joined.right.schema();
    joined
        .on
        .iter()
        .filter_map(|(left_key, right_key)| {
            let (Expr::Column(left_key), Expr::Column(right_key)) = (left_key, right_key) else {
                return None;
            };
            let left = key_ordinal(left_schema, left_key)?;
            let right = key_ordinal(right_schema, right_key)?;
            Some((left, right))
        })
        .collect()
}

fn key_ordinal(schema: &DFSchemaRef, key: &Column) -> Option<usize> {
    schema.iter().position(|(qualifier, field)| {
        field.name() == &key.name && qualifier == key.relation.as_ref()
    })
}

#[must_use]
pub fn qualifier_star_positions(
    plan: &LogicalPlan,
    head: &str,
    rule: NameRule,
    displays: &[String],
    frame_qualifiers: Option<&BTreeMap<String, Vec<String>>>,
) -> Option<Vec<(usize, Vec<String>)>> {
    let schema = plan.schema();
    if displays.len() != schema.fields().len() {
        return None;
    }
    let written = TableReference::parse_str_normalized(head, true);
    let mut positions = Vec::new();
    for (position, ((held, field), _)) in schema.iter().zip(displays).enumerate() {
        if qualifier_matches_position(&written, field, held, rule, frame_qualifiers) {
            let parts = held
                .map(|held| {
                    [held.catalog(), held.schema(), Some(held.table())]
                        .into_iter()
                        .flatten()
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            positions.push((position, parts));
        }
    }
    if positions.is_empty() {
        None
    } else {
        Some(positions)
    }
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
