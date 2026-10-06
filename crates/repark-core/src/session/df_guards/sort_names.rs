use std::collections::BTreeMap;

use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{
    Column, DFSchema, DFSchemaRef, DataFusionError, Result, TableReference, internal_datafusion_err,
};
use datafusion::logical_expr::expr::Alias;
use datafusion::logical_expr::{Distinct, Expr, Join, JoinType, LogicalPlan, Projection};
use repark_common::names::NameRule;

use super::attr_id::{AttrId, Resolution, qualifier_matches_position, resolve};
use super::case_bind::{
    ambiguous_reference, attribute_reference, is_scratch_relation, sort_hits_meet_at_join,
    sort_sourced_twin_engine, unresolved_column,
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

pub(crate) fn below_transparent(plan: &LogicalPlan) -> &LogicalPlan {
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
    let join_dup = !for_sort && join_dup_below_wrappers(plan);
    expr.transform(|node| {
        Ok(match node {
            Expr::Column(column) if column.relation.is_none() => Transformed::yes(
                bind_free_column(column, schema, rule, displays, for_sort, plan, join_dup)?,
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
    plan: &LogicalPlan,
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
            if !for_sort {
                return ambiguous_for_hits(&column, schema, &hits);
            }
            let Some(input) = project_input_schema(plan) else {
                return Err(unresolved_column(&column, schema));
            };
            if sort_hits_meet_at_join(plan, &hits) {
                return Err(unresolved_column(&column, schema));
            }
            if let Some(engine) = sort_sourced_twin_engine(plan, &hits, &column.name, rule) {
                return Ok(attribute_reference(&engine));
            }
            match unique_spelling(input, &column.name, rule) {
                Some(spelling) if spelling != column.name => {
                    Ok(Expr::Column(Column::from_name(spelling)))
                }
                _ => Ok(Expr::Column(column)),
            }
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

fn project_input_schema(plan: &LogicalPlan) -> Option<&DFSchema> {
    let mut node = plan;
    loop {
        let below = below_transparent(node);
        if !std::ptr::eq(below, node) {
            node = below;
            continue;
        }
        if let LogicalPlan::Projection(projection) = node {
            return Some(projection.input.schema());
        }
        return None;
    }
}

fn unique_spelling(schema: &DFSchema, written: &str, rule: NameRule) -> Option<String> {
    let mut matches = schema
        .fields()
        .iter()
        .filter(|field| rule.matches(written, field.name()))
        .map(|field| field.name().clone());
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

#[must_use]
pub fn project_input_spelling(plan: &LogicalPlan, written: &str, rule: NameRule) -> Option<String> {
    unique_spelling(project_input_schema(plan)?, written, rule)
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeNameOffense {
    pub qualifier: Vec<String>,
    pub written: String,
    pub hits: Vec<usize>,
}

#[allow(clippy::missing_errors_doc)]
pub fn free_expr_names(expr: &Expr, qualified_only: bool) -> Result<Vec<(Vec<String>, String)>> {
    let mut names = Vec::new();
    expr.apply(|node| {
        Ok(match node {
            Expr::Exists(_) | Expr::InSubquery(_) | Expr::ScalarSubquery(_) => {
                TreeNodeRecursion::Stop
            }
            Expr::Column(column) => {
                let qualifier = column.relation.as_ref().map_or_else(Vec::new, |relation| {
                    [
                        relation.catalog(),
                        relation.schema(),
                        Some(relation.table()),
                    ]
                    .into_iter()
                    .flatten()
                    .map(str::to_string)
                    .collect::<Vec<_>>()
                });
                if qualifier.is_empty() != qualified_only {
                    names.push((qualifier, column.name.clone()));
                }
                TreeNodeRecursion::Continue
            }
            _ => TreeNodeRecursion::Continue,
        })
    })?;
    Ok(names)
}

#[allow(clippy::missing_errors_doc)]
pub fn refuse_free_names(
    schema: &DFSchema,
    rule: NameRule,
    displays: &[String],
    frame_qualifiers: Option<&BTreeMap<String, Vec<String>>>,
    names: &[(Vec<String>, String)],
) -> Result<Option<FreeNameOffense>> {
    if displays.len() != schema.fields().len() {
        return Ok(None);
    }
    for (qualifier, written) in names {
        let dotted = qualifier.join(".");
        let Resolution::Ambiguous(hits) = resolve(
            schema,
            written,
            (!qualifier.is_empty()).then_some(dotted.as_str()),
            rule,
            displays,
            frame_qualifiers,
        )?
        else {
            continue;
        };
        if duplicate_engine_hit(schema, rule, &hits) {
            continue;
        }
        return Ok(Some(FreeNameOffense {
            qualifier: qualifier.clone(),
            written: written.clone(),
            hits,
        }));
    }
    Ok(None)
}

fn duplicate_engine_hit(schema: &DFSchema, rule: NameRule, hits: &[usize]) -> bool {
    let engines = hits
        .iter()
        .filter_map(|position| schema.fields().get(*position))
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    engines.iter().enumerate().any(|(index, left)| {
        engines[index + 1..]
            .iter()
            .any(|right| rule.matches(left, right))
    })
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

    #[test]
    fn free_expr_names_splits_qualified_and_plain_leaves() {
        use datafusion::common::Column;
        use datafusion::logical_expr::Expr;

        let expr =
            Expr::Column(Column::new_unqualified("v")) + Expr::Column(Column::new(Some("q"), "w"));
        assert_eq!(
            super::free_expr_names(&expr, false).unwrap(),
            vec![(Vec::new(), "v".to_string())]
        );
        assert_eq!(
            super::free_expr_names(&expr, true).unwrap(),
            vec![(vec!["q".to_string()], "w".to_string())]
        );
    }

    #[test]
    fn free_expr_names_stops_at_subquery_scope() {
        use datafusion::common::Column;
        use datafusion::logical_expr::expr::Exists;
        use datafusion::logical_expr::{Expr, Subquery};

        let inner = Expr::Column(Column::new_unqualified("inner_v"));
        let plan = empty(keyed(&["inner_v"], &["a9"]));
        let exists = Expr::Exists(Exists {
            subquery: Subquery {
                subquery: std::sync::Arc::new(plan),
                outer_ref_columns: vec![inner],
                spans: datafusion::common::Spans::new(),
            },
            negated: false,
        });
        let expr = Expr::Column(Column::new_unqualified("v")).and(exists);
        assert_eq!(
            super::free_expr_names(&expr, false).unwrap(),
            vec![(Vec::new(), "v".to_string())]
        );
    }

    #[test]
    fn refuse_free_names_reports_reminted_duplicate_display() {
        use repark_common::names::NameRule;

        let schema = keyed(&["id", "l_v", "r_v"], &["a1", "a2", "a3"]);
        let displays = ["id", "v", "v"].map(str::to_string);
        let offense = super::refuse_free_names(
            &schema,
            NameRule::IgnoreCase,
            &displays,
            None,
            &[(Vec::new(), "v".to_string())],
        )
        .unwrap()
        .unwrap();
        assert_eq!(offense.written, "v");
        assert!(offense.qualifier.is_empty());
        assert_eq!(offense.hits, vec![1, 2]);
    }

    #[test]
    fn refuse_free_names_defers_to_duplicate_engine_fields() {
        use repark_common::names::NameRule;

        let schema = keyed(&["id", "v", "v"], &["a1", "a2", "a3"]);
        let displays = ["id", "v", "v"].map(str::to_string);
        let offense = super::refuse_free_names(
            &schema,
            NameRule::IgnoreCase,
            &displays,
            None,
            &[(Vec::new(), "v".to_string())],
        )
        .unwrap();
        assert!(offense.is_none());
    }

    #[test]
    fn refuse_free_names_skips_unique_and_missing_names() {
        use repark_common::names::NameRule;

        let schema = keyed(&["id", "l_v", "r_v"], &["a1", "a2", "a3"]);
        let displays = ["id", "v", "v"].map(str::to_string);
        for written in ["id", "zzz"] {
            let offense = super::refuse_free_names(
                &schema,
                NameRule::IgnoreCase,
                &displays,
                None,
                &[(Vec::new(), written.to_string())],
            )
            .unwrap();
            assert!(offense.is_none());
        }
    }

    #[test]
    fn refuse_free_names_matches_facade_qualifiers() {
        use std::collections::BTreeMap;

        use repark_common::names::NameRule;

        let schema = keyed(&["id", "l_v", "r_v"], &["a1", "a2", "a3"]);
        let displays = ["id", "v", "v"].map(str::to_string);
        let quals = BTreeMap::from([
            ("a2".to_string(), vec!["q".to_string()]),
            ("a3".to_string(), vec!["q".to_string()]),
        ]);
        let offense = super::refuse_free_names(
            &schema,
            NameRule::IgnoreCase,
            &displays,
            Some(&quals),
            &[(vec!["q".to_string()], "v".to_string())],
        )
        .unwrap()
        .unwrap();
        assert_eq!(offense.qualifier, vec!["q".to_string()]);
        assert_eq!(offense.hits, vec![1, 2]);
    }
}
