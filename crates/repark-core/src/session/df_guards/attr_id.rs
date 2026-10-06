use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{BuildHasher, RandomState};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{
    Column, DFSchema, DFSchemaRef, Result, ScalarValue, TableReference, internal_datafusion_err,
    internal_err,
};
use datafusion::logical_expr::expr::{Alias, Exists, InSubquery, intersect_metadata_for_union};
use datafusion::logical_expr::utils::grouping_set_to_exprlist;
use datafusion::logical_expr::{
    Aggregate, Distinct, DistinctOn, Expr, Filter, Join, Limit, LogicalPlan, LogicalPlanBuilder,
    Projection, RecursiveQuery, Sort, SortExpr, Subquery, SubqueryAlias, Union, Unnest, Window,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use repark_common::names::NameRule;

pub(crate) const ATTR_KEY: &str = "repark.attr";

static NEXT_ATTR: AtomicU64 = AtomicU64::new(1);

static PROCESS_PREFIX: LazyLock<String> =
    LazyLock::new(|| format!("a{:016x}", RandomState::new().hash_one(std::process::id())));

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AttrId(String);

impl AttrId {
    #[must_use]
    pub fn mint() -> Self {
        Self(format!(
            "{}{:012x}",
            PROCESS_PREFIX.as_str(),
            NEXT_ATTR.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[must_use]
    pub fn is_native(&self) -> bool {
        self.0.len() == PROCESS_PREFIX.len() + 12 && self.0.starts_with(PROCESS_PREFIX.as_str())
    }

    pub(crate) fn native(field: &Field) -> Option<Self> {
        Self::of(field).filter(Self::is_native)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub fn from_token(raw: &str) -> Self {
        Self(raw.to_string())
    }

    #[must_use]
    pub fn of(field: &Field) -> Option<Self> {
        field.metadata().get(ATTR_KEY).cloned().map(Self)
    }

    fn metadata(&self) -> FieldMetadata {
        FieldMetadata::from(BTreeMap::from([(ATTR_KEY.to_string(), self.0.clone())]))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    Bound(Vec<usize>),
    Ambiguous(Vec<usize>),
    Missing,
}

#[must_use]
pub fn attribute_ids(schema: &DFSchema) -> Vec<Option<AttrId>> {
    schema
        .fields()
        .iter()
        .map(|field| AttrId::of(field))
        .collect()
}

#[must_use]
pub fn plan_is_relation(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::EmptyRelation(empty) => !empty.schema.fields().is_empty(),
        LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::Repartition(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::TableScan(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Unnest(_)
        | LogicalPlan::RecursiveQuery(_) => true,
        _ => false,
    }
}

#[must_use]
pub fn plan_is_stamped(plan: &LogicalPlan) -> bool {
    stamped_state(plan).unwrap_or(false)
}

#[must_use]
pub fn alias_with_fresh_id(expr: Expr, name: &str) -> Expr {
    Expr::Alias(Alias {
        expr: Box::new(expr),
        relation: None,
        name: name.to_string(),
        metadata: Some(AttrId::mint().metadata()),
    })
}

fn stamped_state(plan: &LogicalPlan) -> Result<bool> {
    if !plan_is_relation(plan) {
        return Ok(true);
    }
    if let LogicalPlan::Projection(projection) = plan {
        let computed = computed_outputs(&projection.input)?;
        let below = projection.input.schema();
        return Ok(projection.expr.iter().zip(projection.schema.iter()).all(
            |(expr, (_qualifier, field))| {
                let inherits = match strip_aliases(expr) {
                    Expr::Column(column) => below
                        .maybe_index_of_column(column)
                        .is_some_and(|index| !computed.get(index).copied().unwrap_or(true)),
                    _ => false,
                };
                own_id(expr).is_some() || (inherits && AttrId::native(field).is_some())
            },
        ));
    }
    if let LogicalPlan::Union(union) = plan {
        let first = union
            .inputs
            .first()
            .map(|input| first_input_ids(input))
            .unwrap_or_default();
        return Ok(union
            .schema
            .fields()
            .iter()
            .enumerate()
            .all(|(position, field)| {
                matches!(first.get(position).cloned().flatten(), Some(id) if AttrId::of(field).as_ref() == Some(&id))
            }));
    }
    let computed = computed_outputs(plan)?;
    Ok(plan
        .schema()
        .fields()
        .iter()
        .zip(computed)
        .all(|(field, computed)| !computed && AttrId::native(field).is_some()))
}

#[allow(clippy::missing_errors_doc)]
pub fn stamp(plan: LogicalPlan) -> Result<LogicalPlan> {
    if !plan_is_relation(&plan) {
        return Ok(plan);
    }
    if let LogicalPlan::Projection(projection) = &plan {
        return stamp_projection(projection).map(|stamped| stamped.unwrap_or(plan));
    }
    let minted = match &plan {
        LogicalPlan::Union(union) => union_ids(union),
        _ => plan
            .schema()
            .fields()
            .iter()
            .zip(computed_outputs(&plan)?)
            .map(|(field, computed)| {
                (computed || AttrId::native(field).is_none()).then(AttrId::mint)
            })
            .collect::<Vec<_>>(),
    };
    if minted.iter().all(Option::is_none) {
        return Ok(plan);
    }
    project_ids(plan, &minted)
}

fn union_ids(union: &Union) -> Vec<Option<AttrId>> {
    let first = union
        .inputs
        .first()
        .map(|input| first_input_ids(input))
        .unwrap_or_default();
    union
        .schema
        .fields()
        .iter()
        .enumerate()
        .map(
            |(position, field)| match first.get(position).cloned().flatten() {
                Some(id) if AttrId::of(field).as_ref() == Some(&id) => None,
                Some(id) => Some(id),
                None => Some(AttrId::mint()),
            },
        )
        .collect()
}

fn first_input_ids(input: &LogicalPlan) -> Vec<Option<AttrId>> {
    let LogicalPlan::Projection(projection) = input else {
        return attribute_ids(input.schema());
    };
    let below = projection.input.schema();
    projection
        .expr
        .iter()
        .map(|expr| {
            chain_id(expr).or_else(|| match strip_aliases(expr) {
                Expr::Column(column) => below
                    .maybe_index_of_column(column)
                    .and_then(|index| AttrId::native(below.field(index))),
                _ => None,
            })
        })
        .collect()
}

fn chain_id(expr: &Expr) -> Option<AttrId> {
    let mut node = expr;
    while let Expr::Alias(alias) = node {
        if let Some(id) = alias
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.inner().get(ATTR_KEY).cloned())
            .map(AttrId)
            .filter(AttrId::is_native)
        {
            return Some(id);
        }
        node = &alias.expr;
    }
    None
}

fn stamp_projection(projection: &Projection) -> Result<Option<LogicalPlan>> {
    let computed = computed_outputs(&projection.input)?;
    let below = projection.input.schema();
    let mut changed = false;
    let expr = projection
        .expr
        .iter()
        .zip(projection.schema.iter())
        .map(|(expr, (qualifier, field))| {
            let inherits = match strip_aliases(expr) {
                Expr::Column(column) => below
                    .maybe_index_of_column(column)
                    .is_some_and(|index| !computed.get(index).copied().unwrap_or(true)),
                _ => false,
            };
            if own_id(expr).is_some() || (inherits && AttrId::native(field).is_some()) {
                return expr.clone();
            }
            changed = true;
            with_id(expr.clone(), qualifier, field.name(), &AttrId::mint())
        })
        .collect::<Vec<_>>();
    if !changed {
        return Ok(None);
    }
    Projection::try_new(expr, Arc::clone(&projection.input))
        .map(|stamped| Some(LogicalPlan::Projection(stamped)))
}

fn own_id(expr: &Expr) -> Option<AttrId> {
    let Expr::Alias(alias) = expr else {
        return None;
    };
    alias
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.inner().get(ATTR_KEY).cloned())
        .map(AttrId)
        .filter(AttrId::is_native)
}

fn computed_outputs(plan: &LogicalPlan) -> Result<Vec<bool>> {
    let mut node = plan;
    while let LogicalPlan::Filter(Filter { input, .. })
    | LogicalPlan::Sort(Sort { input, .. })
    | LogicalPlan::Limit(Limit { input, .. }) = node
    {
        node = input.as_ref();
    }
    let mut computed = match node {
        LogicalPlan::Window(window) => computed_outputs(&window.input)?,
        LogicalPlan::Aggregate(aggregate) => computed_group_keys(aggregate)?,
        other => return Ok(vec![false; other.schema().fields().len()]),
    };
    computed.resize(node.schema().fields().len(), true);
    Ok(computed)
}

fn computed_group_keys(aggregate: &Aggregate) -> Result<Vec<bool>> {
    let below = computed_outputs(&aggregate.input)?;
    let input = aggregate.input.schema();
    Ok(grouping_set_to_exprlist(&aggregate.group_expr)?
        .into_iter()
        .map(|expr| match strip_aliases(expr) {
            Expr::Column(column) => input
                .maybe_index_of_column(column)
                .is_none_or(|index| below.get(index).copied().unwrap_or(true)),
            _ => true,
        })
        .collect())
}

fn strip_aliases(expr: &Expr) -> &Expr {
    let mut inner = expr;
    while let Expr::Alias(alias) = inner {
        inner = &alias.expr;
    }
    inner
}

fn with_id(expr: Expr, qualifier: Option<&TableReference>, name: &str, id: &AttrId) -> Expr {
    match expr {
        Expr::Alias(alias) => {
            let mut metadata = alias.metadata.clone().unwrap_or_default();
            metadata.extend(id.metadata());
            Expr::Alias(alias.with_metadata(Some(metadata)))
        }
        other => other.alias_qualified_with_metadata(qualifier.cloned(), name, Some(id.metadata())),
    }
}

fn project_ids(plan: LogicalPlan, ids: &[Option<AttrId>]) -> Result<LogicalPlan> {
    let expr = plan
        .schema()
        .iter()
        .zip(ids)
        .map(|((qualifier, field), id)| {
            let column = Expr::Column(Column::new(qualifier.cloned(), field.name()));
            match id {
                Some(id) => with_id(column, qualifier, field.name(), id),
                None => column,
            }
        })
        .collect::<Vec<_>>();
    Projection::try_new(expr, Arc::new(plan)).map(LogicalPlan::Projection)
}

#[derive(Debug, Default)]
pub struct StripAttributeIds;

impl OptimizerRule for StripAttributeIds {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "repark_strip_attribute_ids"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_up_with_subqueries(drop_node_ids)
    }
}

#[allow(clippy::missing_errors_doc)]
pub fn strip_for_execution(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(drop_node_ids).data()
}

fn drop_node_ids(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut keyed = schema_carries_id(plan.schema());
    if !keyed {
        plan.apply_expressions(|expr| {
            keyed = expr.exists(|node| Ok(expr_node_carries_id(node)))?;
            Ok(if keyed {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
    }
    if !keyed {
        return Ok(Transformed::no(plan));
    }
    let plan = plan
        .map_expressions(|expr| expr.transform_up(|node| Ok(drop_expr_id(node))))?
        .data;
    let plan = match plan {
        LogicalPlan::Union(union) => {
            let schema = union_schema_from_inputs(&union)?;
            LogicalPlan::Union(Union {
                inputs: union.inputs,
                schema,
            })
        }
        other => other.recompute_schema()?,
    };
    Ok(Transformed::yes(plan))
}

fn schema_carries_id(schema: &DFSchema) -> bool {
    schema
        .fields()
        .iter()
        .any(|field| field.metadata().contains_key(ATTR_KEY))
}

fn expr_node_carries_id(expr: &Expr) -> bool {
    match expr {
        Expr::Alias(alias) => alias
            .metadata
            .as_ref()
            .is_some_and(|metadata| metadata.inner().contains_key(ATTR_KEY)),
        Expr::OuterReferenceColumn(field, _) => field.metadata().contains_key(ATTR_KEY),
        _ => false,
    }
}

fn drop_expr_id(expr: Expr) -> Transformed<Expr> {
    if !expr_node_carries_id(&expr) {
        return Transformed::no(expr);
    }
    Transformed::yes(match expr {
        Expr::Alias(alias) => {
            let mut inner = alias
                .metadata
                .as_ref()
                .map(|metadata| metadata.inner().clone())
                .unwrap_or_default();
            inner.remove(ATTR_KEY);
            let metadata = (!inner.is_empty()).then(|| FieldMetadata::new(inner));
            match *alias.expr {
                Expr::Column(column)
                    if metadata.is_none()
                        && column.name == alias.name
                        && column.relation == alias.relation =>
                {
                    Expr::Column(column)
                }
                expr => Expr::Alias(Alias {
                    expr: Box::new(expr),
                    metadata,
                    ..alias
                }),
            }
        }
        Expr::OuterReferenceColumn(field, column) => {
            let mut clean = field.as_ref().clone();
            clean.metadata_mut().remove(ATTR_KEY);
            Expr::OuterReferenceColumn(Arc::new(clean), column)
        }
        other => other,
    })
}

fn union_schema_from_inputs(union: &Union) -> Result<DFSchemaRef> {
    let fields = union
        .schema
        .iter()
        .enumerate()
        .map(|(position, (qualifier, field))| {
            let metadata = intersect_metadata_for_union(
                union
                    .inputs
                    .iter()
                    .filter_map(|input| input.schema().fields().get(position))
                    .map(|input_field| input_field.metadata()),
            );
            let field = field.as_ref().clone().with_metadata(metadata);
            (qualifier.cloned(), Arc::new(field))
        })
        .collect::<Vec<_>>();
    DFSchema::new_with_metadata(fields, union.schema.metadata().clone())?
        .with_functional_dependencies(union.schema.functional_dependencies().clone())
        .map(Arc::new)
}

type JoinSides<'a> = (&'a [Arc<Field>], &'a [Arc<Field>]);

fn join_sides(plan: &LogicalPlan, left_width: usize) -> Result<JoinSides<'_>> {
    let fields = plan.schema().fields();
    if left_width > fields.len() {
        return internal_err!(
            "join re-mint: left width {left_width} exceeds the {} joined fields",
            fields.len()
        );
    }
    Ok(fields.split_at(left_width))
}

#[allow(clippy::missing_errors_doc)]
pub fn join_collisions(plan: &LogicalPlan, left_width: usize) -> Result<HashSet<AttrId>> {
    let (left, right) = join_sides(plan, left_width)?;
    let held = left
        .iter()
        .filter_map(|field| AttrId::of(field))
        .collect::<HashSet<_>>();
    Ok(right
        .iter()
        .filter_map(|field| AttrId::of(field))
        .filter(|id| held.contains(id))
        .collect())
}

#[allow(clippy::missing_errors_doc)]
pub fn remint_shared<S: BuildHasher>(
    plan: LogicalPlan,
    left_width: usize,
    shared: &HashSet<AttrId, S>,
) -> Result<(LogicalPlan, HashMap<AttrId, AttrId>)> {
    let (_, right) = join_sides(&plan, left_width)?;
    let mut fresh = HashMap::new();
    for field in right {
        if let Some(id) = AttrId::of(field).filter(|id| shared.contains(id)) {
            fresh.entry(id).or_insert_with(AttrId::mint);
        }
    }
    remint_with_map(plan, left_width, &fresh).map(|plan| (plan, fresh))
}

#[allow(clippy::missing_errors_doc)]
pub fn remint_with_map<S: BuildHasher>(
    plan: LogicalPlan,
    left_width: usize,
    remint: &HashMap<AttrId, AttrId, S>,
) -> Result<LogicalPlan> {
    let (left, right) = join_sides(&plan, left_width)?;
    let reminted = left
        .iter()
        .map(|_| None)
        .chain(
            right
                .iter()
                .map(|field| AttrId::of(field).and_then(|id| remint.get(&id).cloned())),
        )
        .collect::<Vec<_>>();
    if reminted.iter().all(Option::is_none) {
        return Ok(plan);
    }
    project_ids(plan, &reminted)
}

#[allow(clippy::missing_errors_doc)]
pub fn copy_attribute_ids(plan: LogicalPlan, source: &LogicalPlan) -> Result<LogicalPlan> {
    let held = attribute_ids(plan.schema());
    let wanted = attribute_ids(source.schema());
    if held.len() != wanted.len() {
        return internal_err!(
            "attribute id carry: source width {} differs from the {} carried fields",
            wanted.len(),
            held.len()
        );
    }
    let carried = wanted
        .iter()
        .zip(held.iter())
        .map(|(source_id, held_id)| source_id.clone().or(held_id.clone()))
        .collect::<Vec<_>>();
    if carried == held {
        return Ok(plan);
    }
    project_ids(plan, &carried)
}

#[allow(clippy::missing_errors_doc)]
pub fn strip(plan: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Join(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Unnest(_)
        | LogicalPlan::RecursiveQuery(_)
        | LogicalPlan::Distinct(_) => strip_stored(plan),
        LogicalPlan::Filter(filter) => {
            let predicate = strip_expr(filter.predicate)?;
            let input = strip_child(filter.input)?;
            Filter::try_new(predicate, input).map(LogicalPlan::Filter)
        }
        LogicalPlan::Sort(sort) => {
            let expr = sort
                .expr
                .into_iter()
                .map(|item| {
                    strip_expr(item.expr).map(|expr| SortExpr {
                        expr,
                        asc: item.asc,
                        nulls_first: item.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let input = strip_child(sort.input)?;
            LogicalPlanBuilder::from(input).sort(expr)?.build()
        }
        LogicalPlan::Repartition(repartition) => {
            let input = strip_child(repartition.input)?;
            LogicalPlanBuilder::from(input)
                .repartition(repartition.partitioning_scheme)?
                .build()
        }
        LogicalPlan::Subquery(subquery) => {
            let input = strip_child(Arc::clone(&subquery.subquery))?;
            Ok(LogicalPlan::Subquery(subquery.with_plan(input)))
        }
        LogicalPlan::Limit(limit) => {
            let skip = limit_usize(limit.skip.as_deref(), "skip")?.unwrap_or(0);
            let fetch = limit_usize(limit.fetch.as_deref(), "fetch")?;
            let input = strip_child(limit.input)?;
            LogicalPlanBuilder::from(input).limit(skip, fetch)?.build()
        }
        LogicalPlan::Values(values) => {
            loud_if_keyed(values.schema.as_ref(), "Values")?;
            Ok(LogicalPlan::Values(values))
        }
        LogicalPlan::EmptyRelation(empty) => {
            loud_if_keyed(empty.schema.as_ref(), "EmptyRelation")?;
            Ok(LogicalPlan::EmptyRelation(empty))
        }
        LogicalPlan::TableScan(scan) => {
            loud_if_keyed(scan.projected_schema.as_ref(), "TableScan")?;
            Ok(LogicalPlan::TableScan(scan))
        }
        LogicalPlan::Extension(_)
        | LogicalPlan::Statement(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::Analyze(_)
        | LogicalPlan::Dml(_)
        | LogicalPlan::Ddl(_)
        | LogicalPlan::Copy(_)
        | LogicalPlan::DescribeTable(_) => Ok(plan),
    }
}

fn loud_if_keyed(schema: &DFSchema, what: &str) -> Result<()> {
    if schema
        .fields()
        .iter()
        .any(|field| AttrId::of(field).is_some())
    {
        return Err(internal_datafusion_err!(
            "strip reached a keyed {what}; providers are born clean"
        ));
    }
    Ok(())
}

fn limit_usize(expr: Option<&Expr>, what: &str) -> Result<Option<usize>> {
    expr.map(|expr| match expr {
        Expr::Literal(ScalarValue::Int64(Some(n)), _) => usize::try_from(*n)
            .map_err(|_| internal_datafusion_err!("strip cannot rebuild a negative {what} limit")),
        _ => Err(internal_datafusion_err!(
            "strip cannot rebuild a non-literal {what} limit"
        )),
    })
    .transpose()
}

fn strip_child(input: Arc<LogicalPlan>) -> Result<Arc<LogicalPlan>> {
    strip(Arc::unwrap_or_clone(input)).map(Arc::new)
}

fn strip_exprs(exprs: Vec<Expr>) -> Result<Vec<Expr>> {
    exprs.into_iter().map(strip_expr).collect()
}

#[allow(clippy::missing_errors_doc)]
#[allow(clippy::too_many_lines)]
fn strip_stored(plan: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Projection(projection) => {
            let expr = strip_exprs(projection.expr)?;
            let input = strip_child(projection.input)?;
            Projection::try_new(expr, input).map(LogicalPlan::Projection)
        }
        LogicalPlan::Window(window) => {
            let expr = strip_exprs(window.window_expr)?;
            let input = strip_child(window.input)?;
            let schema = cleaned_df_schema(window.schema.as_ref())?;
            Window::try_new_with_schema(expr, input, schema).map(LogicalPlan::Window)
        }
        LogicalPlan::Aggregate(aggregate) => {
            let group_expr = strip_exprs(aggregate.group_expr)?;
            let aggr_expr = strip_exprs(aggregate.aggr_expr)?;
            let input = strip_child(aggregate.input)?;
            let schema = cleaned_df_schema(aggregate.schema.as_ref())?;
            Aggregate::try_new_with_schema(input, group_expr, aggr_expr, schema)
                .map(LogicalPlan::Aggregate)
        }
        LogicalPlan::Join(join) => {
            let on = join
                .on
                .into_iter()
                .map(|(left, right)| Ok((strip_expr(left)?, strip_expr(right)?)))
                .collect::<Result<Vec<_>>>()?;
            let filter = join.filter.map(strip_expr).transpose()?;
            let left = strip_child(join.left)?;
            let right = strip_child(join.right)?;
            Join::try_new(
                left,
                right,
                on,
                filter,
                join.join_type,
                join.join_constraint,
                join.null_equality,
                join.null_aware,
            )
            .map(LogicalPlan::Join)
        }
        LogicalPlan::Union(union) => strip_union(union.inputs),
        LogicalPlan::SubqueryAlias(alias) => {
            let input = strip_child(alias.input)?;
            SubqueryAlias::try_new(input, alias.alias).map(LogicalPlan::SubqueryAlias)
        }
        LogicalPlan::Unnest(unnest) => {
            let input = strip_child(unnest.input)?;
            Unnest::try_new(input, unnest.exec_columns, unnest.options).map(LogicalPlan::Unnest)
        }
        LogicalPlan::RecursiveQuery(query) => {
            let static_term = strip_child(query.static_term)?;
            let recursive_term = strip_child(query.recursive_term)?;
            RecursiveQuery::try_new(query.name, static_term, recursive_term, query.is_distinct)
                .map(LogicalPlan::RecursiveQuery)
        }
        LogicalPlan::Distinct(Distinct::All(input)) => {
            Ok(LogicalPlan::Distinct(Distinct::All(strip_child(input)?)))
        }
        LogicalPlan::Distinct(Distinct::On(distinct)) => {
            let on_expr = strip_exprs(distinct.on_expr)?;
            let select_expr = strip_exprs(distinct.select_expr)?;
            let sort_expr = distinct
                .sort_expr
                .map(|exprs| {
                    exprs
                        .into_iter()
                        .map(|item| {
                            strip_expr(item.expr).map(|expr| SortExpr {
                                expr,
                                asc: item.asc,
                                nulls_first: item.nulls_first,
                            })
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .transpose()?;
            let input = strip_child(distinct.input)?;
            DistinctOn::try_new(on_expr, select_expr, sort_expr, input)
                .map(Distinct::On)
                .map(LogicalPlan::Distinct)
        }
        other => Ok(other),
    }
}

fn strip_union(inputs: Vec<Arc<LogicalPlan>>) -> Result<LogicalPlan> {
    let inputs = inputs
        .into_iter()
        .map(strip_child)
        .collect::<Result<Vec<_>>>()?;
    match Union::try_new(inputs.clone()) {
        Ok(union) => Ok(LogicalPlan::Union(union)),
        Err(strict) => match Union::try_new_by_name(inputs.clone()) {
            Ok(union) => Ok(LogicalPlan::Union(union)),
            Err(_) => match Union::try_new_with_loose_types(inputs) {
                Ok(union) => Ok(LogicalPlan::Union(union)),
                Err(_) => Err(strict),
            },
        },
    }
}

fn cleaned_df_schema(schema: &DFSchema) -> Result<DFSchemaRef> {
    let qualified = schema
        .iter()
        .map(|(qualifier, field)| {
            let mut stripped = field.as_ref().clone();
            stripped.metadata_mut().remove(ATTR_KEY);
            (qualifier.cloned(), Arc::new(stripped))
        })
        .collect::<Vec<_>>();
    DFSchema::new_with_metadata(qualified, schema.metadata().clone()).map(Arc::new)
}

fn strip_expr(expr: Expr) -> Result<Expr> {
    expr.transform_up(|node| match node {
        Expr::Alias(alias) => match alias.metadata {
            Some(meta) if meta.inner().contains_key(ATTR_KEY) => {
                let mut inner = meta.inner().clone();
                inner.remove(ATTR_KEY);
                Ok(Transformed::yes(Expr::Alias(Alias {
                    metadata: Some(FieldMetadata::new(inner)),
                    ..alias
                })))
            }
            _ => Ok(Transformed::no(Expr::Alias(alias))),
        },
        Expr::ScalarSubquery(subquery) => {
            let plan = strip(Arc::unwrap_or_clone(subquery.subquery.clone()))?;
            Ok(Transformed::yes(Expr::ScalarSubquery(Subquery {
                subquery: Arc::new(plan),
                ..subquery
            })))
        }
        Expr::Exists(exists) => {
            let plan = strip(Arc::unwrap_or_clone(exists.subquery.subquery.clone()))?;
            Ok(Transformed::yes(Expr::Exists(Exists {
                subquery: Subquery {
                    subquery: Arc::new(plan),
                    ..exists.subquery
                },
                ..exists
            })))
        }
        Expr::InSubquery(query) => {
            let plan = strip(Arc::unwrap_or_clone(query.subquery.subquery.clone()))?;
            Ok(Transformed::yes(Expr::InSubquery(InSubquery {
                subquery: Subquery {
                    subquery: Arc::new(plan),
                    ..query.subquery
                },
                ..query
            })))
        }
        _ => Ok(Transformed::no(node)),
    })
    .map(|transformed| transformed.data)
}

#[must_use]
pub fn strip_schema_ids(schema: SchemaRef) -> SchemaRef {
    if schema
        .fields()
        .iter()
        .all(|field| AttrId::of(field).is_none())
    {
        return schema;
    }
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            let mut stripped = field.as_ref().clone();
            stripped.metadata_mut().remove(ATTR_KEY);
            Arc::new(stripped)
        })
        .collect::<Vec<_>>();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

pub(crate) fn strip_record_batches(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let stripped = strip_schema_ids(Arc::clone(&schema));
    if Arc::ptr_eq(&stripped, &schema) {
        return Ok((schema, batches));
    }
    let schema = stripped;
    let batches = batches
        .into_iter()
        .map(|batch| {
            RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec()).map_err(|error| {
                internal_datafusion_err!("stripped batch keeps its columns: {error}")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((schema, batches))
}

#[allow(clippy::missing_errors_doc)]
pub fn resolve(
    schema: &DFSchema,
    written: &str,
    qualifier: Option<&str>,
    rule: NameRule,
    displays: &[String],
    frame_qualifiers: Option<&BTreeMap<String, Vec<String>>>,
) -> Result<Resolution> {
    if displays.len() != schema.fields().len() {
        return internal_err!(
            "resolve: {} display names for {} fields",
            displays.len(),
            schema.fields().len()
        );
    }
    let relation = qualifier.map(|text| TableReference::parse_str_normalized(text, true));
    let mut hits = Vec::new();
    let mut ids = BTreeSet::new();
    for (position, ((held, field), display)) in schema.iter().zip(displays).enumerate() {
        let relation_matches = match &relation {
            None => true,
            Some(written) => {
                qualifier_matches_position(written, field, held, rule, frame_qualifiers)
            }
        };
        if !relation_matches || !rule.matches(written, display) {
            continue;
        }
        let id = AttrId::of(field).ok_or_else(|| {
            internal_datafusion_err!(
                "resolve: field {position} ({}) carries no attribute id",
                field.name()
            )
        })?;
        hits.push(position);
        ids.insert(id);
    }
    Ok(match ids.len() {
        0 => Resolution::Missing,
        1 => Resolution::Bound(hits),
        _ => Resolution::Ambiguous(hits),
    })
}

pub(super) fn qualifier_matches_position(
    written: &TableReference,
    field: &Field,
    held: Option<&TableReference>,
    rule: NameRule,
    frame_qualifiers: Option<&BTreeMap<String, Vec<String>>>,
) -> bool {
    let facade =
        frame_qualifiers.and_then(|map| AttrId::of(field).and_then(|id| map.get(id.as_str())));
    match facade {
        Some(names) => names.iter().any(|name| {
            written.catalog().is_none()
                && written.schema().is_none()
                && rule.matches(written.table(), name)
        }),
        None => match held {
            Some(held) => same_relation(written, held, rule),
            None => false,
        },
    }
}

pub(super) fn same_relation(
    written: &TableReference,
    held: &TableReference,
    rule: NameRule,
) -> bool {
    let written_parts = [written.catalog(), written.schema(), Some(written.table())];
    let held_parts = [held.catalog(), held.schema(), Some(held.table())];
    written_parts
        .iter()
        .zip(held_parts.iter())
        .all(
            |(written_part, held_part)| match (written_part, held_part) {
                (Some(written_part), Some(held_part)) => rule.matches(written_part, held_part),
                (Some(_), None) => false,
                (None, _) => true,
            },
        )
}
