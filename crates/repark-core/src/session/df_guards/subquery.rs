use std::hash::{Hash, Hasher};
use std::mem::size_of_val;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{
    Column, DFSchema, DFSchemaRef, Result, ScalarValue, Spans, TableReference, exec_err, plan_err,
};
use datafusion::functions_aggregate::count::count_all;
use datafusion::logical_expr::expr::{AggregateFunction, Exists, InSubquery, SetComparison};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Filter, Join, JoinType, LogicalPlan,
    LogicalPlanBuilder, Projection, Signature, Subquery, SubqueryAlias, Volatility, col, lit, not,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::prelude::SessionContext;

const SINGLE_ROW_UDAF: &str = "__repark_single_row";

const ANY_ROW_UDAF: &str = "__repark_any_row";

const EXISTS_COUNT_ALIAS: &str = "__repark_exists_count";

#[allow(clippy::missing_errors_doc)]
pub fn resolve_bound_expr(expr: Expr, frame_schema: &DFSchema) -> Result<Expr> {
    let scopes = vec![Arc::new(frame_schema.clone())];
    resolve_expr(expr, &scopes, scopes.len())
}

#[allow(clippy::missing_errors_doc)]
pub fn resolve_scoped_expr(expr: Expr, scopes: &[DFSchemaRef]) -> Result<Expr> {
    resolve_expr(expr, scopes, scopes.len())
}

#[allow(clippy::missing_errors_doc)]
pub fn resolve_subquery_plan(plan: LogicalPlan, outer_schema: &DFSchemaRef) -> Result<LogicalPlan> {
    resolve_plan(plan, std::slice::from_ref(outer_schema))
}

fn resolve_expr(expr: Expr, scopes: &[DFSchemaRef], own: usize) -> Result<Expr> {
    expr.transform(|node| {
        Ok(match node {
            Expr::ScalarSubquery(subquery) => {
                Transformed::yes(Expr::ScalarSubquery(resolve_subquery(subquery, scopes)?))
            }
            Expr::Exists(exists) => Transformed::yes(Expr::Exists(Exists::new(
                resolve_subquery(exists.subquery, scopes)?,
                exists.negated,
            ))),
            Expr::InSubquery(in_subquery) => Transformed::yes(Expr::InSubquery(InSubquery::new(
                in_subquery.expr,
                resolve_subquery(in_subquery.subquery, scopes)?,
                in_subquery.negated,
            ))),
            Expr::SetComparison(set_comparison) => {
                Transformed::yes(Expr::SetComparison(SetComparison {
                    expr: set_comparison.expr,
                    subquery: resolve_subquery(set_comparison.subquery, scopes)?,
                    op: set_comparison.op,
                    quantifier: set_comparison.quantifier,
                }))
            }
            Expr::OuterReferenceColumn(field, column) => match first_scope_hit(&column, scopes) {
                Some(index) if index < own => Transformed::yes(Expr::Column(column)),
                Some(index) => Transformed::yes(typed_outer_ref(&scopes[index], column)?),
                None => Transformed::no(Expr::OuterReferenceColumn(field, column)),
            },
            Expr::Column(column) => match first_scope_hit(&column, scopes) {
                Some(index) if index < own => Transformed::no(Expr::Column(column)),
                Some(index) => Transformed::yes(typed_outer_ref(&scopes[index], column)?),
                None => Transformed::no(Expr::Column(column)),
            },
            _ => Transformed::no(node),
        })
    })
    .map(|transformed| transformed.data)
}

fn resolve_plan(plan: LogicalPlan, outer_scopes: &[DFSchemaRef]) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|node| {
        if let LogicalPlan::Join(join) = &node
            && let Some((subquery, alias)) = lateral_parts(&join.right)
        {
            let mut scopes = vec![Arc::clone(join.left.schema())];
            scopes.extend(outer_scopes.iter().cloned());
            let resolved = resolve_plan((*subquery.subquery).clone(), &scopes)?;
            let rebuilt = LogicalPlan::Subquery(Subquery {
                outer_ref_columns: resolved.all_out_ref_exprs(),
                subquery: Arc::new(resolved),
                spans: Spans::new(),
            });
            let right = match alias {
                Some(name) => {
                    LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(rebuilt), name)?)
                }
                None => rebuilt,
            };
            let join = LogicalPlan::Join(rewire_join_right(join.clone(), right)?);
            let mut own_scopes: Vec<DFSchemaRef> = join
                .inputs()
                .into_iter()
                .map(|input| Arc::clone(input.schema()))
                .collect();
            let own = own_scopes.len();
            own_scopes.extend(outer_scopes.iter().cloned());
            return join.map_expressions(|expr| {
                resolve_expr(expr, &own_scopes, own).map(Transformed::yes)
            });
        }
        let mut scopes: Vec<DFSchemaRef> = node
            .inputs()
            .into_iter()
            .map(|input| Arc::clone(input.schema()))
            .collect();
        let own = scopes.len();
        scopes.extend(outer_scopes.iter().cloned());
        node.map_expressions(|expr| resolve_expr(expr, &scopes, own).map(Transformed::yes))
    })
    .map(|transformed| transformed.data)
}

fn resolve_subquery(subquery: Subquery, scopes: &[DFSchemaRef]) -> Result<Subquery> {
    let plan = resolve_plan((*subquery.subquery).clone(), scopes)?;
    Ok(Subquery {
        outer_ref_columns: plan.all_out_ref_exprs(),
        subquery: Arc::new(plan),
        spans: subquery.spans,
    })
}

fn first_scope_hit(column: &Column, scopes: &[DFSchemaRef]) -> Option<usize> {
    scopes
        .iter()
        .position(|schema| schema.index_of_column(column).is_ok())
}

fn typed_outer_ref(scope: &DFSchemaRef, column: Column) -> Result<Expr> {
    let index = scope.index_of_column(&column)?;
    let (_, field) = scope.qualified_field(index);
    Ok(Expr::OuterReferenceColumn(Arc::clone(field), column))
}

fn rewire_join_right(join: Join, right: LogicalPlan) -> Result<Join> {
    Join::try_new(
        join.left,
        Arc::new(right),
        join.on,
        join.filter,
        join.join_type,
        join.join_constraint,
        join.null_equality,
        join.null_aware,
    )
}

fn substitute_hoisted_ref(
    expr: Expr,
    left_schema: &DFSchemaRef,
    right_schema: &DFSchemaRef,
    hoisted_named: &[(String, Expr)],
    alias: Option<&TableReference>,
) -> Result<Expr> {
    expr.transform(|node| {
        let Expr::Column(column) = &node else {
            return Ok(Transformed::no(node));
        };
        let relation_ok = match (&column.relation, alias) {
            (None, _) => true,
            (Some(relation), Some(name)) => relation == name,
            (Some(_), None) => false,
        };
        if !relation_ok
            || left_schema.index_of_column(column).is_ok()
            || right_schema.index_of_column(column).is_ok()
        {
            return Ok(Transformed::no(node));
        }
        match hoisted_named
            .iter()
            .find(|(name, _)| name.as_str() == column.name())
        {
            Some((_, replacement)) => Ok(Transformed::yes(replacement.clone())),
            None => Ok(Transformed::no(node)),
        }
    })
    .map(|transformed| transformed.data)
}

fn lateral_parts(right: &LogicalPlan) -> Option<(&Subquery, Option<TableReference>)> {
    match right {
        LogicalPlan::Subquery(subquery) => Some((subquery, None)),
        LogicalPlan::SubqueryAlias(subquery_alias) => match subquery_alias.input.as_ref() {
            LogicalPlan::Subquery(subquery) => Some((subquery, Some(subquery_alias.alias.clone()))),
            _ => None,
        },
        _ => None,
    }
}

#[derive(Debug)]
pub struct ReparkScalarSubqueryGuard;

impl OptimizerRule for ReparkScalarSubqueryGuard {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.map_expressions(|expr| {
            expr.transform(|node| match node {
                Expr::ScalarSubquery(subquery) => Ok(Transformed::yes(Expr::ScalarSubquery(
                    guard_scalar_subquery(subquery)?,
                ))),
                _ => Ok(Transformed::no(node)),
            })
        })
    }

    fn name(&self) -> &'static str {
        "repark_scalar_subquery_guard"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }
}

fn guard_scalar_subquery(subquery: Subquery) -> Result<Subquery> {
    if plan_is_singleton(subquery.subquery.as_ref()) {
        return Ok(subquery);
    }
    let correlated = !subquery.outer_ref_columns.is_empty();
    let (subplan, fetch) = if correlated {
        strip_correlated_limit((*subquery.subquery).clone())?
    } else {
        ((*subquery.subquery).clone(), None)
    };
    let head = Expr::Column(Column::from(subplan.schema().qualified_field(0)));
    let head_name = subplan.schema().fields()[0].name().clone();
    let guard = match fetch {
        Some(n) if n <= 1 => any_row_call(head),
        _ => single_row_call(head),
    };
    let wrapped = LogicalPlanBuilder::from(subplan)
        .aggregate(Vec::<Expr>::new(), vec![guard.alias(head_name)])?
        .build()?;
    Ok(Subquery {
        outer_ref_columns: subquery.outer_ref_columns,
        subquery: Arc::new(wrapped),
        spans: subquery.spans,
    })
}

fn strip_correlated_limit(plan: LogicalPlan) -> Result<(LogicalPlan, Option<i64>)> {
    Ok(match plan {
        LogicalPlan::Limit(limit) => {
            let fetch = match limit.fetch.as_deref() {
                Some(Expr::Literal(ScalarValue::Int64(Some(n)), _)) => Some(*n),
                _ => None,
            };
            let skipped = matches!(
                limit.skip.as_deref(),
                None | Some(Expr::Literal(ScalarValue::Int64(Some(0)), _))
            );
            match (fetch, skipped) {
                (Some(0), true) => {
                    let (input, _) = strip_correlated_limit((*limit.input).clone())?;
                    (
                        LogicalPlan::Filter(Filter::try_new(lit(false), Arc::new(input))?),
                        Some(0),
                    )
                }
                (Some(n), true) => {
                    let (input, deeper) = strip_correlated_limit((*limit.input).clone())?;
                    (input, Some(deeper.map_or(n, |d| d.min(n))))
                }
                _ => (LogicalPlan::Limit(limit), None),
            }
        }
        node @ (LogicalPlan::Projection(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Sort(_)) => {
            let mut inputs = node.inputs().into_iter().cloned();
            match (inputs.next(), inputs.next()) {
                (Some(input), None) => {
                    let (input, fetch) = strip_correlated_limit(input)?;
                    (node.with_new_exprs(node.expressions(), vec![input])?, fetch)
                }
                _ => (node, None),
            }
        }
        node => (node, None),
    })
}

fn row_guard_call(arg: Expr, udaf: AggregateUDF) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        Arc::new(udaf),
        vec![arg],
        false,
        None,
        vec![],
        None,
    ))
}

fn single_row_call(arg: Expr) -> Expr {
    row_guard_call(arg, single_row_udaf())
}

fn any_row_call(arg: Expr) -> Expr {
    row_guard_call(arg, any_row_udaf())
}

#[derive(Debug)]
pub struct ReparkProjectionExists;

impl OptimizerRule for ReparkProjectionExists {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Projection(_) => plan.map_expressions(|expr| {
                expr.transform(|node| match node {
                    Expr::Exists(exists) => Ok(Transformed::yes(exists_as_scalar(&exists)?)),
                    _ => Ok(Transformed::no(node)),
                })
            }),
            _ => Ok(Transformed::no(plan)),
        }
    }

    fn name(&self) -> &'static str {
        "repark_projection_exists"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

fn plan_is_singleton(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(aggregate) => aggregate.group_expr.is_empty(),
        LogicalPlan::Projection(projection) => plan_is_singleton(&projection.input),
        LogicalPlan::Filter(filter) => plan_is_singleton(&filter.input),
        LogicalPlan::SubqueryAlias(alias) => plan_is_singleton(&alias.input),
        LogicalPlan::Sort(sort) => plan_is_singleton(&sort.input),
        LogicalPlan::Limit(limit) => {
            let small = matches!(
                limit.fetch.as_deref(),
                Some(Expr::Literal(ScalarValue::Int64(Some(n)), _)) if *n <= 1
            );
            if small && !plan_has_outer_refs(&limit.input) {
                true
            } else {
                plan_is_singleton(&limit.input)
            }
        }
        _ => false,
    }
}

fn exists_as_scalar(exists: &Exists) -> Result<Expr> {
    let original_name = Expr::Exists(exists.clone()).schema_name().to_string();
    let inner = (*exists.subquery.subquery).clone();
    let check = col(EXISTS_COUNT_ALIAS).gt(lit(0i64));
    let check = if exists.negated { not(check) } else { check };
    let plan = LogicalPlanBuilder::from(inner)
        .aggregate(
            Vec::<Expr>::new(),
            vec![count_all().alias(EXISTS_COUNT_ALIAS)],
        )?
        .project([check])?
        .build()?;
    let scalar = Expr::ScalarSubquery(Subquery {
        outer_ref_columns: plan.all_out_ref_exprs(),
        subquery: Arc::new(plan),
        spans: Spans::new(),
    });
    Ok(scalar.alias(original_name))
}

#[derive(Debug)]
pub struct ReparkLateralProjectionHoist;

impl OptimizerRule for ReparkLateralProjectionHoist {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Join(join) = plan else {
            return Ok(Transformed::no(plan));
        };
        if !matches!(join.join_type, JoinType::Inner | JoinType::Left) {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
        let Some((subquery, alias)) = lateral_parts(&join.right) else {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        };
        let subplan = (*subquery.subquery).clone();
        if !plan_has_outer_refs(&subplan) {
            let right = match alias {
                Some(name) => {
                    LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(subplan), name)?)
                }
                None => subplan,
            };
            return Ok(Transformed::yes(LogicalPlan::Join(rewire_join_right(
                join, right,
            )?)));
        }
        let root_projection = match &subplan {
            LogicalPlan::Projection(projection) => Some(projection),
            _ => None,
        };
        if let Some(offender) = lateral_correlated_refusal(&subplan, root_projection)? {
            return plan_err!(
                "[UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.CORRELATED_REFERENCE] \
                 Unsupported subquery expression: Expressions referencing the outer \
                 query are not supported outside of WHERE/HAVING clauses: \"{}\". \
                 SQLSTATE: 0A000",
                offender.schema_name()
            );
        }
        let Some(projection) = root_projection else {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        };
        Ok(Transformed::yes(hoist_lateral_projection(
            join,
            projection,
            alias.as_ref(),
        )?))
    }

    fn name(&self) -> &'static str {
        "repark_lateral_projection_hoist"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

fn hoist_lateral_projection(
    mut join: Join,
    projection: &Projection,
    alias: Option<&TableReference>,
) -> Result<LogicalPlan> {
    let hoisted = projection
        .expr
        .iter()
        .map(|expr| {
            expr.clone()
                .transform(|node| match node {
                    Expr::OuterReferenceColumn(_, column) => {
                        Ok(Transformed::yes(Expr::Column(column)))
                    }
                    _ => Ok(Transformed::no(node)),
                })
                .map(|transformed| transformed.data)
        })
        .collect::<Result<Vec<Expr>>>()?;
    let mut right_inner = (*projection.input).clone();
    let hoisted_use_inner = hoisted.iter().any(|expr| {
        expr.exists(|node| Ok(matches!(node, Expr::Column(_))))
            .is_ok_and(|found| found)
            && expr
                .exists(|node| {
                    Ok(matches!(node, Expr::Column(column) if {
                        right_inner.schema().index_of_column(column).is_ok()
                    }))
                })
                .unwrap_or(false)
    });
    if !hoisted_use_inner {
        right_inner = LogicalPlanBuilder::from(right_inner)
            .project([lit(true).alias("__repark_lateral_rows")])?
            .build()?;
    }
    let right_outer_refs = right_inner.all_out_ref_exprs();
    let hoisted = requalify_hoisted(hoisted, right_inner.schema(), alias)?;
    let new_right = match (alias, right_outer_refs.is_empty()) {
        (Some(name), true) => {
            LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(right_inner), name.clone())?)
        }
        (Some(name), false) => LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(LogicalPlan::Subquery(Subquery {
                outer_ref_columns: right_outer_refs,
                subquery: Arc::new(right_inner),
                spans: Spans::new(),
            })),
            name.clone(),
        )?),
        (None, true) => right_inner,
        (None, false) => LogicalPlan::Subquery(Subquery {
            outer_ref_columns: right_outer_refs,
            subquery: Arc::new(right_inner),
            spans: Spans::new(),
        }),
    };
    let hoisted_named: Vec<(String, Expr)> = hoisted
        .iter()
        .map(|expr| match expr {
            Expr::Alias(alias) => (alias.name.clone(), (*alias.expr).clone()),
            Expr::Column(column) => (column.name().to_string(), expr.clone()),
            other => (other.schema_name().to_string(), other.clone()),
        })
        .collect();
    let left_schema = Arc::clone(join.left.schema());
    let right_schema = Arc::clone(new_right.schema());
    rewrite_join_predicates(
        &mut join,
        &left_schema,
        &right_schema,
        &hoisted_named,
        alias,
    )?;
    let left_columns: Vec<Expr> = (0..left_schema.fields().len())
        .map(|index| Expr::Column(Column::from(left_schema.qualified_field(index))))
        .collect();
    let hoisted_out: Vec<Expr> = match alias {
        Some(name) => hoisted
            .into_iter()
            .map(|expr| match expr {
                Expr::Alias(mut alias_expr) => {
                    alias_expr.relation = Some(name.clone());
                    Expr::Alias(alias_expr)
                }
                other => {
                    let output = match &other {
                        Expr::Column(column) => column.name().to_string(),
                        expr => expr.schema_name().to_string(),
                    };
                    other.alias_qualified(Some(name.clone()), output)
                }
            })
            .collect(),
        None => hoisted,
    };
    LogicalPlanBuilder::from(LogicalPlan::Join(rewire_join_right(join, new_right)?))
        .project(left_columns.into_iter().chain(hoisted_out))?
        .build()
}

fn requalify_hoisted(
    hoisted: Vec<Expr>,
    right_schema: &DFSchemaRef,
    alias: Option<&TableReference>,
) -> Result<Vec<Expr>> {
    let Some(name) = alias else {
        return Ok(hoisted);
    };
    hoisted
        .into_iter()
        .map(|expr| {
            expr.transform(|node| match node {
                Expr::Column(column) if right_schema.index_of_column(&column).is_ok() => {
                    Ok(Transformed::yes(Expr::Column(Column::new(
                        Some(name.clone()),
                        column.name().to_string(),
                    ))))
                }
                _ => Ok(Transformed::no(node)),
            })
            .map(|transformed| transformed.data)
        })
        .collect()
}

fn rewrite_join_predicates(
    join: &mut Join,
    left_schema: &DFSchemaRef,
    right_schema: &DFSchemaRef,
    hoisted_named: &[(String, Expr)],
    alias: Option<&TableReference>,
) -> Result<()> {
    join.on = join
        .on
        .iter()
        .map(|(left_key, right_key)| {
            Ok((
                substitute_hoisted_ref(
                    left_key.clone(),
                    left_schema,
                    right_schema,
                    hoisted_named,
                    alias,
                )?,
                substitute_hoisted_ref(
                    right_key.clone(),
                    left_schema,
                    right_schema,
                    hoisted_named,
                    alias,
                )?,
            ))
        })
        .collect::<Result<Vec<(Expr, Expr)>>>()?;
    join.filter = join
        .filter
        .take()
        .map(|filter| {
            substitute_hoisted_ref(filter, left_schema, right_schema, hoisted_named, alias)
        })
        .transpose()?;
    Ok(())
}

fn lateral_correlated_refusal(
    subplan: &LogicalPlan,
    root_projection: Option<&Projection>,
) -> Result<Option<Expr>> {
    let mut refusal: Option<Expr> = None;
    subplan.apply(|node| {
        if !node.contains_outer_reference() {
            return Ok(TreeNodeRecursion::Continue);
        }
        let allowed = matches!(node, LogicalPlan::Filter(_))
            || root_projection.is_some_and(|projection| {
                matches!(node, LogicalPlan::Projection(node_projection)
                    if std::ptr::eq(node_projection, projection))
            });
        if allowed {
            return Ok(TreeNodeRecursion::Continue);
        }
        refusal = node.expressions().into_iter().find(Expr::contains_outer);
        Ok(TreeNodeRecursion::Stop)
    })?;
    Ok(refusal)
}

fn plan_has_outer_refs(plan: &LogicalPlan) -> bool {
    plan.apply(|node| {
        if node.contains_outer_reference() {
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })
    .is_ok_and(|recursion| recursion == TreeNodeRecursion::Stop)
}

pub fn register_single_row_guard(context: &SessionContext) {
    context.register_udaf(single_row_udaf());
    context.register_udaf(any_row_udaf());
}

pub fn single_row_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(ReparkSingleRow::new(SINGLE_ROW_UDAF, true))
}

pub fn any_row_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(ReparkSingleRow::new(ANY_ROW_UDAF, false))
}

#[derive(Debug)]
struct ReparkSingleRow {
    signature: Signature,
    udaf_name: &'static str,
    strict: bool,
}

impl ReparkSingleRow {
    fn new(udaf_name: &'static str, strict: bool) -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            udaf_name,
            strict,
        }
    }
}

impl PartialEq for ReparkSingleRow {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for ReparkSingleRow {}

impl Hash for ReparkSingleRow {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl AggregateUDFImpl for ReparkSingleRow {
    fn name(&self) -> &'static str {
        self.udaf_name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        arg_types.first().cloned().ok_or_else(|| {
            datafusion::common::DataFusionError::Internal(
                "single_row expects exactly one argument".to_string(),
            )
        })
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let input_type = args
            .expr_fields
            .first()
            .map_or(DataType::Null, |field| field.data_type().clone());
        Ok(Box::new(SingleRowAccumulator {
            seen: 0_u64,
            value: ScalarValue::try_from(&input_type).unwrap_or(ScalarValue::Null),
            strict: self.strict,
        }))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format_state_name(args.name, "seen"),
                DataType::Int64,
                false,
            )),
            Arc::new(
                args.return_field
                    .as_ref()
                    .clone()
                    .with_name(format_state_name(args.name, "value")),
            ),
        ])
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        ScalarValue::try_from(data_type)
    }
}

#[derive(Debug)]
struct SingleRowAccumulator {
    seen: u64,
    value: ScalarValue,
    strict: bool,
}

impl Accumulator for SingleRowAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(array) = values.first() else {
            return Ok(());
        };
        for index in 0..array.len() {
            self.seen += 1;
            if self.strict && self.seen > 1 {
                return exec_err!(
                    "[SCALAR_SUBQUERY_TOO_MANY_ROWS] More than one row returned by a \
                     subquery used as an expression. SQLSTATE: 21000"
                );
            }
            if self.seen == 1 {
                self.value = ScalarValue::try_from_array(array.as_ref(), index)?;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.value.clone())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Int64(Some(i64::try_from(self.seen).unwrap_or(i64::MAX))),
            self.value.clone(),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let Some(counts) = states.first() else {
            return Ok(());
        };
        let Some(counts) = counts.as_any().downcast_ref::<Int64Array>() else {
            return self.update_batch(states);
        };
        let Some(values) = states.get(1) else {
            return Ok(());
        };
        for index in 0..counts.len() {
            let incoming = u64::try_from(counts.value(index)).unwrap_or(0);
            if incoming == 0 {
                continue;
            }
            let first = self.seen == 0;
            self.seen += incoming;
            if self.strict && self.seen > 1 {
                return exec_err!(
                    "[SCALAR_SUBQUERY_TOO_MANY_ROWS] More than one row returned by a \
                     subquery used as an expression. SQLSTATE: 21000"
                );
            }
            if first {
                self.value = ScalarValue::try_from_array(values.as_ref(), index)?;
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.value.size() - size_of_val(&self.value)
    }
}
