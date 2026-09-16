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
    Accumulator, AggregateUDF, AggregateUDFImpl, Expr, Join, JoinType, LogicalPlan,
    LogicalPlanBuilder, Projection, Signature, Subquery, SubqueryAlias, Volatility, col, lit, not,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::prelude::SessionContext;

const SINGLE_ROW_UDAF: &str = "__repark_single_row";

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
    let head = Expr::Column(Column::from(subquery.subquery.schema().qualified_field(0)));
    let head_name = subquery.subquery.schema().fields()[0].name().clone();
    let wrapped = LogicalPlanBuilder::from((*subquery.subquery).clone())
        .aggregate(
            Vec::<Expr>::new(),
            vec![single_row_call(head).alias(head_name)],
        )?
        .build()?;
    Ok(Subquery {
        outer_ref_columns: subquery.outer_ref_columns,
        subquery: Arc::new(wrapped),
        spans: subquery.spans,
    })
}

fn single_row_call(arg: Expr) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        Arc::new(single_row_udaf()),
        vec![arg],
        false,
        None,
        vec![],
        None,
    ))
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
            small || plan_is_singleton(&limit.input)
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
        let new_right = match (alias, right_outer_refs.is_empty()) {
            (Some(name), _) => {
                LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(right_inner), name)?)
            }
            (None, true) => right_inner,
            (None, false) => LogicalPlan::Subquery(Subquery {
                outer_ref_columns: right_outer_refs,
                subquery: Arc::new(right_inner),
                spans: Spans::new(),
            }),
        };
        let left_columns: Vec<Expr> = (0..join.left.schema().fields().len())
            .map(|index| Expr::Column(Column::from(join.left.schema().qualified_field(index))))
            .collect();
        let new_plan =
            LogicalPlanBuilder::from(LogicalPlan::Join(rewire_join_right(join, new_right)?))
                .project(left_columns.into_iter().chain(hoisted))?
                .build()?;
        Ok(Transformed::yes(new_plan))
    }

    fn name(&self) -> &'static str {
        "repark_lateral_projection_hoist"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
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
}

pub fn single_row_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(ReparkSingleRow::new())
}

#[derive(Debug)]
struct ReparkSingleRow {
    signature: Signature,
}

impl ReparkSingleRow {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
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
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &'static str {
        SINGLE_ROW_UDAF
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
}

impl Accumulator for SingleRowAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(array) = values.first() else {
            return Ok(());
        };
        for index in 0..array.len() {
            self.seen += 1;
            if self.seen > 1 {
                return exec_err!(
                    "[SCALAR_SUBQUERY_TOO_MANY_ROWS] More than one row returned by a \
                     subquery used as an expression. SQLSTATE: 21000"
                );
            }
            self.value = ScalarValue::try_from_array(array.as_ref(), index)?;
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
            self.seen += incoming;
            if self.seen > 1 {
                return exec_err!(
                    "[SCALAR_SUBQUERY_TOO_MANY_ROWS] More than one row returned by a \
                     subquery used as an expression. SQLSTATE: 21000"
                );
            }
            self.value = ScalarValue::try_from_array(values.as_ref(), index)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.value.size() - size_of_val(&self.value)
    }
}
