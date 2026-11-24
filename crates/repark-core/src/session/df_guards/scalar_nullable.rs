use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, DFSchema, DFSchemaRef, Result, ScalarValue, exec_err, plan_err};
use datafusion::logical_expr::expr::{Alias, Exists, InSubquery, ScalarFunction, SetComparison};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::logical_plan::{Limit, Projection, Subquery, Union, Values};
use datafusion::logical_expr::{
    ColumnarValue, Distinct, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::{AnalyzerRule, ApplyOrder, OptimizerConfig, OptimizerRule};

pub(crate) const SCALAR_NULLABLE_NAME: &str = "__repark_scalar_nullable__";

const ANALYZER_RULE_NAME: &str = "repark_scalar_subquery_nullability";

const STRIP_RULE_NAME: &str = "repark_strip_scalar_nullable";

const TYPE_COERCION_RULE_NAME: &str = "type_coercion";

const SPARK_NONNULL_NAME: &str = "__repark_spark_nonnull__";

const TIMESTAMP_NTZ_CAST_NAME: &str = "__repark_cast_timestamp_ntz__";

const ZONE_SEAM_NAME: &str = "__repark_zone_seam__";

static SCALAR_NULLABLE: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(ScalarNullable {
        signature: Signature::any(1, Volatility::Volatile),
    }))
});

#[must_use]
pub fn udf() -> Arc<ScalarUDF> {
    Arc::clone(&SCALAR_NULLABLE)
}

#[must_use]
pub(crate) fn scalar_nullable_call(expr: Expr) -> Expr {
    udf().call(vec![expr])
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ScalarNullable {
    signature: Signature,
}

impl ScalarUDFImpl for ScalarNullable {
    fn name(&self) -> &str {
        SCALAR_NULLABLE_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn schema_name(&self, args: &[Expr]) -> Result<String> {
        let [argument] = args else {
            return plan_err!("{SCALAR_NULLABLE_NAME} takes one argument");
        };
        Ok(argument.schema_name().to_string())
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [data_type] = arg_types else {
            return plan_err!("{SCALAR_NULLABLE_NAME} takes one argument");
        };
        Ok(data_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return plan_err!("{SCALAR_NULLABLE_NAME} takes one argument");
        };
        Ok(Arc::new(
            field
                .as_ref()
                .clone()
                .with_name(self.name())
                .with_nullable(true),
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut values = args.args;
        let (Some(value), true) = (values.pop(), values.is_empty()) else {
            return exec_err!("{SCALAR_NULLABLE_NAME} takes one argument");
        };
        Ok(value)
    }
}

#[must_use]
pub fn analyzer_rule() -> Arc<dyn AnalyzerRule + Send + Sync> {
    Arc::new(ReparkScalarSubqueryNullability)
}

#[allow(clippy::missing_errors_doc)]
pub fn insert_before_coercion(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules
        .iter()
        .position(|rule| rule.name() == TYPE_COERCION_RULE_NAME)
    else {
        return plan_err!(
            "cannot seat {ANALYZER_RULE_NAME}: {TYPE_COERCION_RULE_NAME} missing from analyzer list"
        );
    };
    rules.insert(position, analyzer_rule());
    Ok(rules)
}

#[derive(Debug, Default)]
pub(crate) struct ReparkScalarSubqueryNullability;

impl AnalyzerRule for ReparkScalarSubqueryNullability {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        wrap_plan(plan).data()
    }

    fn name(&self) -> &str {
        ANALYZER_RULE_NAME
    }
}

impl OptimizerRule for ReparkScalarSubqueryNullability {
    fn name(&self) -> &str {
        ANALYZER_RULE_NAME
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        wrap_plan(plan)
    }
}

#[derive(Debug, Default)]
pub(crate) struct StripScalarNullable;

impl OptimizerRule for StripScalarNullable {
    fn name(&self) -> &str {
        STRIP_RULE_NAME
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        strip_plan(plan)
    }
}

pub(crate) fn wrap_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if !plan_has_candidate_scalar_subquery(&plan)? {
        return Ok(Transformed::no(plan));
    }
    rewrite_wrap_plan(plan)
}

fn plan_has_candidate_scalar_subquery(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            found = expr.exists(|inner| {
                Ok(matches!(
                    inner,
                    Expr::ScalarSubquery(subquery) if scalar_subquery_needs_wrap(subquery)
                ))
            })?;
            Ok(if found {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn scalar_subquery_needs_wrap(subquery: &Subquery) -> bool {
    let fields = subquery.subquery.schema().fields();
    fields.len() == 1
        && !fields[0].is_nullable()
        && !guaranteed_at_least_one_row(subquery.subquery.as_ref())
        && !plan_has_unexpanded_generator(subquery.subquery.as_ref())
}

fn plan_has_unexpanded_generator(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if let LogicalPlan::Projection(projection) = node
            && projection_has_unexpanded_generator(&projection.expr)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn projection_has_unexpanded_generator(expressions: &[Expr]) -> bool {
    expressions.iter().any(|expr| {
        let mut found = false;
        let _ = expr.apply(|node| match node {
            Expr::ScalarSubquery(_)
            | Expr::Exists(_)
            | Expr::InSubquery(_)
            | Expr::SetComparison(_) => Ok(TreeNodeRecursion::Jump),
            Expr::ScalarFunction(call) if is_unexpanded_generator_placeholder(call.name()) => {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        });
        found
    })
}

fn plan_input_schema(plan: &LogicalPlan) -> DFSchema {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    schema
}

fn cast_wrapper_has_nullable_operand(expr: &Expr, schema: &DFSchema) -> bool {
    let operand = match expr {
        Expr::Cast(cast) => cast.expr.as_ref(),
        Expr::ScalarFunction(call)
            if matches!(
                call.func.name(),
                "to_timestamp" | TIMESTAMP_NTZ_CAST_NAME | ZONE_SEAM_NAME
            ) =>
        {
            let Some(first) = call.args.first() else {
                return false;
            };
            first
        }
        _ => return false,
    };
    operand.nullable(schema).unwrap_or(false)
}

fn rewrite_wrap_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let Transformed {
        data: plan,
        transformed: children_changed,
        ..
    } = plan.map_children(rewrite_wrap_plan)?;
    let input_schema = plan_input_schema(&plan);
    let preserver = NamePreserver::new(&plan);
    let Transformed {
        data: plan,
        transformed: exprs_changed,
        ..
    } = plan.map_expressions(|expr| {
        let saved = preserver.save(&expr);
        expr.transform_up(|node| rewrite_wrap_expr(node, &input_schema))?
            .map_data(|rewritten| Ok(saved.restore(rewritten)))
    })?;
    if children_changed || exprs_changed {
        Ok(Transformed::yes(recompute_plan_schema(plan)?))
    } else {
        Ok(Transformed::no(plan))
    }
}

fn rewrite_wrap_expr(expr: Expr, input_schema: &DFSchema) -> Result<Transformed<Expr>> {
    match expr {
        Expr::ScalarSubquery(mut subquery) => {
            let Transformed {
                data: inner_plan,
                transformed: inner_changed,
                ..
            } = rewrite_wrap_plan(Arc::unwrap_or_clone(subquery.subquery))?;
            subquery.subquery = Arc::new(inner_plan);
            if scalar_subquery_needs_wrap(&subquery) {
                let wrapped = wrap_subquery_output(Arc::unwrap_or_clone(subquery.subquery))?;
                subquery.subquery = Arc::new(wrapped);
                Ok(Transformed::yes(Expr::ScalarSubquery(subquery)))
            } else if inner_changed {
                Ok(Transformed::yes(Expr::ScalarSubquery(subquery)))
            } else {
                Ok(Transformed::no(Expr::ScalarSubquery(subquery)))
            }
        }
        Expr::Exists(Exists {
            mut subquery,
            negated,
        }) => {
            let Transformed {
                data: inner_plan,
                transformed,
                ..
            } = rewrite_wrap_plan(Arc::unwrap_or_clone(subquery.subquery))?;
            subquery.subquery = Arc::new(inner_plan);
            Ok(Transformed::new(
                Expr::Exists(Exists { subquery, negated }),
                transformed,
                TreeNodeRecursion::Continue,
            ))
        }
        Expr::InSubquery(InSubquery {
            expr,
            mut subquery,
            negated,
        }) => {
            let Transformed {
                data: inner_plan,
                transformed,
                ..
            } = rewrite_wrap_plan(Arc::unwrap_or_clone(subquery.subquery))?;
            subquery.subquery = Arc::new(inner_plan);
            Ok(Transformed::new(
                Expr::InSubquery(InSubquery {
                    expr,
                    subquery,
                    negated,
                }),
                transformed,
                TreeNodeRecursion::Continue,
            ))
        }
        Expr::SetComparison(SetComparison {
            expr,
            mut subquery,
            op,
            quantifier,
        }) => {
            let Transformed {
                data: inner_plan,
                transformed,
                ..
            } = rewrite_wrap_plan(Arc::unwrap_or_clone(subquery.subquery))?;
            subquery.subquery = Arc::new(inner_plan);
            Ok(Transformed::new(
                Expr::SetComparison(SetComparison {
                    expr,
                    subquery,
                    op,
                    quantifier,
                }),
                transformed,
                TreeNodeRecursion::Continue,
            ))
        }
        Expr::ScalarFunction(ScalarFunction { func, mut args })
            if func.name() == SPARK_NONNULL_NAME
                && args.len() == 1
                && cast_wrapper_has_nullable_operand(&args[0], input_schema) =>
        {
            let Some(inner) = args.pop() else {
                return plan_err!("{SPARK_NONNULL_NAME} missing inner expression");
            };
            Ok(Transformed::yes(inner))
        }
        other => Ok(Transformed::no(other)),
    }
}

fn wrap_subquery_output(plan: LogicalPlan) -> Result<LogicalPlan> {
    let (relation, field) = plan.schema().qualified_field(0);
    let relation = relation.cloned();
    let name = field.name().clone();
    match plan {
        LogicalPlan::Projection(Projection {
            mut expr, input, ..
        }) if expr.len() == 1 => {
            let Some(only_expr) = expr.pop() else {
                return plan_err!("scalar subquery projection has no output expression");
            };
            let wrapped = match only_expr {
                Expr::Alias(Alias {
                    expr: inner,
                    relation: alias_relation,
                    name: alias_name,
                    metadata: alias_metadata,
                }) => scalar_nullable_call(*inner).alias_qualified_with_metadata(
                    alias_relation,
                    alias_name,
                    alias_metadata,
                ),
                other => scalar_nullable_call(other).alias_qualified(relation, name),
            };
            Ok(LogicalPlan::Projection(Projection::try_new(
                vec![wrapped],
                input,
            )?))
        }
        other => {
            let column = Expr::Column(Column::new(relation.clone(), name.clone()));
            let wrapped = scalar_nullable_call(column).alias_qualified(relation, name);
            Ok(LogicalPlan::Projection(Projection::try_new(
                vec![wrapped],
                Arc::new(other),
            )?))
        }
    }
}

pub(crate) fn strip_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if !plan_has_outer_marker(&plan)? {
        return Ok(Transformed::no(plan));
    }
    rewrite_strip_plan(plan)
}

fn plan_has_outer_marker(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_expressions(|expr| {
        found = expr_has_outer_marker(expr)?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    if found {
        return Ok(true);
    }
    for input in plan.inputs() {
        if plan_has_outer_marker(input)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn expr_has_outer_marker(expr: &Expr) -> Result<bool> {
    let mut found = false;
    expr.apply(|node| match node {
        Expr::ScalarSubquery(_)
        | Expr::Exists(_)
        | Expr::InSubquery(_)
        | Expr::SetComparison(_) => Ok(TreeNodeRecursion::Jump),
        Expr::ScalarFunction(call) if call.name() == SCALAR_NULLABLE_NAME => {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        }
        _ => Ok(TreeNodeRecursion::Continue),
    })?;
    Ok(found)
}

fn rewrite_strip_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let Transformed {
        data: plan,
        transformed: children_changed,
        ..
    } = plan.map_children(rewrite_strip_plan)?;
    let preserver = NamePreserver::new(&plan);
    let Transformed {
        data: plan,
        transformed: exprs_changed,
        ..
    } = plan.map_expressions(|expr| {
        let saved = preserver.save(&expr);
        strip_outer_marker_expr(expr)?.map_data(|stripped| Ok(saved.restore(stripped)))
    })?;
    if children_changed || exprs_changed {
        Ok(Transformed::yes(recompute_plan_schema(plan)?))
    } else {
        Ok(Transformed::no(plan))
    }
}

fn strip_outer_marker_expr(expr: Expr) -> Result<Transformed<Expr>> {
    expr.transform_down_up(
        |node| match node {
            Expr::ScalarSubquery(_)
            | Expr::Exists(_)
            | Expr::InSubquery(_)
            | Expr::SetComparison(_) => Ok(Transformed::new(node, false, TreeNodeRecursion::Jump)),
            other => Ok(Transformed::no(other)),
        },
        |node| match node {
            Expr::ScalarFunction(ScalarFunction { func, mut args })
                if func.name() == SCALAR_NULLABLE_NAME && args.len() == 1 =>
            {
                let Some(inner) = args.pop() else {
                    return plan_err!("{SCALAR_NULLABLE_NAME} missing inner expression");
                };
                Ok(Transformed::yes(inner))
            }
            other => Ok(Transformed::no(other)),
        },
    )
}

fn recompute_plan_schema(plan: LogicalPlan) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Union(Union { inputs, schema }) => {
            let fields = schema
                .iter()
                .enumerate()
                .map(|(index, (qualifier, field))| {
                    let input_nullable = inputs.iter().any(|input| {
                        input
                            .schema()
                            .fields()
                            .get(index)
                            .is_some_and(|input_field| input_field.is_nullable())
                    });
                    let updated = Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_nullable(field.is_nullable() || input_nullable),
                    );
                    (qualifier.cloned(), updated)
                })
                .collect();
            let updated_schema = DFSchema::new_with_metadata(fields, schema.metadata().clone())?;
            Ok(LogicalPlan::Union(Union {
                inputs,
                schema: DFSchemaRef::new(updated_schema),
            }))
        }
        LogicalPlan::Values(Values { schema, values }) => {
            let empty = DFSchema::empty();
            let fields = schema
                .iter()
                .enumerate()
                .map(|(index, (qualifier, field))| {
                    let row_nullable = values.iter().any(|row| {
                        row.get(index)
                            .and_then(|expr| expr.nullable(&empty).ok())
                            .unwrap_or(false)
                    });
                    let updated = Arc::new(
                        field
                            .as_ref()
                            .clone()
                            .with_nullable(field.is_nullable() || row_nullable),
                    );
                    (qualifier.cloned(), updated)
                })
                .collect();
            let updated_schema = DFSchema::new_with_metadata(fields, schema.metadata().clone())?;
            Ok(LogicalPlan::Values(Values {
                schema: DFSchemaRef::new(updated_schema),
                values,
            }))
        }
        other => other.recompute_schema(),
    }
}

#[must_use]
pub(crate) fn guaranteed_at_least_one_row(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::EmptyRelation(empty) => empty.produce_one_row,
        LogicalPlan::Values(values) => !values.values.is_empty(),
        LogicalPlan::Aggregate(aggregate) => aggregate.group_expr.is_empty(),
        LogicalPlan::Projection(projection) => {
            !projection_may_drop_rows(&projection.expr)
                && guaranteed_at_least_one_row(projection.input.as_ref())
        }
        LogicalPlan::SubqueryAlias(alias) => guaranteed_at_least_one_row(alias.input.as_ref()),
        LogicalPlan::Subquery(subquery) => guaranteed_at_least_one_row(subquery.subquery.as_ref()),
        LogicalPlan::Sort(sort) => {
            sort.fetch.is_none_or(|fetch| fetch >= 1)
                && guaranteed_at_least_one_row(sort.input.as_ref())
        }
        LogicalPlan::Window(window) => guaranteed_at_least_one_row(window.input.as_ref()),
        LogicalPlan::Repartition(repartition) => {
            guaranteed_at_least_one_row(repartition.input.as_ref())
        }
        LogicalPlan::Distinct(Distinct::All(input)) => guaranteed_at_least_one_row(input.as_ref()),
        LogicalPlan::Distinct(Distinct::On(on)) => guaranteed_at_least_one_row(on.input.as_ref()),
        LogicalPlan::Limit(limit) => {
            limit_keeps_at_least_one_row(limit) && guaranteed_at_least_one_row(limit.input.as_ref())
        }
        LogicalPlan::Filter(filter) => {
            matches!(
                filter.predicate,
                Expr::Literal(ScalarValue::Boolean(Some(true)), _)
            ) && guaranteed_at_least_one_row(filter.input.as_ref())
        }
        LogicalPlan::Union(union) => union
            .inputs
            .iter()
            .any(|input| guaranteed_at_least_one_row(input.as_ref())),
        _ => false,
    }
}

fn projection_may_drop_rows(expressions: &[Expr]) -> bool {
    expressions.iter().any(|expr| {
        expr.exists(|node| {
            Ok(match node {
                Expr::Unnest(_) => true,
                Expr::ScalarFunction(call) => is_row_dropping_generator(call.name()),
                _ => false,
            })
        })
        .unwrap_or(true)
    })
}

fn is_unexpanded_generator_placeholder(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "explode"
            | "explode_outer"
            | "posexplode"
            | "posexplode_outer"
            | "inline"
            | "inline_outer"
            | "json_tuple"
            | "stack"
            | "__repark_gen_alias"
            | "__repark_gen_bare"
    )
}

fn is_row_dropping_generator(name: &str) -> bool {
    is_unexpanded_generator_placeholder(name) || name.eq_ignore_ascii_case("unnest")
}

fn limit_keeps_at_least_one_row(limit: &Limit) -> bool {
    let skip_is_zero = limit
        .skip
        .as_deref()
        .is_none_or(|expr| integer_literal_value(expr) == Some(0));
    let fetch_is_positive = limit
        .fetch
        .as_deref()
        .is_none_or(|expr| integer_literal_value(expr).is_some_and(|value| value >= 1));
    skip_is_zero && fetch_is_positive
}

fn integer_literal_value(expr: &Expr) -> Option<i128> {
    let Expr::Literal(scalar, _) = expr else {
        return None;
    };
    match scalar {
        ScalarValue::Int8(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int16(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int32(Some(value)) => Some(i128::from(*value)),
        ScalarValue::Int64(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt8(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt16(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt32(Some(value)) => Some(i128::from(*value)),
        ScalarValue::UInt64(Some(value)) => Some(i128::from(*value)),
        _ => None,
    }
}

#[cfg(test)]
#[path = "scalar_nullable_tests.rs"]
mod tests;
