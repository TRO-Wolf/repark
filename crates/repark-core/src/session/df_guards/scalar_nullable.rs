use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::metadata::FieldMetadata;
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
    rules.insert(position, Arc::new(ReparkScalarSubqueryNullability));
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
}

fn rewrite_wrap_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let Transformed {
        data: plan,
        transformed: children_changed,
        ..
    } = plan.map_children(rewrite_wrap_plan)?;
    let preserver = NamePreserver::new(&plan);
    let Transformed {
        data: plan,
        transformed: exprs_changed,
        ..
    } = plan.map_expressions(|expr| {
        let saved = preserver.save(&expr);
        expr.transform_up(rewrite_wrap_expr)?
            .map_data(|rewritten| Ok(saved.restore(rewritten)))
    })?;
    if children_changed || exprs_changed {
        Ok(Transformed::yes(recompute_plan_schema(plan)?))
    } else {
        Ok(Transformed::no(plan))
    }
}

fn rewrite_wrap_expr(expr: Expr) -> Result<Transformed<Expr>> {
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
        other => Ok(Transformed::no(other)),
    }
}

fn wrap_subquery_output(plan: LogicalPlan) -> Result<LogicalPlan> {
    let (relation, field) = plan.schema().qualified_field(0);
    let relation = relation.cloned();
    let name = field.name().clone();
    let metadata = field_metadata(field);
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
                other => scalar_nullable_call(other)
                    .alias_qualified_with_metadata(relation, name, metadata),
            };
            Ok(LogicalPlan::Projection(Projection::try_new(
                vec![wrapped],
                input,
            )?))
        }
        other => {
            let column = Expr::Column(Column::new(relation.clone(), name.clone()));
            let wrapped = scalar_nullable_call(column)
                .alias_qualified_with_metadata(relation, name, metadata);
            Ok(LogicalPlan::Projection(Projection::try_new(
                vec![wrapped],
                Arc::new(other),
            )?))
        }
    }
}

fn field_metadata(field: &Field) -> Option<FieldMetadata> {
    (!field.metadata().is_empty()).then(|| FieldMetadata::from(field.metadata().clone()))
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

fn is_row_dropping_generator(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "explode"
            | "explode_outer"
            | "posexplode"
            | "posexplode_outer"
            | "inline"
            | "inline_outer"
            | "stack"
            | "unnest"
            | "__repark_gen_alias"
    )
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
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, Int64Array, ListArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::common::{DFSchema, ScalarValue};
    use datafusion::datasource::MemTable;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::logical_expr::expr::{AggregateFunction, Sort, Unnest};
    use datafusion::logical_expr::logical_plan::{
        Aggregate, Distinct, EmptyRelation, Filter, Limit, Projection, Sort as SortPlan,
        SubqueryAlias, Union, Values,
    };
    use datafusion::logical_expr::{Expr, GroupingSet, LogicalPlan, LogicalPlanBuilder, col, lit};
    use datafusion::optimizer::{AnalyzerRule, OptimizerContext, OptimizerRule};

    use super::{
        ANALYZER_RULE_NAME, ReparkScalarSubqueryNullability, SCALAR_NULLABLE_NAME,
        StripScalarNullable, guaranteed_at_least_one_row, insert_before_coercion, udf,
    };
    use crate::ReparkSession;

    fn one_row_plan() -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: true,
            schema: Arc::new(DFSchema::empty()),
        })
    }

    fn zero_row_plan() -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        })
    }

    fn count_star_expr() -> Expr {
        Expr::AggregateFunction(AggregateFunction::new_udf(
            count_udaf(),
            vec![lit(1_i64)],
            false,
            None,
            vec![],
            None,
        ))
    }

    fn nonnull_scan_plan() -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
        let table = MemTable::try_new(schema, vec![vec![]]).unwrap();
        LogicalPlanBuilder::scan(
            "empty_nn",
            datafusion::datasource::provider_as_source(Arc::new(table)),
            None,
        )
        .unwrap()
        .build()
        .unwrap()
    }

    #[test]
    fn row_guarantee_empty_relations_and_values() {
        assert!(guaranteed_at_least_one_row(&one_row_plan()));
        assert!(!guaranteed_at_least_one_row(&zero_row_plan()));
        let non_empty_values = LogicalPlan::Values(Values {
            schema: Arc::new(
                DFSchema::from_unqualified_fields(
                    vec![Field::new("c", DataType::Int32, false)].into(),
                    HashMap::new(),
                )
                .unwrap(),
            ),
            values: vec![vec![lit(1_i32)]],
        });
        assert!(guaranteed_at_least_one_row(&non_empty_values));
        let empty_values = LogicalPlan::Values(Values {
            schema: Arc::new(
                DFSchema::from_unqualified_fields(
                    vec![Field::new("c", DataType::Int32, false)].into(),
                    HashMap::new(),
                )
                .unwrap(),
            ),
            values: vec![],
        });
        assert!(!guaranteed_at_least_one_row(&empty_values));
        assert!(!guaranteed_at_least_one_row(&nonnull_scan_plan()));
    }

    #[test]
    fn row_guarantee_aggregates_and_grouping_sets() {
        let scan = Arc::new(nonnull_scan_plan());
        let ungrouped = LogicalPlan::Aggregate(
            Aggregate::try_new(Arc::clone(&scan), vec![], vec![count_star_expr()]).unwrap(),
        );
        assert!(guaranteed_at_least_one_row(&ungrouped));
        let grouped = LogicalPlan::Aggregate(
            Aggregate::try_new(Arc::clone(&scan), vec![col("b")], vec![count_star_expr()]).unwrap(),
        );
        assert!(!guaranteed_at_least_one_row(&grouped));
        let rollup = LogicalPlan::Aggregate(
            Aggregate::try_new(
                Arc::clone(&scan),
                vec![Expr::GroupingSet(GroupingSet::Rollup(vec![col("b")]))],
                vec![count_star_expr()],
            )
            .unwrap(),
        );
        assert!(!guaranteed_at_least_one_row(&rollup));
        let cube = LogicalPlan::Aggregate(
            Aggregate::try_new(
                Arc::clone(&scan),
                vec![Expr::GroupingSet(GroupingSet::Cube(vec![col("b")]))],
                vec![count_star_expr()],
            )
            .unwrap(),
        );
        assert!(!guaranteed_at_least_one_row(&cube));
        let grouping_sets = LogicalPlan::Aggregate(
            Aggregate::try_new(
                Arc::clone(&scan),
                vec![Expr::GroupingSet(GroupingSet::GroupingSets(vec![vec![]]))],
                vec![count_star_expr()],
            )
            .unwrap(),
        );
        assert!(!guaranteed_at_least_one_row(&grouping_sets));
        let with_having = LogicalPlan::Filter(
            Filter::try_new(col("count(Int64(1))").gt(lit(0_i64)), Arc::new(ungrouped)).unwrap(),
        );
        assert!(!guaranteed_at_least_one_row(&with_having));
    }

    #[test]
    fn row_guarantee_projection_sort_limit_filter_distinct_union() {
        let lit_proj = LogicalPlan::Projection(
            Projection::try_new(vec![lit(1_i32)], Arc::new(one_row_plan())).unwrap(),
        );
        assert!(guaranteed_at_least_one_row(&lit_proj));
        let empty_list = ScalarValue::List(Arc::new(ListArray::new_null(
            Arc::new(Field::new("item", DataType::Int32, true)),
            1,
        )));
        let unnest_proj = LogicalPlan::Projection(
            Projection::try_new(
                vec![Expr::Unnest(Unnest::new(lit(empty_list)))],
                Arc::new(one_row_plan()),
            )
            .unwrap(),
        );
        assert!(!guaranteed_at_least_one_row(&unnest_proj));
        let sort_no_fetch = LogicalPlan::Sort(SortPlan {
            expr: vec![Sort::new(lit(1_i32), true, false)],
            input: Arc::new(lit_proj.clone()),
            fetch: None,
        });
        assert!(guaranteed_at_least_one_row(&sort_no_fetch));
        let sort_fetch_one = LogicalPlan::Sort(SortPlan {
            expr: vec![Sort::new(lit(1_i32), true, false)],
            input: Arc::new(lit_proj.clone()),
            fetch: Some(1),
        });
        assert!(guaranteed_at_least_one_row(&sort_fetch_one));
        let sort_fetch_zero = LogicalPlan::Sort(SortPlan {
            expr: vec![Sort::new(lit(1_i32), true, false)],
            input: Arc::new(lit_proj.clone()),
            fetch: Some(0),
        });
        assert!(!guaranteed_at_least_one_row(&sort_fetch_zero));
        let limit_one = LogicalPlan::Limit(Limit {
            skip: Some(Box::new(lit(0_i32))),
            fetch: Some(Box::new(lit(1_u64))),
            input: Arc::new(lit_proj.clone()),
        });
        assert!(guaranteed_at_least_one_row(&limit_one));
        let limit_zero = LogicalPlan::Limit(Limit {
            skip: None,
            fetch: Some(Box::new(lit(0_i64))),
            input: Arc::new(lit_proj.clone()),
        });
        assert!(!guaranteed_at_least_one_row(&limit_zero));
        let limit_offset = LogicalPlan::Limit(Limit {
            skip: Some(Box::new(lit(1_i64))),
            fetch: Some(Box::new(lit(1_i64))),
            input: Arc::new(lit_proj.clone()),
        });
        assert!(!guaranteed_at_least_one_row(&limit_offset));
        let filter_true =
            LogicalPlan::Filter(Filter::try_new(lit(true), Arc::new(lit_proj.clone())).unwrap());
        assert!(guaranteed_at_least_one_row(&filter_true));
        let filter_false =
            LogicalPlan::Filter(Filter::try_new(lit(false), Arc::new(lit_proj.clone())).unwrap());
        assert!(!guaranteed_at_least_one_row(&filter_false));
        let distinct_all = LogicalPlan::Distinct(Distinct::All(Arc::new(lit_proj.clone())));
        assert!(guaranteed_at_least_one_row(&distinct_all));
        let alias = LogicalPlan::SubqueryAlias(
            SubqueryAlias::try_new(Arc::new(lit_proj.clone()), "a").unwrap(),
        );
        assert!(guaranteed_at_least_one_row(&alias));
        let zero_proj = LogicalPlan::Projection(
            Projection::try_new(vec![lit(1_i32)], Arc::new(zero_row_plan())).unwrap(),
        );
        let union_guaranteed = LogicalPlan::Union(
            Union::try_new(vec![Arc::new(zero_proj.clone()), Arc::new(lit_proj)]).unwrap(),
        );
        assert!(guaranteed_at_least_one_row(&union_guaranteed));
        let union_empty = LogicalPlan::Union(
            Union::try_new(vec![Arc::new(zero_proj.clone()), Arc::new(zero_proj)]).unwrap(),
        );
        assert!(!guaranteed_at_least_one_row(&union_empty));
    }

    #[test]
    fn seat_helper_places_rule_immediately_before_type_coercion() {
        let rules = insert_before_coercion(datafusion::optimizer::Analyzer::new().rules).unwrap();
        let names: Vec<&str> = rules.iter().map(|rule| rule.name()).collect();
        let seat = names
            .iter()
            .position(|name| *name == ANALYZER_RULE_NAME)
            .unwrap();
        let coercion = names
            .iter()
            .position(|name| *name == "type_coercion")
            .unwrap();
        assert_eq!(seat + 1, coercion);
    }

    #[tokio::test]
    async fn analyzer_wraps_empty_subquery_and_keeps_count_star_nonnull() {
        let session = ReparkSession::new().unwrap();
        let empty_schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, false)]));
        let empty_tbl = MemTable::try_new(empty_schema, vec![vec![]]).unwrap();
        session
            .context()
            .register_table("empty_nn", Arc::new(empty_tbl))
            .unwrap();
        let left_schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let left_batch = RecordBatch::try_new(
            Arc::clone(&left_schema),
            vec![Arc::new(Int32Array::from(vec![1, 2]))],
        )
        .unwrap();
        let left_tbl = MemTable::try_new(left_schema, vec![vec![left_batch]]).unwrap();
        session
            .context()
            .register_table("l_nn", Arc::new(left_tbl))
            .unwrap();

        let empty_df = session
            .sql("SELECT (SELECT b FROM empty_nn) IS NULL AS r")
            .await
            .unwrap();
        let empty_opt = empty_df.clone().into_optimized_plan().unwrap();
        let empty_plan_text = empty_opt.display_indent().to_string();
        assert!(!empty_plan_text.contains(SCALAR_NULLABLE_NAME));
        let empty_batches = empty_df.collect().await.unwrap();
        let flag = empty_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap()
            .value(0);
        assert!(flag);

        let count_df = session
            .sql("SELECT (SELECT COUNT(*) FROM empty_nn) IS NULL AS r")
            .await
            .unwrap();
        let count_opt = count_df.clone().into_optimized_plan().unwrap();
        assert!(
            count_opt
                .display_indent()
                .to_string()
                .contains("Boolean(false)")
        );
        let count_batches = count_df.collect().await.unwrap();
        let count_flag = count_batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap()
            .value(0);
        assert!(!count_flag);

        let cte_df = session
            .sql("WITH c AS (SELECT id, (SELECT b FROM empty_nn) AS r FROM l_nn) SELECT id, r IS NULL AS n, COALESCE(r, 42) AS c FROM c ORDER BY id")
            .await
            .unwrap();
        let cte_batches = cte_df.collect().await.unwrap();
        let n_col = cte_batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::BooleanArray>()
            .unwrap();
        let c_col = cte_batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(n_col.value(0) && n_col.value(1));
        assert_eq!(c_col.value(0), 42);
        assert_eq!(c_col.value(1), 42);

        let raw_plan = session
            .context()
            .state()
            .create_logical_plan("SELECT (SELECT b FROM empty_nn) AS r")
            .await
            .unwrap();
        let analyzed_once = ReparkScalarSubqueryNullability
            .analyze(raw_plan, session.context().state().config_options())
            .unwrap();
        assert!(analyzed_once.schema().field(0).is_nullable());
        let optimizer_ctx = OptimizerContext::new();
        let second = ReparkScalarSubqueryNullability
            .rewrite(analyzed_once.clone(), &optimizer_ctx)
            .unwrap();
        assert!(!second.transformed);
        let stripped_surviving = StripScalarNullable
            .rewrite(analyzed_once, &optimizer_ctx)
            .unwrap();
        assert!(!stripped_surviving.transformed);
        assert!(udf().name() == SCALAR_NULLABLE_NAME);
    }
}
