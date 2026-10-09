use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{BinaryExpr, Expr, ExprSchemable, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;
use repark_functions::cast_map::spark_sql_name;

use super::map_ordering::{input_schema, render};

#[derive(Debug, Default)]
pub struct JoinConditionRefusals;

impl AnalyzerRule for JoinConditionRefusals {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.apply_with_subqueries(|node| {
            if let LogicalPlan::Join(join) = node
                && let Some(filter) = &join.filter
            {
                refuse_filter(filter, &input_schema(node))?;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(plan)
    }

    fn name(&self) -> &'static str {
        "join_condition_refusals"
    }
}

pub fn insert_join_condition_rule_before_coercion(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules.iter().position(|rule| rule.name() == "type_coercion") else {
        return Err(DataFusionError::Plan(
            "join condition refusals require the default type_coercion analyzer rule".to_string(),
        ));
    };
    rules.insert(position, Arc::new(JoinConditionRefusals));
    Ok(rules)
}

fn refuse_filter(filter: &Expr, schema: &DFSchema) -> Result<()> {
    if let Ok(data_type) = filter.get_type(schema)
        && data_type != DataType::Boolean
    {
        return Err(not_boolean(filter, &data_type));
    }
    if expression_is_nondeterministic(filter) {
        return Err(nondeterministic(filter));
    }
    Ok(())
}

fn not_boolean(filter: &Expr, data_type: &DataType) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE] The join condition \"{}\" has the invalid type \
         \"{}\", expected \"BOOLEAN\". SQLSTATE: 42K0E",
        render(filter),
        spark_sql_name(data_type)
    ))
}

fn nondeterministic(filter: &Expr) -> DataFusionError {
    DataFusionError::Plan(format!(
        "[INVALID_NON_DETERMINISTIC_EXPRESSIONS] The operator expects a deterministic expression, \
         but the actual expression is \"({})\". SQLSTATE: 42K0E",
        render_full(filter)
    ))
}

fn render_full(expr: &Expr) -> String {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            format!("{} {op} {}", parenthesized(left), parenthesized(right))
        }
        other => render(other),
    }
}

fn parenthesized(expr: &Expr) -> String {
    match expr {
        binary @ Expr::BinaryExpr(_) => format!("({})", render_full(binary)),
        other => render(other),
    }
}

fn expression_is_nondeterministic(expr: &Expr) -> bool {
    let mut nested: Vec<LogicalPlan> = Vec::new();
    let mut found = false;
    let _ = expr.apply(|node| {
        if found {
            return Ok(TreeNodeRecursion::Stop);
        }
        match node {
            Expr::ScalarFunction(function)
                if repark_core::frame_names::NONDETERMINISTIC_FUNCTION_NAMES
                    .contains(&function.name().to_ascii_lowercase().as_str()) =>
            {
                found = true;
                Ok(TreeNodeRecursion::Stop)
            }
            Expr::ScalarSubquery(query) => {
                nested.push(query.subquery.as_ref().clone());
                Ok(TreeNodeRecursion::Continue)
            }
            Expr::Exists(exists) => {
                nested.push(exists.subquery.subquery.as_ref().clone());
                Ok(TreeNodeRecursion::Continue)
            }
            Expr::InSubquery(query) => {
                nested.push(query.subquery.subquery.as_ref().clone());
                Ok(TreeNodeRecursion::Continue)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        }
    });
    found || nested.iter().any(plan_is_nondeterministic)
}

fn plan_is_nondeterministic(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if found {
            return Ok(TreeNodeRecursion::Stop);
        }
        if node
            .expressions()
            .iter()
            .any(expression_is_nondeterministic)
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}
