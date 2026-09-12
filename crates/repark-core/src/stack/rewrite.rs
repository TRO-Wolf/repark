use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult};
use datafusion::common::{Result, ScalarValue, plan_err};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::AnalyzerRule;

use super::{UnpivotNode, parse_stack_n};

#[derive(Debug, Default)]
pub struct StackRewrite;

impl AnalyzerRule for StackRewrite {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "stack_rewrite"
    }
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Projection(projection) = &plan else {
        return Ok(Transformed::no(plan));
    };
    let mut stack_index = None;
    for (index, expr) in projection.expr.iter().enumerate() {
        if stack_call(expr).is_some() {
            if stack_index.is_some() {
                return plan_err!("Only one generator allowed per select list");
            }
            stack_index = Some(index);
        }
    }
    let Some(stack_index) = stack_index else {
        return Ok(Transformed::no(plan));
    };
    let Some((n, stack_args, output_names)) = stack_call(&projection.expr[stack_index]) else {
        return Ok(Transformed::no(plan));
    };
    let n = parse_stack_n(n)
        .map_err(|error| datafusion::common::DataFusionError::Plan(error.to_string()))?;
    let mut passthrough: Vec<Expr> = Vec::new();
    passthrough.extend(
        projection
            .expr
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != stack_index)
            .map(|(_, expr)| expr.clone()),
    );
    let passthrough_count = passthrough.len();
    let mut projected = passthrough;
    projected.extend(stack_args);
    let input = LogicalPlanBuilder::from(projection.input.as_ref().clone())
        .project(projected)?
        .build()?;
    let node = UnpivotNode::try_new(input, n, passthrough_count, output_names.as_deref())?;
    Ok(Transformed::yes(LogicalPlan::Extension(Extension {
        node: Arc::new(node),
    })))
}

fn stack_call(expr: &Expr) -> Option<(i64, Vec<Expr>, Option<Vec<String>>)> {
    match expr {
        Expr::Alias(alias) => {
            let (n, args, _) = stack_call(alias.expr.as_ref())?;
            Some((n, args, Some(vec![alias.name.clone()])))
        }
        Expr::ScalarFunction(function) if is_stack(function) => {
            if function.args.len() < 2 {
                return None;
            }
            let n = literal_n(&function.args[0])?;
            Some((n, function.args[1..].to_vec(), None))
        }
        _ => None,
    }
}

fn is_stack(function: &ScalarFunction) -> bool {
    function.func.name() == "stack"
}

fn literal_n(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Literal(value, _) => match value {
            ScalarValue::Int8(Some(value)) => Some(i64::from(*value)),
            ScalarValue::Int16(Some(value)) => Some(i64::from(*value)),
            ScalarValue::Int32(Some(value)) => Some(i64::from(*value)),
            ScalarValue::Int64(Some(value)) => Some(*value),
            ScalarValue::UInt8(Some(value)) => Some(i64::from(*value)),
            ScalarValue::UInt16(Some(value)) => Some(i64::from(*value)),
            ScalarValue::UInt32(Some(value)) => Some(i64::from(*value)),
            ScalarValue::UInt64(Some(value)) => i64::try_from(*value).ok(),
            _ => None,
        },
        Expr::Cast(cast) => literal_n(cast.expr.as_ref()),
        Expr::Alias(alias) => literal_n(alias.expr.as_ref()),
        _ => None,
    }
}
