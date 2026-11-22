use std::sync::{Arc, LazyLock};

use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, exec_err, plan_err};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};

use super::{node_schema, reads_input};

pub(crate) const STATS_BARRIER_NAME: &str = "__repark_stats_barrier__";

const RULE_NAME: &str = "repark_stats_barrier";

static STATS_BARRIER: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| {
    Arc::new(ScalarUDF::from(StatsBarrier {
        signature: Signature::any(1, Volatility::Immutable),
    }))
});

#[must_use]
pub(crate) fn stats_barrier_udf() -> Arc<ScalarUDF> {
    Arc::clone(&STATS_BARRIER)
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct StatsBarrier {
    signature: Signature,
}

impl ScalarUDFImpl for StatsBarrier {
    fn name(&self) -> &str {
        STATS_BARRIER_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [data_type] = arg_types else {
            return plan_err!("{STATS_BARRIER_NAME} takes one argument");
        };
        Ok(data_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [field] = args.arg_fields else {
            return plan_err!("{STATS_BARRIER_NAME} takes one argument");
        };
        Ok(Arc::new(field.as_ref().clone().with_name(self.name())))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let mut values = args.args;
        let (Some(value), true) = (values.pop(), values.is_empty()) else {
            return exec_err!("{STATS_BARRIER_NAME} takes one argument");
        };
        Ok(value)
    }
}

#[derive(Debug, Default)]
pub(crate) struct StatsBarrierRule;

impl OptimizerRule for StatsBarrierRule {
    fn name(&self) -> &str {
        RULE_NAME
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        barrier_plan(plan)
    }
}

pub(crate) fn barrier_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if !plan_has_candidate(&plan)? {
        return Ok(Transformed::no(plan));
    }
    plan.transform_up_with_subqueries(barrier_node)
}

fn plan_has_candidate(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        found = node_has_candidate(node)?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn node_has_candidate(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_expressions(|expr| {
        found = expr.exists(|node| Ok(is_candidate(node)))?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

fn is_candidate(expr: &Expr) -> bool {
    matches!(expr, Expr::Cast(Cast { expr: operand, .. }) if is_candidate_operand(operand))
}

fn is_barrier(expr: &Expr) -> bool {
    matches!(expr, Expr::ScalarFunction(call) if call.name() == STATS_BARRIER_NAME)
}

fn barrier_node(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    if !node_has_candidate(&plan)? {
        return Ok(Transformed::no(plan));
    }
    let schema = node_schema(&plan)?;
    let preserver = NamePreserver::new(&plan);
    let rewritten = plan.map_expressions(|expr| {
        let saved = preserver.save(&expr);
        let barriered = expr.transform_up(|node| Ok(barrier_cast(node, &schema)))?;
        Ok(barriered.update_data(|node| saved.restore(node)))
    })?;
    if rewritten.transformed {
        rewritten.map_data(LogicalPlan::recompute_schema)
    } else {
        Ok(rewritten)
    }
}

fn barrier_cast(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    match expr {
        Expr::Cast(Cast {
            expr: operand,
            field,
        }) if needs_barrier(&operand, &field, schema) => Transformed::yes(Expr::Cast(Cast {
            expr: Box::new(stats_barrier_udf().call(vec![*operand])),
            field,
        })),
        other => Transformed::no(other),
    }
}

fn needs_barrier(operand: &Expr, field: &FieldRef, schema: &DFSchema) -> bool {
    is_candidate_operand(operand)
        && operand
            .get_type(schema)
            .is_ok_and(|source| !keeps_order(&source, field.data_type()))
}

fn is_candidate_operand(operand: &Expr) -> bool {
    !is_barrier(operand) && reads_input(operand)
}

#[must_use]
pub(crate) fn keeps_order(source: &DataType, target: &DataType) -> bool {
    let (source, target) = (value_type(source), value_type(target));
    source == target
        || (source.is_numeric() || *source == DataType::Boolean)
            && target.is_numeric()
            && float_keeps_order(source, target)
        || is_text(source) && is_text(target)
        || is_binary(source) && is_binary(target)
        || temporal_keeps_order(source, target)
}

fn float_keeps_order(source: &DataType, target: &DataType) -> bool {
    match (float_width(source), float_width(target)) {
        (None, _) => true,
        (Some(from), Some(to)) => from <= to,
        (Some(_), None) => false,
    }
}

fn float_width(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Float16 => Some(16),
        DataType::Float32 => Some(32),
        DataType::Float64 => Some(64),
        _ => None,
    }
}

fn value_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => value.as_ref(),
        other => other,
    }
}

fn is_text(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_binary(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView
    )
}

fn temporal_keeps_order(source: &DataType, target: &DataType) -> bool {
    match (source, target) {
        (
            DataType::Date32 | DataType::Date64,
            DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None),
        )
        | (DataType::Timestamp(_, None), DataType::Date32 | DataType::Date64) => true,
        (DataType::Timestamp(_, from), DataType::Timestamp(_, to)) => {
            from.is_some() == to.is_some()
        }
        _ => false,
    }
}
