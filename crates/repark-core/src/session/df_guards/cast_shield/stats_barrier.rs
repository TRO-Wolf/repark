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
            && numeric_keeps_order(source, target)
        || is_text(source) && is_text(target)
        || is_binary(source) && is_binary(target)
        || temporal_keeps_order(source, target)
}

fn numeric_keeps_order(source: &DataType, target: &DataType) -> bool {
    if let Some(from) = float_width(source) {
        return float_width(target).is_some_and(|to| from <= to);
    }
    if let Some(to) = float_width(target) {
        return non_float_fits_float(source, to);
    }
    if let (Some((from_low, from_high)), Some((to_low, to_high))) =
        (int_bounds(source), int_bounds(target))
    {
        return from_low >= to_low && from_high <= to_high;
    }
    if let (Some((precision, scale)), Some((to_low, to_high))) =
        (decimal_shape(source), int_bounds(target))
    {
        return to_low < 0
            && decimal_max_integer(precision, scale)
                .is_some_and(|top| to_low <= -top && top <= to_high);
    }
    if let (Some((from_precision, from_scale)), Some((to_precision, to_scale))) =
        (decimal_shape(source), decimal_shape(target))
    {
        let from_int = i32::from(from_precision) - i32::from(from_scale);
        let to_int = i32::from(to_precision) - i32::from(to_scale);
        return if to_scale < from_scale {
            to_int > from_int
        } else {
            to_int >= from_int
        };
    }
    if let (Some((from_low, from_high)), Some((to_precision, to_scale))) =
        (int_bounds(source), decimal_shape(target))
    {
        if to_scale < 0 {
            return false;
        }
        let integer_digits = i32::from(to_precision) - i32::from(to_scale);
        if integer_digits >= 20 {
            return true;
        }
        let max_abs = from_low.unsigned_abs().max(from_high.unsigned_abs());
        return decimal_max_integer(to_precision, to_scale)
            .and_then(|top| u128::try_from(top).ok())
            .is_some_and(|top| max_abs <= top);
    }
    false
}

fn non_float_fits_float(source: &DataType, target_bits: u8) -> bool {
    match target_bits {
        64 => true,
        32 => decimal_shape(source)
            .is_none_or(|(precision, scale)| i32::from(precision) - i32::from(scale) <= 38),
        16 => matches!(source, DataType::Boolean | DataType::Int8 | DataType::UInt8),
        _ => false,
    }
}

fn int_bounds(data_type: &DataType) -> Option<(i128, i128)> {
    match data_type {
        DataType::Boolean => Some((0, 1)),
        DataType::Int8 => Some((i128::from(i8::MIN), i128::from(i8::MAX))),
        DataType::Int16 => Some((i128::from(i16::MIN), i128::from(i16::MAX))),
        DataType::Int32 => Some((i128::from(i32::MIN), i128::from(i32::MAX))),
        DataType::Int64 => Some((i128::from(i64::MIN), i128::from(i64::MAX))),
        DataType::UInt8 => Some((0, i128::from(u8::MAX))),
        DataType::UInt16 => Some((0, i128::from(u16::MAX))),
        DataType::UInt32 => Some((0, i128::from(u32::MAX))),
        DataType::UInt64 => Some((0, i128::from(u64::MAX))),
        _ => None,
    }
}

fn decimal_shape(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale)
        | DataType::Decimal256(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}

fn decimal_max_integer(precision: u8, scale: i8) -> Option<i128> {
    let integer_digits = i32::from(precision) - i32::from(scale);
    if integer_digits <= 0 {
        return Some(0);
    }
    let exponent = u32::try_from(integer_digits).ok()?;
    10i128.checked_pow(exponent)?.checked_sub(1)
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
