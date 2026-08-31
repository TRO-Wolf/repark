//! Spark decimal division, DEC-8 planning, and DEC-6 overflow checks.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Decimal128Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, i256};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, exec_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::logical_expr::sqlparser::ast::BinaryOperator;
use datafusion::logical_expr::{
    BinaryExpr, ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;

use crate::ansi::spark_ansi_enabled_from_options;
use crate::decimal_precision::{
    arrow_result_type, decimal128_parts, spark_div_result_type, spark_result_type,
};

pub(crate) const DECIMAL_DIV_NAME: &str = "__repark_spark_decimal_div__";
pub(crate) const DECIMAL_ADD_NAME: &str = "__repark_spark_decimal_add__";
pub(crate) const DECIMAL_SUB_NAME: &str = "__repark_spark_decimal_sub__";
pub(crate) const DECIMAL_MUL_NAME: &str = "__repark_spark_decimal_mul__";

/// Install the DEC-8 `ExprPlanner`.
pub fn register_spark_decimal_planner(ctx: &SessionContext) {
    let planner: Arc<dyn ExprPlanner> = Arc::new(SparkDecimalExprPlanner);
    let _ = ctx.state_ref().write().register_expr_planner(planner);
}

/// Rewrite clean decimal division and overflow-capable `(38, ·)` add/sub/mul onto UDFs.
#[derive(Debug, Default)]
pub struct SparkDecimalRewrite;

impl AnalyzerRule for SparkDecimalRewrite {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)] // `AnalyzerRule::name` ties the lifetime to &self
    fn name(&self) -> &str {
        "spark_decimal_rewrite"
    }
}

/// DEC-8: replace Arrow-refusing decimal `*` at SQL plan construction.
#[derive(Debug)]
pub struct SparkDecimalExprPlanner;

impl ExprPlanner for SparkDecimalExprPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        if expr.op != BinaryOperator::Multiply {
            return Ok(PlannerResult::Original(expr));
        }
        let (Ok(left_type), Ok(right_type)) =
            (expr.left.get_type(schema), expr.right.get_type(schema))
        else {
            return Ok(PlannerResult::Original(expr));
        };
        let Some(left_decimal) = decimal128_parts(&left_type) else {
            return Ok(PlannerResult::Original(expr));
        };
        let Some(right_decimal) = decimal128_parts(&right_type) else {
            return Ok(PlannerResult::Original(expr));
        };
        if arrow_result_type(Operator::Multiply, left_decimal, right_decimal).is_some() {
            return Ok(PlannerResult::Original(expr));
        }
        Ok(PlannerResult::Planned(udf_call(
            spark_decimal_mul_udf(),
            expr.left,
            expr.right,
        )))
    }
}

fn rewrite_plan(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten = expr.transform_down(|node| Ok(rewrite_expr(node, &schema)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    if let Expr::ScalarFunction(function) = &expr
        && is_spark_decimal_udf(function.func.name())
    {
        return Transformed::new(expr, false, TreeNodeRecursion::Stop);
    }
    // U4a CAST-after: leave the type-only wrap so literal non-null pins stay.
    if let Expr::Cast(cast) = &expr
        && let Expr::BinaryExpr(inner) = cast.expr.as_ref()
        && matches!(
            inner.op,
            Operator::Plus | Operator::Minus | Operator::Multiply
        )
    {
        return Transformed::new(expr, false, TreeNodeRecursion::Stop);
    }
    let Expr::BinaryExpr(binary) = expr else {
        return Transformed::no(expr);
    };
    match binary.op {
        Operator::Divide => rewrite_decimal_div(binary, schema),
        Operator::Plus | Operator::Minus => rewrite_overflow_capable(binary, schema),
        _ => Transformed::no(Expr::BinaryExpr(binary)),
    }
}

fn rewrite_decimal_div(binary: BinaryExpr, schema: &DFSchema) -> Transformed<Expr> {
    let (Ok(left_type), Ok(right_type)) =
        (binary.left.get_type(schema), binary.right.get_type(schema))
    else {
        return Transformed::no(Expr::BinaryExpr(binary));
    };
    if decimal128_parts(&left_type).is_none() || decimal128_parts(&right_type).is_none() {
        return Transformed::no(Expr::BinaryExpr(binary));
    }
    Transformed::yes(udf_call(
        spark_decimal_div_udf(),
        *binary.left,
        *binary.right,
    ))
}

/// DEC-6: wrap only when Spark and Arrow already agree on precision 38 (no CAST-after).
fn rewrite_overflow_capable(binary: BinaryExpr, schema: &DFSchema) -> Transformed<Expr> {
    let (Ok(left_type), Ok(right_type)) =
        (binary.left.get_type(schema), binary.right.get_type(schema))
    else {
        return Transformed::no(Expr::BinaryExpr(binary));
    };
    let Some(left_decimal) = decimal128_parts(&left_type) else {
        return Transformed::no(Expr::BinaryExpr(binary));
    };
    let Some(right_decimal) = decimal128_parts(&right_type) else {
        return Transformed::no(Expr::BinaryExpr(binary));
    };
    let Some(spark) = spark_result_type(binary.op, left_decimal, right_decimal) else {
        return Transformed::no(Expr::BinaryExpr(binary));
    };
    let Some(arrow) = arrow_result_type(binary.op, left_decimal, right_decimal) else {
        return Transformed::no(Expr::BinaryExpr(binary));
    };
    if spark != arrow || spark.0 != 38 {
        return Transformed::no(Expr::BinaryExpr(binary));
    }
    let Some(udf) = arith_udf(binary.op) else {
        return Transformed::no(Expr::BinaryExpr(binary));
    };
    Transformed::yes(udf_call(udf, *binary.left, *binary.right))
}

fn arith_udf(operator: Operator) -> Option<Arc<ScalarUDF>> {
    match operator {
        Operator::Plus => Some(spark_decimal_add_udf()),
        Operator::Minus => Some(spark_decimal_sub_udf()),
        Operator::Multiply => Some(spark_decimal_mul_udf()),
        _ => None,
    }
}

fn is_spark_decimal_udf(name: &str) -> bool {
    matches!(
        name,
        DECIMAL_DIV_NAME | DECIMAL_ADD_NAME | DECIMAL_SUB_NAME | DECIMAL_MUL_NAME
    )
}

fn udf_call(udf: Arc<ScalarUDF>, left: Expr, right: Expr) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(udf, vec![left, right]))
}

#[must_use]
pub fn spark_decimal_div_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkDecimalDiv::new()))
}

#[must_use]
pub fn spark_decimal_add_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkDecimalAdd::new()))
}

#[must_use]
pub fn spark_decimal_sub_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkDecimalSub::new()))
}

#[must_use]
pub fn spark_decimal_mul_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkDecimalMul::new()))
}

macro_rules! decimal_arith_udf {
    ($type_name:ident, $name_literal:literal, $operator:expr, $result_fn:expr) => {
        #[derive(Debug)]
        struct $type_name {
            signature: Signature,
        }

        impl $type_name {
            fn new() -> Self {
                Self {
                    signature: Signature::any(2, Volatility::Immutable),
                }
            }
        }

        impl PartialEq for $type_name {
            fn eq(&self, _other: &Self) -> bool {
                true
            }
        }

        impl Eq for $type_name {}

        impl Hash for $type_name {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.name().hash(state);
            }
        }

        impl ScalarUDFImpl for $type_name {
            crate::shim_udf_boilerplate!($name_literal);

            fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
                let (left, right) = two_decimal_parts(arg_types, $name_literal)?;
                let (precision, scale) = $result_fn(left, right).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "'{}' cannot compute a Spark result type for {left:?} and {right:?}",
                        $name_literal
                    ))
                })?;
                Ok(DataType::Decimal128(precision, scale))
            }

            fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
                let arg_types: Vec<DataType> = args
                    .arg_fields
                    .iter()
                    .map(|field| field.data_type().clone())
                    .collect();
                let data_type = self.return_type(&arg_types)?;
                let nullable = matches!(
                    $name_literal,
                    DECIMAL_DIV_NAME | DECIMAL_ADD_NAME | DECIMAL_SUB_NAME
                ) || args.arg_fields.iter().any(|field| field.is_nullable());
                Ok(Arc::new(Field::new($name_literal, data_type, nullable)))
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                invoke_decimal_op($operator, &args)
            }
        }
    };
}

decimal_arith_udf!(
    SparkDecimalDiv,
    "__repark_spark_decimal_div__",
    Operator::Divide,
    spark_div_result_type
);
decimal_arith_udf!(
    SparkDecimalAdd,
    "__repark_spark_decimal_add__",
    Operator::Plus,
    |left, right| spark_result_type(Operator::Plus, left, right)
);
decimal_arith_udf!(
    SparkDecimalSub,
    "__repark_spark_decimal_sub__",
    Operator::Minus,
    |left, right| spark_result_type(Operator::Minus, left, right)
);
decimal_arith_udf!(
    SparkDecimalMul,
    "__repark_spark_decimal_mul__",
    Operator::Multiply,
    |left, right| spark_result_type(Operator::Multiply, left, right)
);

fn two_decimal_parts(arg_types: &[DataType], name: &str) -> Result<((u8, i8), (u8, i8))> {
    if arg_types.len() != 2 {
        return exec_err!("'{name}' expects two decimal arguments");
    }
    let left = decimal128_parts(&arg_types[0]).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "'{name}' expects DECIMAL128 arguments, got {}",
            arg_types[0]
        ))
    })?;
    let right = decimal128_parts(&arg_types[1]).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "'{name}' expects DECIMAL128 arguments, got {}",
            arg_types[1]
        ))
    })?;
    Ok((left, right))
}

pub(crate) fn try_decimal_op(
    operator: Operator,
    args: &ScalarFunctionArgs,
) -> Result<ColumnarValue> {
    invoke_decimal_op_with_ansi(operator, args, false)
}

fn invoke_decimal_op(operator: Operator, args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let ansi_enabled = spark_ansi_enabled_from_options(args.config_options.as_ref());
    invoke_decimal_op_with_ansi(operator, args, ansi_enabled)
}

fn invoke_decimal_op_with_ansi(
    operator: Operator,
    args: &ScalarFunctionArgs,
    ansi_enabled: bool,
) -> Result<ColumnarValue> {
    let DataType::Decimal128(precision, scale) = *args.return_field.data_type() else {
        return exec_err!(
            "spark decimal UDF promised Decimal128, got {}",
            args.return_field.data_type()
        );
    };
    let result = (precision, scale);
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays.len() != 2 {
        return exec_err!("spark decimal UDF expects two arguments");
    }
    let left = decimal128_array(arrays[0].as_ref())?;
    let right = decimal128_array(arrays[1].as_ref())?;
    if left.len() != right.len() {
        return exec_err!("spark decimal UDF argument lengths differ");
    }
    let left_meta = decimal128_parts(left.data_type()).ok_or_else(|| {
        DataFusionError::Execution("left decimal argument lost its Decimal128 type".to_string())
    })?;
    let right_meta = decimal128_parts(right.data_type()).ok_or_else(|| {
        DataFusionError::Execution("right decimal argument lost its Decimal128 type".to_string())
    })?;
    let mut values: Vec<Option<i128>> = Vec::with_capacity(left.len());
    for row in 0..left.len() {
        let left_value = left.is_valid(row).then(|| left.value(row));
        let right_value = right.is_valid(row).then(|| right.value(row));
        values.push(eval_one(
            operator,
            left_value,
            left_meta,
            right_value,
            right_meta,
            result,
            ansi_enabled,
        )?);
    }
    let array = Decimal128Array::from(values).with_precision_and_scale(precision, scale)?;
    Ok(ColumnarValue::Array(Arc::new(array)))
}

fn decimal128_array(array: &dyn Array) -> Result<Decimal128Array> {
    array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "spark decimal UDF expected Decimal128Array, got {}",
                array.data_type()
            ))
        })
}

fn eval_one(
    operator: Operator,
    left: Option<i128>,
    left_meta: (u8, i8),
    right: Option<i128>,
    right_meta: (u8, i8),
    result: (u8, i8),
    ansi_enabled: bool,
) -> Result<Option<i128>> {
    let Some(left_value) = left else {
        return Ok(None);
    };
    let Some(right_value) = right else {
        return Ok(None);
    };
    if operator == Operator::Divide && right_value == 0 {
        if ansi_enabled {
            return Err(divide_by_zero_error());
        }
        return Ok(None);
    }
    let computed = match operator {
        Operator::Plus => checked_add_sub(
            left_value,
            left_meta,
            right_value,
            right_meta,
            result,
            false,
        )?,
        Operator::Minus => {
            checked_add_sub(left_value, left_meta, right_value, right_meta, result, true)?
        }
        Operator::Multiply => checked_mul(left_value, left_meta, right_value, right_meta, result)?,
        Operator::Divide => checked_div(left_value, left_meta, right_value, right_meta, result)?,
        _ => {
            return exec_err!("spark decimal UDF does not implement {operator}");
        }
    };
    if exceeds_precision(computed, result.0) {
        if ansi_enabled {
            return Err(numeric_out_of_range_error(result.0, result.1));
        }
        return Ok(None);
    }
    computed
        .to_i128()
        .ok_or_else(|| {
            DataFusionError::Execution(
                "spark decimal result does not fit in decimal128's i128 payload".to_string(),
            )
        })
        .map(Some)
}

fn checked_add_sub(
    left: i128,
    left_meta: (u8, i8),
    right: i128,
    right_meta: (u8, i8),
    result: (u8, i8),
    subtract: bool,
) -> Result<i256> {
    let left_scaled = scale_unscaled(
        i256::from_i128(left),
        i32::from(left_meta.1),
        i32::from(result.1),
    )?;
    let right_scaled = scale_unscaled(
        i256::from_i128(right),
        i32::from(right_meta.1),
        i32::from(result.1),
    )?;
    if subtract {
        left_scaled.checked_sub(right_scaled)
    } else {
        left_scaled.checked_add(right_scaled)
    }
    .ok_or_else(|| DataFusionError::Execution("decimal add/sub overflowed i256".to_string()))
}

fn checked_mul(
    left: i128,
    left_meta: (u8, i8),
    right: i128,
    right_meta: (u8, i8),
    result: (u8, i8),
) -> Result<i256> {
    let product = i256::from_i128(left)
        .checked_mul(i256::from_i128(right))
        .ok_or_else(|| {
            DataFusionError::Execution("decimal multiply overflowed i256".to_string())
        })?;
    let product_scale = i32::from(left_meta.1) + i32::from(right_meta.1);
    scale_unscaled(product, product_scale, i32::from(result.1))
}

fn checked_div(
    left: i128,
    left_meta: (u8, i8),
    right: i128,
    right_meta: (u8, i8),
    result: (u8, i8),
) -> Result<i256> {
    // unscaled = left * 10^(result_s + right_s - left_s) / right, ROUND_HALF_UP.
    let scale_up = i32::from(result.1) + i32::from(right_meta.1) - i32::from(left_meta.1);
    let numerator = scale_unscaled(i256::from_i128(left), 0, scale_up)?;
    let denominator = i256::from_i128(right);
    div_half_up(numerator, denominator)
}

fn scale_unscaled(value: i256, from_scale: i32, to_scale: i32) -> Result<i256> {
    if to_scale == from_scale {
        return Ok(value);
    }
    if to_scale > from_scale {
        let delta = u32::try_from(to_scale - from_scale).map_err(|_| {
            DataFusionError::Execution("decimal scale-up exponent does not fit u32".to_string())
        })?;
        let factor = power_of_ten(delta)?;
        return value.checked_mul(factor).ok_or_else(|| {
            DataFusionError::Execution("decimal scale-up overflowed i256".to_string())
        });
    }
    let delta = u32::try_from(from_scale - to_scale).map_err(|_| {
        DataFusionError::Execution("decimal scale-down exponent does not fit u32".to_string())
    })?;
    let factor = power_of_ten(delta)?;
    div_half_up(value, factor)
}

/// Java / Spark `ROUND_HALF_UP`: nearest-neighbor, ties away from zero.
fn div_half_up(numerator: i256, denominator: i256) -> Result<i256> {
    if denominator == i256::ZERO {
        return exec_err!("decimal kernel asked to divide by zero");
    }
    let quotient = numerator.checked_div(denominator).ok_or_else(|| {
        DataFusionError::Execution("decimal division overflowed i256".to_string())
    })?;
    let remainder = numerator.checked_rem(denominator).ok_or_else(|| {
        DataFusionError::Execution("decimal remainder overflowed i256".to_string())
    })?;
    if remainder == i256::ZERO {
        return Ok(quotient);
    }
    let abs_remainder = remainder.checked_abs().ok_or_else(|| {
        DataFusionError::Execution("decimal remainder abs overflowed i256".to_string())
    })?;
    let abs_denominator = denominator.checked_abs().ok_or_else(|| {
        DataFusionError::Execution("decimal denominator abs overflowed i256".to_string())
    })?;
    let doubled = abs_remainder
        .checked_mul(i256::from_i128(2))
        .ok_or_else(|| {
            DataFusionError::Execution("decimal half-up doubling overflowed i256".to_string())
        })?;
    if doubled < abs_denominator {
        return Ok(quotient);
    }
    let bump = if numerator.is_negative() == denominator.is_negative() {
        i256::ONE
    } else {
        i256::MINUS_ONE
    };
    quotient.checked_add(bump).ok_or_else(|| {
        DataFusionError::Execution("decimal half-up bump overflowed i256".to_string())
    })
}

fn exceeds_precision(value: i256, precision: u8) -> bool {
    let Ok(limit) = power_of_ten(u32::from(precision)) else {
        return true;
    };
    match value.checked_abs() {
        Some(magnitude) => magnitude >= limit,
        None => true,
    }
}

fn power_of_ten(exponent: u32) -> Result<i256> {
    i256::from_i128(10)
        .checked_pow(exponent)
        .ok_or_else(|| DataFusionError::Execution(format!("10^{exponent} overflowed i256")))
}

fn divide_by_zero_error() -> DataFusionError {
    DataFusionError::Execution(
        "[DIVIDE_BY_ZERO] Division by zero. Use try_divide to tolerate divisor being 0 \
         and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" \
         to bypass this error. (ArithmeticException)"
            .to_string(),
    )
}

fn numeric_out_of_range_error(precision: u8, scale: i8) -> DataFusionError {
    DataFusionError::Execution(format!(
        "[NUMERIC_VALUE_OUT_OF_RANGE] cannot be represented as Decimal({precision}, {scale}). \
         If necessary set \"spark.sql.ansi.enabled\" to \"false\" to bypass this error. \
         (ArithmeticException)"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    use crate::analyzer_rules;

    fn spark_door_config(ansi_enabled: bool) -> datafusion::prelude::SessionConfig {
        let mut config = crate::ansi::with_spark_ansi_config(
            datafusion::prelude::SessionConfig::new(),
            ansi_enabled,
        );
        // Match SparkExtension DEC-1 / U2 so bare `7.0` and the 38-nines token are DECIMAL.
        config.options_mut().sql_parser.parse_float_as_decimal = true;
        config
    }

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new_with_config(spark_door_config(true));
        register_spark_decimal_planner(&ctx);
        for rule in analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    fn ctx_legacy() -> SessionContext {
        let ctx = SessionContext::new_with_config(spark_door_config(false));
        register_spark_decimal_planner(&ctx);
        for rule in analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    async fn batch(ctx: &SessionContext, sql: &str) -> RecordBatch {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        assert_eq!(batches.len(), 1, "expected a single batch for {sql}");
        batches.into_iter().next().unwrap()
    }

    fn decimal128_cell(batch: &RecordBatch) -> (u8, i8, Option<i128>) {
        let (precision, scale) = match batch.schema().field(0).data_type() {
            DataType::Decimal128(precision, scale) => (*precision, *scale),
            other => panic!("expected Decimal128, got {other:?}"),
        };
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap_or_else(|| panic!("column 0 is not Decimal128Array"));
        let value = array.is_valid(0).then(|| array.value(0));
        (precision, scale, value)
    }

    #[test]
    fn spark_div_formula_matches_photographed_halves() {
        assert_eq!(spark_div_result_type((10, 2), (10, 2)), Some((23, 13)));
        assert_eq!(spark_div_result_type((10, 0), (10, 0)), Some((21, 11)));
        assert_eq!(spark_div_result_type((38, 0), (38, 0)), Some((38, 6)));
        assert_eq!(spark_div_result_type((2, 0), (2, 0)), Some((8, 6)));
        assert_eq!(spark_div_result_type((2, 1), (2, 1)), Some((8, 6)));
    }

    #[test]
    fn half_up_matches_repeating_money() {
        // 123 / 456 at Spark scale 13: 123 * 10^13 / 456 → 2697368421053.
        let value = checked_div(123, (10, 2), 456, (10, 2), (23, 13)).unwrap();
        assert_eq!(value.to_i128(), Some(2_697_368_421_053));
    }

    #[tokio::test]
    async fn div_same_precision_is_spark_23_13() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(1.23 AS DECIMAL(10,2)) / CAST(4.56 AS DECIMAL(10,2)) AS v",
        )
        .await;
        assert_eq!(decimal128_cell(&batch), (23, 13, Some(2_697_368_421_053)));
    }

    #[tokio::test]
    async fn div_repeating_money_keeps_thirteen_places() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(10.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)) AS v",
        )
        .await;
        assert_eq!(decimal128_cell(&batch), (23, 13, Some(33_333_333_333_333)));
    }

    #[tokio::test]
    async fn bare_float_literals_use_spark_div_formula() {
        let ctx = ctx();
        let batch = batch(&ctx, "SELECT 7.0 / 2.0 AS v").await;
        assert_eq!(decimal128_cell(&batch), (8, 6, Some(3_500_000)));
    }

    #[tokio::test]
    async fn integer_division_still_promotes_to_float64() {
        let ctx = ctx();
        let batch = batch(&ctx, "SELECT 5/2 AS v").await;
        assert!(
            matches!(batch.schema().field(0).data_type(), DataType::Float64),
            "SparkExprSemantics must still run after this rule; got {:?}",
            batch.schema().field(0).data_type()
        );
    }

    #[tokio::test]
    async fn decimal_div_by_zero_raises_under_default_ansi() {
        let ctx = ctx();
        let error = match ctx
            .sql("SELECT CAST(1 AS DECIMAL(38,0)) / CAST(0 AS DECIMAL(38,0)) AS v")
            .await
        {
            Err(error) => error.to_string(),
            Ok(frame) => frame
                .collect()
                .await
                .expect_err("ANSI /0 must raise")
                .to_string(),
        };
        assert!(
            error.contains("DIVIDE_BY_ZERO"),
            "expected DIVIDE_BY_ZERO, got {error}"
        );
    }

    #[tokio::test]
    async fn decimal_div_by_zero_is_null_at_spark_type_when_ansi_false() {
        let ctx = ctx_legacy();
        let batch = batch(
            &ctx,
            "SELECT CAST(1 AS DECIMAL(38,0)) / CAST(0 AS DECIMAL(38,0)) AS v",
        )
        .await;
        assert_eq!(decimal128_cell(&batch), (38, 6, None));
    }

    #[tokio::test]
    async fn mul_38_20_plans_at_spark_38_6() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(1 AS DECIMAL(38,20)) * CAST(1 AS DECIMAL(38,20)) AS v",
        )
        .await;
        assert_eq!(decimal128_cell(&batch), (38, 6, Some(1_000_000)));
    }

    #[tokio::test]
    async fn overflow_max_plus_one_raises_under_default_ansi() {
        let ctx = ctx();
        let sql = "SELECT CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) \
                   + CAST('1' AS DECIMAL(38,0)) AS v";
        let error = match ctx.sql(sql).await {
            Err(error) => error.to_string(),
            Ok(frame) => frame
                .collect()
                .await
                .expect_err("ANSI overflow must raise")
                .to_string(),
        };
        assert!(
            error.contains("NUMERIC_VALUE_OUT_OF_RANGE"),
            "expected NUMERIC_VALUE_OUT_OF_RANGE, got {error}"
        );
    }

    #[tokio::test]
    async fn overflow_max_plus_one_is_null_when_ansi_false() {
        let ctx = ctx_legacy();
        let sql = "SELECT CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) \
                   + CAST('1' AS DECIMAL(38,0)) AS v";
        let batch = batch(&ctx, sql).await;
        let (precision, scale, value) = decimal128_cell(&batch);
        assert_eq!((precision, scale), (38, 0));
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn unbounded_add_is_not_rewritten_to_a_udf() {
        // (10,2)+(10,2) is not a DEC-6 wrap; U4a CAST-after is not inserted either.
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(1.23 AS DECIMAL(10,2)) + CAST(4.56 AS DECIMAL(10,2)) AS v",
        )
        .await;
        assert_eq!(decimal128_cell(&batch), (11, 2, Some(579)));
    }

    #[tokio::test]
    async fn the_div_rewrite_is_idempotent() {
        let ctx = ctx();
        let sql = "SELECT CAST(1.23 AS DECIMAL(10,2)) / CAST(4.56 AS DECIMAL(10,2)) AS v";
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let once = crate::analyze_eagerly(&ctx.state(), plan).unwrap();
        let twice = crate::analyze_eagerly(&ctx.state(), once.clone()).unwrap();
        assert_eq!(
            once.schema().field(0).data_type(),
            &DataType::Decimal128(23, 13)
        );
        assert_eq!(
            once.schema().field(0).data_type(),
            twice.schema().field(0).data_type()
        );
    }
}
