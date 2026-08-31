//! Spark integer `+` / `-` / `*` overflow checks (F-Y10-1).

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int32Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::registry::FunctionRegistry;
use datafusion::logical_expr::sqlparser::ast::BinaryOperator;
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::SessionContext;

use crate::ansi::spark_ansi_enabled_from_options;

pub(crate) const INTEGER_ADD_NAME: &str = "__repark_spark_int_add__";
pub(crate) const INTEGER_SUB_NAME: &str = "__repark_spark_int_sub__";
pub(crate) const INTEGER_MUL_NAME: &str = "__repark_spark_int_mul__";

/// Install the integer-overflow `ExprPlanner`.
pub fn register_spark_integer_planner(ctx: &SessionContext) {
    let planner: Arc<dyn ExprPlanner> = Arc::new(SparkIntegerExprPlanner);
    let _ = ctx.state_ref().write().register_expr_planner(planner);
}

/// Analyzer rule plus planner for a native (ANSI-door) session.
pub fn install_integer_overflow(ctx: &SessionContext) {
    register_spark_integer_planner(ctx);
    ctx.add_analyzer_rule(Arc::new(SparkIntegerOverflow));
}

/// Rewrite integer `+` / `-` / `*` onto checked UDFs.
#[derive(Debug, Default)]
pub struct SparkIntegerOverflow;

impl AnalyzerRule for SparkIntegerOverflow {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_integer_overflow"
    }
}

/// Plan-time rewrite so `TypeCoercion` cannot widen `INT + 1` to Int64 first.
#[derive(Debug)]
pub struct SparkIntegerExprPlanner;

impl ExprPlanner for SparkIntegerExprPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        let Some(operator) = sql_operator(&expr.op) else {
            return Ok(PlannerResult::Original(expr));
        };
        let (Ok(left_type), Ok(right_type)) =
            (expr.left.get_type(schema), expr.right.get_type(schema))
        else {
            return Ok(PlannerResult::Original(expr));
        };
        let Some(result_type) =
            spark_integer_result_type(&expr.left, &left_type, &expr.right, &right_type)
        else {
            return Ok(PlannerResult::Original(expr));
        };
        let Some(udf) = arith_udf(operator) else {
            return Ok(PlannerResult::Original(expr));
        };
        Ok(PlannerResult::Planned(udf_call(
            udf,
            cast_to(expr.left, result_type.clone()),
            cast_to(expr.right, result_type),
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
        && is_spark_integer_udf(function.func.name())
    {
        return Transformed::new(expr, false, TreeNodeRecursion::Stop);
    }
    let Expr::BinaryExpr(binary) = expr else {
        return Transformed::no(expr);
    };
    if !matches!(
        binary.op,
        Operator::Plus | Operator::Minus | Operator::Multiply
    ) {
        return Transformed::no(Expr::BinaryExpr(binary));
    }
    let (Ok(left_type), Ok(right_type)) =
        (binary.left.get_type(schema), binary.right.get_type(schema))
    else {
        return Transformed::no(Expr::BinaryExpr(binary));
    };
    let Some(result_type) =
        spark_integer_result_type(&binary.left, &left_type, &binary.right, &right_type)
    else {
        return Transformed::no(Expr::BinaryExpr(binary));
    };
    let Some(udf) = arith_udf(binary.op) else {
        return Transformed::no(Expr::BinaryExpr(binary));
    };
    Transformed::yes(udf_call(
        udf,
        cast_to(*binary.left, result_type.clone()),
        cast_to(*binary.right, result_type),
    ))
}

fn sql_operator(operator: &BinaryOperator) -> Option<Operator> {
    match operator {
        BinaryOperator::Plus => Some(Operator::Plus),
        BinaryOperator::Minus => Some(Operator::Minus),
        BinaryOperator::Multiply => Some(Operator::Multiply),
        _ => None,
    }
}

fn arith_udf(operator: Operator) -> Option<Arc<ScalarUDF>> {
    match operator {
        Operator::Plus => Some(spark_integer_add_udf()),
        Operator::Minus => Some(spark_integer_sub_udf()),
        Operator::Multiply => Some(spark_integer_mul_udf()),
        _ => None,
    }
}

fn is_spark_integer_udf(name: &str) -> bool {
    matches!(name, INTEGER_ADD_NAME | INTEGER_SUB_NAME | INTEGER_MUL_NAME)
}

fn udf_call(udf: Arc<ScalarUDF>, left: Expr, right: Expr) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(udf, vec![left, right]))
}

fn cast_to(expr: Expr, data_type: DataType) -> Expr {
    Expr::Cast(Cast::new(Box::new(expr), data_type))
}

fn spark_integer_result_type(
    left_expr: &Expr,
    left_type: &DataType,
    right_expr: &Expr,
    right_type: &DataType,
) -> Option<DataType> {
    let left_width = operand_width(left_expr, left_type)?;
    let right_width = operand_width(right_expr, right_type)?;
    if !is_typed_integer_expr(left_expr, left_type)
        && !is_typed_integer_expr(right_expr, right_type)
    {
        return None;
    }
    Some(left_width.wider(right_width).data_type())
}

#[derive(Clone, Copy)]
enum IntegerWidth {
    Int32,
    Int64,
}

impl IntegerWidth {
    fn data_type(self) -> DataType {
        match self {
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
        }
    }

    fn wider(self, other: Self) -> Self {
        match (self, other) {
            (Self::Int64, _) | (_, Self::Int64) => Self::Int64,
            (Self::Int32, Self::Int32) => Self::Int32,
        }
    }
}

fn operand_width(expr: &Expr, data_type: &DataType) -> Option<IntegerWidth> {
    if let Some(value) = integer_literal_i64(expr) {
        return Some(if i32::try_from(value).is_ok() {
            IntegerWidth::Int32
        } else {
            IntegerWidth::Int64
        });
    }
    match data_type {
        DataType::Int32 => Some(IntegerWidth::Int32),
        DataType::Int64 => Some(IntegerWidth::Int64),
        _ => None,
    }
}

fn is_typed_integer_expr(expr: &Expr, data_type: &DataType) -> bool {
    if integer_literal_i64(expr).is_some() {
        return false;
    }
    matches!(data_type, DataType::Int32 | DataType::Int64)
}

fn integer_literal_i64(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Alias(alias) => integer_literal_i64(alias.expr.as_ref()),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Some(*value),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int16(Some(value)), _) => Some(i64::from(*value)),
        Expr::Literal(ScalarValue::Int8(Some(value)), _) => Some(i64::from(*value)),
        _ => None,
    }
}

#[must_use]
pub fn spark_integer_add_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkIntegerAdd::new()))
}

#[must_use]
pub fn spark_integer_sub_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkIntegerSub::new()))
}

#[must_use]
pub fn spark_integer_mul_udf() -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::from(SparkIntegerMul::new()))
}

macro_rules! integer_arith_udf {
    ($type_name:ident, $name_literal:literal, $operator:expr) => {
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
                integer_return_type(arg_types, $name_literal)
            }

            fn return_field_from_args(&self, args: ReturnFieldArgs<'_>) -> Result<FieldRef> {
                let arg_types: Vec<DataType> = args
                    .arg_fields
                    .iter()
                    .map(|field| field.data_type().clone())
                    .collect();
                let data_type = self.return_type(&arg_types)?;
                let nullable = args.arg_fields.iter().any(|field| field.is_nullable());
                Ok(Arc::new(Field::new($name_literal, data_type, nullable)))
            }

            fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
                invoke_integer_op($operator, &args)
            }
        }
    };
}

integer_arith_udf!(SparkIntegerAdd, "__repark_spark_int_add__", Operator::Plus);
integer_arith_udf!(SparkIntegerSub, "__repark_spark_int_sub__", Operator::Minus);
integer_arith_udf!(
    SparkIntegerMul,
    "__repark_spark_int_mul__",
    Operator::Multiply
);

fn integer_return_type(arg_types: &[DataType], name: &str) -> Result<DataType> {
    if arg_types.len() != 2 {
        return exec_err!("'{name}' expects two integer arguments");
    }
    match (&arg_types[0], &arg_types[1]) {
        (DataType::Int32, DataType::Int32) => Ok(DataType::Int32),
        (DataType::Int64, DataType::Int64) => Ok(DataType::Int64),
        (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => {
            Ok(DataType::Int64)
        }
        (left, right) => Err(DataFusionError::Plan(format!(
            "'{name}' expects INT32 or INT64 arguments, got {left} and {right}"
        ))),
    }
}

fn invoke_integer_op(operator: Operator, args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
    let ansi_enabled = spark_ansi_enabled_from_options(args.config_options.as_ref());
    let arrays = ColumnarValue::values_to_arrays(&args.args)?;
    if arrays.len() != 2 {
        return exec_err!("spark integer UDF expects two arguments");
    }
    match args.return_field.data_type() {
        DataType::Int32 => {
            let left = int32_array(arrays[0].as_ref())?;
            let right = int32_array(arrays[1].as_ref())?;
            let array = eval_int32(operator, &left, &right, ansi_enabled)?;
            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        DataType::Int64 => {
            let left = int64_array(arrays[0].as_ref())?;
            let right = int64_array(arrays[1].as_ref())?;
            let array = eval_int64(operator, &left, &right, ansi_enabled)?;
            Ok(ColumnarValue::Array(Arc::new(array)))
        }
        other => exec_err!("spark integer UDF promised Int32 or Int64, got {other}"),
    }
}

fn int32_array(array: &dyn Array) -> Result<Int32Array> {
    array
        .as_any()
        .downcast_ref::<Int32Array>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "spark integer UDF expected Int32Array, got {}",
                array.data_type()
            ))
        })
}

fn int64_array(array: &dyn Array) -> Result<Int64Array> {
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "spark integer UDF expected Int64Array, got {}",
                array.data_type()
            ))
        })
}

fn eval_int32(
    operator: Operator,
    left: &Int32Array,
    right: &Int32Array,
    ansi_enabled: bool,
) -> Result<Int32Array> {
    if left.len() != right.len() {
        return exec_err!("spark integer UDF argument lengths differ");
    }
    let mut values: Vec<Option<i32>> = Vec::with_capacity(left.len());
    for row in 0..left.len() {
        if !left.is_valid(row) || !right.is_valid(row) {
            values.push(None);
            continue;
        }
        values.push(Some(eval_i32(
            operator,
            left.value(row),
            right.value(row),
            ansi_enabled,
        )?));
    }
    Ok(Int32Array::from(values))
}

fn eval_int64(
    operator: Operator,
    left: &Int64Array,
    right: &Int64Array,
    ansi_enabled: bool,
) -> Result<Int64Array> {
    if left.len() != right.len() {
        return exec_err!("spark integer UDF argument lengths differ");
    }
    let mut values: Vec<Option<i64>> = Vec::with_capacity(left.len());
    for row in 0..left.len() {
        if !left.is_valid(row) || !right.is_valid(row) {
            values.push(None);
            continue;
        }
        values.push(Some(eval_i64(
            operator,
            left.value(row),
            right.value(row),
            ansi_enabled,
        )?));
    }
    Ok(Int64Array::from(values))
}

fn eval_i32(operator: Operator, left: i32, right: i32, ansi_enabled: bool) -> Result<i32> {
    let checked = match operator {
        Operator::Plus => left.checked_add(right),
        Operator::Minus => left.checked_sub(right),
        Operator::Multiply => left.checked_mul(right),
        _ => return exec_err!("spark integer UDF does not implement {operator}"),
    };
    match checked {
        Some(value) => Ok(value),
        None if ansi_enabled => Err(arithmetic_overflow_error(operator, false)),
        None => Ok(match operator {
            Operator::Plus => left.wrapping_add(right),
            Operator::Minus => left.wrapping_sub(right),
            Operator::Multiply => left.wrapping_mul(right),
            _ => left,
        }),
    }
}

fn eval_i64(operator: Operator, left: i64, right: i64, ansi_enabled: bool) -> Result<i64> {
    let checked = match operator {
        Operator::Plus => left.checked_add(right),
        Operator::Minus => left.checked_sub(right),
        Operator::Multiply => left.checked_mul(right),
        _ => return exec_err!("spark integer UDF does not implement {operator}"),
    };
    match checked {
        Some(value) => Ok(value),
        None if ansi_enabled => Err(arithmetic_overflow_error(operator, true)),
        None => Ok(match operator {
            Operator::Plus => left.wrapping_add(right),
            Operator::Minus => left.wrapping_sub(right),
            Operator::Multiply => left.wrapping_mul(right),
            _ => left,
        }),
    }
}

fn arithmetic_overflow_error(operator: Operator, is_long: bool) -> DataFusionError {
    let kind = if is_long { "long" } else { "integer" };
    let try_name = match operator {
        Operator::Minus => "try_subtract",
        Operator::Multiply => "try_multiply",
        _ => "try_add",
    };
    DataFusionError::Execution(format!(
        "[ARITHMETIC_OVERFLOW] {kind} overflow. Use '{try_name}' to tolerate overflow \
         and return NULL instead. If necessary set \"spark.sql.ansi.enabled\" to \"false\" \
         to bypass this error. (ArithmeticException)"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::Array;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::col;
    use datafusion::prelude::SessionContext;

    use crate::analyzer_rules;

    fn spark_door_config(ansi_enabled: bool) -> datafusion::prelude::SessionConfig {
        crate::ansi::with_spark_ansi_config(datafusion::prelude::SessionConfig::new(), ansi_enabled)
    }

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new_with_config(spark_door_config(true));
        register_spark_integer_planner(&ctx);
        for rule in analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    fn ctx_legacy() -> SessionContext {
        let ctx = SessionContext::new_with_config(spark_door_config(false));
        register_spark_integer_planner(&ctx);
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

    async fn collect_error(ctx: &SessionContext, sql: &str) -> String {
        match ctx.sql(sql).await {
            Err(error) => error.to_string(),
            Ok(frame) => frame
                .collect()
                .await
                .expect_err("ANSI integer overflow must raise")
                .to_string(),
        }
    }

    fn int32_cell(batch: &RecordBatch) -> Option<i32> {
        assert!(
            matches!(batch.schema().field(0).data_type(), DataType::Int32),
            "expected Int32, got {:?}",
            batch.schema().field(0).data_type()
        );
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("Int32Array");
        array.is_valid(0).then(|| array.value(0))
    }

    fn int64_cell(batch: &RecordBatch) -> Option<i64> {
        assert!(
            matches!(batch.schema().field(0).data_type(), DataType::Int64),
            "expected Int64, got {:?}",
            batch.schema().field(0).data_type()
        );
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64Array");
        array.is_valid(0).then(|| array.value(0))
    }

    /// pins: f-y10-1-int-overflow/C-001, C-002
    #[tokio::test]
    async fn untyped_one_plus_one_stays_int64_on_planner_session() {
        let ctx = ctx();
        let batch = batch(&ctx, "SELECT 1 + 1 AS v").await;
        assert_eq!(int64_cell(&batch), Some(2));
    }

    /// pins: f-y10-1-int-overflow/C-001, C-002
    #[tokio::test]
    async fn untyped_int_max_plus_one_widens_to_int64() {
        let ctx = ctx();
        let batch = batch(&ctx, "SELECT 2147483647 + 1 AS v").await;
        assert_eq!(int64_cell(&batch), Some(2_147_483_648));
    }

    /// pins: f-y10-1-int-overflow/C-001, C-002
    #[tokio::test]
    async fn untyped_int_max_plus_one_widens_when_ansi_false() {
        let ctx = ctx_legacy();
        let batch = batch(&ctx, "SELECT 2147483647 + 1 AS v").await;
        assert_eq!(int64_cell(&batch), Some(2_147_483_648));
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn int32_add_max_plus_one_raises_under_default_ansi() {
        let ctx = ctx();
        let error =
            collect_error(&ctx, "SELECT CAST(2147483647 AS INT) + CAST(1 AS INT) AS v").await;
        assert!(
            error.contains("ARITHMETIC_OVERFLOW"),
            "expected ARITHMETIC_OVERFLOW, got {error}"
        );
        assert!(
            error.contains("try_add"),
            "Spark needle names try_add, got {error}"
        );
        assert!(
            error.contains("integer overflow"),
            "INT overflow names integer, got {error}"
        );
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn int32_add_cast_plus_literal_raises_under_default_ansi() {
        let ctx = ctx();
        let error = collect_error(&ctx, "SELECT CAST(2147483647 AS INT) + 1 AS v").await;
        assert!(
            error.contains("ARITHMETIC_OVERFLOW"),
            "CAST(INT)+1 must not widen, got {error}"
        );
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn int32_add_max_plus_one_wraps_when_ansi_false() {
        let ctx = ctx_legacy();
        let batch = batch(&ctx, "SELECT CAST(2147483647 AS INT) + CAST(1 AS INT) AS v").await;
        assert_eq!(int32_cell(&batch), Some(-2_147_483_648));
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn int32_add_cast_plus_literal_wraps_int32_when_ansi_false() {
        let ctx = ctx_legacy();
        let batch = batch(&ctx, "SELECT CAST(2147483647 AS INT) + 1 AS v").await;
        assert_eq!(int32_cell(&batch), Some(-2_147_483_648));
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn int32_add_control_stays_int32() {
        let ctx = ctx();
        let batch = batch(&ctx, "SELECT CAST(2147483646 AS INT) + CAST(1 AS INT) AS v").await;
        assert_eq!(int32_cell(&batch), Some(2_147_483_647));
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn int32_sub_min_minus_one_raises_under_default_ansi() {
        let ctx = ctx();
        let error = collect_error(
            &ctx,
            "SELECT CAST(-2147483648 AS INT) - CAST(1 AS INT) AS v",
        )
        .await;
        assert!(
            error.contains("ARITHMETIC_OVERFLOW") && error.contains("try_subtract"),
            "expected subtract overflow, got {error}"
        );
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn int32_sub_min_minus_one_wraps_when_ansi_false() {
        let ctx = ctx_legacy();
        let batch = batch(
            &ctx,
            "SELECT CAST(-2147483648 AS INT) - CAST(1 AS INT) AS v",
        )
        .await;
        assert_eq!(int32_cell(&batch), Some(2_147_483_647));
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn int32_mul_max_times_two_raises_under_default_ansi() {
        let ctx = ctx();
        let error =
            collect_error(&ctx, "SELECT CAST(2147483647 AS INT) * CAST(2 AS INT) AS v").await;
        assert!(
            error.contains("ARITHMETIC_OVERFLOW") && error.contains("try_multiply"),
            "expected multiply overflow, got {error}"
        );
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn int32_mul_max_times_two_wraps_when_ansi_false() {
        let ctx = ctx_legacy();
        let batch = batch(&ctx, "SELECT CAST(2147483647 AS INT) * CAST(2 AS INT) AS v").await;
        assert_eq!(int32_cell(&batch), Some(-2));
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn int32_mul_min_times_neg_one_raises_under_default_ansi() {
        let ctx = ctx();
        let error = collect_error(
            &ctx,
            "SELECT CAST(-2147483648 AS INT) * CAST(-1 AS INT) AS v",
        )
        .await;
        assert!(
            error.contains("ARITHMETIC_OVERFLOW"),
            "MIN * -1 overflows, got {error}"
        );
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn int64_add_max_plus_one_raises_under_default_ansi() {
        let ctx = ctx();
        let error = collect_error(
            &ctx,
            "SELECT CAST(9223372036854775807 AS BIGINT) + CAST(1 AS BIGINT) AS v",
        )
        .await;
        assert!(
            error.contains("ARITHMETIC_OVERFLOW") && error.contains("long overflow"),
            "BIGINT overflow names long, got {error}"
        );
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn int64_add_max_plus_one_wraps_when_ansi_false() {
        let ctx = ctx_legacy();
        let batch = batch(
            &ctx,
            "SELECT CAST(9223372036854775807 AS BIGINT) + CAST(1 AS BIGINT) AS v",
        )
        .await;
        assert_eq!(int64_cell(&batch), Some(i64::MIN));
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn int64_sub_min_minus_one_wraps_when_ansi_false() {
        let ctx = ctx_legacy();
        let batch = batch(
            &ctx,
            "SELECT CAST(-9223372036854775808 AS BIGINT) - CAST(1 AS BIGINT) AS v",
        )
        .await;
        assert_eq!(int64_cell(&batch), Some(i64::MAX));
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn int64_mul_max_times_two_wraps_when_ansi_false() {
        let ctx = ctx_legacy();
        let batch = batch(
            &ctx,
            "SELECT CAST(9223372036854775807 AS BIGINT) * CAST(2 AS BIGINT) AS v",
        )
        .await;
        assert_eq!(int64_cell(&batch), Some(-2));
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn explicit_bigint_cast_plus_one_does_not_narrow() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(CAST(2147483647 AS INT) AS BIGINT) + 1 AS v",
        )
        .await;
        assert_eq!(int64_cell(&batch), Some(2_147_483_648));
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn facade_int32_add_cols_raises_under_default_ansi() {
        let ctx = ctx();
        let error = match ctx
            .sql("SELECT CAST(2147483647 AS INT) AS a, CAST(1 AS INT) AS b")
            .await
        {
            Err(error) => error.to_string(),
            Ok(frame) => match frame.select(vec![(col("a") + col("b")).alias("v")]) {
                Err(error) => error.to_string(),
                Ok(selected) => selected
                    .collect()
                    .await
                    .expect_err("facade int32 add overflow must raise")
                    .to_string(),
            },
        };
        assert!(
            error.contains("ARITHMETIC_OVERFLOW"),
            "facade col+col must raise, got {error}"
        );
    }

    /// pins: f-y10-1-int-overflow/C-002
    #[tokio::test]
    async fn null_plus_one_is_null() {
        let ctx = ctx();
        let batch = batch(&ctx, "SELECT CAST(NULL AS INT) + CAST(1 AS INT) AS v").await;
        assert_eq!(int32_cell(&batch), None);
    }

    /// pins: f-y10-1-int-overflow/C-005
    #[tokio::test]
    async fn perf_measure_non_overflow_int32_add() {
        if std::env::var("REPARK_PERF_MEASURE").ok().as_deref() != Some("1") {
            return;
        }
        let checked = ctx();
        let baseline = SessionContext::new();
        let sql = "SELECT CAST(1 AS INT) + CAST(2 AS INT) AS v";
        let _ = batch(&checked, sql).await;
        let _ = batch(&baseline, sql).await;
        let start = std::time::Instant::now();
        for _ in 0..200 {
            let _ = batch(&checked, sql).await;
        }
        let checked_elapsed = start.elapsed();
        let start = std::time::Instant::now();
        for _ in 0..200 {
            let _ = batch(&baseline, sql).await;
        }
        let baseline_elapsed = start.elapsed();
        let ratio = checked_elapsed.as_secs_f64() / baseline_elapsed.as_secs_f64().max(1e-9);
        eprintln!(
            "F-Y10-1 non-overflow int32 add: checked={checked_elapsed:?} \
             baseline={baseline_elapsed:?} ratio={ratio:.3}"
        );
        assert!(
            ratio < 10.0,
            "checked integer add is {ratio:.1}x baseline; order-of-magnitude regression"
        );
    }
}
