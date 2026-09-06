//! Spark `DecimalPrecision` analyzer rule.

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{BinaryExpr, Cast, Expr, ExprSchemable, LogicalPlan, Operator};
use datafusion::optimizer::AnalyzerRule;

/// Spark / Arrow decimal128 limits.
pub(crate) const MAX_PRECISION: i32 = 38;
pub(crate) const MINIMUM_ADJUSTED_SCALE: i32 = 6;

/// Spark result-type + integer-literal min-precision over type-coerced `+ − *`.
#[derive(Debug, Default)]
pub struct SparkDecimalPrecision;

impl AnalyzerRule for SparkDecimalPrecision {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)] // `AnalyzerRule::name` ties the lifetime to &self
    fn name(&self) -> &str {
        "spark_decimal_precision"
    }
}

/// Rewrite one plan node's expressions against its merged input schema, keeping output field names.
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

/// Stop on an already-correct Spark-typed `CAST` so a second analyze cannot recompute the formula.
fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Transformed<Expr> {
    if let Expr::Cast(cast) = &expr
        && let Expr::BinaryExpr(inner) = cast.expr.as_ref()
        && is_decimal_arithmetic(inner.op)
        && let Some(spark) = spark_type_after_min_precision(inner, schema)
        && decimal128_parts(cast.field.data_type()) == Some(spark)
    {
        return Transformed::new(expr, false, TreeNodeRecursion::Stop);
    }
    let Expr::BinaryExpr(binary) = expr else {
        return Transformed::no(expr);
    };
    if !is_decimal_arithmetic(binary.op) {
        return Transformed::no(Expr::BinaryExpr(binary));
    }
    rewrite_decimal_arithmetic(binary, schema)
}

fn is_decimal_arithmetic(operator: Operator) -> bool {
    matches!(
        operator,
        Operator::Plus | Operator::Minus | Operator::Multiply
    )
}

/// U3 then U4a on one `+ − *` node.
fn rewrite_decimal_arithmetic(binary: BinaryExpr, schema: &DFSchema) -> Transformed<Expr> {
    let (Ok(left_type), Ok(right_type)) =
        (binary.left.get_type(schema), binary.right.get_type(schema))
    else {
        return Transformed::no(Expr::BinaryExpr(binary));
    };
    let operator = binary.op;
    let original = Expr::BinaryExpr(BinaryExpr::new(
        binary.left.clone(),
        operator,
        binary.right.clone(),
    ));
    let left = min_precision_integer_literal(*binary.left, &right_type);
    let right = min_precision_integer_literal(*binary.right, &left_type);
    let (Ok(left_type), Ok(right_type)) = (left.get_type(schema), right.get_type(schema)) else {
        return rebuilt(left, operator, right, &original);
    };
    let Some(left_decimal) = decimal128_parts(&left_type) else {
        return rebuilt(left, operator, right, &original);
    };
    let Some(right_decimal) = decimal128_parts(&right_type) else {
        return rebuilt(left, operator, right, &original);
    };
    let Some(spark) = spark_result_type(operator, left_decimal, right_decimal) else {
        return rebuilt(left, operator, right, &original);
    };
    let next = Expr::BinaryExpr(BinaryExpr::new(Box::new(left), operator, Box::new(right)));
    if arrow_result_type(operator, left_decimal, right_decimal) == Some(spark) {
        if next == original {
            return Transformed::no(next);
        }
        return Transformed::yes(next);
    }
    Transformed::new(
        Expr::Cast(Cast::new(
            Box::new(next),
            DataType::Decimal128(spark.0, spark.1),
        )),
        true,
        TreeNodeRecursion::Stop,
    )
}

fn spark_type_after_min_precision(binary: &BinaryExpr, schema: &DFSchema) -> Option<(u8, i8)> {
    let left_type = binary.left.get_type(schema).ok()?;
    let right_type = binary.right.get_type(schema).ok()?;
    let left = min_precision_integer_literal(*binary.left.clone(), &right_type);
    let right = min_precision_integer_literal(*binary.right.clone(), &left_type);
    let left_type = left.get_type(schema).ok()?;
    let right_type = right.get_type(schema).ok()?;
    let left_decimal = decimal128_parts(&left_type)?;
    let right_decimal = decimal128_parts(&right_type)?;
    spark_result_type(binary.op, left_decimal, right_decimal)
}

fn rebuilt(left: Expr, operator: Operator, right: Expr, original: &Expr) -> Transformed<Expr> {
    let next = Expr::BinaryExpr(BinaryExpr::new(Box::new(left), operator, Box::new(right)));
    if next == *original {
        Transformed::no(next)
    } else {
        Transformed::yes(next)
    }
}

/// U3: cast bare integer literals beside decimals to minimum-precision `DECIMAL(digits, 0)`.
fn min_precision_integer_literal(operand: Expr, other_type: &DataType) -> Expr {
    if decimal128_parts(other_type).is_none() {
        return operand;
    }
    if let Some(value) = integer_literal_value(&operand) {
        return cast_integer_literal_to_min_precision(operand, value);
    }
    let Expr::Cast(cast) = &operand else {
        return operand;
    };
    let Some(value) = integer_literal_value(&cast.expr) else {
        return operand;
    };
    let Ok(native_type) = cast.expr.get_type(&DFSchema::empty()) else {
        return operand;
    };
    if !is_default_integer_to_decimal(cast.field.data_type(), &native_type) {
        return operand;
    }
    cast_integer_literal_to_min_precision(*cast.expr.clone(), value)
}

fn cast_integer_literal_to_min_precision(integer_literal: Expr, value: i128) -> Expr {
    let precision = min_precision_digits(value);
    Expr::Cast(Cast::new(
        Box::new(integer_literal),
        DataType::Decimal128(precision, 0),
    ))
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

/// Spark `DecimalType.forType` / DataFusion `coerce_numeric_type_to_decimal128`.
fn is_default_integer_to_decimal(cast_target: &DataType, native: &DataType) -> bool {
    matches!(
        (cast_target, native),
        (
            DataType::Decimal128(20, 0),
            DataType::Int64 | DataType::UInt64 | DataType::Int32
        ) | (DataType::Decimal128(10, 0), DataType::UInt32)
            | (
                DataType::Decimal128(5, 0),
                DataType::Int16 | DataType::UInt16
            )
            | (DataType::Decimal128(3, 0), DataType::Int8 | DataType::UInt8)
    )
}

/// Digits of the integer value (Spark `fromLiteral`).
fn min_precision_digits(value: i128) -> u8 {
    let magnitude = value.unsigned_abs();
    if magnitude == 0 {
        return 1;
    }
    let digits = magnitude.ilog10() + 1;
    match u8::try_from(digits) {
        Ok(count) if (1..=38).contains(&count) => count,
        _ => 38,
    }
}

pub(crate) fn decimal128_parts(data_type: &DataType) -> Option<(u8, i8)> {
    match data_type {
        DataType::Decimal128(precision, scale) => Some((*precision, *scale)),
        _ => None,
    }
}

/// Spark `resultDecimalType` + `adjustPrecisionScale` (`allowPrecisionLoss=true`).
pub(crate) fn spark_result_type(
    operator: Operator,
    left: (u8, i8),
    right: (u8, i8),
) -> Option<(u8, i8)> {
    let (left_precision, left_scale) = signed_parts(left)?;
    let (right_precision, right_scale) = signed_parts(right)?;
    let (unbounded_precision, unbounded_scale) = match operator {
        Operator::Plus | Operator::Minus => {
            unbounded_add_sub(left_precision, left_scale, right_precision, right_scale)
        }
        Operator::Multiply => (
            left_precision + right_precision + 1,
            left_scale + right_scale,
        ),
        Operator::Divide => {
            return spark_div_result_type(left, right);
        }
        _ => return None,
    };
    Some(adjust_precision_scale(unbounded_precision, unbounded_scale))
}

/// Spark `/` unbounded formula: `s = max(6, s1+p2+1)`, `p = p1-s1+s2+s`, then the 38-clamp.
pub(crate) fn spark_div_result_type(left: (u8, i8), right: (u8, i8)) -> Option<(u8, i8)> {
    let (left_precision, left_scale) = signed_parts(left)?;
    let (right_precision, right_scale) = signed_parts(right)?;
    let unbounded_scale = (left_scale + right_precision + 1).max(MINIMUM_ADJUSTED_SCALE);
    let unbounded_precision = left_precision - left_scale + right_scale + unbounded_scale;
    Some(adjust_precision_scale(unbounded_precision, unbounded_scale))
}

/// Arrow / Hive keep-scale clamp; `None` when mul scale would exceed 38 (plan refuse).
pub(crate) fn arrow_result_type(
    operator: Operator,
    left: (u8, i8),
    right: (u8, i8),
) -> Option<(u8, i8)> {
    let (left_precision, left_scale) = signed_parts(left)?;
    let (right_precision, right_scale) = signed_parts(right)?;
    match operator {
        Operator::Plus | Operator::Minus => {
            let (unbounded_precision, unbounded_scale) =
                unbounded_add_sub(left_precision, left_scale, right_precision, right_scale);
            Some((
                bounded_precision(unbounded_precision.min(MAX_PRECISION)),
                bounded_scale(unbounded_scale, MAX_PRECISION),
            ))
        }
        Operator::Multiply => {
            let scale = left_scale + right_scale;
            if scale > MAX_PRECISION {
                return None;
            }
            let precision = (left_precision + right_precision + 1).min(MAX_PRECISION);
            Some((
                bounded_precision(precision),
                bounded_scale(scale, precision),
            ))
        }
        _ => None,
    }
}

fn unbounded_add_sub(
    left_precision: i32,
    left_scale: i32,
    right_precision: i32,
    right_scale: i32,
) -> (i32, i32) {
    let scale = left_scale.max(right_scale);
    let precision = (left_precision - left_scale).max(right_precision - right_scale) + scale + 1;
    (precision, scale)
}

/// Spark `DecimalType.adjustPrecisionScale` with `allowPrecisionLoss=true`.
fn adjust_precision_scale(precision: i32, scale: i32) -> (u8, i8) {
    if precision <= MAX_PRECISION {
        let bounded = bounded_precision(precision);
        return (bounded, bounded_scale(scale, i32::from(bounded)));
    }
    let integer_digits = precision - scale;
    let min_scale = scale.min(MINIMUM_ADJUSTED_SCALE);
    let adjusted_scale = (MAX_PRECISION - integer_digits).max(min_scale);
    (
        bounded_precision(MAX_PRECISION),
        bounded_scale(adjusted_scale, MAX_PRECISION),
    )
}

fn signed_parts(parts: (u8, i8)) -> Option<(i32, i32)> {
    let precision = i32::from(parts.0);
    let scale = i32::from(parts.1);
    if precision < 1 || scale < 0 {
        return None;
    }
    Some((precision, scale))
}

fn bounded_precision(value: i32) -> u8 {
    let clamped = value.clamp(1, MAX_PRECISION);
    u8::try_from(clamped).unwrap_or(38)
}

fn bounded_scale(value: i32, precision: i32) -> i8 {
    let clamped = value.clamp(0, precision.clamp(0, MAX_PRECISION));
    i8::try_from(clamped).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{Array, Decimal128Array};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;

    use crate::analyze_eagerly;
    use crate::analyzer_rules;

    fn ctx() -> SessionContext {
        let ctx = SessionContext::new();
        crate::decimal_spark::register_spark_decimal_planner(&ctx);
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

    // === Formula pins ===

    #[test]
    fn adjust_precision_scale_matches_photographed_spark_halves() {
        assert_eq!(
            spark_result_type(Operator::Multiply, (38, 10), (38, 10)),
            Some((38, 6)),
            "(38,10)*(38,10)"
        );
        assert_eq!(
            spark_result_type(Operator::Plus, (38, 18), (38, 18)),
            Some((38, 17)),
            "(38,18)+(38,18)"
        );
        assert_eq!(
            spark_result_type(Operator::Minus, (38, 18), (38, 18)),
            Some((38, 17)),
            "(38,18)-(38,18)"
        );
        assert_eq!(
            spark_result_type(Operator::Plus, (38, 10), (38, 10)),
            Some((38, 9)),
            "(38,10)+(38,10)"
        );
        assert_eq!(
            spark_result_type(Operator::Multiply, (38, 20), (38, 20)),
            Some((38, 6)),
            "(38,20)*(38,20)"
        );
        assert_eq!(
            spark_result_type(Operator::Multiply, (38, 0), (38, 0)),
            Some((38, 0)),
            "(38,0)*(38,0)"
        );
        assert_eq!(
            spark_result_type(Operator::Plus, (10, 2), (10, 2)),
            Some((11, 2)),
            "unbounded add stays under 38"
        );
        assert_eq!(
            spark_result_type(Operator::Multiply, (10, 2), (10, 2)),
            Some((21, 4)),
            "unbounded mul stays under 38"
        );
        assert_eq!(
            spark_result_type(Operator::Multiply, (1, 0), (10, 2)),
            Some((12, 2)),
            "fromLiteral 5 * DECIMAL(10,2)"
        );
        assert_eq!(
            spark_result_type(Operator::Multiply, (2, 0), (10, 2)),
            Some((13, 2)),
            "fromLiteral 50 * DECIMAL(10,2)"
        );
    }

    #[test]
    fn arrow_mul_refuses_when_scale_sum_exceeds_38() {
        assert_eq!(
            arrow_result_type(Operator::Multiply, (38, 20), (38, 20)),
            None
        );
        assert_eq!(
            arrow_result_type(Operator::Multiply, (38, 10), (38, 10)),
            Some((38, 20)),
            "Arrow keep-s clamp-p"
        );
        assert_eq!(
            arrow_result_type(Operator::Plus, (38, 18), (38, 18)),
            Some((38, 18)),
            "Arrow add keeps scale 18"
        );
    }

    #[test]
    fn min_precision_digits_matches_from_literal() {
        assert_eq!(min_precision_digits(0), 1);
        assert_eq!(min_precision_digits(5), 1);
        assert_eq!(min_precision_digits(-5), 1);
        assert_eq!(min_precision_digits(50), 2);
        assert_eq!(min_precision_digits(9), 1);
        assert_eq!(min_precision_digits(10), 2);
        assert_eq!(min_precision_digits(i64::MIN.into()), 19);
    }

    // === SQL pins ===

    #[tokio::test]
    async fn integer_literal_times_decimal_is_spark_min_precision() {
        let ctx = ctx();
        let batch = batch(&ctx, "SELECT 5 * CAST(1.50 AS DECIMAL(10,2)) AS v").await;
        assert_eq!(decimal128_cell(&batch), (12, 2, Some(750)));
    }

    #[tokio::test]
    async fn two_digit_integer_literal_times_decimal_widens_one_more() {
        let ctx = ctx();
        let batch = batch(&ctx, "SELECT 50 * CAST(1.50 AS DECIMAL(10,2)) AS v").await;
        assert_eq!(decimal128_cell(&batch), (13, 2, Some(7500)));
    }

    #[tokio::test]
    async fn user_cast_decimal_times_decimal_keeps_declared_precision() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(5 AS DECIMAL(10,0)) * CAST(1.50 AS DECIMAL(10,2)) AS v",
        )
        .await;
        assert_eq!(decimal128_cell(&batch), (21, 2, Some(750)));
    }

    #[tokio::test]
    async fn typed_int_column_times_decimal_keeps_fortype_width() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(5 AS INT) * CAST(1.50 AS DECIMAL(10,2)) AS v",
        )
        .await;
        assert_eq!(decimal128_cell(&batch), (21, 2, Some(750)));
    }

    #[tokio::test]
    async fn typed_bigint_times_decimal_keeps_fortype_long_width() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(5 AS BIGINT) * CAST(1.50 AS DECIMAL(10,2)) AS v",
        )
        .await;
        assert_eq!(decimal128_cell(&batch), (31, 2, Some(750)));
    }

    #[tokio::test]
    async fn values_int_column_times_decimal_is_not_min_precision() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT a * CAST(1.50 AS DECIMAL(10,2)) AS v \
             FROM (VALUES (CAST(5 AS INT))) AS t(a)",
        )
        .await;
        assert_eq!(decimal128_cell(&batch), (21, 2, Some(750)));
    }

    #[tokio::test]
    async fn unbounded_add_stays_bit_exact() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(1.23 AS DECIMAL(10,2)) + CAST(4.56 AS DECIMAL(10,2)) AS v",
        )
        .await;
        assert_eq!(decimal128_cell(&batch), (11, 2, Some(579)));
    }

    #[tokio::test]
    async fn add_38_18_clamps_scale_to_17() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(1 AS DECIMAL(38,18)) + CAST(1 AS DECIMAL(38,18)) AS v",
        )
        .await;
        let (precision, scale, value) = decimal128_cell(&batch);
        assert_eq!((precision, scale), (38, 17));
        assert_eq!(value, Some(200_000_000_000_000_000));
    }

    #[tokio::test]
    async fn add_38_10_clamps_scale_to_9() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(1 AS DECIMAL(38,10)) + CAST(1 AS DECIMAL(38,10)) AS v",
        )
        .await;
        let (precision, scale, value) = decimal128_cell(&batch);
        assert_eq!((precision, scale), (38, 9));
        assert_eq!(value, Some(2_000_000_000));
    }

    #[tokio::test]
    async fn sub_38_18_uses_the_same_clamp_as_add() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(1 AS DECIMAL(38,18)) - CAST(1 AS DECIMAL(38,18)) AS v",
        )
        .await;
        let (precision, scale, value) = decimal128_cell(&batch);
        assert_eq!((precision, scale), (38, 17));
        assert_eq!(value, Some(0));
    }

    #[tokio::test]
    async fn mul_38_10_clamps_scale_to_6() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(1 AS DECIMAL(38,10)) * CAST(1 AS DECIMAL(38,10)) AS v",
        )
        .await;
        let (precision, scale, value) = decimal128_cell(&batch);
        assert_eq!((precision, scale), (38, 6));
        assert_eq!(value, Some(1_000_000));
    }

    #[tokio::test]
    async fn mul_38_20_plans_via_the_expr_planner() {
        // DEC-8: the ExprPlanner replaces Arrow-refusing `*` before `get_type`.
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(1 AS DECIMAL(38,20)) * CAST(1 AS DECIMAL(38,20)) AS v",
        )
        .await;
        assert_eq!(decimal128_cell(&batch), (38, 6, Some(1_000_000)));
    }

    #[tokio::test]
    async fn division_uses_the_spark_formula() {
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(1.23 AS DECIMAL(10,2)) / CAST(4.56 AS DECIMAL(10,2)) AS v",
        )
        .await;
        let (precision, scale, value) = decimal128_cell(&batch);
        assert_eq!(
            (precision, scale),
            (23, 13),
            "U4b: Spark `/` formula, not Arrow s=s1+4"
        );
        assert_eq!(value, Some(2_697_368_421_053));
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
    async fn the_rewrite_is_idempotent_under_a_second_analyze() {
        let ctx = ctx();
        let sql = "SELECT CAST(1 AS DECIMAL(38,10)) * CAST(1 AS DECIMAL(38,10)) AS v";
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let once = analyze_eagerly(&ctx.state(), plan).unwrap();
        let twice = analyze_eagerly(&ctx.state(), once.clone()).unwrap();
        assert_eq!(
            once.schema().field(0).data_type(),
            &DataType::Decimal128(38, 6),
            "clamp rewrite must land on the first analyze"
        );
        assert_eq!(
            once.schema().field(0).data_type(),
            twice.schema().field(0).data_type()
        );
    }

    #[tokio::test]
    async fn add_clamp_value_survives_a_second_analyze() {
        // Operand-scale rewrite ran Spark's formula on a clamped pair and dropped a digit.
        let ctx = ctx();
        let batch = batch(
            &ctx,
            "SELECT CAST(1 AS DECIMAL(38,18)) + CAST(1 AS DECIMAL(38,18)) AS v",
        )
        .await;
        assert_eq!(
            decimal128_cell(&batch),
            (38, 17, Some(200_000_000_000_000_000))
        );
        let sql = "SELECT CAST(1 AS DECIMAL(38,18)) + CAST(1 AS DECIMAL(38,18)) AS v";
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let once = analyze_eagerly(&ctx.state(), plan).unwrap();
        let twice = analyze_eagerly(&ctx.state(), once.clone()).unwrap();
        assert_eq!(
            twice.schema().field(0).data_type(),
            &DataType::Decimal128(38, 17)
        );
    }
}
