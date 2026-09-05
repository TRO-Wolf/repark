use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, Result};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{Expr, ExprSchemable, LogicalPlan, Operator};
use datafusion::optimizer::AnalyzerRule;

use crate::ansi::spark_ansi_enabled_from_options;
use crate::decimal_cast::{
    DECIMAL_CAST_NULLABLE_NAME, SPARK_NONNULL_NAME, datafusion_nullable,
    spark_decimal_cast_nullable_udf, spark_nonnull_udf,
};
use crate::decimal_spark::{DECIMAL_ADD_NAME, DECIMAL_MUL_NAME, DECIMAL_SUB_NAME};

#[derive(Debug, Default)]
pub struct SparkNullability;

impl AnalyzerRule for SparkNullability {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let ansi_enabled = spark_ansi_enabled_from_options(config);
        plan.transform_up_with_subqueries(|node| rewrite_plan(node, ansi_enabled))
            .data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_nullability"
    }
}

fn rewrite_plan(plan: LogicalPlan, ansi_enabled: bool) -> Result<Transformed<LogicalPlan>> {
    let mut schema = DFSchema::empty();
    for input in plan.inputs() {
        schema.merge(input.schema());
    }
    let name_preserver = NamePreserver::new(&plan);
    let transformed = plan.map_expressions(|expr| {
        let saved_name = name_preserver.save(&expr);
        let rewritten =
            expr.transform_down(|node| Ok(rewrite_expr(node, &schema, ansi_enabled)))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_expr(expr: Expr, schema: &DFSchema, ansi_enabled: bool) -> Transformed<Expr> {
    if let Expr::ScalarFunction(function) = &expr
        && matches!(
            function.func.name(),
            DECIMAL_CAST_NULLABLE_NAME | SPARK_NONNULL_NAME
        )
    {
        return Transformed::new(expr, false, TreeNodeRecursion::Stop);
    }
    if let Expr::Cast(cast) = &expr
        && let Some(nonnull) = crate::decimal_cast::nonnull_spark_cast(cast, schema)
    {
        return Transformed::new(nonnull, true, TreeNodeRecursion::Stop);
    }
    if let Expr::BinaryExpr(binary) = &expr
        && binary.op == Operator::IsNotDistinctFrom
        && datafusion_nullable(&expr, schema) == Some(true)
    {
        return Transformed::new(
            Expr::ScalarFunction(ScalarFunction::new_udf(spark_nonnull_udf(), vec![expr])),
            true,
            TreeNodeRecursion::Stop,
        );
    }
    if !ansi_enabled && let Some(nullable) = nullable_decimal_arith(&expr, schema) {
        return Transformed::new(nullable, true, TreeNodeRecursion::Stop);
    }
    Transformed::no(expr)
}

fn nullable_decimal_arith(expr: &Expr, schema: &DFSchema) -> Option<Expr> {
    match expr {
        Expr::BinaryExpr(binary)
            if matches!(
                binary.op,
                Operator::Plus | Operator::Minus | Operator::Multiply
            ) => {}
        Expr::ScalarFunction(function)
            if matches!(
                function.func.name(),
                DECIMAL_ADD_NAME | DECIMAL_SUB_NAME | DECIMAL_MUL_NAME
            ) => {}
        _ => return None,
    }
    let field = expr.to_field(schema).ok()?.1;
    if !matches!(
        field.data_type(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
    ) || field.is_nullable()
    {
        return None;
    }
    Some(Expr::ScalarFunction(ScalarFunction::new_udf(
        spark_decimal_cast_nullable_udf(),
        vec![expr.clone()],
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::prelude::SessionContext;

    use crate::analyzer_rules;

    fn spark_door_config(ansi_enabled: bool) -> datafusion::prelude::SessionConfig {
        let mut config = crate::ansi::with_spark_ansi_config(
            datafusion::prelude::SessionConfig::new(),
            ansi_enabled,
        );
        config.options_mut().sql_parser.parse_float_as_decimal = true;
        config
    }

    fn ctx_ansi(ansi_enabled: bool) -> SessionContext {
        let ctx = SessionContext::new_with_config(spark_door_config(ansi_enabled));
        crate::decimal_spark::register_spark_decimal_planner(&ctx);
        for rule in analyzer_rules() {
            ctx.add_analyzer_rule(rule);
        }
        ctx
    }

    async fn analyzed(ctx: &SessionContext, sql: &str) -> LogicalPlan {
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        crate::analyze_eagerly(&ctx.state(), plan).unwrap()
    }

    async fn flags(ctx: &SessionContext, sql: &str) -> Vec<bool> {
        analyzed(ctx, sql)
            .await
            .schema()
            .fields()
            .iter()
            .map(|field| field.is_nullable())
            .collect()
    }

    #[tokio::test]
    async fn string_float_and_timestamp_casts_are_nullable() {
        let ctx = ctx_ansi(true);
        for sql in [
            "SELECT CAST('1' AS INT) AS v",
            "SELECT CAST('1' AS BIGINT) AS v",
            "SELECT CAST('1.5' AS DOUBLE) AS v",
            "SELECT CAST(s AS DATE) AS v FROM (SELECT '2020-01-01' AS s) t",
            "SELECT CAST(s AS TIMESTAMP) AS v FROM (SELECT '2020-01-01 00:00:00' AS s) t",
            "SELECT CAST('abc' AS DATE) AS v",
            "SELECT CAST('abc' AS TIMESTAMP) AS v",
            "SELECT CAST(CAST(1.5 AS DOUBLE) AS INT) AS v",
            "SELECT CAST(CAST(1.5 AS DOUBLE) AS BIGINT) AS v",
            "SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS INT) AS v",
        ] {
            assert_eq!(flags(&ctx, sql).await, vec![true], "{sql}");
        }
    }

    #[tokio::test]
    async fn typed_literals_and_valid_literal_casts_stay_nonnull() {
        let ctx = ctx_ansi(true);
        for sql in [
            "SELECT DATE '2020-01-01' AS v",
            "SELECT TIMESTAMP '2020-01-01 00:00:00' AS v",
            "SELECT CAST('2020-01-01' AS DATE) AS v",
            "SELECT CAST('2020-01-01 00:00:00' AS TIMESTAMP) AS v",
            "SELECT CAST(DATE '2020-01-01' AS TIMESTAMP) AS v",
            "SELECT CAST(DATE '2020-01-01' AS STRING) AS v",
        ] {
            assert_eq!(flags(&ctx, sql).await, vec![false], "{sql}");
        }
    }

    #[tokio::test]
    async fn decimal_to_integral_casts_are_nullable() {
        let ctx = ctx_ansi(true);
        for sql in [
            "SELECT CAST(CAST(1 AS DECIMAL(10,0)) AS INT) AS v",
            "SELECT CAST(CAST(1 AS DECIMAL(10,0)) AS BIGINT) AS v",
            "SELECT CAST(CAST(1 AS DECIMAL(10,0)) AS SMALLINT) AS v",
            "SELECT CAST(CAST(1 AS DECIMAL(10,0)) AS TINYINT) AS v",
            "SELECT CAST(1.5 AS INT) AS v",
        ] {
            assert_eq!(flags(&ctx, sql).await, vec![true], "{sql}");
        }
        for sql in [
            "SELECT CAST(CAST(1 AS DECIMAL(10,0)) AS DOUBLE) AS v",
            "SELECT CAST(CAST(1 AS DECIMAL(10,0)) AS STRING) AS v",
        ] {
            assert_eq!(flags(&ctx, sql).await, vec![false], "{sql}");
        }
    }

    #[tokio::test]
    async fn lossless_casts_keep_the_nonnull_child() {
        let ctx = ctx_ansi(true);
        for sql in [
            "SELECT CAST(1 AS INT) AS i, CAST(1 AS STRING) AS s",
            "SELECT CAST(CAST(1 AS BIGINT) AS INT) AS v",
            "SELECT CAST(1 AS BIGINT) AS v",
            "SELECT CAST(1 AS DOUBLE) AS v",
            "SELECT CAST(true AS INT) AS v",
            "SELECT CAST(1 AS BOOLEAN) AS v",
            "SELECT CAST(DATE '2020-01-01' AS STRING) AS v",
            "SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS DATE) AS v",
            "SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS BIGINT) AS v",
            "SELECT CAST(1 AS TIMESTAMP) AS v",
        ] {
            let expected = vec![false; flags(&ctx, sql).await.len()];
            assert_eq!(flags(&ctx, sql).await, expected, "{sql}");
        }
    }

    #[tokio::test]
    async fn date_to_timestamp_is_nonnull() {
        let ctx = ctx_ansi(true);
        assert_eq!(
            flags(&ctx, "SELECT CAST(DATE '2020-01-01' AS TIMESTAMP) AS v").await,
            vec![false]
        );
        assert_eq!(
            flags(&ctx, "SELECT CAST(CAST(NULL AS DATE) AS TIMESTAMP) AS v").await,
            vec![true]
        );
    }

    #[tokio::test]
    async fn null_safe_equal_is_nonnull() {
        let ctx = ctx_ansi(true);
        for sql in [
            "SELECT (NULL <=> NULL) AS v",
            "SELECT (1 <=> NULL) AS v",
            "SELECT (a <=> b) AS v FROM (SELECT 1 AS a, 2 AS b) t",
        ] {
            assert_eq!(flags(&ctx, sql).await, vec![false], "{sql}");
        }
    }

    #[tokio::test]
    async fn decimal_arith_is_nullable_only_when_ansi_is_off() {
        let ansi = ctx_ansi(true);
        let legacy = ctx_ansi(false);
        for sql in [
            "SELECT CAST(1 AS DECIMAL(10,0)) + CAST(1 AS DECIMAL(10,0)) AS v",
            "SELECT CAST(1 AS DECIMAL(10,0)) - CAST(1 AS DECIMAL(10,0)) AS v",
            "SELECT CAST(999 AS DECIMAL(10,0)) * CAST(999 AS DECIMAL(10,0)) AS v",
            "SELECT CAST(1 AS DECIMAL(38,0)) + CAST(1 AS DECIMAL(38,0)) AS v",
            "SELECT CAST(1 AS DECIMAL(38,20)) * CAST(1 AS DECIMAL(38,20)) AS v",
        ] {
            assert_eq!(flags(&ansi, sql).await, vec![false], "ansi on: {sql}");
            assert_eq!(flags(&legacy, sql).await, vec![true], "ansi off: {sql}");
        }
    }

    #[tokio::test]
    async fn integer_arith_and_division_keep_datafusion_nullability() {
        let legacy = ctx_ansi(false);
        assert_eq!(
            flags(&legacy, "SELECT CAST(9 AS INT) + CAST(9 AS INT) AS v").await,
            vec![false]
        );
        assert_eq!(
            flags(
                &legacy,
                "SELECT CAST(1 AS DECIMAL(10,0)) / CAST(3 AS DECIMAL(10,0)) AS v"
            )
            .await,
            vec![true]
        );
    }

    #[tokio::test]
    async fn nullability_rewrite_is_idempotent() {
        let ctx = ctx_ansi(false);
        let sql = "SELECT CAST('1' AS INT) AS i, (NULL <=> NULL) AS nse";
        let plan = ctx.state().create_logical_plan(sql).await.unwrap();
        let once = crate::analyze_eagerly(&ctx.state(), plan).unwrap();
        let twice = crate::analyze_eagerly(&ctx.state(), once.clone()).unwrap();
        assert_eq!(
            once.display_indent_schema().to_string(),
            twice.display_indent_schema().to_string()
        );
    }
}
