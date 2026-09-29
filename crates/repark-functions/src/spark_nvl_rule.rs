use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DFSchema, Result, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::functions::expr_fn::get_field;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr_rewriter::NamePreserver;
use datafusion::logical_expr::{BinaryExpr, Cast, Expr, ExprSchemable, LogicalPlan, Operator};
use datafusion::optimizer::AnalyzerRule;

use crate::spark_nvl::{
    CompareRefusal, binary_op_diff_types, compare_for_nullif, invalid_ordering_type,
};
use crate::spark_nvl_udf::nullif_pick_udf;

#[expect(
    clippy::missing_errors_doc,
    reason = "The error contract is documented in map.md under the owner comment ban."
)]
pub fn insert_nullif_rule_before_coercion(
    mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
    let Some(position) = rules.iter().position(|rule| rule.name() == "type_coercion") else {
        return Err(DataFusionError::Plan(
            "spark nullif rewrite requires the default type_coercion analyzer rule".to_owned(),
        ));
    };
    rules.insert(position, Arc::new(SparkNullifRewrite));
    Ok(rules)
}

#[derive(Debug, Default)]
pub struct SparkNullifRewrite;

impl AnalyzerRule for SparkNullifRewrite {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(rewrite_plan).data()
    }

    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "spark_nullif_rewrite"
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
        let rewritten = expr.transform_up(|node| rewrite_expr(node, &schema))?;
        Ok(rewritten.update_data(|node| saved_name.restore(node)))
    })?;
    transformed.map_data(LogicalPlan::recompute_schema)
}

fn rewrite_expr(expr: Expr, schema: &DFSchema) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(function) = &expr else {
        return Ok(Transformed::no(expr));
    };
    if function.func.name() == "nullif" && function.args.len() == 2 {
        let rewritten = rewrite_nullif(
            function.args[0].clone(),
            function.args[1].clone(),
            None,
            schema,
        )?;
        return Ok(Transformed::yes(rewritten));
    }
    if function.func.name() == "nullifzero" && function.args.len() == 1 {
        let zero = Expr::Literal(ScalarValue::Int32(Some(0)), None);
        let rewritten = rewrite_nullif(function.args[0].clone(), zero, Some("0"), schema)?;
        return Ok(Transformed::yes(rewritten));
    }
    Ok(Transformed::no(expr))
}

fn is_null_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(scalar, _) => scalar.is_null(),
        Expr::Cast(cast) => is_null_literal(&cast.expr),
        _ => false,
    }
}

fn rewrite_nullif(
    first: Expr,
    second: Expr,
    second_sql: Option<&str>,
    schema: &DFSchema,
) -> Result<Expr> {
    let (Ok(first_type), Ok(second_type)) = (first.get_type(schema), second.get_type(schema))
    else {
        return Ok(blind_pick(first, second));
    };
    if first_type == DataType::Null && second_type == DataType::Null {
        return Ok(Expr::Literal(ScalarValue::Null, None));
    }
    let first_sql = "...";
    let second_sql = second_sql.unwrap_or("...");
    let leaves = match compare_for_nullif(&first_type, &second_type) {
        Ok(leaves) => leaves,
        Err(CompareRefusal::BinaryOp) => {
            return Err(binary_op_diff_types(
                first_sql,
                second_sql,
                &first_type,
                &second_type,
            ));
        }
        Err(CompareRefusal::Ordering(common)) => {
            return Err(invalid_ordering_type(first_sql, second_sql, &common));
        }
    };
    if leaves.is_empty() {
        return Ok(Expr::Literal(ScalarValue::Null, None));
    }
    if is_null_literal(&first) || is_null_literal(&second) {
        return Ok(first);
    }
    let mut cond: Option<Expr> = None;
    for leaf in &leaves {
        let left = shape_operand(&first, &leaf.path_a, &leaf.type_a, &leaf.common);
        let right = shape_operand(&second, &leaf.path_b, &leaf.type_b, &leaf.common);
        let equals = Expr::BinaryExpr(BinaryExpr::new(
            Box::new(left),
            Operator::Eq,
            Box::new(right),
        ));
        cond = Some(match cond {
            None => equals,
            Some(built) => Expr::BinaryExpr(BinaryExpr::new(
                Box::new(built),
                Operator::And,
                Box::new(equals),
            )),
        });
    }
    let Some(cond) = cond else {
        return Ok(Expr::Literal(ScalarValue::Null, None));
    };
    Ok(Expr::ScalarFunction(ScalarFunction::new_udf(
        nullif_pick_udf(),
        vec![cond, first],
    )))
}

fn blind_pick(first: Expr, second: Expr) -> Expr {
    let cond = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(first.clone()),
        Operator::Eq,
        Box::new(second),
    ));
    Expr::ScalarFunction(ScalarFunction::new_udf(
        nullif_pick_udf(),
        vec![cond, first],
    ))
}

fn shape_operand(expr: &Expr, path: &[String], from_type: &DataType, common: &DataType) -> Expr {
    let mut shaped = expr.clone();
    for name in path {
        shaped = get_field(shaped, name.as_str());
    }
    if from_type == common {
        shaped
    } else {
        Expr::Cast(Cast::new(Box::new(shaped), common.clone()))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Array;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::optimizer::Analyzer;
    use datafusion::prelude::SessionContext;

    use super::*;

    fn ctx() -> SessionContext {
        let rules = insert_nullif_rule_before_coercion(Analyzer::new().rules).unwrap();
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rules(rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        for udf in crate::spark_nvl_udf::functions() {
            ctx.register_udf(udf.as_ref().clone());
        }
        ctx
    }

    async fn one_row(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        let pretty = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .unwrap()
            .to_string();
        pretty.lines().map(str::to_owned).collect()
    }

    async fn cell_is_null(ctx: &SessionContext, sql: &str) -> bool {
        let batches = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"))
            .collect()
            .await
            .unwrap_or_else(|error| panic!("execute {sql}: {error}"));
        assert_eq!(batches.len(), 1, "expected a single batch for {sql}");
        assert_eq!(batches[0].num_rows(), 1, "expected a single row for {sql}");
        batches[0].column(0).is_null(0)
    }

    async fn plan_err(ctx: &SessionContext, sql: &str) -> String {
        ctx.sql(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("{sql} planned, want refusal"))
            .to_string()
    }

    #[tokio::test]
    async fn rule_keeps_first_argument_type() {
        let ctx = ctx();
        let rows = one_row(&ctx, "SELECT nullif(1, 2) AS v").await;
        assert!(rows.iter().any(|row| row.contains('1')), "{rows:?}");
        assert!(
            cell_is_null(&ctx, "SELECT nullif(1, 1) AS v").await,
            "nullif(1, 1) answers null"
        );
    }

    #[tokio::test]
    async fn rule_refuses_like_spark() {
        let ctx = ctx();
        let message = plan_err(&ctx, "SELECT nullif(1, DATE '2024-01-01') AS v").await;
        assert!(
            message.contains("[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES]"),
            "{message}"
        );
        let message = plan_err(&ctx, "SELECT nullifzero(DATE '2024-01-01') AS v").await;
        assert!(
            message.contains("[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES]"),
            "{message}"
        );
        assert!(message.contains("(... = 0)"), "{message}");
        assert!(message.contains("(\"DATE\" and \"INT\")"), "{message}");
    }

    #[tokio::test]
    async fn rewrite_preserves_cardinality_fold() {
        use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
        use datafusion::prelude::SessionConfig;
        let settings = crate::cardinality::ReparkSqlSettings {
            max_array_elements: 100,
            ..crate::cardinality::ReparkSqlSettings::default()
        };
        let config = crate::cardinality::with_repark_sql_config(SessionConfig::new(), settings);
        let mut rules: Vec<Arc<dyn datafusion::optimizer::AnalyzerRule + Send + Sync>> =
            vec![Arc::new(SparkNullifRewrite), Arc::new(TypeCoercion::new())];
        rules.extend(crate::cardinality::analyzer_rules());
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_analyzer_rules(rules)
            .build();
        let ctx = SessionContext::new_with_state(state);
        for udf in crate::spark_nvl_udf::functions() {
            ctx.register_udf(udf.as_ref().clone());
        }
        let sql = "SELECT array_repeat(1, nullif(101, 0)) AS v";
        let frame = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("plan {sql}: {error}"));
        let message = crate::analyze_eagerly(&ctx.state(), frame.logical_plan().clone())
            .expect_err("const over-ceiling must refuse")
            .to_string();
        assert!(
            message.contains(crate::cardinality::MAX_ARRAY_ELEMENTS_KEY),
            "{message}"
        );
    }

    #[tokio::test]
    async fn rule_compares_structs_by_position() {
        let ctx = ctx();
        assert!(
            cell_is_null(&ctx, "SELECT nullif(STRUCT(1 AS a), STRUCT(1 AS b)) AS v").await,
            "positional struct compare answers null on equal fields"
        );
        let rows = one_row(&ctx, "SELECT nullif(STRUCT(1 AS a), STRUCT(2 AS b)) AS v").await;
        assert!(rows.iter().any(|row| row.contains('1')), "{rows:?}");
    }
}
