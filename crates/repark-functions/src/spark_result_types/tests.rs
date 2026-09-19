use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, Int32Type, Int64Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::optimizer::Analyzer;
use datafusion::prelude::SessionContext;

use super::{SparkIntegerLiteral, signed_aggregate_functions, signed_window_functions};

fn ctx_with_types() -> SessionContext {
    let mut rules = Analyzer::new().rules;
    rules.push(Arc::new(SparkIntegerLiteral));
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_analyzer_rules(rules)
        .build();
    let ctx = SessionContext::new_with_state(state);
    for udaf in signed_aggregate_functions() {
        ctx.register_udaf(udaf.as_ref().clone());
    }
    for udwf in signed_window_functions() {
        ctx.register_udwf(udwf.as_ref().clone());
    }
    ctx
}

async fn batch(ctx: &SessionContext, sql: &str) -> RecordBatch {
    let batches = ctx
        .sql(sql)
        .await
        .expect("plan")
        .collect()
        .await
        .expect("run");
    assert_eq!(batches.len(), 1, "expected a single batch for {sql}");
    batches.into_iter().next().expect("one batch")
}

fn rewrite_once(expr: Expr) -> Expr {
    super::narrow_provisional_integer_literals(expr)
        .expect("narrow")
        .data
}

#[test]
fn int64_literal_in_range_narrows_to_int32() {
    let narrowed = rewrite_once(Expr::Literal(ScalarValue::Int64(Some(1)), None));
    assert_eq!(narrowed, Expr::Literal(ScalarValue::Int32(Some(1)), None));
}

#[test]
fn int64_literal_out_of_range_stays_int64() {
    let wide = Expr::Literal(ScalarValue::Int64(Some(2_147_483_648)), None);
    assert_eq!(rewrite_once(wide.clone()), wide);
}

#[test]
fn parenthesized_negative_two_to_31_stays_int64() {
    let negated = Expr::Negative(Box::new(Expr::Literal(
        ScalarValue::Int64(Some(2_147_483_648)),
        None,
    )));
    assert_eq!(rewrite_once(negated.clone()), negated);
}

#[test]
fn int32_literals_pass_through() {
    let narrow = Expr::Literal(ScalarValue::Int32(Some(7)), None);
    assert_eq!(rewrite_once(narrow.clone()), narrow);
}

#[tokio::test]
async fn select_one_answers_int32() {
    let ctx = ctx_with_types();
    let batch = batch(&ctx, "SELECT 1 AS v").await;
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
    assert_eq!(batch.num_rows(), 1);
}

#[tokio::test]
async fn values_one_answers_int32() {
    let ctx = ctx_with_types();
    let batch = batch(&ctx, "SELECT * FROM (VALUES (1), (2)) AS t(v)").await;
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
}

#[tokio::test]
async fn rank_answers_int32_with_values_kept() {
    let ctx = ctx_with_types();
    let batch = batch(
        &ctx,
        "SELECT rank() OVER (ORDER BY x) AS v FROM (VALUES (1), (1), (2)) AS t(x)",
    )
    .await;
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int32);
    let values = batch.column(0).as_primitive::<Int32Type>();
    assert_eq!(values.values(), &[1, 1, 3]);
}

#[tokio::test]
async fn row_number_dense_rank_ntile_answer_int32() {
    let ctx = ctx_with_types();
    for call in ["dense_rank()", "row_number()", "ntile(2)"] {
        let batch = batch(
            &ctx,
            &format!("SELECT {call} OVER (ORDER BY x) AS v FROM (VALUES (1), (2)) AS t(x)"),
        )
        .await;
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Int32,
            "for {call}"
        );
    }
}

#[tokio::test]
async fn unsigned_count_like_answers_int64() {
    let ctx = ctx_with_types();
    let batch = batch(
        &ctx,
        "SELECT regr_count(y, x) AS v FROM (VALUES (1.0, 1.0), (2.0, 2.0)) AS t(y, x)",
    )
    .await;
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
    let values = batch.column(0).as_primitive::<Int64Type>();
    assert_eq!(values.values(), &[2]);
}

#[tokio::test]
async fn approx_alias_answers_int64() {
    let ctx = ctx_with_types();
    for name in ["approx_distinct", "approx_count_distinct"] {
        let batch = batch(
            &ctx,
            &format!("SELECT {name}(x) AS v FROM (VALUES (1), (2), (2)) AS t(x)"),
        )
        .await;
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Int64,
            "for {name}"
        );
    }
}

#[tokio::test]
async fn grouped_unsigned_count_like_answers_int64() {
    let ctx = ctx_with_types();
    let batch = batch(
        &ctx,
        "SELECT x, regr_count(y, x) AS v FROM (VALUES (1.0, 1), (2.0, 1)) AS t(y, x) GROUP BY x",
    )
    .await;
    assert_eq!(batch.schema().field(1).data_type(), &DataType::Int64);
}

#[tokio::test]
async fn signed_count_is_not_wrapped_twice() {
    let ctx = ctx_with_types();
    let batch = batch(&ctx, "SELECT count(*) AS v FROM (VALUES (1), (2)) AS t(x)").await;
    assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
}

async fn analyzed_aggregate_args(ctx: &SessionContext, sql: &str) -> Vec<Expr> {
    let plan = ctx.sql(sql).await.expect("plan").into_unoptimized_plan();
    let analyzed = ctx
        .state()
        .analyzer()
        .execute_and_check(plan, &ctx.state().config_options().clone(), |_, _| {})
        .expect("analyze");
    let mut args = Vec::new();
    analyzed
        .apply(|node| {
            if let LogicalPlan::Aggregate(aggregate) = node {
                for expr in &aggregate.aggr_expr {
                    expr.apply(|inner| {
                        if let Expr::AggregateFunction(call) = inner {
                            args.extend(call.params.args.iter().cloned());
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })
                    .expect("walk aggregate expr");
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("walk plan");
    args
}

fn count_star_expansion() -> Expr {
    Expr::Literal(ScalarValue::Int64(Some(1)), None)
}

#[tokio::test]
async fn count_star_keeps_the_int64_expansion() {
    let ctx = ctx_with_types();
    let args =
        analyzed_aggregate_args(&ctx, "SELECT count(*) FROM (VALUES (1), (2)) AS t(x)").await;
    assert_eq!(args, vec![count_star_expansion()]);
}

#[tokio::test]
async fn count_one_keeps_the_int64_expansion() {
    let ctx = ctx_with_types();
    let args =
        analyzed_aggregate_args(&ctx, "SELECT count(1) FROM (VALUES (1), (2)) AS t(x)").await;
    assert_eq!(args, vec![count_star_expansion()]);
}

#[tokio::test]
async fn count_of_an_int32_one_widens_to_the_expansion() {
    let ctx = ctx_with_types();
    let frame = ctx
        .sql("SELECT * FROM (VALUES (1), (2)) AS t(x)")
        .await
        .expect("plan")
        .aggregate(
            vec![],
            vec![datafusion::functions_aggregate::count::count(
                Expr::Literal(ScalarValue::Int32(Some(1)), None),
            )],
        )
        .expect("aggregate");
    let analyzed = ctx
        .state()
        .analyzer()
        .execute_and_check(
            frame.logical_plan().clone(),
            &ctx.state().config_options().clone(),
            |_, _| {},
        )
        .expect("analyze");
    let rendered = format!("{analyzed}");
    assert!(rendered.contains("count(Int64(1))"), "{rendered}");
    assert_eq!(frame.schema().field(0).name(), "count(Int32(1))");
    assert_eq!(analyzed.schema().field(0).name(), "count(Int32(1))");
}

#[tokio::test]
async fn count_five_and_distinct_one_still_narrow() {
    let ctx = ctx_with_types();
    let args =
        analyzed_aggregate_args(&ctx, "SELECT count(5) FROM (VALUES (1), (2)) AS t(x)").await;
    assert_eq!(args, vec![Expr::Literal(ScalarValue::Int32(Some(5)), None)]);
    let args = analyzed_aggregate_args(
        &ctx,
        "SELECT count(DISTINCT 1) FROM (VALUES (1), (2)) AS t(x)",
    )
    .await;
    assert_eq!(args, vec![Expr::Literal(ScalarValue::Int32(Some(1)), None)]);
}

#[tokio::test]
async fn count_star_filter_literal_still_narrows() {
    let ctx = ctx_with_types();
    let args = analyzed_aggregate_args(
        &ctx,
        "SELECT count(*) FILTER (WHERE x > 1) FROM (VALUES (1), (2)) AS t(x)",
    )
    .await;
    assert_eq!(args, vec![count_star_expansion()]);
    let batch = batch(
        &ctx,
        "SELECT count(*) FILTER (WHERE x > 1) AS v, count(*) AS w, count(1) AS y, count(5) AS z FROM (VALUES (1), (2)) AS t(x)",
    )
    .await;
    for index in 0..4 {
        assert_eq!(batch.schema().field(index).data_type(), &DataType::Int64);
    }
    let firsts: Vec<i64> = (0..4)
        .map(|index| batch.column(index).as_primitive::<Int64Type>().value(0))
        .collect();
    assert_eq!(firsts, vec![1, 2, 2, 2]);
}
