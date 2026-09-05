use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::{DataType, Int32Type, Int64Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::Expr;
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
    super::narrow_expr(expr).expect("narrow").data
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
fn negative_two_to_31_folds_to_int32_min() {
    let negated = Expr::Negative(Box::new(Expr::Literal(
        ScalarValue::Int64(Some(2_147_483_648)),
        None,
    )));
    assert_eq!(
        rewrite_once(negated),
        Expr::Literal(ScalarValue::Int32(Some(i32::MIN)), None)
    );
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
