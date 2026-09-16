use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{Column, NullEquality, ScalarValue, Spans};
use datafusion::logical_expr::expr::Exists;
use datafusion::logical_expr::{
    Expr, Join, JoinConstraint, JoinType, LogicalPlan, Subquery, col, lit,
};
use datafusion::optimizer::{OptimizerContext, OptimizerRule};

use super::super::df_guards::subquery::{
    ReparkLateralProjectionHoist, ReparkProjectionExists, ReparkScalarSubqueryGuard,
    resolve_bound_expr,
};
use crate::ReparkSession;

fn outer_marker(name: &str) -> Expr {
    Expr::OuterReferenceColumn(
        Arc::new(Field::new(name, DataType::Null, true)),
        Column::from_name(name),
    )
}

fn scalar_subquery(plan: LogicalPlan) -> Expr {
    Expr::ScalarSubquery(Subquery {
        outer_ref_columns: Vec::new(),
        subquery: Arc::new(plan),
        spans: Spans::new(),
    })
}

fn exists_expr(plan: LogicalPlan, negated: bool) -> Expr {
    Expr::Exists(Exists::new(
        Subquery {
            outer_ref_columns: Vec::new(),
            subquery: Arc::new(plan),
            spans: Spans::new(),
        },
        negated,
    ))
}

async fn plan_of(session: &ReparkSession, sql: &str) -> LogicalPlan {
    session
        .sql(sql)
        .await
        .expect("probe sql")
        .logical_plan()
        .clone()
}

#[tokio::test]
async fn bound_resolution_unwraps_inner_hits_and_keeps_genuine_outers() {
    let session = ReparkSession::new().unwrap();
    let plan = plan_of(&session, "SELECT 1 AS dept").await;
    let resolved = resolve_bound_expr(outer_marker("dept"), plan.schema()).unwrap();
    assert!(
        matches!(resolved, Expr::Column(_)),
        "inner-hit marker must unwrap to a column, got {resolved:?}"
    );
    let kept = resolve_bound_expr(outer_marker("nope"), plan.schema()).unwrap();
    assert!(
        matches!(kept, Expr::OuterReferenceColumn(_, _)),
        "a name no scope owns stays a marker, got {kept:?}"
    );
}

#[tokio::test]
async fn scalar_guard_wraps_wide_plans_and_skips_aggregates() {
    let session = ReparkSession::new().unwrap();
    let guard = ReparkScalarSubqueryGuard;
    let context = OptimizerContext::new();

    let wide = plan_of(&session, "SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4").await;
    let projection = datafusion::logical_expr::LogicalPlanBuilder::from(
        plan_of(&session, "SELECT 1 AS a").await,
    )
    .project(vec![scalar_subquery(wide).alias("s")])
    .unwrap()
    .build()
    .unwrap();
    let rewritten = guard.rewrite(projection, &context).unwrap().data;
    let display = format!("{rewritten}");
    assert!(
        display.contains("__repark_single_row"),
        "non-singleton scalar subplan must gain the guard, got {display}"
    );

    let singleton = plan_of(&session, "SELECT max(a) AS m FROM (SELECT 1 AS a) t").await;
    let projection = datafusion::logical_expr::LogicalPlanBuilder::from(
        plan_of(&session, "SELECT 1 AS a").await,
    )
    .project(vec![scalar_subquery(singleton).alias("s")])
    .unwrap()
    .build()
    .unwrap();
    let rewritten = guard.rewrite(projection, &context).unwrap().data;
    let display = format!("{rewritten}");
    assert!(
        !display.contains("__repark_single_row"),
        "a zero-group aggregate already yields at most one row, got {display}"
    );
}

#[tokio::test]
async fn exists_in_projection_rewrites_and_preserves_names() {
    let session = ReparkSession::new().unwrap();
    let rule = ReparkProjectionExists;
    let context = OptimizerContext::new();
    let inner = plan_of(&session, "SELECT 1 AS a").await;
    let base = plan_of(&session, "SELECT 1 AS a").await;

    for negated in [false, true] {
        let projection = datafusion::logical_expr::LogicalPlanBuilder::from(base.clone())
            .project(vec![col("a"), exists_expr(inner.clone(), negated)])
            .unwrap()
            .build()
            .unwrap();
        let rewritten = rule.rewrite(projection, &context).unwrap().data;
        let display = format!("{rewritten}");
        assert!(
            display.contains("__repark_exists_count"),
            "EXISTS must lower through the count aggregate, got {display}"
        );
        let name = rewritten.schema().fields()[1].name().clone();
        let expected = if negated { "NOT EXISTS" } else { "EXISTS" };
        assert_eq!(
            name, expected,
            "the rewrite must keep the plan's field name"
        );
    }
}

#[tokio::test]
async fn lateral_hoist_unwraps_uncorrelated_subqueries() {
    let session = ReparkSession::new().unwrap();
    let rule = ReparkLateralProjectionHoist;
    let context = OptimizerContext::new();
    let left = plan_of(&session, "SELECT 1 AS a").await;
    let right = plan_of(&session, "SELECT 2 AS b").await;
    let join = LogicalPlan::Join(
        Join::try_new(
            Arc::new(left),
            Arc::new(LogicalPlan::Subquery(Subquery {
                outer_ref_columns: Vec::new(),
                subquery: Arc::new(right),
                spans: Spans::new(),
            })),
            Vec::new(),
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );
    let rewritten = rule.rewrite(join, &context).unwrap().data;
    let LogicalPlan::Join(join) = rewritten else {
        panic!("uncorrelated lateral must stay a join, got {rewritten}")
    };
    assert!(
        !matches!(*join.right, LogicalPlan::Subquery(_)),
        "uncorrelated marker must unwrap, got {}",
        join.right
    );
}

#[tokio::test]
async fn lateral_hoist_refuses_outer_refs_below_root_projection() {
    let session = ReparkSession::new().unwrap();
    let rule = ReparkLateralProjectionHoist;
    let context = OptimizerContext::new();
    let left = plan_of(&session, "SELECT 1 AS id").await;
    let inner = datafusion::logical_expr::LogicalPlanBuilder::from(
        plan_of(&session, "SELECT 1 AS id").await,
    )
    .project(vec![outer_marker("id").alias("x")])
    .unwrap()
    .build()
    .unwrap();
    let right = datafusion::logical_expr::LogicalPlanBuilder::from(inner)
        .project(vec![col("x"), lit(ScalarValue::Int64(Some(0)))])
        .unwrap()
        .build()
        .unwrap();
    let join = LogicalPlan::Join(
        Join::try_new(
            Arc::new(left),
            Arc::new(LogicalPlan::Subquery(Subquery {
                outer_ref_columns: Vec::new(),
                subquery: Arc::new(right),
                spans: Spans::new(),
            })),
            Vec::new(),
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );
    let result = rule.rewrite(join, &context);
    match result {
        Err(error) => {
            let text = error.to_string();
            assert!(
                text.contains("UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.CORRELATED_REFERENCE"),
                "the refusal must carry Spark's condition, got {text}"
            );
        }
        Ok(_) => panic!("an outer ref below the root projection must refuse"),
    }
}

#[tokio::test]
async fn lateral_hoist_keeps_alias_qualifier_on_outputs() {
    let session = ReparkSession::new().unwrap();
    let rule = ReparkLateralProjectionHoist;
    let context = OptimizerContext::new();
    let left = plan_of(&session, "SELECT 1 AS id").await;
    let inner = datafusion::logical_expr::LogicalPlanBuilder::from(
        plan_of(&session, "SELECT 2 AS b").await,
    )
    .filter(outer_marker("id").gt(lit(ScalarValue::Int64(Some(0)))))
    .unwrap()
    .project(vec![outer_marker("id").alias("dbl")])
    .unwrap()
    .build()
    .unwrap();
    let right =
        datafusion::logical_expr::LogicalPlanBuilder::from(LogicalPlan::Subquery(Subquery {
            outer_ref_columns: vec![outer_marker("id")],
            subquery: Arc::new(inner),
            spans: Spans::new(),
        }))
        .alias("t")
        .unwrap()
        .build()
        .unwrap();
    let join = LogicalPlan::Join(
        Join::try_new(
            Arc::new(left),
            Arc::new(right),
            Vec::new(),
            None,
            JoinType::Inner,
            JoinConstraint::On,
            NullEquality::NullEqualsNothing,
            false,
        )
        .unwrap(),
    );
    let rewritten = rule.rewrite(join, &context).unwrap().data;
    let qualified = Column::new(Some("t"), "dbl");
    assert!(
        rewritten.schema().index_of_column(&qualified).is_ok(),
        "the hoisted output must keep the right side's qualifier, got {}",
        rewritten.schema()
    );
    let LogicalPlan::Projection(projection) = &rewritten else {
        panic!("the hoist must rebuild a projection above the join, got {rewritten}")
    };
    let LogicalPlan::Join(join) = projection.input.as_ref() else {
        panic!(
            "the hoist's projection must sit on the join, got {}",
            projection.input
        )
    };
    let LogicalPlan::SubqueryAlias(alias) = join.right.as_ref() else {
        panic!(
            "the rewritten right must keep SubqueryAlias, got {}",
            join.right
        )
    };
    assert_eq!(alias.alias.to_string(), "t");
    assert!(
        matches!(alias.input.as_ref(), LogicalPlan::Subquery(_)),
        "a still-correlated right keeps its Subquery marker under the alias, got {}",
        alias.input
    );
}

#[tokio::test]
async fn scalar_guard_strips_correlated_limit_into_any_row() {
    let session = ReparkSession::new().unwrap();
    let guard = ReparkScalarSubqueryGuard;
    let context = OptimizerContext::new();

    let filtered = datafusion::logical_expr::LogicalPlanBuilder::from(
        plan_of(&session, "SELECT 1 AS a").await,
    )
    .filter(outer_marker("x").gt(lit(ScalarValue::Int64(Some(0)))))
    .unwrap()
    .limit(0, Some(1))
    .unwrap()
    .build()
    .unwrap();
    let correlated = Expr::ScalarSubquery(Subquery {
        outer_ref_columns: filtered.all_out_ref_exprs(),
        subquery: Arc::new(filtered),
        spans: Spans::new(),
    });
    let projection = datafusion::logical_expr::LogicalPlanBuilder::from(
        plan_of(&session, "SELECT 1 AS a").await,
    )
    .project(vec![correlated.alias("s")])
    .unwrap()
    .build()
    .unwrap();
    let rewritten = guard.rewrite(projection, &context).unwrap().data;
    let display = format!("{rewritten}");
    assert!(
        display.contains("__repark_any_row"),
        "a correlated LIMIT 1 must be stripped and wrapped as an any-row pick, got {display}"
    );
    assert!(
        !display.contains("Limit:"),
        "no Limit node may survive inside a correlated scalar subplan, got {display}"
    );

    let uncorrelated = scalar_subquery(plan_of(&session, "SELECT 1 AS a LIMIT 1").await);
    let projection = datafusion::logical_expr::LogicalPlanBuilder::from(
        plan_of(&session, "SELECT 1 AS a").await,
    )
    .project(vec![uncorrelated.alias("s")])
    .unwrap()
    .build()
    .unwrap();
    let rewritten = guard.rewrite(projection, &context).unwrap().data;
    let display = format!("{rewritten}");
    assert!(
        !display.contains("__repark"),
        "an uncorrelated LIMIT 1 is already singleton, got {display}"
    );
}
