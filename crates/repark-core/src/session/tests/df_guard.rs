//! Pins for the two DataFusion 54.1 guards in `session/df_guards.rs`.

use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::{
    ApplyOrder, Optimizer, OptimizerConfig, OptimizerContext, OptimizerRule,
};

use super::super::df_guards::wrap_leaf_rule_for_test;
use crate::ReparkSession;

/// A bare session carries the scalar-subquery guard in its core defaults.
#[tokio::test]
async fn bare_session_without_extension_carries_df_54_1_subquery_guard() {
    let session = ReparkSession::new().unwrap();
    let options = session.context().copied_config().options().clone();
    assert!(
        !options
            .optimizer
            .enable_physical_uncorrelated_scalar_subquery,
        "a no-extension session must force the pre-54 scalar-subquery rewrite (DF-54.1 guard)"
    );
}

/// The leaf-pushdown flag stays enabled; the wrapper scopes failures to `Unnest` paths.
#[tokio::test]
async fn bare_session_keeps_leaf_expression_pushdown_enabled() {
    let session = ReparkSession::new().unwrap();
    let options = session.context().copied_config().options().clone();
    assert!(
        options.optimizer.enable_leaf_expression_pushdown,
        "guard 2 is a scoped RULE, not a blanket flag — the flag must stay DataFusion's default"
    );
}

/// The wrapper replaces `push_down_leaf_projections` under the same name and order; the three
/// `repark_*` subquery rules splice in at their pinned slots (DF-SUBQUERY-1), checked below.
#[tokio::test]
async fn bare_session_without_extension_scopes_leaf_projection_pushdown() {
    let session = ReparkSession::new().unwrap();
    let state = session.context().state();
    let installed: Vec<String> = state
        .optimizers()
        .iter()
        .map(|rule| rule.name().to_string())
        .collect();
    let stock: Vec<String> = datafusion::optimizer::Optimizer::new()
        .rules
        .iter()
        .map(|rule| rule.name().to_string())
        .collect();
    let mut expected = stock.clone();
    let scalar_to_join = expected
        .iter()
        .position(|name| name == "scalar_subquery_to_join")
        .expect("scalar_subquery_to_join is a stock rule");
    expected.splice(
        scalar_to_join..scalar_to_join,
        [
            "repark_projection_exists".to_string(),
            "repark_scalar_subquery_guard".to_string(),
        ],
    );
    let decorrelate_lateral = expected
        .iter()
        .position(|name| name == "decorrelate_lateral_join")
        .expect("decorrelate_lateral_join is a stock rule");
    expected.insert(
        decorrelate_lateral,
        "repark_lateral_projection_hoist".to_string(),
    );
    assert_eq!(
        installed, expected,
        "the DataFusion rule list plus repark's three subquery rules at their pinned slots"
    );
    let wrapped = state
        .optimizers()
        .iter()
        .find(|rule| rule.name() == "push_down_leaf_projections")
        .map(|rule| format!("{rule:?}"))
        .expect("the leaf-projection rule must still be in the list");
    assert!(
        wrapped.contains("UnnestSafeLeafProjectionPushdown")
            && wrapped.contains("PushDownLeafProjections"),
        "the rule under that name must be repark's wrapper AROUND DataFusion's rule, got {wrapped}"
    );
}

/// A plan without `Unnest` remains byte-identical to stock DataFusion.
#[tokio::test]
async fn a_plan_without_unnest_keeps_the_stock_leaf_pushdown() {
    const QUERY: &str = "SELECT t.s['f1'] AS a FROM \
         (SELECT {'f1': value, 'f2': value} AS s, value AS k FROM generate_series(1,3)) t \
         WHERE t.k > 1";
    let stock = datafusion::prelude::SessionContext::new();
    let stock_plan = stock
        .sql(QUERY)
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap()
        .display_indent()
        .to_string();
    assert!(
        stock_plan.contains("__datafusion_extracted"),
        "the fixture must be a plan the rule actually fires on: {stock_plan}"
    );
    let session = ReparkSession::new().unwrap();
    let repark_plan = session
        .sql(QUERY)
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap()
        .display_indent()
        .to_string();
    assert_eq!(
        repark_plan, stock_plan,
        "a plan with no Unnest must optimize exactly as stock DataFusion does"
    );
}

/// An `Unnest` plan the rule can rewrite remains optimized like stock DataFusion; scope is by
/// failure, not shape.
#[tokio::test]
async fn an_unnest_plan_the_rule_can_rewrite_still_gets_leaf_pushdown() {
    const QUERY: &str = "SELECT t.s['f1'] AS a, x.u FROM \
         (SELECT {'f1': value, 'f2': value} AS s, value AS k FROM generate_series(1,3)) t, \
         (SELECT unnest([1,2]) AS u) x WHERE t.k > 1";
    let stock = datafusion::prelude::SessionContext::new();
    let stock_plan = stock
        .sql(QUERY)
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap()
        .display_indent()
        .to_string();
    assert!(
        stock_plan.contains("Unnest:") && stock_plan.contains("__datafusion_extracted"),
        "the fixture must carry BOTH an Unnest and a fired extraction: {stock_plan}"
    );
    let session = ReparkSession::new().unwrap();
    let repark_plan = session
        .sql(QUERY)
        .await
        .unwrap()
        .into_optimized_plan()
        .unwrap()
        .display_indent()
        .to_string();
    assert_eq!(
        repark_plan, stock_plan,
        "an Unnest in the subtree is not by itself a reason to decline the optimization"
    );
}

/// An explicit `false` still disables both leaf-pushdown passes through the wrapped rule.
#[tokio::test]
async fn explicit_conf_can_still_disable_leaf_expression_pushdown() {
    let session = ReparkSession::builder()
        .config(
            "datafusion.optimizer.enable_leaf_expression_pushdown",
            "false",
        )
        .build()
        .unwrap();
    assert!(
        !session
            .context()
            .copied_config()
            .options()
            .optimizer
            .enable_leaf_expression_pushdown,
        "an explicit datafusion.* conf must reach SessionConfig through the wrapper"
    );
}

/// C2-Q-001: swallow is the Unnest *path*, not whole-plan. A mixed plan (Filter
/// with no `Unnest` UNION ALL an `Unnest`) must still surface an inner-rule error
/// on the non-`Unnest` sibling — wrapping the full walk in `unwrap_or` reds this.
#[tokio::test]
async fn mixed_plan_non_unnest_inner_error_stays_loud() {
    const SQL: &str = "SELECT value FROM generate_series(1, 3) WHERE value > 0 \
         UNION ALL \
         SELECT unnest([1, 2])";
    let context = datafusion::prelude::SessionContext::new();
    let plan = context.sql(SQL).await.unwrap().into_unoptimized_plan();
    let display = plan.display_indent().to_string();
    assert!(
        display.contains("Filter") && display.contains("Unnest"),
        "fixture must mix a non-Unnest Filter with an Unnest: {display}"
    );

    let optimizer = Optimizer::with_rules(vec![wrap_leaf_rule_for_test(Arc::new(BoomOnFilter))]);
    let err = optimizer
        .optimize(plan, &OptimizerContext::new(), |_, _| {})
        .expect_err("non-Unnest Filter inner Err must stay loud on a mixed plan");
    assert!(
        err.to_string().contains("boom-on-filter"),
        "expected boom-on-filter, got {err}"
    );
}

#[tokio::test]
async fn a_left_join_projection_the_rule_cannot_rewrite_keeps_its_field_access() {
    const QUERY: &str = "SELECT get_field(s.st, 'A') AS a, get_field(s.st, 'B') AS b FROM \
         (SELECT s.*, t.x AS p FROM (SELECT 5 AS id, named_struct('A', 22, 'B', 'w') AS st) s \
         LEFT JOIN (SELECT 1 AS id, 7 AS x) t ON s.id = t.id) AS s WHERE p IS NULL";
    let stock = datafusion::prelude::SessionContext::new();
    let refusal = stock
        .sql(QUERY)
        .await
        .unwrap()
        .into_optimized_plan()
        .expect_err("the fixture must be a plan stock DataFusion 54.1 cannot optimize")
        .to_string();
    assert!(refusal.contains("__datafusion_extracted"), "{refusal}");
    let session = ReparkSession::new().unwrap();
    let batches = session.sql(QUERY).await.unwrap().collect().await.unwrap();
    let rendered = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(rendered.contains("| 22 | w |"), "{rendered}");
}

#[tokio::test]
async fn a_projection_inner_error_that_is_not_an_extraction_collision_stays_loud() {
    let context = datafusion::prelude::SessionContext::new();
    let plan = context
        .sql("SELECT value + 1 AS v FROM generate_series(1, 3)")
        .await
        .unwrap()
        .into_unoptimized_plan();
    let display = plan.display_indent().to_string();
    assert!(
        display.contains("Projection") && !display.contains("Unnest"),
        "fixture must be a Projection with no Unnest: {display}"
    );
    let optimizer =
        Optimizer::with_rules(vec![wrap_leaf_rule_for_test(Arc::new(BoomOnProjection))]);
    let err = optimizer
        .optimize(plan, &OptimizerContext::new(), |_, _| {})
        .expect_err("a non-collision inner Err on a Projection must stay loud");
    assert!(
        err.to_string().contains("boom-on-projection"),
        "expected boom-on-projection, got {err}"
    );
}

#[derive(Debug)]
struct BoomOnFilter;

impl OptimizerRule for BoomOnFilter {
    fn name(&self) -> &'static str {
        "boom_on_filter"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::common::Result<Transformed<LogicalPlan>> {
        if matches!(plan, LogicalPlan::Filter(_)) {
            return Err(DataFusionError::Internal("boom-on-filter".to_string()));
        }
        Ok(Transformed::no(plan))
    }
}

#[derive(Debug)]
struct BoomOnProjection;

impl OptimizerRule for BoomOnProjection {
    fn name(&self) -> &'static str {
        "boom_on_projection"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::common::Result<Transformed<LogicalPlan>> {
        if matches!(plan, LogicalPlan::Projection(_)) {
            return Err(DataFusionError::Internal("boom-on-projection".to_string()));
        }
        Ok(Transformed::no(plan))
    }
}
