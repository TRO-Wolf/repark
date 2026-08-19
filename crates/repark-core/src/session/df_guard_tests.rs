//! The DF-54.1 regression-guard pins (DEFECT-2, 2026-08-18).
//!
//! Split out of `tests.rs` when the guard-2 cohort pushed that file past the 1500-line ceiling —
//! the sanctioned "split the module" out, not an EXCEPTIONS row. The subject under test is
//! `session/df_guards.rs`: guard 1 is a `SessionConfig` default, guard 2 is DataFusion's
//! `push_down_leaf_projections` wrapped so it declines on the `Unnest`-carrying plans it
//! miscompiles. Ledger: `task/c25-bugfix-ledger.md` -> DEFECT-2.

use std::sync::Arc;

use datafusion::common::tree_node::Transformed;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::{
    ApplyOrder, Optimizer, OptimizerConfig, OptimizerContext, OptimizerRule,
};

use super::df_guards::wrap_leaf_rule_for_test;
use crate::ReparkSession;

/// G8 (phase-2 design): the DF-54.1 uncorrelated-scalar-subquery regression guard lives in
/// core session defaults, NOT in a door extension — a bare `build()` with no extension must
/// carry it, so extension-less native sessions keep the pre-54 `ScalarSubqueryToJoin` rewrite
/// (fuzzer repros fuzz-42-1/2: the 54.1 physical path drops the query's top-level Sort).
/// Mutation-proof: hoisting the flag into an extension flips this test red.
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

/// DEFECT-2 (2026-08-18), guard 2 of 2 — the ANTI-BLANKET-SKIP pin. `push_down_leaf_projections`
/// miscompiles a multi-pass `Unnest` chain carrying a `get_field` leaf, but the cure must not be
/// `datafusion.optimizer.enable_leaf_expression_pushdown = false`: that flag also governs the
/// rule on plans with no `Unnest` anywhere, where the optimization measured up to ~8x in one run on a
/// filtered wide-struct parquet scan (MEASURED — ledger DEFECT-2 §3). So the flag must stay at
/// DataFusion's default. Mutation-proof: re-introducing the blanket skip flips this red.
#[tokio::test]
async fn bare_session_keeps_leaf_expression_pushdown_enabled() {
    let session = ReparkSession::new().unwrap();
    let options = session.context().copied_config().options().clone();
    assert!(
        options.optimizer.enable_leaf_expression_pushdown,
        "guard 2 is a scoped RULE, not a blanket flag — the flag must stay DataFusion's default"
    );
}

/// The guard itself: a bare `build()` with NO extension installs repark's `Unnest`-scoped wrapper
/// in place of DataFusion's `push_down_leaf_projections`, at the same position, under the same
/// name, with every other rule and the rule ORDER (which the optimizer depends on) left as
/// DataFusion shipped it. CORE, not a door-extension knob (design G8) — the facade's `explode`
/// rewrite and an extension-less native session build the same `Unnest` chain.
/// Mutation-proof: dropping the wrapper, renaming it, or hoisting it onto an extension reds this.
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
    assert_eq!(
        installed, stock,
        "exactly the DataFusion rule list, in DataFusion's order, under DataFusion's names"
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

/// The scope's first half, MEASURED as a plan: a query with **no `Unnest`** optimizes to the
/// byte-identical plan stock DataFusion produces — the extraction is still hoisted below the
/// filter (`__datafusion_extracted_*`). This is the perf finding pinned as a plan rather than a
/// timing: the wrapper costs such queries nothing. Mutation-proof: any blanket skip reds it.
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

/// The scope's second half, and the reason the wrapper declines by FAILURE and not by shape: a
/// plan that *carries* an `Unnest` but whose extraction the rule handles fine still gets the
/// optimization — same plan as stock. Declining on the mere presence of an `Unnest` would strand
/// this shape unoptimized, MEASURED at 11.8x on a 500k × 60-field struct scan (ledger DEFECT-2
/// §3). Mutation-proof: swapping the wrapper back to a shape test reds this.
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

/// The user-facing switch is unchanged and still DataFusion's: the wrapper delegates, and the
/// wrapped rule reads the flag itself, so an explicit `false` still disables both leaf passes.
/// (There is deliberately no knob that restores the miscompile — see `df_guards.rs`.)
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
