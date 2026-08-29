//! DataFusion 54.1 guards carried by every core session, including extension-less sessions.
//!
//! Guard 1 disables the unsafe scalar-subquery physical path by default; explicit DataFusion
//! configuration may re-enable it. Guard 2 wraps `push_down_leaf_projections`, swallowing failures
//! only on an `Unnest` path while leaving successful rewrites and unrelated failures unchanged.
//!
//! Pins: `session/df_guard_tests.rs` (the whole file — seven tests, one per guarantee).

use std::sync::Arc;

use datafusion::common::Result as DataFusionResult;
use datafusion::common::tree_node::{Transformed, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::{ApplyOrder, Optimizer, OptimizerConfig, OptimizerRule};
use datafusion::prelude::{SessionConfig, SessionContext};

/// DataFusion's own name for the pass-2 leaf-projection rule — the wrapper matches on it so a
/// rename upstream degrades to "the guard silently stops applying", which the pins catch, rather
/// than to a wrong rule being wrapped.
const LEAF_PUSHDOWN_RULE_NAME: &str = "push_down_leaf_projections";

/// ===========================================================================================
/// DF 54.1 regression guard 1 of 2 — applied as a CORE `SessionConfig` default.
/// ===========================================================================================
///
/// Disable the unsafe physical uncorrelated-scalar-subquery path by default. Callers may override
/// the flag, but every core session, including extension-less sessions, carries this default.
pub(super) fn apply_df_54_1_config_guards(config: &mut SessionConfig) {
    config
        .options_mut()
        .optimizer
        .enable_physical_uncorrelated_scalar_subquery = false;
}

/// ===========================================================================================
/// The session context, built with DF 54.1 regression guard 2 of 2 installed.
/// ===========================================================================================
///
/// Exactly what [`SessionContext::new_with_config_rt`] does — `SessionStateBuilder` with the
/// config, the runtime and `with_default_features` — plus one replaced optimizer rule. The state
/// is built ONCE and handed to the context, so the context's `session_id` is the state's (a
/// post-hoc state swap would leave the two disagreeing).
pub(super) fn context_with_df_54_1_rule_guards(
    config: SessionConfig,
    runtime: Arc<RuntimeEnv>,
) -> SessionContext {
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(runtime)
        .with_default_features()
        .with_optimizer_rules(unnest_safe_optimizer_rules())
        .build();
    SessionContext::new_with_state(state)
}

/// ===========================================================================================
/// DataFusion's recommended rule list with `push_down_leaf_projections` wrapped.
/// ===========================================================================================
///
/// This is the list `SessionStateBuilder::build` would install by itself (`Optimizer::default()`
/// is `Optimizer::new()`), with exactly one element replaced. Every other rule, and the rule
/// order the optimizer depends on, is DataFusion's.
fn unnest_safe_optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    Optimizer::new()
        .rules
        .into_iter()
        .map(|rule| {
            if rule.name() == LEAF_PUSHDOWN_RULE_NAME {
                Arc::new(UnnestSafeLeafProjectionPushdown { inner: rule })
                    as Arc<dyn OptimizerRule + Send + Sync>
            } else {
                rule
            }
        })
        .collect()
}

/// ===========================================================================================
/// `push_down_leaf_projections`, scoped away from the plan shape it cannot rewrite.
/// ===========================================================================================
///
/// The wrapper swallows inner-rule errors only on the `Unnest` path, including nested subtrees;
/// unrelated siblings remain loud. Successful rewrites stay enabled, and failed rewrites retain
/// the logical plan. The bounded walk includes subqueries so nested paths cannot bypass the check.
#[derive(Debug)]
struct UnnestSafeLeafProjectionPushdown {
    /// DataFusion's own `PushDownLeafProjections`, taken from its recommended rule list.
    inner: Arc<dyn OptimizerRule + Send + Sync>,
}

impl OptimizerRule for UnnestSafeLeafProjectionPushdown {
    /// The wrapped rule's name verbatim: `EXPLAIN VERBOSE` output, the `skip_failed_rules`
    /// reporting and any name-keyed lookup must not be able to tell the wrapper apart.
    fn name(&self) -> &str {
        self.inner.name()
    }

    /// `None`: this wrapper owns recursion. Delegating `Some(TopDown)` lets the
    /// optimizer reconstruct `Unnest` children *outside* [`Self::rewrite`], so a
    /// schema error on a `Projection` *under* `Unnest` (DEFECT-2 mixed-qualifier
    /// `MemTable` scans) bypasses the decline. The owned walk still applies the inner
    /// rule `TopDown` per node; swallow is scoped to the current Unnest *path*
    /// (this node is `Unnest`, an `Unnest` ancestor, or `Unnest` in this subtree) —
    /// a mixed plan's non-`Unnest` sibling still fails loud (C2-Q-001).
    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        walk_inner_rule(self.inner.as_ref(), plan, config)
    }
}

/// `TopDown` walk of the inner leaf-projection rule (the optimizer no longer recurses for us).
fn walk_inner_rule(
    inner: &dyn OptimizerRule,
    plan: LogicalPlan,
    config: &dyn OptimizerConfig,
) -> DataFusionResult<Transformed<LogicalPlan>> {
    let apply_order = inner.apply_order().unwrap_or(ApplyOrder::TopDown);
    plan.rewrite_with_subqueries(&mut InnerRuleWalk {
        inner,
        config,
        apply_order,
        inside_unnest_depth: 0,
    })
}

/// `TreeNodeRewriter` that applies one optimizer rule at `apply_order`, matching
/// DataFusion's private `Rewriter`, and declines only on the Unnest *path*.
struct InnerRuleWalk<'a> {
    inner: &'a dyn OptimizerRule,
    config: &'a dyn OptimizerConfig,
    apply_order: ApplyOrder,
    /// `Unnest` ancestors of the current node (increment `f_down`, decrement `f_up`).
    inside_unnest_depth: usize,
}

impl TreeNodeRewriter for InnerRuleWalk<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: LogicalPlan) -> DataFusionResult<Transformed<LogicalPlan>> {
        let is_unnest = matches!(node, LogicalPlan::Unnest(_));
        if is_unnest {
            self.inside_unnest_depth += 1;
        }
        if self.apply_order != ApplyOrder::TopDown {
            return Ok(Transformed::no(node));
        }
        apply_inner_scoped(self.inner, node, self.config, self.inside_unnest_depth > 0)
    }

    fn f_up(&mut self, node: LogicalPlan) -> DataFusionResult<Transformed<LogicalPlan>> {
        if matches!(node, LogicalPlan::Unnest(_)) {
            self.inside_unnest_depth = self.inside_unnest_depth.saturating_sub(1);
        }
        if self.apply_order == ApplyOrder::BottomUp {
            apply_inner_scoped(self.inner, node, self.config, self.inside_unnest_depth > 0)
        } else {
            Ok(Transformed::no(node))
        }
    }
}

/// Swallow inner-rule errors only on the Unnest path; other subtrees stay loud.
fn apply_inner_scoped(
    inner: &dyn OptimizerRule,
    node: LogicalPlan,
    config: &dyn OptimizerConfig,
    inside_unnest: bool,
) -> DataFusionResult<Transformed<LogicalPlan>> {
    if !inside_unnest && !carries_unnest(&node)? {
        return inner.rewrite(node, config);
    }
    let declined = node.clone();
    Ok(inner
        .rewrite(node, config)
        .unwrap_or(Transformed::no(declined)))
}

/// Test seam: wrap an arbitrary inner rule with the same Unnest-scoped decline.
#[cfg(test)]
pub(super) fn wrap_leaf_rule_for_test(
    inner: Arc<dyn OptimizerRule + Send + Sync>,
) -> Arc<dyn OptimizerRule + Send + Sync> {
    Arc::new(UnnestSafeLeafProjectionPushdown { inner })
}

/// ===========================================================================================
/// Detect `Unnest` in this subtree, including expression subqueries.
/// ===========================================================================================
///
/// Stop at the first hit, but scan every visited node and subquery so rule-shape changes cannot
/// bypass the scope check. The iterative walk is bounded by the plan structure.
fn carries_unnest(plan: &LogicalPlan) -> DataFusionResult<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        if matches!(node, LogicalPlan::Unnest(_)) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(found)
}
