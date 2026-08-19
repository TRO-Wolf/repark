//! The DataFusion 54.1 regression guards carried as CORE `ReparkSession` defaults.
//!
//! Each guard neutralizes one upstream optimizer rule that miscompiles a plan repark legitimately
//! builds. They live here, not on a door [`crate::SessionExtension`] (phase-2 design G8): an
//! extension-less native session builds the same plans and must carry the same guards.
//!
//! The two guards are deliberately at **different altitudes**, because the two bugs are:
//!
//! * Guard 1 (scalar subquery) is a `SessionConfig` default — the bad path is a whole physical
//!   planning mode, there is no sub-shape that is safe, and DataFusion's own flag is the switch.
//!   It stays a *default*, not a lock: `session.rs`'s `apply_datafusion_config_keys` runs AFTER
//!   this, so `.config("datafusion.optimizer.<flag>", "true")` re-enables it for a caller who
//!   knows their plans are unaffected.
//! * Guard 2 (leaf-projection pushdown) is a **rule wrapper**, not a flag. Only plans that carry
//!   an `Unnest` trip the bug, and even among those only the ones the rule actually fails on, so
//!   the rule is left ON everywhere and the wrapper declines exactly where it breaks. Turning
//!   DataFusion's flag off instead would have cost every nested-column query in the engine —
//!   MEASURED up to ~8x in one best-of-3 run on a filtered wide-struct parquet scan (load-sensitive; the direction and width-monotonicity reproduce, the exact ratio does not) (see `task/c25-bugfix-ledger.md` →
//!   DEFECT-2 §3).
//!
//! Pins: `session/df_guard_tests.rs` (the whole file — six tests, one per guarantee).

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
/// The new default-on physical uncorrelated-scalar-subquery path (`ScalarSubqueryExec`
/// wrapping) drops the query's top-level Sort — `SELECT … WHERE x < (SELECT …) ORDER BY …`
/// returns unsorted rows (fuzzer repros fuzz-42-1/2, 2026-08-01; minimal: no `SortExec` in the
/// physical plan). Force the pre-54 rewrite (`ScalarSubqueryToJoin`) until upstream fixes;
/// re-enable is gated on the banked repros passing WITH the flag on. Phase-2 design G8: this
/// guard is a CORE session default, never a door-extension knob — an extension-less native
/// session must carry it (pinned by
/// `bare_session_without_extension_carries_df_54_1_subquery_guard`).
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
/// DataFusion 54.1's pass-2 leaf-expression rule fires whenever a `Projection` carries a
/// `MoveTowardsLeafNodes` expression (for repark: the `get_field` every `dynamicFlatten` /
/// repeated `explode` emits) and walks the subtree relocating it toward the scan. On a plan that
/// stacks `Unnest` under such a projection it has two independent upstream bugs (both MEASURED
/// 2026-08-18, DEFECT-2):
///
/// 1. `try_push_into_inputs` rebuilds the node it pushes through with
///    `with_new_exprs(node.expressions(), …)`; `LogicalPlan::Unnest::expressions()` hands out the
///    unnest exec column while `Unnest::with_new_exprs` asserts that vector empty →
///    `Internal error: Assertion failed: expr.is_empty()`.
/// 2. Merging a pushed pass-through column into a projection that already re-aliases the same
///    name (`q."id" AS "id"`) lands qualified `q.id` beside unqualified `id` in one `DFSchema` →
///    `Schema contains qualified field name … and unqualified field name … which would be
///    ambiguous`.
///
/// Both are optimizer-only: with the rule declining, the *identical* logical plan executes and
/// returns the *identical* rows. So the guard is a **decline, not a rewrite** — repark never
/// tries to fix the pushed plan, it just keeps the plan the rule failed to rewrite.
///
/// # The scope, in two steps, and what each step costs
///
/// 1. **No `Unnest` in the subtree → delegate untouched, errors included.** The rule's
///    `apply_order` is `TopDown`, so `plan` here is the subtree the push can reach; without an
///    `Unnest` in it neither bug is reachable. These plans get stock DataFusion behavior at
///    stock cost — no clone, no catch, and a failure from the rule still propagates loud so an
///    unrelated upstream bug cannot hide behind this guard. That matters: the optimization is
///    worth up to ~8x in one measured run on a filtered wide-struct parquet scan (load-sensitive ratio; direction and width-monotonicity reproduce — ledger DEFECT-2
///    §3), which is exactly what a blanket
///    `datafusion.optimizer.enable_leaf_expression_pushdown = false` would have thrown away on
///    every query in the engine to fix a bug only unnest plans have.
/// 2. **`Unnest` in the subtree → try the rule, and decline only if it actually fails.** The
///    optimization still applies to every unnest plan the rule *can* rewrite; only the shapes it
///    genuinely miscompiles keep the unoptimized plan. Declining by shape instead would have
///    cost **11.8x** on a wide-struct scan that merely has an unnest elsewhere in the same
///    subtree (MEASURED, same ledger section: 9.1s → 107.5s, 500k rows × 60 struct fields).
///
/// **The trade in step 2, recorded.** The error is swallowed rather than surfaced — repark-core
/// carries no logging dependency, so the decline is silent. It is observable in the only place
/// that matters: `EXPLAIN` shows the un-pushed plan. The swallow is bounded to
/// `Unnest`-carrying subtrees and to a rule that is a pure optimization, so the worst case is a
/// slower plan, never a wrong one — and DataFusion's own `skip_failed_rules` option is the same
/// bargain made globally.
///
/// The user-facing switch is unchanged and still DataFusion's: setting
/// `datafusion.optimizer.enable_leaf_expression_pushdown = false` disables both leaf passes
/// entirely (the inner rule checks the flag itself). There is deliberately no knob that restores
/// the miscompile.
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

    /// `None`: this wrapper owns recursion. The inner rule is `TopDown`; if we delegated
    /// `Some(TopDown)` the optimizer's `Rewriter` would reconstruct `Unnest` children
    /// *outside* [`Self::rewrite`], and a schema error there would bypass the decline
    /// (native `dynamic_flatten` Unnest+`get_field` plans). Catching the full walk keeps
    /// the Python explode-rewrite plans working and covers that reconstruction path.
    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> DataFusionResult<Transformed<LogicalPlan>> {
        if !carries_unnest(&plan)? {
            // Step 1: not our shape — stock rule, stock cost, stock errors.
            return walk_inner_rule(self.inner.as_ref(), plan, config);
        }
        // Step 2: our shape. The clone is the price of being able to decline AFTER the
        // failure instead of before it, and it is paid only on unnest-carrying plans.
        let declined = plan.clone();
        Ok(walk_inner_rule(self.inner.as_ref(), plan, config).unwrap_or(Transformed::no(declined)))
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
    })
}

/// `TreeNodeRewriter` that applies one optimizer rule at `apply_order`, matching
/// DataFusion's private `Rewriter`.
struct InnerRuleWalk<'a> {
    inner: &'a dyn OptimizerRule,
    config: &'a dyn OptimizerConfig,
    apply_order: ApplyOrder,
}

impl TreeNodeRewriter for InnerRuleWalk<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: LogicalPlan) -> DataFusionResult<Transformed<LogicalPlan>> {
        if self.apply_order == ApplyOrder::TopDown {
            self.inner.rewrite(node, self.config)
        } else {
            Ok(Transformed::no(node))
        }
    }

    fn f_up(&mut self, node: LogicalPlan) -> DataFusionResult<Transformed<LogicalPlan>> {
        if self.apply_order == ApplyOrder::BottomUp {
            self.inner.rewrite(node, self.config)
        } else {
            Ok(Transformed::no(node))
        }
    }
}

/// ===========================================================================================
/// Does this subtree contain an `Unnest` node (subqueries included)?
/// ===========================================================================================
///
/// Short-circuits on the first hit. Subqueries are walked because the inner rule's pass-1 sibling
/// rewrites with `transform_down_with_subqueries`, so an `Unnest` inside a scalar/`IN` subquery is
/// as reachable as one in the main tree.
///
/// Cost: one walk per node the `TopDown` rule visits, i.e. O(plan²) in the worst case for a plan
/// with no `Unnest` anywhere (nothing short-circuits it). Deliberately NOT short-circuited on
/// "is this node a `Projection`" — that would encode the inner rule's own fast path, and a
/// widened rule upstream would then slip past the scope check silently. MEASURED: the wrapper is
/// at parity with stock DataFusion on both benchmark shapes (ledger DEFECT-2 §3(b)), so on real
/// plans this sits under the noise; revisit if plans get pathologically deep.
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
