use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::{AnalyzerRule, ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::prelude::SessionContext;

use crate::extension::SignedZeroPolicy;

mod keys;
mod literals;
mod udf;

pub(super) use keys::SignedZeroKeys;
pub(super) use literals::SignedZeroLiterals;
pub(crate) use udf::normalize_zero_call;

pub(super) fn install_literal_seat(context: &SessionContext, policy: SignedZeroPolicy) {
    context.add_analyzer_rule(Arc::new(Switched {
        rule: SignedZeroLiterals,
        on: policy.literals,
    }));
}

pub(super) fn register_normalize_zero(context: &SessionContext) {
    context.register_udf(udf::normalize_zero_udf().as_ref().clone());
}

pub(super) fn literal_optimizer_seat(
    policy: SignedZeroPolicy,
) -> Arc<dyn OptimizerRule + Send + Sync> {
    Arc::new(Switched {
        rule: SignedZeroLiterals,
        on: policy.literals,
    })
}

pub(super) fn key_seat(policy: SignedZeroPolicy) -> Arc<dyn OptimizerRule + Send + Sync> {
    Arc::new(Switched {
        rule: SignedZeroKeys,
        on: policy.keys,
    })
}

#[derive(Debug)]
struct Switched<R> {
    rule: R,
    on: bool,
}

impl<R: AnalyzerRule> AnalyzerRule for Switched<R> {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        if self.on {
            self.rule.analyze(plan, config)
        } else {
            Ok(plan)
        }
    }

    fn name(&self) -> &str {
        AnalyzerRule::name(&self.rule)
    }
}

impl<R: OptimizerRule> OptimizerRule for Switched<R> {
    fn name(&self) -> &str {
        OptimizerRule::name(&self.rule)
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        if self.on {
            self.rule.apply_order()
        } else {
            None
        }
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if self.on {
            self.rule.rewrite(plan, config)
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

#[cfg(test)]
mod key_tests;
#[cfg(test)]
mod tests;
