use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result};
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::AnalyzerRule;

use crate::timestamp_ns_cast::is_timestamp_ns_cast;

static GATE_OPEN_COUNT: AtomicU64 = AtomicU64::new(0);
static FALLBACK_COUNT: AtomicU64 = AtomicU64::new(0);

#[must_use]
pub fn gate_open_count() -> u64 {
    GATE_OPEN_COUNT.load(Ordering::Relaxed)
}

#[must_use]
pub fn fallback_count() -> u64 {
    FALLBACK_COUNT.load(Ordering::Relaxed)
}

pub fn reset_counts() {
    GATE_OPEN_COUNT.store(0, Ordering::Relaxed);
    FALLBACK_COUNT.store(0, Ordering::Relaxed);
}

#[must_use]
pub fn fallback_log_line(reason: &str) -> String {
    format!("timestamp_ns_coercion fallback: {reason}")
}

#[must_use]
pub fn plan_opens_gate(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply_with_subqueries(|node| {
        if found {
            return Ok(TreeNodeRecursion::Stop);
        }
        if node_holds_nanosecond_leaf(node) || node_calls_ns_kernel(node) {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn node_holds_nanosecond_leaf(node: &LogicalPlan) -> bool {
    match node {
        LogicalPlan::TableScan(scan) => scan
            .source
            .schema()
            .fields()
            .iter()
            .any(|field| type_holds_nanosecond(field.data_type())),
        LogicalPlan::EmptyRelation(empty) => empty
            .schema
            .fields()
            .iter()
            .any(|field| type_holds_nanosecond(field.data_type())),
        _ => false,
    }
}

fn node_calls_ns_kernel(node: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = node.apply_expressions(|expr| {
        let _ = expr.apply(|leaf| {
            if !found && is_timestamp_ns_cast(leaf) {
                found = true;
            }
            Ok(TreeNodeRecursion::Continue)
        });
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn type_holds_nanosecond(data_type: &DataType) -> bool {
    let mut stack = vec![data_type];
    while let Some(current) = stack.pop() {
        match current {
            DataType::Timestamp(TimeUnit::Nanosecond, _) => return true,
            DataType::List(field)
            | DataType::LargeList(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => stack.push(field.data_type()),
            DataType::Struct(fields) => {
                for field in fields {
                    stack.push(field.data_type());
                }
            }
            DataType::Union(fields, _) => {
                for (_, field) in fields.iter() {
                    stack.push(field.data_type());
                }
            }
            DataType::Dictionary(_, value) => stack.push(value),
            DataType::RunEndEncoded(_, values) => stack.push(values.data_type()),
            _ => {}
        }
    }
    false
}

#[derive(Debug)]
pub struct SparkTimestampNsCoercion {
    inner: Arc<dyn AnalyzerRule + Send + Sync>,
}

impl SparkTimestampNsCoercion {
    #[must_use]
    pub fn new(inner: Arc<dyn AnalyzerRule + Send + Sync>) -> Self {
        Self { inner }
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn wrap_first_type_coercion(
        mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    ) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
        let Some(position) = rules.iter().position(|rule| rule.name() == "type_coercion") else {
            return Err(DataFusionError::Plan(
                "timestamp_ns_coercion requires the default type_coercion analyzer rule"
                    .to_string(),
            ));
        };
        let inner = Arc::clone(&rules[position]);
        rules[position] = Arc::new(Self::new(inner));
        Ok(rules)
    }
}

impl AnalyzerRule for SparkTimestampNsCoercion {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        if plan_opens_gate(&plan) {
            GATE_OPEN_COUNT.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.analyze(plan, config)
    }

    fn name(&self) -> &'static str {
        "type_coercion"
    }
}

#[cfg(test)]
mod tests;
