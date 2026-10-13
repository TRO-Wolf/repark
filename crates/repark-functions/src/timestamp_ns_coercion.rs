use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::{Expr, LogicalPlan};
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

pub type FallbackSink = Arc<dyn Fn(&str) + Send + Sync>;

pub struct SparkTimestampNsCoercion {
    inner: Arc<dyn AnalyzerRule + Send + Sync>,
    fallback_sink: Option<FallbackSink>,
}

impl std::fmt::Debug for SparkTimestampNsCoercion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SparkTimestampNsCoercion")
            .field("inner", &self.inner)
            .field("has_fallback_sink", &self.has_fallback_sink())
            .finish_non_exhaustive()
    }
}

impl SparkTimestampNsCoercion {
    #[must_use]
    pub fn new(inner: Arc<dyn AnalyzerRule + Send + Sync>) -> Self {
        Self {
            inner,
            fallback_sink: None,
        }
    }

    #[must_use]
    pub fn with_fallback_sink(self, sink: FallbackSink) -> Self {
        Self {
            inner: self.inner,
            fallback_sink: Some(sink),
        }
    }

    #[must_use]
    pub fn has_fallback_sink(&self) -> bool {
        self.fallback_sink.is_some()
    }

    #[allow(clippy::missing_errors_doc)]
    pub fn wrap_first_type_coercion(
        mut rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
        fallback_sink: FallbackSink,
    ) -> Result<Vec<Arc<dyn AnalyzerRule + Send + Sync>>> {
        let Some(position) = rules.iter().position(|rule| rule.name() == "type_coercion") else {
            return Err(DataFusionError::Plan(
                "timestamp_ns_coercion requires the default type_coercion analyzer rule"
                    .to_string(),
            ));
        };
        let inner = Arc::clone(&rules[position]);
        rules[position] = Arc::new(Self::new(inner).with_fallback_sink(fallback_sink));
        Ok(rules)
    }

    fn fallback(
        &self,
        plan: LogicalPlan,
        config: &ConfigOptions,
        reason: &str,
    ) -> Result<LogicalPlan> {
        FALLBACK_COUNT.fetch_add(1, Ordering::Relaxed);
        if let Some(sink) = &self.fallback_sink {
            sink(&fallback_log_line(reason));
        }
        self.inner.analyze(plan, config)
    }
}

fn is_reader_call(name: &str) -> bool {
    [
        "to_timestamp",
        "to_timestamp_ntz",
        "to_timestamp_ltz",
        "timestamp",
    ]
    .iter()
    .any(|want| name.eq_ignore_ascii_case(want))
}

fn is_live_timestamp_literal(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::TimestampSecond(Some(_), _)
            | ScalarValue::TimestampMillisecond(Some(_), _)
            | ScalarValue::TimestampMicrosecond(Some(_), _)
            | ScalarValue::TimestampNanosecond(Some(_), _)
    )
}

fn is_timestamp_target(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Timestamp(_, _))
}

fn trigger_reason(expr: &Expr) -> Option<&'static str> {
    match expr {
        Expr::Cast(cast) if is_timestamp_target(cast.field.data_type()) => {
            Some("written Timestamp cast")
        }
        Expr::TryCast(cast) if is_timestamp_target(cast.field.data_type()) => {
            Some("written Timestamp try-cast")
        }
        Expr::Literal(value, _) if is_live_timestamp_literal(value) => {
            Some("non-null Timestamp literal")
        }
        Expr::ScalarFunction(function) if is_reader_call(function.func.name()) => {
            Some("timestamp reader call")
        }
        Expr::AggregateFunction(function) if is_reader_call(function.func.name()) => {
            Some("timestamp reader call")
        }
        Expr::WindowFunction(function) if is_reader_call(function.fun.name()) => {
            Some("timestamp reader call")
        }
        _ => None,
    }
}

fn fallback_reason(plan: &LogicalPlan) -> Option<&'static str> {
    let mut reason = None;
    let _ = plan.apply_with_subqueries(|node| {
        if reason.is_some() {
            return Ok(TreeNodeRecursion::Stop);
        }
        let _ = node.apply_expressions(|expr| {
            let _ = expr.apply(|leaf| {
                if reason.is_none() {
                    reason = trigger_reason(leaf);
                }
                Ok(TreeNodeRecursion::Continue)
            });
            Ok(TreeNodeRecursion::Continue)
        });
        Ok(TreeNodeRecursion::Continue)
    });
    reason
}

impl AnalyzerRule for SparkTimestampNsCoercion {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        if !plan_opens_gate(&plan) {
            return self.inner.analyze(plan, config);
        }
        GATE_OPEN_COUNT.fetch_add(1, Ordering::Relaxed);
        if let Some(reason) = fallback_reason(&plan) {
            return self.fallback(plan, config, reason);
        }
        let backup = plan.clone();
        match crate::timestamp_ns_coercion_sites::rewrite_sites(plan) {
            Ok(Some(rewritten)) => match self.inner.analyze(rewritten, config) {
                Ok(done) => Ok(done),
                Err(_) => self.fallback(backup, config, "rewrite was rejected"),
            },
            Ok(None) => self.inner.analyze(backup, config),
            Err(error) => self.fallback(backup, config, &error.to_string()),
        }
    }

    fn name(&self) -> &'static str {
        "type_coercion"
    }
}

#[cfg(test)]
mod feed_tests;
#[cfg(test)]
mod tests;
