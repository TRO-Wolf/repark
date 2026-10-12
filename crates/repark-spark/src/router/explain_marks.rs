use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::Result;
use datafusion::common::display::StringifiedPlan;
use datafusion::execution::context::{QueryPlanner, SessionState};
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::explain::ExplainExec;
use repark_functions::null_narrowing::{NARROWED_BESIDE_NULL_NAME, NARROWED_BESIDE_VALUE_NAME};

#[derive(Debug)]
pub(crate) struct UnmarkedExplain {
    inner: Arc<dyn QueryPlanner + Send + Sync>,
}

impl UnmarkedExplain {
    pub(crate) fn over(
        inner: Arc<dyn QueryPlanner + Send + Sync>,
    ) -> Arc<dyn QueryPlanner + Send + Sync> {
        Arc::new(Self { inner })
    }
}

#[async_trait]
impl QueryPlanner for UnmarkedExplain {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let planned = self
            .inner
            .create_physical_plan(logical_plan, session_state)
            .await?;
        Ok(unmarked(planned))
    }
}

fn unmarked(planned: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let Some(explain) = planned.as_ref().downcast_ref::<ExplainExec>() else {
        return planned;
    };
    if !explain
        .stringified_plans()
        .iter()
        .any(|row| holds_mark(&row.plan))
    {
        return planned;
    }
    let rows = (explain.stringified_plans().iter())
        .map(|row| StringifiedPlan::new(row.plan_type.clone(), strip_marks(&row.plan)))
        .collect();
    Arc::new(ExplainExec::new(explain.schema(), rows, explain.verbose()))
}

const HIDDEN: [&str; 2] = [NARROWED_BESIDE_NULL_NAME, NARROWED_BESIDE_VALUE_NAME];

fn holds_mark(text: &str) -> bool {
    HIDDEN.iter().any(|name| text.contains(name))
}

fn strip_marks(text: &str) -> String {
    let mut plain = text.to_string();
    for name in HIDDEN {
        let call = format!("{name}(");
        while let Some(start) = plain.find(&call) {
            let open = start + call.len();
            let Some(close) = closing(&plain, open) else {
                break;
            };
            plain.replace_range(close..=close, "");
            plain.replace_range(start..open, "");
        }
    }
    plain
}

fn closing(text: &str, open: usize) -> Option<usize> {
    let mut depth = 0_usize;
    let mut quoted = false;
    for (offset, letter) in text[open..].char_indices() {
        match letter {
            '"' => quoted = !quoted,
            '(' if !quoted => depth += 1,
            ')' if !quoted && depth == 0 => return Some(open + offset),
            ')' if !quoted => depth -= 1,
            _ => {}
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::AsArray;
    use datafusion::logical_expr::PlanType;
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[test]
    fn a_mark_is_erased_from_a_printed_plan_and_its_argument_kept() {
        let marked = format!(
            "Projection: coalesce(CAST({NARROWED_BESIDE_VALUE_NAME}(t.c + f(\")\", 1)) AS \
             Timestamp(µs, \"UTC\")), {NARROWED_BESIDE_NULL_NAME}(t.d)), \
             max({NARROWED_BESIDE_VALUE_NAME}(t.e)) AS v"
        );
        assert_eq!(
            strip_marks(&marked),
            "Projection: coalesce(CAST(t.c + f(\")\", 1) AS Timestamp(µs, \"UTC\")), t.d), \
             max(t.e) AS v"
        );
        assert_eq!(strip_marks("Projection: t.c"), "Projection: t.c");
    }

    #[tokio::test]
    async fn an_explain_plan_prints_no_mark_and_repeats_as_the_engine_does() {
        let ctx = SessionContext::new();
        let explained = ctx.sql("EXPLAIN VERBOSE SELECT 1 AS c").await.unwrap();
        let schema = Arc::new(explained.schema().as_arrow().clone());
        let analyzed = |rule: &str| PlanType::AnalyzedLogicalPlan {
            analyzer_name: rule.to_string(),
        };
        let marked = format!("Projection: f({NARROWED_BESIDE_VALUE_NAME}(t.c))");
        let kept = format!("Projection: g({NARROWED_BESIDE_NULL_NAME}(t.c))");
        let rows = vec![
            StringifiedPlan::new(PlanType::InitialLogicalPlan, "Projection: f(t.c)"),
            StringifiedPlan::new(analyzed("a"), marked),
            StringifiedPlan::new(analyzed("b"), kept),
            StringifiedPlan::new(PlanType::FinalLogicalPlan, "Projection: g(t.c)"),
        ];
        let explain: Arc<dyn ExecutionPlan> =
            Arc::new(ExplainExec::new(Arc::clone(&schema), rows, true));
        let cleaned = unmarked(Arc::clone(&explain));
        assert!(!Arc::ptr_eq(&cleaned, &explain));
        let batches = collect(cleaned, ctx.task_ctx()).await.unwrap();
        let shown: Vec<String> = batches
            .iter()
            .flat_map(|batch| {
                let plans = batch.column(1).as_string::<i32>();
                plans
                    .iter()
                    .flatten()
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            shown,
            vec![
                "Projection: f(t.c)",
                "SAME TEXT AS ABOVE",
                "Projection: g(t.c)",
                "Projection: g(t.c)",
            ]
        );
        let plain: Arc<dyn ExecutionPlan> = Arc::new(ExplainExec::new(
            Arc::clone(&schema),
            vec![StringifiedPlan::new(
                PlanType::FinalLogicalPlan,
                "Projection: t.c",
            )],
            false,
        ));
        assert!(Arc::ptr_eq(&unmarked(Arc::clone(&plain)), &plain));
        let other: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        assert!(Arc::ptr_eq(&unmarked(Arc::clone(&other)), &other));
    }
}
