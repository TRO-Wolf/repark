use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{
    Distinct, DistinctOn, Execute, Expr, LogicalPlan, Partitioning, Statement,
};
use pyo3::prelude::PyResult;
use repark_core::column_resolution::{on_grown_stack_with, remaining_stack, run_on_grown_stack};
use std::future::Future;
use std::io;
use tokio::runtime::{Builder, Runtime};

use crate::exceptions::AnalysisException;

pub(crate) const GROWN_STACK_SEGMENT_BYTES: usize = 128 * 1024 * 1024;
pub(crate) const RUNTIME_THREAD_STACK_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const DEEP_NESTING_DEPTH: usize = 16;
pub(crate) const MAX_EXPRESSION_DEPTH: usize = 1500;
pub(crate) const MAX_PLAN_DEPTH: usize = 8192;
pub(crate) const GROWN_SEGMENT_BYTES_PER_PLAN_LEVEL: usize = 128 * 1024;
pub(crate) const MAX_GROWN_SEGMENT_BYTES: usize = 1024 * 1024 * 1024;
pub(crate) const GROWN_SQL_TEXT_LEN: usize = 4096;
pub(crate) const SMALL_STACK_REMAINING_BYTES: usize = 2 * 1024 * 1024;
pub(crate) const EXPR_GROWN_BYTES_PER_LEVEL: usize = 8 * 1024;

pub(crate) fn build_shared_runtime() -> io::Result<Runtime> {
    Builder::new_multi_thread()
        .enable_all()
        .thread_stack_size(RUNTIME_THREAD_STACK_BYTES)
        .build()
}

pub(crate) fn block_on<F: Future>(runtime: &Runtime, future: F) -> F::Output {
    runtime.block_on(on_grown_stack_with(
        GROWN_STACK_SEGMENT_BYTES,
        GROWN_STACK_SEGMENT_BYTES,
        future,
    ))
}

pub(crate) fn block_on_grown_if<F: Future>(runtime: &Runtime, future: F, grown: bool) -> F::Output {
    if grown {
        block_on(runtime, future)
    } else {
        runtime.block_on(future)
    }
}

pub(crate) fn block_on_grown_sized<F: Future>(
    runtime: &Runtime,
    future: F,
    segment: Option<usize>,
) -> F::Output {
    match segment {
        Some(bytes) => runtime.block_on(on_grown_stack_with(bytes, bytes, future)),
        None => runtime.block_on(future),
    }
}

pub(crate) fn run_grown_if<T>(runtime: &Runtime, grown: bool, work: impl FnOnce() -> T) -> T {
    block_on_grown_if(runtime, async { work() }, grown)
}

pub(crate) fn grow_expr_if_needed<T>(depth: usize, work: impl FnOnce() -> T) -> T {
    let need = depth
        .saturating_mul(EXPR_GROWN_BYTES_PER_LEVEL)
        .min(MAX_GROWN_SEGMENT_BYTES);
    run_on_grown_stack(need, need, work)
}

pub(crate) fn grown_clone(expr: &Expr) -> Expr {
    grow_expr_if_needed(expression_depth(expr), || expr.clone())
}

#[derive(Debug)]
pub(crate) struct PlanDepths {
    pub(crate) plan: usize,
    pub(crate) limited: usize,
    pub(crate) expression: usize,
}

fn union_free_depth(node: &LogicalPlan) -> usize {
    usize::from(!matches!(node, LogicalPlan::Union(_)))
}

pub(crate) fn plan_depths(plan: &LogicalPlan) -> PlanDepths {
    let mut deepest_plan = 0;
    let mut deepest_limited = 0;
    let mut deepest_expression = 0;
    let mut pending_plans = vec![(plan, 1_usize, union_free_depth(plan))];
    while let Some((node, plan_depth, limited_depth)) = pending_plans.pop() {
        deepest_plan = deepest_plan.max(plan_depth);
        deepest_limited = deepest_limited.max(limited_depth);
        for input in node.inputs() {
            pending_plans.push((
                input,
                plan_depth + 1,
                limited_depth + union_free_depth(input),
            ));
        }
        for expr in node_expressions(node) {
            let (depth, subqueries) = expression_depth_and_subqueries(expr);
            deepest_expression = deepest_expression.max(depth);
            for subquery in subqueries {
                pending_plans.push((
                    subquery,
                    plan_depth + 1,
                    limited_depth + union_free_depth(subquery),
                ));
            }
        }
    }
    PlanDepths {
        plan: deepest_plan,
        limited: deepest_limited,
        expression: deepest_expression,
    }
}

fn node_expressions(node: &LogicalPlan) -> Vec<&Expr> {
    match node {
        LogicalPlan::Projection(p) => p.expr.iter().collect(),
        LogicalPlan::Filter(p) => vec![&p.predicate],
        LogicalPlan::Window(p) => p.window_expr.iter().collect(),
        LogicalPlan::Aggregate(p) => p.group_expr.iter().chain(p.aggr_expr.iter()).collect(),
        LogicalPlan::Sort(p) => p.expr.iter().map(|sort| &sort.expr).collect(),
        LogicalPlan::Join(p) => {
            p.on.iter()
                .flat_map(|(left, right)| [left, right])
                .chain(p.filter.iter())
                .collect()
        }
        LogicalPlan::Repartition(p) => match &p.partitioning_scheme {
            Partitioning::Hash(exprs, _) | Partitioning::DistributeBy(exprs) => {
                exprs.iter().collect()
            }
            Partitioning::RoundRobinBatch(_) => Vec::new(),
        },
        LogicalPlan::TableScan(p) => p.filters.iter().collect(),
        LogicalPlan::Limit(p) => p
            .skip
            .iter()
            .chain(p.fetch.iter())
            .map(AsRef::as_ref)
            .collect(),
        LogicalPlan::Values(p) => p.values.iter().flatten().collect(),
        LogicalPlan::Distinct(Distinct::On(DistinctOn {
            on_expr,
            select_expr,
            sort_expr,
            ..
        })) => on_expr
            .iter()
            .chain(select_expr.iter())
            .chain(sort_expr.iter().flatten().map(|sort| &sort.expr))
            .collect(),
        LogicalPlan::Statement(s) => match s {
            Statement::Execute(Execute { parameters, .. }) => parameters.iter().collect(),
            _ => Vec::new(),
        },
        LogicalPlan::Extension(_)
        | LogicalPlan::Unnest(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::RecursiveQuery(_)
        | LogicalPlan::Subquery(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::Analyze(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::Distinct(Distinct::All(_))
        | LogicalPlan::Dml(_)
        | LogicalPlan::Ddl(_)
        | LogicalPlan::Copy(_)
        | LogicalPlan::DescribeTable(_) => Vec::new(),
    }
}

pub(crate) fn expression_depth(expr: &Expr) -> usize {
    expression_depth_and_subqueries(expr).0
}

fn expression_depth_and_subqueries(expr: &Expr) -> (usize, Vec<&LogicalPlan>) {
    let mut deepest = 0;
    let mut subqueries = Vec::new();
    let mut pending = vec![(expr, 1_usize)];
    while let Some((node, depth)) = pending.pop() {
        deepest = deepest.max(depth);
        match node {
            Expr::ScalarSubquery(query) => subqueries.push(query.subquery.as_ref()),
            Expr::Exists(exists) => subqueries.push(exists.subquery.subquery.as_ref()),
            Expr::InSubquery(query) => subqueries.push(query.subquery.subquery.as_ref()),
            _ => {}
        }
        let _ = node.apply_children(|child| {
            pending.push((child, depth + 1));
            Ok(TreeNodeRecursion::Continue)
        });
    }
    (deepest, subqueries)
}

pub(crate) fn refuse_overdeep_plan(plan: &LogicalPlan) -> PyResult<PlanDepths> {
    let depths = refuse_overdeep_plan_inputs(plan)?;
    refuse_expression_depth(depths.expression)?;
    Ok(depths)
}

pub(crate) fn refuse_overdeep_plan_inputs(plan: &LogicalPlan) -> PyResult<PlanDepths> {
    let depths = plan_depths(plan);
    if depths.limited > MAX_PLAN_DEPTH {
        return Err(AnalysisException::new_err(format!(
            "plan depth {} exceeds the supported maximum of {MAX_PLAN_DEPTH} (deep-plan limit)",
            depths.limited
        )));
    }
    Ok(depths)
}

pub(crate) fn refuse_expression_depth(depth: usize) -> PyResult<()> {
    if depth > MAX_EXPRESSION_DEPTH {
        return Err(AnalysisException::new_err(format!(
            "expression depth {depth} exceeds the supported maximum of {MAX_EXPRESSION_DEPTH} (deep-expression limit)"
        )));
    }
    Ok(())
}

pub(crate) fn drive_segment_bytes(depths: &PlanDepths) -> Option<usize> {
    if depths.plan <= DEEP_NESTING_DEPTH && depths.expression <= DEEP_NESTING_DEPTH {
        None
    } else {
        Some(
            depths
                .plan
                .saturating_mul(GROWN_SEGMENT_BYTES_PER_PLAN_LEVEL)
                .clamp(GROWN_STACK_SEGMENT_BYTES, MAX_GROWN_SEGMENT_BYTES),
        )
    }
}

pub(crate) fn sql_needs_grown_stack(query: &str) -> bool {
    query.len() > GROWN_SQL_TEXT_LEN
}

pub(crate) fn sql_drive_grown(query: &str) -> bool {
    sql_needs_grown_stack(query) || stack_is_small()
}

pub(crate) fn stack_is_small() -> bool {
    remaining_stack().is_none_or(|remaining| remaining < SMALL_STACK_REMAINING_BYTES)
}

pub(crate) fn frame_drive_segment(frame: &DataFrame) -> PyResult<Option<usize>> {
    let depths = refuse_overdeep_plan_inputs(frame.logical_plan())?;
    if drive_segment_bytes(&depths).is_none() && stack_is_small() {
        return Ok(Some(GROWN_STACK_SEGMENT_BYTES));
    }
    Ok(drive_segment_bytes(&depths))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hint::black_box;
    use std::panic::AssertUnwindSafe;

    fn recurse_with_kilobyte_frames(levels: u32) -> u32 {
        fn descend(remaining: u32, tally: u32) -> u32 {
            let pad = [1u8; 1024];
            black_box(pad);
            if remaining == 0 {
                tally
            } else {
                descend(remaining - 1, tally + 1)
            }
        }
        descend(levels, 0)
    }

    fn test_runtime() -> Runtime {
        Runtime::new().expect("a test runtime builds")
    }

    #[test]
    fn block_on_returns_the_future_output() {
        let runtime = test_runtime();
        assert_eq!(block_on(&runtime, async { 40 + 2 }), 42);
    }

    #[test]
    fn block_on_drives_recursion_beyond_default_thread_stacks() {
        let runtime = test_runtime();
        let levels = 4_000;
        assert_eq!(
            block_on(&runtime, async { recurse_with_kilobyte_frames(levels) }),
            levels,
            "twelve megabytes of recursion complete on the grown stack"
        );
    }

    #[test]
    fn block_on_grown_if_drives_deep_recursion_when_grown() {
        let runtime = test_runtime();
        let levels = 4_000;
        assert_eq!(
            block_on_grown_if(
                &runtime,
                async { recurse_with_kilobyte_frames(levels) },
                true
            ),
            levels,
            "twelve megabytes of recursion complete on the grown stack"
        );
    }

    #[test]
    fn block_on_grown_if_returns_output_when_ungrown() {
        let runtime = test_runtime();
        assert_eq!(block_on_grown_if(&runtime, async { 40 + 2 }, false), 42);
    }

    #[test]
    fn plan_depths_split_shallow_and_deep_chains() {
        use datafusion::logical_expr::lit;
        use datafusion::prelude::SessionContext;
        let context = SessionContext::new();
        let shallow = context.read_empty().expect("an empty frame builds");
        let depths = plan_depths(shallow.logical_plan());
        assert_eq!(depths.plan, 1);
        assert_eq!(depths.expression, 0);
        assert_eq!(drive_segment_bytes(&depths), None);
        let mut chained = shallow.filter(lit(true)).expect("a first filter builds");
        for _ in 0..DEEP_NESTING_DEPTH {
            chained = chained.filter(lit(true)).expect("a chained filter builds");
        }
        let depths = plan_depths(chained.logical_plan());
        assert_eq!(depths.plan, DEEP_NESTING_DEPTH + 2);
        assert_eq!(
            drive_segment_bytes(&depths),
            Some(GROWN_STACK_SEGMENT_BYTES)
        );
    }

    #[test]
    fn plan_depths_see_through_scalar_subqueries() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion::common::Spans;
        use datafusion::logical_expr::Subquery;
        use datafusion::logical_expr::{Operator, binary_expr, lit};
        use datafusion::prelude::SessionContext;
        use std::sync::Arc;
        let context = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch_of = || {
            arrow::array::RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )
            .expect("a probe batch builds")
        };
        let mut deep = context.read_batch(batch_of()).expect("a deep frame builds");
        for _ in 0..DEEP_NESTING_DEPTH + 4 {
            deep = deep
                .filter(binary_expr(
                    datafusion::prelude::col("a"),
                    Operator::Gt,
                    lit(0_i64),
                ))
                .expect("a chained filter builds");
        }
        let subquery = Expr::ScalarSubquery(Subquery {
            subquery: Arc::new(deep.logical_plan().clone()),
            outer_ref_columns: Vec::new(),
            spans: Spans::new(),
        });
        let outer = context
            .read_batch(batch_of())
            .expect("an outer frame builds")
            .filter(binary_expr(subquery, Operator::Gt, lit(0_i64)))
            .expect("a subquery predicate builds");
        let depths = plan_depths(outer.logical_plan());
        assert!(
            depths.plan > DEEP_NESTING_DEPTH,
            "the subquery plan depth counts toward the verdict: {}",
            depths.plan
        );
        assert!(drive_segment_bytes(&depths).is_some());
    }

    #[test]
    fn refuse_expression_depth_splits_at_the_cap() {
        use datafusion::logical_expr::{Operator, binary_expr, lit};
        use datafusion::prelude::col;
        let mut capped = col("a");
        for _ in 0..MAX_EXPRESSION_DEPTH - 1 {
            capped = binary_expr(capped, Operator::Plus, lit(1));
        }
        let depth = expression_depth(&capped);
        assert_eq!(depth, MAX_EXPRESSION_DEPTH);
        assert!(refuse_expression_depth(depth).is_ok());
        let over = binary_expr(capped, Operator::Plus, lit(1));
        let error =
            refuse_expression_depth(expression_depth(&over)).expect_err("past the cap refuses");
        assert!(
            error.to_string().contains("deep-expression limit"),
            "the refusal names the limit: {error}"
        );
    }

    #[test]
    fn refuse_overdeep_plan_splits_at_the_plan_cap() {
        use datafusion::logical_expr::Filter;
        use datafusion::logical_expr::LogicalPlanBuilder;
        use datafusion::logical_expr::lit;
        use std::sync::Arc;
        let mut plan = LogicalPlanBuilder::empty(false)
            .build()
            .expect("an empty plan builds");
        for _ in 0..=MAX_PLAN_DEPTH {
            plan = LogicalPlan::Filter(
                Filter::try_new(lit(true), Arc::new(plan)).expect("a filter wraps"),
            );
        }
        let error = refuse_overdeep_plan(&plan).expect_err("a plan past the cap refuses");
        assert!(
            error.to_string().contains("deep-plan limit"),
            "the refusal names the limit: {error}"
        );
    }

    #[test]
    fn plan_depths_ignore_union_spines_for_the_plan_cap() {
        use datafusion::logical_expr::LogicalPlanBuilder;
        use datafusion::logical_expr::Union;
        use std::sync::Arc;
        std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(|| {
                let leaf = LogicalPlanBuilder::empty(false)
                    .build()
                    .expect("an empty plan builds");
                let mut plan = leaf.clone();
                for _ in 0..MAX_PLAN_DEPTH + 8 {
                    plan = LogicalPlan::Union(
                        Union::try_new(vec![Arc::new(plan), Arc::new(leaf.clone())])
                            .expect("a union wraps"),
                    );
                }
                let depths = plan_depths(&plan);
                assert!(
                    depths.plan > MAX_PLAN_DEPTH,
                    "the union spine runs past the cap: {}",
                    depths.plan
                );
                assert_eq!(depths.limited, 1);
                assert!(refuse_overdeep_plan_inputs(&plan).is_ok());
            })
            .expect("a big-stack test thread spawns")
            .join()
            .expect("the big-stack test thread joins");
    }

    #[test]
    fn drive_segment_bytes_scales_with_plan_depth() {
        let deep = PlanDepths {
            plan: MAX_PLAN_DEPTH,
            limited: MAX_PLAN_DEPTH,
            expression: 1,
        };
        assert_eq!(drive_segment_bytes(&deep), Some(MAX_GROWN_SEGMENT_BYTES));
        let expression_only = PlanDepths {
            plan: 2,
            limited: 2,
            expression: DEEP_NESTING_DEPTH + 1,
        };
        assert_eq!(
            drive_segment_bytes(&expression_only),
            Some(GROWN_STACK_SEGMENT_BYTES)
        );
    }

    #[test]
    fn sql_growth_gate_splits_at_the_length_bound() {
        assert!(!sql_needs_grown_stack("SELECT 1"));
        assert!(sql_needs_grown_stack(&" ".repeat(GROWN_SQL_TEXT_LEN + 1)));
        assert!(sql_drive_grown(&" ".repeat(GROWN_SQL_TEXT_LEN + 1)));
        assert!(sql_drive_grown(&" ".repeat(2 * 1024 * 1024)));
    }

    #[test]
    fn block_on_drives_a_borrowing_future() {
        let runtime = test_runtime();
        let borrowed = 21_u32;
        assert_eq!(
            block_on(&runtime, async { borrowed * 2 }),
            42,
            "a future borrowing a caller local drives without an owned rewrite"
        );
    }

    #[test]
    fn block_on_propagates_a_future_panic_to_the_caller() {
        let runtime = test_runtime();
        let caught = std::panic::catch_unwind(AssertUnwindSafe(|| {
            block_on(&runtime, async {
                panic!("deep-stack-probe-panic");
            });
        }));
        let payload = caught.expect_err("a panicking future must unwind through block_on");
        let text = payload
            .downcast_ref::<&str>()
            .expect("the probe payload is a static str");
        assert!(
            text.contains("deep-stack-probe-panic"),
            "the panic payload crosses the grown stack unchanged: {text}"
        );
    }

    #[test]
    fn shared_runtime_blocking_threads_survive_deep_recursion() {
        let runtime = build_shared_runtime().expect("the shared runtime builds");
        let levels = 4_000;
        let joined =
            runtime.block_on(runtime.spawn_blocking(move || recurse_with_kilobyte_frames(levels)));
        assert_eq!(
            joined.expect("the blocking task joins"),
            levels,
            "twelve megabytes of recursion complete on a runtime blocking thread"
        );
    }

    #[test]
    fn shared_runtime_drives_futures() {
        let runtime = build_shared_runtime().expect("the shared runtime builds");
        assert_eq!(runtime.block_on(async { 7 * 6 }), 42);
    }

    #[test]
    fn grown_expression_clone_and_drop_serve_sub_megabyte_threads() {
        use crate::column::PyColumn;
        use datafusion::logical_expr::{Operator, binary_expr, lit};
        use datafusion::prelude::col;
        let mut deep = col("a");
        for _ in 0..2000 {
            deep = binary_expr(deep, Operator::Plus, lit(1));
        }
        let held = PyColumn::from_expr(deep);
        std::thread::Builder::new()
            .stack_size(512 * 1024)
            .spawn(move || {
                let copied = held.expr();
                assert_eq!(expression_depth(&copied), held.expression_depth());
            })
            .expect("a small-stack test thread spawns")
            .join()
            .expect("the small-stack test thread joins");
    }

    #[test]
    fn grown_column_combine_and_drop_survive_deep_trees() {
        use crate::column::PyColumn;
        use datafusion::logical_expr::{Operator, binary_expr, lit};
        use datafusion::prelude::col;
        const _: () = assert!(20000 * EXPR_GROWN_BYTES_PER_LEVEL <= MAX_GROWN_SEGMENT_BYTES);
        let mut deep = binary_expr(col("a"), Operator::Eq, lit(0_i64));
        for index in 1_i64..20000 {
            deep = binary_expr(
                deep,
                Operator::Or,
                binary_expr(col("a"), Operator::Eq, lit(index)),
            );
        }
        let depth = expression_depth(&deep);
        assert!(depth > MAX_EXPRESSION_DEPTH);
        std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || {
                let left = PyColumn::from_expr(deep);
                let right = PyColumn::from_expr(lit(true));
                let copied = left.clone();
                assert_eq!(copied.expression_depth(), depth);
                let combined = left.or_(&right).expect("a deep or-combine builds");
                assert_eq!(combined.expression_depth(), depth + 1);
            })
            .expect("a small-stack test thread spawns")
            .join()
            .expect("the small-stack test thread joins");
    }

    #[test]
    fn frame_drive_segment_grows_past_the_expression_cap() {
        use datafusion::logical_expr::{Operator, binary_expr, lit};
        use datafusion::prelude::SessionContext;
        let context = SessionContext::new();
        let mut over = lit(true);
        for _ in 0..MAX_EXPRESSION_DEPTH {
            over = binary_expr(over, Operator::And, lit(true));
        }
        let frame = context
            .read_empty()
            .expect("an empty frame builds")
            .filter(over)
            .expect("a deep predicate builds");
        assert!(
            frame_drive_segment(&frame)
                .expect("terminals grow past the expression cap")
                .is_some()
        );
    }
}
