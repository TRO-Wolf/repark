use std::sync::Arc;
use std::time::Instant;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::{Optimizer, OptimizerContext};
use datafusion::prelude::SessionContext;
use repark_core::ReparkSession;

const ROW_COUNT: i64 = 200_000;
const CAST_SIZES: [usize; 3] = [50, 250, 2500];
const TIMED_REPS: usize = 3;

fn cast_aggregate_sql(cast_count: usize) -> String {
    let items = (0..cast_count)
        .map(|index| format!("CAST(sum(id + {index}) AS VARCHAR) AS c{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {items} FROM cast_src")
}

fn literal_sql(cast_count: usize) -> String {
    let items = (0..cast_count)
        .map(|index| format!("1 AS c{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {items}")
}

fn bare_aggregate_sql(cast_count: usize) -> String {
    let items = (0..cast_count)
        .map(|index| format!("sum(id + {index}) AS c{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {items} FROM cast_src")
}

fn cast_standalone_sql(cast_count: usize) -> String {
    let items = (0..cast_count)
        .map(|index| format!("CAST((id + {index}) AS VARCHAR) AS c{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT {items} FROM cast_src")
}

fn register_cast_source(ctx: &SessionContext) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let ids = Int64Array::from((0..ROW_COUNT).collect::<Vec<i64>>());
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids)]).unwrap();
    let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
    let _registered = ctx.register_table("cast_src", Arc::new(table)).unwrap();
}

fn median(samples: &[f64]) -> f64 {
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted[sorted.len() / 2]
}

fn row(arm: &str, cast_count: usize, phase: &str, seconds: f64) {
    println!("CAST_PROFILE arm={arm} casts={cast_count} phase={phase} seconds={seconds:.6}");
}

fn medians_by_phase(
    arm: &str,
    cast_count: usize,
    reps: &[Vec<(String, f64)>],
) -> Vec<(String, f64)> {
    let mut ordered: Vec<String> = Vec::new();
    for rep in reps {
        for (phase, _) in rep {
            if !ordered.contains(phase) {
                ordered.push(phase.clone());
            }
        }
    }
    let mut medians = Vec::new();
    for phase in ordered {
        let samples: Vec<f64> = reps
            .iter()
            .filter_map(|rep| {
                rep.iter()
                    .find(|(name, _)| *name == phase)
                    .map(|(_, seconds)| *seconds)
            })
            .collect();
        let value = median(&samples);
        row(arm, cast_count, &phase, value);
        medians.push((phase, value));
    }
    medians
}

async fn measure_rep(state: &SessionState, ctx: &SessionContext, sql: &str) -> Vec<(String, f64)> {
    let mut samples: Vec<(String, f64)> = Vec::new();
    let dialect = state.config().options().sql_parser.dialect;
    let started = Instant::now();
    let statement = state.sql_to_statement(sql, &dialect).unwrap();
    samples.push(("parse".to_string(), started.elapsed().as_secs_f64()));
    let started = Instant::now();
    let plan = state.statement_to_plan(statement).await.unwrap();
    samples.push(("sql_to_rel".to_string(), started.elapsed().as_secs_f64()));
    let started = Instant::now();
    let analyzed = state
        .analyzer()
        .execute_and_check(plan, state.config_options(), |_, _| {})
        .unwrap();
    samples.push((
        "analyzer_execute_and_check".to_string(),
        started.elapsed().as_secs_f64(),
    ));
    let started = Instant::now();
    let _optimized = state
        .optimizer()
        .optimize(analyzed, state, |_, _| {})
        .unwrap();
    samples.push((
        "optimizer_optimize".to_string(),
        started.elapsed().as_secs_f64(),
    ));
    let started = Instant::now();
    let frame = ctx.sql(sql).await.unwrap();
    samples.push(("ctx_sql".to_string(), started.elapsed().as_secs_f64()));
    let started = Instant::now();
    let _physical = frame.create_physical_plan().await.unwrap();
    samples.push((
        "create_physical_plan".to_string(),
        started.elapsed().as_secs_f64(),
    ));
    samples
}

fn per_rule_walls(
    arm: &str,
    cast_count: usize,
    state: &SessionState,
    plan: &LogicalPlan,
) -> LogicalPlan {
    let mut working = plan.clone();
    for rule in &state.analyzer().rules {
        let started = Instant::now();
        working = rule.analyze(working, state.config_options()).unwrap();
        row(
            arm,
            cast_count,
            &format!("analyzer_rule:{}", rule.name()),
            started.elapsed().as_secs_f64(),
        );
    }
    working
}

fn per_optimizer_rule_walls(
    arm: &str,
    cast_count: usize,
    state: &SessionState,
    analyzed: &LogicalPlan,
) {
    let mut working = analyzed.clone();
    for rule in &state.optimizer().rules {
        let single = Optimizer::with_rules(vec![Arc::clone(rule)]);
        let config = OptimizerContext::new_with_config_options(Arc::clone(state.config_options()))
            .with_max_passes(1);
        let started = Instant::now();
        match single.optimize(working.clone(), &config, |_, _| {}) {
            Ok(plan) => {
                working = plan;
                row(
                    arm,
                    cast_count,
                    &format!("optimizer_rule:{}", rule.name()),
                    started.elapsed().as_secs_f64(),
                );
            }
            Err(error) => {
                row(
                    arm,
                    cast_count,
                    &format!("optimizer_rule:{}:err:{error}", rule.name()),
                    started.elapsed().as_secs_f64(),
                );
            }
        }
    }
}

async fn variant_probe(arm: &str, ctx: &SessionContext, cast_count: usize) {
    let state = ctx.state();
    let dialect = state.config().options().sql_parser.dialect;
    let mut builder_samples: Vec<f64> = Vec::new();
    for _ in 0..=TIMED_REPS {
        let scan = ctx.sql("SELECT id FROM cast_src").await.unwrap();
        let aggr_exprs: Vec<datafusion::prelude::Expr> = (0..cast_count)
            .map(|index| {
                datafusion::functions_aggregate::expr_fn::sum(
                    datafusion::prelude::col("id")
                        + datafusion::prelude::lit(i64::try_from(index).unwrap()),
                )
            })
            .collect();
        let started = Instant::now();
        let _frame = scan.aggregate(Vec::new(), aggr_exprs).unwrap();
        builder_samples.push(started.elapsed().as_secs_f64());
    }
    builder_samples.remove(0);
    row(
        arm,
        cast_count,
        "variant:builder_aggregate:dataframe_aggregate",
        median(&builder_samples),
    );
    for (variant, sql) in [
        ("bare_aggregate", bare_aggregate_sql(cast_count)),
        ("cast_standalone", cast_standalone_sql(cast_count)),
    ] {
        let mut parse_samples: Vec<f64> = Vec::new();
        let mut rel_samples: Vec<f64> = Vec::new();
        for _ in 0..=TIMED_REPS {
            let started = Instant::now();
            let statement = state.sql_to_statement(&sql, &dialect).unwrap();
            parse_samples.push(started.elapsed().as_secs_f64());
            let started = Instant::now();
            let _plan = state.statement_to_plan(statement).await.unwrap();
            rel_samples.push(started.elapsed().as_secs_f64());
        }
        parse_samples.remove(0);
        rel_samples.remove(0);
        row(
            arm,
            cast_count,
            &format!("variant:{variant}:parse"),
            median(&parse_samples),
        );
        row(
            arm,
            cast_count,
            &format!("variant:{variant}:sql_to_rel"),
            median(&rel_samples),
        );
    }
}

async fn profile_arm(arm: &str, ctx: &SessionContext, cast_count: usize) {
    let state = ctx.state();
    let sql = cast_aggregate_sql(cast_count);
    let literals = literal_sql(cast_count);
    let mut reps: Vec<Vec<(String, f64)>> = Vec::new();
    for _ in 0..=TIMED_REPS {
        reps.push(measure_rep(&state, ctx, &sql).await);
    }
    reps.remove(0);
    let started = Instant::now();
    let lit_statement = state
        .sql_to_statement(&literals, &state.config().options().sql_parser.dialect)
        .unwrap();
    let _lit_plan = state.statement_to_plan(lit_statement).await.unwrap();
    row(
        arm,
        cast_count,
        "parse_and_rel_literals_same_width",
        started.elapsed().as_secs_f64(),
    );
    let medians = medians_by_phase(arm, cast_count, &reps);
    let dialect = state.config().options().sql_parser.dialect;
    let statement = state.sql_to_statement(&sql, &dialect).unwrap();
    let plan = state.statement_to_plan(statement).await.unwrap();
    let analyzed = per_rule_walls(arm, cast_count, &state, &plan);
    per_optimizer_rule_walls(arm, cast_count, &state, &analyzed);
    let plan_equiv: f64 = medians
        .iter()
        .filter(|(phase, _)| {
            matches!(
                phase.as_str(),
                "parse" | "sql_to_rel" | "analyzer_execute_and_check"
            )
        })
        .map(|(_, seconds)| *seconds)
        .sum();
    row(arm, cast_count, "session_sql_equivalent", plan_equiv);
}

#[tokio::test]
#[ignore = "perf probe: cargo test --release -p repark-core --test cast_plan_profile -- --ignored --nocapture"]
async fn cast_over_aggregate_plan_phase_profile() {
    let stock = SessionContext::new();
    register_cast_source(&stock);
    let session = ReparkSession::builder().build().unwrap();
    let repark_ctx = session.context().clone();
    register_cast_source(&repark_ctx);
    for cast_count in CAST_SIZES {
        profile_arm("stock", &stock, cast_count).await;
        profile_arm("repark", &repark_ctx, cast_count).await;
        variant_probe("stock", &stock, cast_count).await;
        variant_probe("repark", &repark_ctx, cast_count).await;
    }
}
