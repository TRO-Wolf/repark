use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::compute::{concat_batches, sort_to_indices, take_record_batch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{JoinType, NullEquality};
use datafusion::datasource::memory::{MemTable, MemorySourceConfig};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::{
    ColumnarValue, Operator, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::logical_expr::{WindowFrame, WindowFunctionDefinition};
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::window::WindowExpr;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::windows::{WindowAggExec, create_window_expr};
use datafusion::physical_plan::{
    ExecutionPlan, ExecutionPlanProperties, Partitioning, collect, displayable,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use futures::StreamExt;

use super::projection::{
    PARALLEL_PROJECTION_MIN_ROWS, ParallelProjectionExec, ParallelProjectionRule,
};
use super::{ParallelSinglePartitionConfig, ParallelWindowRule};
use crate::ReparkSession;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("x", DataType::Float64, true),
        Field::new("y", DataType::Float64, true),
        Field::new("z", DataType::Float64, true),
    ]))
}

fn batch(start: i64, rows: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + rows).collect();
    let column = |scale: f64| -> ArrayRef {
        Arc::new(
            ids.iter()
                .map(|&id| {
                    let x = f64::from(u32::try_from(id).unwrap_or_default());
                    if id % 19 == 7 {
                        None
                    } else {
                        Some((x * scale).sin() * 100.0 + x * 0.001)
                    }
                })
                .collect::<Float64Array>(),
        )
    };
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids.clone())),
            column(0.37),
            column(1.13),
            column(2.71),
        ],
    )
    .expect("probe batch")
}

fn batches(sizes: &[i64]) -> Vec<RecordBatch> {
    let mut start = 0;
    sizes
        .iter()
        .map(|&rows| {
            let made = batch(start, rows);
            start += rows;
            made
        })
        .collect()
}

fn big() -> i64 {
    i64::try_from(PARALLEL_PROJECTION_MIN_ROWS).unwrap_or(i64::MAX) * 3
}

fn source(sizes: &[i64]) -> Arc<dyn ExecutionPlan> {
    MemorySourceConfig::try_new_exec(&[batches(sizes)], schema(), None).expect("probe source")
}

fn column(name: &str) -> Arc<dyn PhysicalExpr> {
    let index = schema().index_of(name).expect("probe column");
    Arc::new(Column::new(name, index))
}

fn at(name: &str, index: usize) -> Arc<dyn PhysicalExpr> {
    Arc::new(Column::new(name, index))
}

fn binary(
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(left, op, right))
}

fn float(value: f64) -> Arc<dyn PhysicalExpr> {
    Arc::new(Literal::new(ScalarValue::Float64(Some(value))))
}

fn item(expr: Arc<dyn PhysicalExpr>, alias: &str) -> ProjectionExpr {
    ProjectionExpr {
        expr,
        alias: alias.to_owned(),
    }
}

fn project(input: Arc<dyn ExecutionPlan>, exprs: Vec<ProjectionExpr>) -> Arc<dyn ExecutionPlan> {
    Arc::new(ProjectionExec::try_new(exprs, input).expect("probe projection"))
}

fn round_robin(input: Arc<dyn ExecutionPlan>, preserve: bool) -> Arc<dyn ExecutionPlan> {
    let repartition = RepartitionExec::try_new(input, Partitioning::RoundRobinBatch(4))
        .expect("probe round robin");
    if preserve {
        Arc::new(repartition.with_preserve_order())
    } else {
        Arc::new(repartition)
    }
}

fn by_id() -> LexOrdering {
    LexOrdering::new([PhysicalSortExpr::new_default(column("id"))]).expect("ordering")
}

fn sorted(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(SortExec::new(by_id(), input))
}

fn whole_sum(input: &Arc<dyn ExecutionPlan>, name: &str) -> Arc<dyn WindowExpr> {
    let index = input.schema().index_of(name).expect("window column");
    create_window_expr(
        &WindowFunctionDefinition::AggregateUDF(sum_udaf()),
        format!("sum_{name}"),
        &[Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>],
        &[],
        &[],
        Arc::new(WindowFrame::new(None)),
        input.schema(),
        false,
        false,
        None,
    )
    .expect("window expression")
}

fn window(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let exprs = vec![whole_sum(&input, "x"), whole_sum(&input, "y")];
    Arc::new(WindowAggExec::try_new(exprs, input, true).expect("window"))
}

fn options(enabled: bool) -> ConfigOptions {
    let mut options = ConfigOptions::new();
    options
        .extensions
        .insert(ParallelSinglePartitionConfig { enabled });
    options
}

fn optimize(plan: Arc<dyn ExecutionPlan>, enabled: bool) -> Arc<dyn ExecutionPlan> {
    ParallelProjectionRule
        .optimize(plan, &options(enabled))
        .expect("rule")
}

fn text(plan: &Arc<dyn ExecutionPlan>) -> String {
    displayable(plan.as_ref()).indent(true).to_string()
}

fn context(target_partitions: usize) -> Arc<TaskContext> {
    Arc::new(
        TaskContext::default().with_session_config(
            SessionConfig::new()
                .with_target_partitions(target_partitions)
                .with_batch_size(1 << 20),
        ),
    )
}

async fn run(plan: Arc<dyn ExecutionPlan>) -> Result<RecordBatch> {
    let schema = plan.schema();
    let batches = collect(plan, context(8)).await?;
    Ok(concat_batches(&schema, &batches)?)
}

fn sorted_by_id(batch: &RecordBatch) -> RecordBatch {
    let indices = sort_to_indices(batch.column(0), None, None).expect("sort ids");
    take_record_batch(batch, &indices).expect("take")
}

fn assert_bit_identical(got: &RecordBatch, expected: &RecordBatch) {
    assert_eq!(got.schema(), expected.schema());
    assert_eq!(got.num_rows(), expected.num_rows());
    for (index, (left, right)) in got.columns().iter().zip(expected.columns()).enumerate() {
        assert_eq!(
            left.to_data().nulls(),
            right.to_data().nulls(),
            "column {index} validity"
        );
        match (
            left.as_any().downcast_ref::<Float64Array>(),
            right.as_any().downcast_ref::<Float64Array>(),
        ) {
            (Some(left), Some(right)) => {
                for (row, (a, b)) in left.values().iter().zip(right.values().iter()).enumerate() {
                    assert_eq!(a.to_bits(), b.to_bits(), "column {index} row {row}");
                }
            }
            _ => assert_eq!(left, right, "column {index}"),
        }
    }
}

fn find<'a>(plan: &'a Arc<dyn ExecutionPlan>, name: &str) -> Vec<&'a Arc<dyn ExecutionPlan>> {
    let mut found = Vec::new();
    let mut stack = vec![plan];
    while let Some(node) = stack.pop() {
        if node.name() == name {
            found.push(node);
        }
        stack.extend(node.children());
    }
    found
}

fn round_robins(plan: &Arc<dyn ExecutionPlan>) -> usize {
    find(plan, "RepartitionExec")
        .iter()
        .filter(|node| matches!(node.output_partitioning(), Partitioning::RoundRobinBatch(_)))
        .count()
}

fn parallel_batches(plan: &Arc<dyn ExecutionPlan>) -> usize {
    find(plan, "ParallelProjectionExec")
        .iter()
        .filter_map(|node| node.metrics())
        .flat_map(|metrics| {
            metrics
                .iter()
                .filter_map(|metric| match metric.value() {
                    MetricValue::Count { name, count } if name == "parallel_batches" => {
                        Some(count.value())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .sum()
}

fn sql_session(enabled: bool, sizes: &[i64]) -> SessionContext {
    let session = ReparkSession::builder()
        .target_partitions(4)
        .config("datafusion.optimizer.repartition_file_scans", "false")
        .parallel_single_partition(enabled)
        .build()
        .expect("session");
    let context = session.context().clone();
    let table = MemTable::try_new(schema(), vec![batches(sizes)]).expect("table");
    context
        .register_table("t", Arc::new(table))
        .expect("register");
    context.register_udf(ScalarUDF::new_from_impl(SeededRand::new()));
    context
}

async fn sql_run(context: &SessionContext, query: &str) -> (Arc<dyn ExecutionPlan>, RecordBatch) {
    let frame = context.sql(query).await.expect("sql");
    let plan = frame.create_physical_plan().await.expect("plan");
    let schema = plan.schema();
    let batches = collect(Arc::clone(&plan), context.task_ctx())
        .await
        .expect("collect");
    (plan, concat_batches(&schema, &batches).expect("concat"))
}

const OWNER_PROJECTION: &str = "SELECT id, \
    round(x * 1.5, 4) AS r1, round(y * 2.5, 4) AS r2, round(z * 3.5, 4) AS r3, \
    round(x + y, 4) AS r4, round(y + z, 4) AS r5, round(x - z, 4) AS r6, \
    round(x * y, 4) AS r7, round(y * z, 4) AS r8, round(x * z, 4) AS r9, \
    round(x / 7.0, 4) AS r10, round(y / 11.0, 4) AS r11, round(z / 13.0, 4) AS r12, \
    round(x + y + z, 4) AS r13, \
    x / y AS d1, y / z AS d2, round(z * 0.5, 4) / x AS d3, \
    CASE WHEN x > y THEN x ELSE y END AS c1, w \
    FROM (SELECT id, x, y, z, sum(x) OVER () AS w FROM t) AS s";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_projection_bit_identical() {
    let sizes = [big(), 4096, big()];
    let (plan_on, got) = sql_run(&sql_session(true, &sizes), OWNER_PROJECTION).await;
    let (plan_off, expected) = sql_run(&sql_session(false, &sizes), OWNER_PROJECTION).await;
    let node = find(&plan_on, "ParallelProjectionExec");
    assert_eq!(node.len(), 1, "{}", text(&plan_on));
    assert_eq!(round_robins(&plan_on), 0, "{}", text(&plan_on));
    assert_eq!(round_robins(&plan_off), 1, "{}", text(&plan_off));
    assert_eq!(parallel_batches(&plan_on), 1, "{}", text(&plan_on));
    assert_eq!(got.num_columns(), 19);
    assert_eq!(got.num_rows(), expected.num_rows());
    assert_bit_identical(&got, &sorted_by_id(&expected));
}

fn sandwich(merge: bool) -> Arc<dyn ExecutionPlan> {
    let level_one = window(sorted(source(&[700, 900, 400])));
    let spread = round_robin(level_one, merge);
    let p_b = project(
        spread,
        vec![
            item(
                binary(column("x"), Operator::Multiply, float(2.0)),
                "common",
            ),
            item(column("id"), "id"),
            item(column("x"), "x"),
            item(column("y"), "y"),
            item(column("z"), "z"),
        ],
    );
    let p_a = project(
        p_b,
        vec![
            item(at("id", 1), "id"),
            item(at("x", 2), "x"),
            item(at("y", 3), "y"),
            item(at("z", 4), "z"),
            item(at("common", 0), "ema"),
            item(
                binary(at("common", 0), Operator::Divide, at("y", 3)),
                "test",
            ),
        ],
    );
    let merged: Arc<dyn ExecutionPlan> = if merge {
        Arc::new(SortPreservingMergeExec::new(by_id(), p_a))
    } else {
        Arc::new(CoalescePartitionsExec::new(p_a))
    };
    let p_c = project(
        window(merged),
        vec![
            item(at("id", 0), "id"),
            item(at("x", 1), "x"),
            item(at("y", 2), "y"),
            item(at("ema", 4), "ema"),
            item(at("test", 5), "test"),
        ],
    );
    let top_spread = round_robin(p_c, merge);
    let p_d = project(
        top_spread,
        vec![
            item(at("id", 0), "id"),
            item(at("x", 1), "x"),
            item(binary(at("x", 1), Operator::Multiply, float(3.0)), "a"),
            item(binary(at("y", 2), Operator::Plus, float(1.0)), "b"),
            item(binary(at("ema", 3), Operator::Divide, at("x", 1)), "c"),
        ],
    );
    project(
        p_d,
        vec![
            item(at("id", 0), "id"),
            item(at("a", 2), "a"),
            item(binary(at("a", 2), Operator::Minus, at("b", 3)), "e"),
            item(binary(at("c", 4), Operator::Plus, at("x", 1)), "f"),
        ],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_projection_removes_rr_spm_sandwich() {
    for merge in [true, false] {
        let base = sandwich(merge);
        assert_eq!(round_robins(&base), 2);
        let head = optimize(Arc::clone(&base), true);
        assert_eq!(round_robins(&head), 0, "{}", text(&head));
        assert_eq!(head.output_partitioning().partition_count(), 1);
        let above = if merge {
            "SortPreservingMergeExec"
        } else {
            "CoalescePartitionsExec"
        };
        let merges = find(&head, above);
        assert_eq!(merges.len(), 1);
        let chain_top = merges[0].children()[0];
        assert_eq!(chain_top.output_partitioning().partition_count(), 1);
        assert_eq!(chain_top.name(), "ProjectionExec");
        assert_eq!(chain_top.children()[0].name(), "ProjectionExec");
        assert_eq!(
            chain_top.children()[0].children()[0].name(),
            "WindowAggExec"
        );
        assert_eq!(
            find(&head, "ParallelProjectionExec").len(),
            2,
            "{}",
            text(&head)
        );
        let expected = run(base).await.expect("base");
        let got = run(head).await.expect("head");
        if merge {
            assert_bit_identical(&got, &expected);
        } else {
            assert_bit_identical(&sorted_by_id(&got), &sorted_by_id(&expected));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_projection_small_input_serial() {
    let exprs = || {
        vec![
            item(column("id"), "id"),
            item(binary(column("x"), Operator::Multiply, float(2.0)), "a"),
            item(binary(column("y"), Operator::Plus, column("z")), "b"),
            item(binary(column("z"), Operator::Divide, float(3.0)), "c"),
        ]
    };
    let small = optimize(project(source(&[700, 900, 400]), exprs()), true);
    assert_eq!(find(&small, "ParallelProjectionExec").len(), 1);
    let got = run(Arc::clone(&small)).await.expect("small");
    assert_eq!(parallel_batches(&small), 0);
    let expected = run(project(source(&[700, 900, 400]), exprs()))
        .await
        .expect("serial");
    assert_bit_identical(&got, &expected);

    let large = optimize(project(source(&[big(), 300]), exprs()), true);
    let got = run(Arc::clone(&large)).await.expect("large");
    assert_eq!(parallel_batches(&large), 1);
    let expected = run(project(source(&[big(), 300]), exprs()))
        .await
        .expect("serial");
    assert_bit_identical(&got, &expected);
}

#[test]
fn parallel_projection_respects_parent_ordering() {
    let spread = || {
        project(
            round_robin(window(sorted(source(&[700, 900, 400]))), true),
            vec![
                item(column("id"), "id"),
                item(binary(column("x"), Operator::Multiply, float(2.0)), "a"),
            ],
        )
    };
    let by_a = LexOrdering::new([PhysicalSortExpr::new_default(at("a", 1))]).expect("ordering");
    let wrong_order: Arc<dyn ExecutionPlan> =
        Arc::new(SortPreservingMergeExec::new(by_a, spread()));
    let kept = optimize(Arc::clone(&wrong_order), true);
    assert_eq!(text(&kept), text(&wrong_order));
    assert_eq!(round_robins(&kept), 1);

    let hashed: Arc<dyn ExecutionPlan> = Arc::new(
        RepartitionExec::try_new(
            source(&[700, 900, 400]),
            Partitioning::Hash(vec![column("id")], 4),
        )
        .expect("hash"),
    );
    let join: Arc<dyn ExecutionPlan> = Arc::new(
        HashJoinExec::try_new(
            hashed,
            spread(),
            vec![(column("id"), at("id", 0))],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )
        .expect("join"),
    );
    let kept = optimize(Arc::clone(&join), true);
    assert_eq!(text(&kept), text(&join));
    assert_eq!(round_robins(&kept), 1);

    let ordered: Arc<dyn ExecutionPlan> = Arc::new(SortPreservingMergeExec::new(by_id(), spread()));
    assert_eq!(round_robins(&optimize(ordered, true)), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_projection_flag_off() {
    let base = sandwich(true);
    let kept = optimize(Arc::clone(&base), false);
    assert_eq!(text(&kept), text(&base));
    assert_eq!(round_robins(&kept), 2);

    let sizes = [big(), 4096, big()];
    let (plan, _) = sql_run(&sql_session(false, &sizes), OWNER_PROJECTION).await;
    let shown = text(&plan);
    assert!(!shown.contains("ParallelProjectionExec"), "{shown}");
    assert!(shown.contains("RoundRobinBatch"), "{shown}");
    let (plan, _) = sql_run(&sql_session(true, &sizes), OWNER_PROJECTION).await;
    let shown = text(&plan);
    assert!(
        shown.contains("ParallelProjectionExec: expr=[id@1 as id, round(x@2 * 1.5, 4) as r1"),
        "{shown}"
    );
    assert!(!shown.contains("RoundRobinBatch"), "{shown}");
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SeededRand {
    signature: Signature,
}

impl SeededRand {
    fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Int64], Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for SeededRand {
    fn name(&self) -> &'static str {
        "seeded_rand"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let seed = match args.args.first() {
            Some(ColumnarValue::Scalar(ScalarValue::Int64(Some(seed)))) => *seed,
            _ => {
                return Err(DataFusionError::Execution(
                    "seeded_rand wants a seed".into(),
                ));
            }
        };
        let mut state = u64::from_ne_bytes(seed.to_ne_bytes()) | 1;
        let values: Float64Array = (0..args.number_rows)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                Some(f64::from(u32::try_from(state >> 40).unwrap_or_default()))
            })
            .collect();
        Ok(ColumnarValue::Array(Arc::new(values)))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_projection_volatile_unchanged() {
    let sizes = [big(), 900, big()];
    let query = "SELECT id, seeded_rand(42) AS r, x * 2.0 AS a, x + y AS b, w \
                 FROM (SELECT id, x, y, z, sum(x) OVER () AS w FROM t) AS s";
    let (plan_on, got) = sql_run(&sql_session(true, &sizes), query).await;
    let (plan_off, expected) = sql_run(&sql_session(false, &sizes), query).await;
    assert_eq!(text(&plan_on), text(&plan_off));
    assert_eq!(round_robins(&plan_on), 1, "{}", text(&plan_on));
    assert_bit_identical(&sorted_by_id(&got), &sorted_by_id(&expected));

    let udf = Arc::new(ScalarUDF::new_from_impl(SeededRand::new()));
    let call: Arc<dyn PhysicalExpr> = Arc::new(
        datafusion::physical_expr::ScalarFunctionExpr::try_new(
            udf,
            vec![Arc::new(Literal::new(ScalarValue::Int64(Some(42))))],
            &schema(),
            Arc::new(ConfigOptions::new()),
        )
        .expect("call"),
    );
    let chain = project(
        project(
            round_robin(window(source(&[big()])), false),
            vec![
                item(column("id"), "id"),
                item(call, "r"),
                item(binary(column("x"), Operator::Multiply, float(2.0)), "a"),
            ],
        ),
        vec![
            item(at("id", 0), "id"),
            item(binary(at("r", 1), Operator::Plus, at("a", 2)), "s"),
            item(binary(at("a", 2), Operator::Minus, float(1.0)), "t"),
        ],
    );
    let kept = optimize(Arc::clone(&chain), true);
    assert_eq!(text(&kept), text(&chain));
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Failing {
    name: String,
    delay_ms: u64,
    signature: Signature,
}

impl ScalarUDFImpl for Failing {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        std::thread::sleep(Duration::from_millis(self.delay_ms));
        Err(DataFusionError::Execution(format!(
            "probe failure {}",
            self.name
        )))
    }
}

fn failing(name: &str, delay_ms: u64) -> Arc<dyn PhysicalExpr> {
    let udf = Arc::new(ScalarUDF::new_from_impl(Failing {
        name: name.to_owned(),
        delay_ms,
        signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
    }));
    Arc::new(
        datafusion::physical_expr::ScalarFunctionExpr::try_new(
            udf,
            vec![column("x")],
            &schema(),
            Arc::new(ConfigOptions::new()),
        )
        .expect("failing call"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_projection_lowest_index_error() {
    let plan = optimize(
        project(
            source(&[big()]),
            vec![
                item(column("id"), "id"),
                item(failing("first", 300), "a"),
                item(binary(column("y"), Operator::Plus, float(1.0)), "b"),
                item(failing("second", 0), "c"),
            ],
        ),
        true,
    );
    assert_eq!(find(&plan, "ParallelProjectionExec").len(), 1);
    let error = run(plan).await.expect_err("both probes fail").to_string();
    assert!(error.contains("probe failure first"), "{error}");
    assert!(!error.contains("probe failure second"), "{error}");
}

#[derive(Debug, Default)]
struct Gate {
    started: AtomicBool,
    released: AtomicBool,
    finished: AtomicBool,
    counted: AtomicUsize,
}

#[derive(Debug)]
struct Gated {
    name: String,
    block: bool,
    gate: Arc<Gate>,
    signature: Signature,
}

impl PartialEq for Gated {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Gated {}

impl std::hash::Hash for Gated {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl ScalarUDFImpl for Gated {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if self.block {
            self.gate.started.store(true, Ordering::Release);
            let deadline = std::time::Instant::now() + Duration::from_secs(20);
            while !self.gate.released.load(Ordering::Acquire) {
                if std::time::Instant::now() > deadline {
                    return Err(DataFusionError::Execution(
                        "gated block never released".into(),
                    ));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            self.gate.finished.store(true, Ordering::Release);
        } else {
            self.gate.counted.fetch_add(1, Ordering::SeqCst);
        }
        args.args
            .into_iter()
            .next()
            .ok_or_else(|| DataFusionError::Execution("gated wants one argument".into()))
    }
}

fn gated(name: &str, block: bool, gate: &Arc<Gate>) -> Arc<dyn PhysicalExpr> {
    let udf = Arc::new(ScalarUDF::new_from_impl(Gated {
        name: name.to_owned(),
        block,
        gate: Arc::clone(gate),
        signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
    }));
    Arc::new(
        datafusion::physical_expr::ScalarFunctionExpr::try_new(
            udf,
            vec![column("x")],
            &schema(),
            Arc::new(ConfigOptions::new()),
        )
        .expect("gated call"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_projection_drop_cancels() {
    let gate = Arc::new(Gate::default());
    let plan = optimize(
        project(
            source(&[big()]),
            vec![
                item(gated("block", true, &gate), "a"),
                item(gated("count", false, &gate), "b"),
            ],
        ),
        true,
    );
    assert!(plan.downcast_ref::<ParallelProjectionExec>().is_some());
    let mut stream = plan.execute(0, context(1)).expect("stream");
    let first = tokio::time::timeout(Duration::from_millis(100), stream.next()).await;
    assert!(
        first.is_err(),
        "the blocked expression holds the only permit"
    );
    while !gate.started.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    drop(stream);
    gate.released.store(true, Ordering::Release);
    while !gate.finished.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(gate.counted.load(Ordering::SeqCst), 0);
}

fn sorted_single_batch() -> Arc<dyn ExecutionPlan> {
    let source = MemorySourceConfig::try_new(&[vec![batch(0, big())]], schema(), None)
        .and_then(|source| source.try_with_sort_information(vec![by_id()]))
        .expect("sorted source");
    DataSourceExec::from_data_source(source)
}

fn l1_shape(level_one: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let spread = round_robin(level_one, true);
    assert!(
        text(&spread).contains("input_partitions=1, maintains_sort_order=true"),
        "{}",
        text(&spread)
    );
    assert!(spread.output_ordering().is_some());
    let level = project(
        spread,
        vec![
            item(column("id"), "id"),
            item(binary(column("x"), Operator::Multiply, float(2.0)), "a"),
            item(binary(column("y"), Operator::Plus, float(1.0)), "b"),
        ],
    );
    Arc::new(SortPreservingMergeExec::new(by_id(), level))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_projection_drops_order_preserving_round_robin() {
    let serial_window = window(sorted_single_batch());
    let parallel_window = ParallelWindowRule
        .optimize(Arc::clone(&serial_window), &options(true))
        .expect("window rule");
    assert_eq!(parallel_window.name(), "ParallelWindowExec");
    for level_one in [serial_window, parallel_window] {
        let base = l1_shape(level_one);
        let head = optimize(Arc::clone(&base), true);
        assert_eq!(round_robins(&head), 0, "{}", text(&head));
        let merge = find(&head, "SortPreservingMergeExec");
        assert_eq!(merge.len(), 1);
        let child = merge[0].children()[0];
        assert_eq!(child.output_partitioning().partition_count(), 1);
        assert_eq!(child.name(), "ParallelProjectionExec");
        let requirement =
            LexOrdering::new([PhysicalSortExpr::new_default(at("id", 0))]).expect("ordering");
        assert!(
            child
                .equivalence_properties()
                .ordering_satisfy(requirement)
                .expect("ordering check")
        );
        let expected = run(base).await.expect("base");
        let got = run(head).await.expect("head");
        assert_bit_identical(&got, &expected);
    }
}

#[test]
fn parallel_projection_keeps_multi_batch_fan_out() {
    let exprs = || {
        vec![
            item(column("id"), "id"),
            item(binary(column("x"), Operator::Multiply, float(2.0)), "a"),
            item(binary(column("y"), Operator::Plus, float(1.0)), "b"),
        ]
    };
    let unordered: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(project(
        round_robin(source(&[700, 900, 400]), false),
        exprs(),
    )));
    let ordered: Arc<dyn ExecutionPlan> = Arc::new(SortPreservingMergeExec::new(
        by_id(),
        project(round_robin(sorted(source(&[700, 900, 400])), true), exprs()),
    ));
    let root = project(round_robin(sorted_single_batch(), false), exprs());
    for plan in [unordered, ordered, root] {
        let kept = optimize(Arc::clone(&plan), true);
        assert_eq!(text(&kept), text(&plan));
        assert_eq!(round_robins(&kept), 1);
    }
}
