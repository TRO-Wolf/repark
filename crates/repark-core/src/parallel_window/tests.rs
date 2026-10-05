use std::cell::RefCell;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::config::ConfigOptions;
use datafusion::datasource::memory::{MemTable, MemorySourceConfig};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{
    Operator, PartitionEvaluator, Signature, Volatility, WindowFrame, WindowFunctionDefinition,
    WindowUDF, WindowUDFImpl,
};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_expr::window::WindowExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::windows::{WindowAggExec, create_window_expr};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    SendableRecordBatchStream, collect, displayable,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::scalar::ScalarValue;
use futures::StreamExt;

use super::exec::{InputOrder, ParallelWindowExec};
use super::{ParallelSinglePartitionConfig, ParallelWindowRule};
use crate::ReparkSession;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Mode {
    Scale(u32),
    Band(usize),
    Fail,
    SlowFail,
    Block,
    Count,
    Stack,
}

#[derive(Debug, Default)]
struct ProbeState {
    computes: AtomicUsize,
    counted: AtomicUsize,
    started: AtomicBool,
    released: AtomicBool,
    finished: AtomicBool,
    stack_seen: Mutex<Vec<usize>>,
}

#[derive(Debug)]
struct Probe {
    name: String,
    mode: Mode,
    state: Arc<ProbeState>,
    signature: Signature,
}

impl PartialEq for Probe {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.mode == other.mode
    }
}

impl Eq for Probe {}

impl Hash for Probe {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.mode.hash(state);
    }
}

fn probe(name: &str, mode: Mode, state: &Arc<ProbeState>) -> Arc<WindowUDF> {
    Arc::new(WindowUDF::new_from_impl(Probe {
        name: name.to_owned(),
        mode,
        state: Arc::clone(state),
        signature: Signature::any(1, Volatility::Immutable),
    }))
}

impl WindowUDFImpl for Probe {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(ProbeEvaluator {
            name: self.name.clone(),
            mode: self.mode,
            state: Arc::clone(&self.state),
        }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(field_args.name(), DataType::Float64, true).into())
    }
}

#[derive(Debug)]
struct ProbeEvaluator {
    name: String,
    mode: Mode,
    state: Arc<ProbeState>,
}

thread_local! {
    static BANDS: RefCell<Option<(usize, Vec<Vec<f64>>)>> = const { RefCell::new(None) };
}

fn floats(values: &[ArrayRef]) -> Result<Float64Array> {
    values
        .first()
        .and_then(|array| array.as_any().downcast_ref::<Float64Array>())
        .cloned()
        .ok_or_else(|| DataFusionError::Execution("probe wants one Float64 argument".into()))
}

fn wait_until(flag: &AtomicBool) {
    while !flag.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(1));
    }
}

impl PartitionEvaluator for ProbeEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], _num_rows: usize) -> Result<ArrayRef> {
        let input = floats(values)?;
        match self.mode {
            Mode::Scale(factor) => Ok(Arc::new(
                input
                    .iter()
                    .map(|value| value.map(|x| x * f64::from(factor) + 0.25))
                    .collect::<Float64Array>(),
            )),
            Mode::Band(band) => {
                let key = input.values().as_ptr() as usize;
                let cached = BANDS.with(|cell| {
                    cell.borrow()
                        .as_ref()
                        .filter(|(seen, _)| *seen == key)
                        .and_then(|(_, bands)| bands.get(band).cloned())
                });
                let column = cached.unwrap_or_else(|| {
                    self.state.computes.fetch_add(1, Ordering::SeqCst);
                    let bands: Vec<Vec<f64>> = (1..=3)
                        .map(|offset| {
                            input
                                .values()
                                .iter()
                                .map(|x| x + f64::from(offset))
                                .collect()
                        })
                        .collect();
                    let column = bands[band].clone();
                    BANDS.with(|cell| *cell.borrow_mut() = Some((key, bands)));
                    column
                });
                Ok(Arc::new(Float64Array::from(column)))
            }
            Mode::Fail => Err(DataFusionError::Execution(format!(
                "probe failure {}",
                self.name
            ))),
            Mode::SlowFail => {
                std::thread::sleep(Duration::from_millis(300));
                Err(DataFusionError::Execution(format!(
                    "probe failure {}",
                    self.name
                )))
            }
            Mode::Block => {
                self.state.started.store(true, Ordering::Release);
                wait_until(&self.state.released);
                self.state.finished.store(true, Ordering::Release);
                Ok(Arc::new(input))
            }
            Mode::Count => {
                self.state.counted.fetch_add(1, Ordering::SeqCst);
                Ok(Arc::new(input))
            }
            Mode::Stack => {
                let remaining = stacker::remaining_stack().unwrap_or(0);
                self.state
                    .stack_seen
                    .lock()
                    .map_err(|_| DataFusionError::Execution("stack lock".into()))?
                    .push(remaining);
                Ok(Arc::new(input))
            }
        }
    }
}

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
                    if id % 17 == 5 {
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

fn batches() -> Vec<RecordBatch> {
    vec![batch(0, 700), batch(700, 900), batch(1600, 400)]
}

fn column(name: &str) -> Arc<dyn PhysicalExpr> {
    let index = schema().index_of(name).expect("probe column");
    Arc::new(Column::new(name, index))
}

fn window_expr(
    udf: &Arc<WindowUDF>,
    arg: Arc<dyn PhysicalExpr>,
    partition_by: &[Arc<dyn PhysicalExpr>],
) -> Arc<dyn WindowExpr> {
    create_window_expr(
        &WindowFunctionDefinition::WindowUDF(Arc::clone(udf)),
        udf.name().to_owned(),
        &[arg],
        partition_by,
        &[],
        Arc::new(WindowFrame::new(None)),
        schema(),
        false,
        false,
        None,
    )
    .expect("probe window expression")
}

fn source(partitions: usize) -> Arc<dyn ExecutionPlan> {
    let mut groups: Vec<Vec<RecordBatch>> = vec![Vec::new(); partitions];
    for (index, batch) in batches().into_iter().enumerate() {
        groups[index % partitions].push(batch);
    }
    MemorySourceConfig::try_new_exec(&groups, schema(), None).expect("probe source")
}

fn window(
    input: Arc<dyn ExecutionPlan>,
    exprs: Vec<Arc<dyn WindowExpr>>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(WindowAggExec::try_new(exprs, input, true).expect("probe window"))
}

fn optimize(plan: Arc<dyn ExecutionPlan>, enabled: bool) -> Arc<dyn ExecutionPlan> {
    let mut options = ConfigOptions::new();
    options
        .extensions
        .insert(ParallelSinglePartitionConfig { enabled });
    ParallelWindowRule.optimize(plan, &options).expect("rule")
}

fn parallel_node(plan: &Arc<dyn ExecutionPlan>) -> Option<&ParallelWindowExec> {
    plan.downcast_ref::<ParallelWindowExec>()
}

fn context(target_partitions: usize) -> Arc<TaskContext> {
    Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_target_partitions(target_partitions)),
    )
}

async fn run(plan: Arc<dyn ExecutionPlan>, target_partitions: usize) -> Result<RecordBatch> {
    let schema = plan.schema();
    let batches = collect(plan, context(target_partitions)).await?;
    Ok(concat_batches(&schema, &batches)?)
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

fn scale_exprs(state: &Arc<ProbeState>) -> Vec<Arc<dyn WindowExpr>> {
    vec![
        window_expr(&probe("s1", Mode::Scale(1), state), column("x"), &[]),
        window_expr(&probe("s2", Mode::Scale(2), state), column("y"), &[]),
        window_expr(&probe("s3", Mode::Scale(3), state), column("x"), &[]),
        window_expr(&probe("s4", Mode::Scale(4), state), column("z"), &[]),
        window_expr(&probe("s5", Mode::Scale(5), state), column("y"), &[]),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_window_matches_serial_window_across_batches() {
    let state = Arc::new(ProbeState::default());
    let serial = window(source(1), scale_exprs(&state));
    let parallel = optimize(Arc::clone(&serial), true);
    let node = parallel_node(&parallel).expect("rule fires");
    assert_eq!(node.groups(), &[vec![0, 2], vec![1, 4], vec![3]]);
    let expected = run(serial, 4).await.expect("serial");
    let got = run(parallel, 4).await.expect("parallel");
    assert_eq!(got.num_rows(), 2000);
    assert_bit_identical(&got, &expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_window_keeps_multi_output_siblings() {
    let state = Arc::new(ProbeState::default());
    let exprs = vec![
        window_expr(&probe("upper", Mode::Band(0), &state), column("x"), &[]),
        window_expr(&probe("middle", Mode::Band(1), &state), column("x"), &[]),
        window_expr(&probe("lower", Mode::Band(2), &state), column("x"), &[]),
        window_expr(&probe("other", Mode::Scale(7), &state), column("y"), &[]),
    ];
    let serial = window(source(1), exprs);
    let parallel = optimize(Arc::clone(&serial), true);
    assert_eq!(
        parallel_node(&parallel).expect("rule fires").groups(),
        &[vec![0, 1, 2], vec![3]]
    );
    let got = run(parallel, 8).await.expect("parallel");
    assert_eq!(state.computes.load(Ordering::SeqCst), 1);
    let expected = run(serial, 8).await.expect("serial");
    assert_bit_identical(&got, &expected);
}

#[test]
fn parallel_window_skips_partitioned_and_single_group() {
    let state = Arc::new(ProbeState::default());
    let one_group = window(
        source(1),
        vec![
            window_expr(&probe("a", Mode::Scale(1), &state), column("x"), &[]),
            window_expr(&probe("b", Mode::Scale(2), &state), column("x"), &[]),
        ],
    );
    assert!(
        optimize(one_group, true)
            .downcast_ref::<WindowAggExec>()
            .is_some()
    );

    let partitioned = window(
        source(1),
        vec![
            window_expr(
                &probe("a", Mode::Scale(1), &state),
                column("x"),
                &[column("id")],
            ),
            window_expr(
                &probe("b", Mode::Scale(2), &state),
                column("y"),
                &[column("id")],
            ),
        ],
    );
    assert!(
        optimize(partitioned, true)
            .downcast_ref::<WindowAggExec>()
            .is_some()
    );

    let many_partitions = window(source(3), scale_exprs(&state));
    assert!(
        optimize(many_partitions, true)
            .downcast_ref::<WindowAggExec>()
            .is_some()
    );

    let qualifying = window(source(1), scale_exprs(&state));
    assert!(parallel_node(&optimize(qualifying, true)).is_some());
}

#[test]
fn parallel_window_flag_off_keeps_window_agg_exec() {
    let state = Arc::new(ProbeState::default());
    let qualifying = window(source(1), scale_exprs(&state));
    assert!(parallel_node(&optimize(Arc::clone(&qualifying), true)).is_some());
    let kept = optimize(qualifying, false);
    assert!(kept.downcast_ref::<WindowAggExec>().is_some());
    assert!(parallel_node(&kept).is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_window_lowest_index_error() {
    let state = Arc::new(ProbeState::default());
    let exprs = vec![
        window_expr(&probe("first", Mode::SlowFail, &state), column("x"), &[]),
        window_expr(&probe("fine", Mode::Scale(1), &state), column("y"), &[]),
        window_expr(&probe("second", Mode::Fail, &state), column("z"), &[]),
    ];
    let plan = optimize(window(source(1), exprs), true);
    assert_eq!(parallel_node(&plan).expect("rule fires").groups().len(), 3);
    let error = run(plan, 8).await.expect_err("both probes fail");
    let text = error.to_string();
    assert!(text.contains("probe failure first"), "{text}");
    assert!(!text.contains("probe failure second"), "{text}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_window_drop_cancels() {
    let state = Arc::new(ProbeState::default());
    let exprs = vec![
        window_expr(&probe("block", Mode::Block, &state), column("x"), &[]),
        window_expr(&probe("count", Mode::Count, &state), column("y"), &[]),
    ];
    let plan = optimize(window(source(1), exprs), true);
    assert!(parallel_node(&plan).is_some());
    let mut stream = plan.execute(0, context(1)).expect("stream");
    let first = tokio::time::timeout(Duration::from_millis(100), stream.next()).await;
    assert!(first.is_err(), "the blocked group holds the only permit");
    while !state.started.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    drop(stream);
    state.released.store(true, Ordering::Release);
    while !state.finished.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(state.counted.load(Ordering::SeqCst), 0);
}

fn deep_argument(depth: usize) -> Arc<dyn PhysicalExpr> {
    let mut expr = column("x");
    for _ in 0..depth {
        expr = Arc::new(BinaryExpr::new(
            expr,
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Float64(Some(1.0)))),
        ));
    }
    expr
}

const RUNTIME_STACK_BYTES: usize = 32 * 1024 * 1024;
const DEEP_ARGUMENT_DEPTH: usize = 4000;

#[test]
fn parallel_window_deep_arg_runtime_stack() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_stack_size(RUNTIME_STACK_BYTES)
        .build()
        .expect("runtime");
    let state = Arc::new(ProbeState::default());
    let task_state = Arc::clone(&state);
    let outcome = runtime.block_on(runtime.spawn(async move {
        let exprs = vec![
            window_expr(
                &probe("deep", Mode::Stack, &task_state),
                deep_argument(DEEP_ARGUMENT_DEPTH),
                &[],
            ),
            window_expr(
                &probe("plain", Mode::Scale(1), &task_state),
                column("y"),
                &[],
            ),
        ];
        let plan = optimize(window(source(1), exprs), true);
        assert!(parallel_node(&plan).is_some());
        let result = run(Arc::clone(&plan), 4)
            .await
            .map(|batch| batch.num_rows());
        drop(plan);
        result
    }));
    assert_eq!(outcome.expect("join").expect("deep window"), 2000);
    let seen = state.stack_seen.lock().expect("stack lock").clone();
    assert_eq!(seen.len(), 1);
    assert!(
        seen[0] > RUNTIME_STACK_BYTES / 2,
        "remaining stack {}",
        seen[0]
    );
}

fn sql_session(enabled: Option<bool>) -> SessionContext {
    let mut builder = ReparkSession::builder()
        .target_partitions(4)
        .config("datafusion.optimizer.repartition_file_scans", "false");
    if let Some(enabled) = enabled {
        builder = builder.parallel_single_partition(enabled);
    }
    let session = builder.build().expect("session");
    let context = session.context().clone();
    let table = MemTable::try_new(schema(), vec![batches()]).expect("table");
    context
        .register_table("t", Arc::new(table))
        .expect("register");
    context
}

async fn sql_run(context: &SessionContext, query: &str) -> (String, RecordBatch) {
    let frame = context.sql(query).await.expect("sql");
    let plan = frame.create_physical_plan().await.expect("plan");
    let text = displayable(plan.as_ref()).indent(true).to_string();
    let schema = plan.schema();
    let batches = collect(plan, context.task_ctx()).await.expect("collect");
    (text, concat_batches(&schema, &batches).expect("concat"))
}

const NON_TA_QUERIES: [(&str, bool); 4] = [
    (
        "SELECT id, x, sum(x) OVER () AS s, avg(y) OVER () AS a, max(x) OVER () AS m, \
         count(z) OVER () AS c FROM t",
        true,
    ),
    (
        "SELECT id, lag(x) OVER (ORDER BY id) AS l, row_number() OVER (ORDER BY id) AS rn, \
         sum(y) OVER (ORDER BY id) AS run FROM t",
        false,
    ),
    (
        "SELECT id, sum(x) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED \
         FOLLOWING) AS whole, first_value(y) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING \
         AND UNBOUNDED FOLLOWING) AS head, lag(z, 2) OVER (ORDER BY id) AS l2 FROM t",
        true,
    ),
    (
        "SELECT id, row_number() OVER (ORDER BY id) AS rn, sum(x) OVER () AS s, \
         min(z) OVER () AS lo FROM t",
        true,
    ),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_window_non_ta_windows_bit_identical() {
    let on = sql_session(None);
    let off = sql_session(Some(false));
    for (query, fires) in NON_TA_QUERIES {
        let (plan_on, got) = sql_run(&on, query).await;
        let (plan_off, expected) = sql_run(&off, query).await;
        assert_eq!(
            plan_on.contains("ParallelWindowExec"),
            fires,
            "{query}\n{plan_on}"
        );
        assert!(!plan_off.contains("ParallelWindowExec"), "{plan_off}");
        assert_bit_identical(&got, &expected);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_window_flag_off_session_keeps_window_agg_exec() {
    let (plan, _) = sql_run(&sql_session(Some(false)), NON_TA_QUERIES[0].0).await;
    assert!(plan.contains("WindowAggExec"), "{plan}");
    assert!(!plan.contains("ParallelWindowExec"), "{plan}");
    let (plan, _) = sql_run(&sql_session(Some(true)), NON_TA_QUERIES[0].0).await;
    assert!(
        plan.contains("ParallelWindowExec: wdw=[sum(t.x) ROWS BETWEEN"),
        "{plan}"
    );
}

#[derive(Debug)]
struct DelayedExec {
    partitions: usize,
    properties: Arc<PlanProperties>,
}

fn delayed(partitions: usize) -> Arc<dyn ExecutionPlan> {
    Arc::new(DelayedExec {
        partitions,
        properties: Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema()),
            Partitioning::UnknownPartitioning(partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )),
    })
}

impl DisplayAs for DelayedExec {
    fn fmt_as(&self, _mode: DisplayFormatType, formatter: &mut fmt::Formatter) -> fmt::Result {
        write!(formatter, "DelayedExec: partitions={}", self.partitions)
    }
}

impl ExecutionPlan for DelayedExec {
    fn name(&self) -> &'static str {
        "DelayedExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let wait = u64::try_from(self.partitions - partition).unwrap_or_default() * 60;
        let start = i64::try_from(partition).unwrap_or_default() * 100;
        let stream = futures::stream::once(async move {
            std::thread::sleep(Duration::from_millis(wait));
            Ok(batch(start, 100))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema(), stream)))
    }
}

fn aggregate_expr(name: &str) -> Arc<dyn WindowExpr> {
    create_window_expr(
        &WindowFunctionDefinition::AggregateUDF(sum_udaf()),
        format!("sum({name})"),
        &[column(name)],
        &[],
        &[],
        Arc::new(WindowFrame::new(None)),
        schema(),
        false,
        false,
        None,
    )
    .expect("aggregate window expression")
}

fn ids(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ids")
        .values()
        .to_vec()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn series_order_current_row_order_partition_index() {
    let state = Arc::new(ProbeState::default());
    let exprs = vec![
        window_expr(&probe("pass", Mode::Count, &state), column("x"), &[]),
        window_expr(&probe("scale", Mode::Scale(3), &state), column("y"), &[]),
    ];
    let plan = window(
        Arc::new(CoalescePartitionsExec::new(delayed(4))),
        exprs.clone(),
    );
    let optimized = optimize(plan, true);
    let node = parallel_node(&optimized).expect("partition-index arm fires");
    assert_eq!(node.input_order(), InputOrder::PartitionIndex);
    assert!(node.children()[0].downcast_ref::<DelayedExec>().is_some());
    assert_eq!(
        optimized.output_partitioning().partition_count(),
        1,
        "one output partition"
    );
    let source_order: Vec<i64> = (0..400).collect();
    let expected = concat_batches(
        &schema(),
        &(0..4).map(|p| batch(p * 100, 100)).collect::<Vec<_>>(),
    )
    .expect("source");
    for _ in 0..3 {
        let got = run(Arc::clone(&optimized), 8)
            .await
            .expect("partition index");
        assert_eq!(ids(&got), source_order);
        assert_eq!(
            got.column(4),
            expected.column(1),
            "pass-through follows source order"
        );
    }
    let rebuilt = Arc::clone(&optimized)
        .with_new_children(vec![delayed(4)])
        .expect("rebuild");
    assert_eq!(
        parallel_node(&rebuilt).expect("rebuilt").input_order(),
        InputOrder::PartitionIndex
    );
    assert_eq!(rebuilt.output_partitioning().partition_count(), 1);
    let got = run(rebuilt, 8).await.expect("rebuilt run");
    assert_eq!(ids(&got), source_order);

    let one = window(
        Arc::new(CoalescePartitionsExec::new(delayed(4))),
        vec![exprs[0].clone()],
    );
    let one = optimize(one, true);
    assert_eq!(
        parallel_node(&one).expect("one group fires").input_order(),
        InputOrder::PartitionIndex
    );
    assert_eq!(ids(&run(one, 8).await.expect("one group")), source_order);

    let off = optimize(
        window(Arc::new(CoalescePartitionsExec::new(delayed(4))), exprs),
        false,
    );
    assert!(off.downcast_ref::<WindowAggExec>().is_some());
}

#[test]
fn partition_index_keeps_aggregate_over_coalesce() {
    let state = Arc::new(ProbeState::default());
    let aggregates = optimize(
        window(
            Arc::new(CoalescePartitionsExec::new(delayed(4))),
            vec![aggregate_expr("x"), aggregate_expr("y")],
        ),
        true,
    );
    let node = parallel_node(&aggregates).expect("the S2b arm still fires");
    assert_eq!(node.input_order(), InputOrder::Single);
    assert!(
        node.children()[0]
            .downcast_ref::<CoalescePartitionsExec>()
            .is_some()
    );
    let single = optimize(
        window(
            Arc::new(CoalescePartitionsExec::new(delayed(4))),
            vec![aggregate_expr("x")],
        ),
        true,
    );
    assert!(single.downcast_ref::<WindowAggExec>().is_some());
    let mixed = optimize(
        window(
            Arc::new(CoalescePartitionsExec::new(delayed(4))),
            vec![
                window_expr(&probe("pass", Mode::Count, &state), column("x"), &[]),
                aggregate_expr("y"),
            ],
        ),
        true,
    );
    assert_eq!(
        parallel_node(&mixed).expect("mixed").input_order(),
        InputOrder::Single
    );
    let fetched = optimize(
        window(
            Arc::new(CoalescePartitionsExec::new(delayed(4)).with_fetch(Some(5))),
            vec![window_expr(
                &probe("pass", Mode::Count, &state),
                column("x"),
                &[],
            )],
        ),
        true,
    );
    assert!(fetched.downcast_ref::<WindowAggExec>().is_some());
}
