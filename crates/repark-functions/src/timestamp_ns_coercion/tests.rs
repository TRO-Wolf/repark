use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::expr::{BinaryExpr, InList, ScalarFunction};
use datafusion::logical_expr::{
    Case, Cast, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, ScalarUDF, TableSource,
    TryCast, col, lit,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion_spark::function::array::spark_array::SparkArray;

use super::{
    SparkTimestampNsCoercion, fallback_count, fallback_log_line, gate_open_count, plan_opens_gate,
    reset_counts,
};
use crate::timestamp_ns_cast::{
    NARROW_TIMESTAMP_NS_NAME, TIMESTAMP_NS_CAST_NAME, TIMESTAMPTZ_NS_CAST_NAME,
    timestamp_ns_cast_expr,
};

static SERIAL: Mutex<()> = Mutex::new(());

#[derive(Debug, Default)]
struct PassThroughRule;

impl AnalyzerRule for PassThroughRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        Ok(plan)
    }

    fn name(&self) -> &'static str {
        "pass_through"
    }
}

fn micro_field() -> Field {
    Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true)
}

fn scan_of(fields: Vec<Field>) -> LogicalPlan {
    scan_named("t", fields)
}

fn scan_named(table: &str, fields: Vec<Field>) -> LogicalPlan {
    let schema = Arc::new(Schema::new(fields));
    let mem = MemTable::try_new(schema, vec![Vec::new()]).expect("memory table builds");
    let source = provider_as_source(Arc::new(mem));
    LogicalPlanBuilder::scan(table, source, None)
        .expect("scan builds")
        .build()
        .expect("plan builds")
}

struct RecordingRule {
    want: Arc<dyn TableSource>,
    matched: Mutex<bool>,
}

impl std::fmt::Debug for RecordingRule {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RecordingRule")
            .field("matched", &self.matched)
            .finish_non_exhaustive()
    }
}

impl RecordingRule {
    fn new(want: Arc<dyn TableSource>) -> Self {
        Self {
            want,
            matched: Mutex::new(false),
        }
    }
}

impl AnalyzerRule for RecordingRule {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        let _ = plan.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                *self.matched.lock().expect("test serial lock") =
                    Arc::ptr_eq(&scan.source, &self.want);
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        Ok(plan)
    }

    fn name(&self) -> &'static str {
        "recording"
    }
}

fn project_of(scan: LogicalPlan, exprs: Vec<Expr>) -> LogicalPlan {
    LogicalPlanBuilder::from(scan)
        .project(exprs)
        .expect("project builds")
        .build()
        .expect("plan builds")
}

#[test]
fn gate_stays_closed_for_a_microsecond_table() {
    let plan = scan_of(vec![
        Field::new("id", DataType::Int64, true),
        micro_field(),
        Field::new(
            "tsz",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            true,
        ),
    ]);
    assert!(!plan_opens_gate(&plan));
}

#[test]
fn gate_opens_for_a_timestamp_ns_column() {
    let plan = scan_of(vec![Field::new(
        "ns",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )]);
    assert!(plan_opens_gate(&plan));
}

#[test]
fn gate_opens_for_a_timestamptz_ns_column() {
    let plan = scan_of(vec![Field::new(
        "tzns",
        DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
        true,
    )]);
    assert!(plan_opens_gate(&plan));
}

#[test]
fn gate_opens_for_a_nanosecond_leaf_inside_a_struct() {
    let inner = DataType::Struct(Fields::from(vec![Field::new(
        "ns",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )]));
    let plan = scan_of(vec![Field::new("s", inner, true)]);
    assert!(plan_opens_gate(&plan));
}

#[test]
fn gate_opens_for_a_nanosecond_leaf_inside_a_list() {
    let inner = DataType::List(Arc::new(Field::new_list_field(
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )));
    let plan = scan_of(vec![Field::new("l", inner, true)]);
    assert!(plan_opens_gate(&plan));
}

#[test]
fn gate_opens_for_a_nanosecond_leaf_inside_a_map() {
    let entries = Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new(
                "value",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
        ])),
        false,
    ));
    let plan = scan_of(vec![Field::new("m", DataType::Map(entries, false), true)]);
    assert!(plan_opens_gate(&plan));
}

#[test]
fn gate_stays_closed_for_a_written_cast_to_nanoseconds() {
    let scan = scan_of(vec![micro_field()]);
    let target = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let cast = Expr::Cast(Cast::new(Box::new(col("ts")), target));
    let plan = project_of(scan, vec![cast.alias("v")]);
    assert!(!plan_opens_gate(&plan));
}

#[test]
fn gate_stays_closed_for_a_nanosecond_literal() {
    let scan = scan_of(vec![micro_field()]);
    let literal = lit(ScalarValue::TimestampNanosecond(
        Some(1_767_000_000_000_000_000),
        None,
    ));
    let plan = project_of(scan, vec![col("ts"), literal.alias("t")]);
    assert!(!plan_opens_gate(&plan));
}

#[test]
fn gate_opens_for_a_kernel_call_over_a_microsecond_table() {
    let scan = scan_of(vec![micro_field()]);
    let call = timestamp_ns_cast_expr(col("ts"), false).alias("v");
    let plan = project_of(scan, vec![call]);
    assert!(plan_opens_gate(&plan));
}

#[test]
fn gate_opens_for_a_zoned_kernel_call_over_a_microsecond_table() {
    let scan = scan_of(vec![micro_field()]);
    let call = timestamp_ns_cast_expr(col("ts"), true).alias("v");
    let plan = project_of(scan, vec![call]);
    assert!(plan_opens_gate(&plan));
}

#[test]
fn open_analysis_increments_the_gate_open_counter_once() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let before_open = gate_open_count();
    let before_fallback = fallback_count();
    let plan = scan_of(vec![Field::new(
        "ns",
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        true,
    )]);
    let wrapper = SparkTimestampNsCoercion::new(Arc::new(PassThroughRule));
    let config = ConfigOptions::new();
    let _ = wrapper.analyze(plan, &config).expect("open plan analyzes");
    assert_eq!(gate_open_count() - before_open, 1);
    assert_eq!(fallback_count() - before_fallback, 0);
}

#[test]
fn closed_analysis_leaves_both_counters_alone() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let before_open = gate_open_count();
    let before_fallback = fallback_count();
    let plan = scan_of(vec![micro_field()]);
    let wrapper = SparkTimestampNsCoercion::new(Arc::new(PassThroughRule));
    let config = ConfigOptions::new();
    let _ = wrapper
        .analyze(plan, &config)
        .expect("closed plan analyzes");
    assert_eq!(gate_open_count() - before_open, 0);
    assert_eq!(fallback_count() - before_fallback, 0);
}

#[test]
fn counters_reset_to_zero() {
    let _guard = SERIAL.lock().expect("test serial lock");
    reset_counts();
    assert_eq!(gate_open_count(), 0);
    assert_eq!(fallback_count(), 0);
}

#[test]
fn closed_gate_hands_the_inner_rule_the_plan_it_was_given() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let before_open = gate_open_count();
    let schema = Arc::new(Schema::new(vec![micro_field()]));
    let table = MemTable::try_new(schema, vec![Vec::new()]).expect("memory table builds");
    let source = provider_as_source(Arc::new(table));
    let want = Arc::clone(&source);
    let plan = LogicalPlanBuilder::scan("t", source, None)
        .expect("scan builds")
        .build()
        .expect("plan builds");
    let before = format!("{plan:?}");
    let inner = Arc::new(RecordingRule::new(want));
    let held = Arc::clone(&inner);
    let wrapper = SparkTimestampNsCoercion::new(held);
    let config = ConfigOptions::new();
    let out = wrapper
        .analyze(plan, &config)
        .expect("closed plan analyzes");
    assert!(*inner.matched.lock().expect("test serial lock"));
    assert_eq!(format!("{out:?}"), before);
    assert_eq!(gate_open_count() - before_open, 0);
}

#[test]
fn fallback_log_line_carries_the_reason() {
    let line = fallback_log_line("written cast");
    assert!(line.contains("written cast"), "unexpected line: {line}");
}

fn ns_field(name: &str) -> Field {
    Field::new(name, DataType::Timestamp(TimeUnit::Nanosecond, None), true)
}

fn tzns_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
        true,
    )
}

fn null_lit() -> Expr {
    lit(ScalarValue::Null)
}

fn call_of(func: Arc<ScalarUDF>, args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(func, args))
}

fn coalesce_call(args: Vec<Expr>) -> Expr {
    let udf = datafusion::functions::core::coalesce::CoalesceFunc::new();
    call_of(Arc::new(ScalarUDF::from(udf)), args)
}

fn nvl_call(args: Vec<Expr>) -> Expr {
    let udf = datafusion::functions::core::nvl::NVLFunc::new();
    call_of(Arc::new(ScalarUDF::from(udf)), args)
}

fn array_call(args: Vec<Expr>) -> Expr {
    call_of(Arc::new(ScalarUDF::from(SparkArray::new())), args)
}

fn if_call(args: Vec<Expr>) -> Expr {
    call_of(datafusion_spark::function::conditional::r#if(), args)
}

fn case_call(column: &str) -> Expr {
    Expr::Case(Case {
        expr: None,
        when_then_expr: vec![(Box::new(lit(true)), Box::new(col(column)))],
        else_expr: Some(Box::new(null_lit())),
    })
}

fn analyzed(plan: LogicalPlan) -> LogicalPlan {
    let wrapper = SparkTimestampNsCoercion::new(Arc::new(PassThroughRule));
    let config = ConfigOptions::new();
    wrapper.analyze(plan, &config).expect("plan analyzes")
}

fn output_type(plan: &LogicalPlan) -> DataType {
    plan.schema().fields()[0].data_type().clone()
}

fn plan_calls_kernel(plan: &LogicalPlan, kernel: &str) -> bool {
    let mut found = false;
    let _ = plan.apply_with_subqueries(|node| {
        let _ = node.apply_expressions(|expr| {
            let _ = expr.apply(|leaf| {
                if let Expr::ScalarFunction(function) = leaf
                    && function.func.name() == kernel
                {
                    found = true;
                }
                Ok(TreeNodeRecursion::Continue)
            });
            Ok(TreeNodeRecursion::Continue)
        });
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn assert_kernel_rewrite(out: &LogicalPlan, kernel: &str) {
    assert!(
        plan_calls_kernel(out, kernel),
        "missing {kernel} in {out:?}"
    );
    assert!(
        !plan_calls_kernel(out, NARROW_TIMESTAMP_NS_NAME),
        "narrow kernel in {out:?}"
    );
}

fn assert_ns_output(out: &LogicalPlan, zone: Option<Arc<str>>) {
    assert_eq!(
        output_type(out),
        DataType::Timestamp(TimeUnit::Nanosecond, zone)
    );
}

fn assert_list_ns_output(out: &LogicalPlan, zone: Option<Arc<str>>) {
    let DataType::List(item) = output_type(out) else {
        panic!("expected a list output, got {:?}", output_type(out));
    };
    assert_eq!(
        item.data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, zone)
    );
}

fn union_of(left: LogicalPlan, right: LogicalPlan) -> LogicalPlan {
    LogicalPlanBuilder::from(left)
        .union(right)
        .expect("union builds")
        .build()
        .expect("plan builds")
}

#[test]
fn coalesce_ns_null_rewrites_the_null_with_the_kernel() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns")]);
    let plan = project_of(scan, vec![coalesce_call(vec![col("ns"), null_lit()])]);
    let out = analyzed(plan);
    assert_ns_output(&out, None);
    assert_kernel_rewrite(&out, TIMESTAMP_NS_CAST_NAME);
}

#[test]
fn coalesce_tzns_null_rewrites_the_null_with_the_kernel() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![tzns_field("tzns")]);
    let plan = project_of(scan, vec![coalesce_call(vec![col("tzns"), null_lit()])]);
    let out = analyzed(plan);
    assert_ns_output(&out, Some(Arc::from("UTC")));
    assert_kernel_rewrite(&out, TIMESTAMPTZ_NS_CAST_NAME);
}

#[test]
fn array_ns_null_rewrites_the_null_with_the_kernel() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns")]);
    let plan = project_of(scan, vec![array_call(vec![col("ns"), null_lit()])]);
    let out = analyzed(plan);
    assert_list_ns_output(&out, None);
    assert_kernel_rewrite(&out, TIMESTAMP_NS_CAST_NAME);
}

#[test]
fn array_tzns_null_rewrites_the_null_with_the_kernel() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![tzns_field("tzns")]);
    let plan = project_of(scan, vec![array_call(vec![col("tzns"), null_lit()])]);
    let out = analyzed(plan);
    assert_list_ns_output(&out, Some(Arc::from("UTC")));
    assert_kernel_rewrite(&out, TIMESTAMPTZ_NS_CAST_NAME);
}

#[test]
fn case_ns_null_rewrites_the_null_with_the_kernel() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns")]);
    let plan = project_of(scan, vec![case_call("ns")]);
    let out = analyzed(plan);
    assert_ns_output(&out, None);
    assert_kernel_rewrite(&out, TIMESTAMP_NS_CAST_NAME);
}

#[test]
fn case_tzns_null_rewrites_the_null_with_the_kernel() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![tzns_field("tzns")]);
    let plan = project_of(scan, vec![case_call("tzns")]);
    let out = analyzed(plan);
    assert_ns_output(&out, Some(Arc::from("UTC")));
    assert_kernel_rewrite(&out, TIMESTAMPTZ_NS_CAST_NAME);
}

#[test]
fn if_ns_null_rewrites_the_null_with_the_kernel() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns")]);
    let plan = project_of(scan, vec![if_call(vec![lit(true), col("ns"), null_lit()])]);
    let out = analyzed(plan);
    assert_ns_output(&out, None);
    assert_kernel_rewrite(&out, TIMESTAMP_NS_CAST_NAME);
}

#[test]
fn if_tzns_null_rewrites_the_null_with_the_kernel() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![tzns_field("tzns")]);
    let plan = project_of(
        scan,
        vec![if_call(vec![lit(true), col("tzns"), null_lit()])],
    );
    let out = analyzed(plan);
    assert_ns_output(&out, Some(Arc::from("UTC")));
    assert_kernel_rewrite(&out, TIMESTAMPTZ_NS_CAST_NAME);
}

#[test]
fn union_ns_null_branch_rewrites_the_null_with_the_kernel() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let valued = project_of(scan_of(vec![ns_field("v")]), vec![col("v")]);
    let nulls = project_of(scan_of(vec![micro_field()]), vec![null_lit().alias("v")]);
    let out = analyzed(union_of(valued, nulls));
    assert_ns_output(&out, None);
    assert_kernel_rewrite(&out, TIMESTAMP_NS_CAST_NAME);
}

#[test]
fn union_tzns_null_branch_rewrites_the_null_with_the_kernel() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let valued = project_of(scan_of(vec![tzns_field("v")]), vec![col("v")]);
    let nulls = project_of(scan_of(vec![micro_field()]), vec![null_lit().alias("v")]);
    let out = analyzed(union_of(valued, nulls));
    assert_ns_output(&out, Some(Arc::from("UTC")));
    assert_kernel_rewrite(&out, TIMESTAMPTZ_NS_CAST_NAME);
}

#[test]
fn rewrite_is_idempotent_and_moves_only_the_gate_counter() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns")]);
    let plan = project_of(scan, vec![coalesce_call(vec![col("ns"), null_lit()])]);
    let once = analyzed(plan);
    let before_open = gate_open_count();
    let before_fallback = fallback_count();
    let twice = analyzed(once.clone());
    assert_eq!(format!("{twice:?}"), format!("{once:?}"));
    assert_eq!(gate_open_count() - before_open, 1);
    assert_eq!(fallback_count() - before_fallback, 0);
}

fn assert_falls_back(plan: LogicalPlan) {
    let before = format!("{plan:?}");
    let before_open = gate_open_count();
    let before_fallback = fallback_count();
    let out = analyzed(plan);
    assert_eq!(format!("{out:?}"), before);
    assert_eq!(gate_open_count() - before_open, 1);
    assert_eq!(fallback_count() - before_fallback, 1);
}

#[test]
fn written_timestamp_cast_falls_back_to_inner() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns")]);
    let cast = Expr::Cast(Cast::new(
        Box::new(col("ns")),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    ));
    assert_falls_back(project_of(scan, vec![cast.alias("v")]));
}

#[test]
fn written_timestamp_trycast_falls_back_to_inner() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns")]);
    let cast = Expr::TryCast(TryCast::new(
        Box::new(col("ns")),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    ));
    assert_falls_back(project_of(scan, vec![cast.alias("v")]));
}

#[test]
fn typed_null_cast_falls_back_to_inner() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns")]);
    let cast = Expr::Cast(Cast::new(
        Box::new(null_lit()),
        DataType::Timestamp(TimeUnit::Microsecond, None),
    ));
    assert_falls_back(project_of(scan, vec![col("ns"), cast.alias("v")]));
}

#[test]
fn live_timestamp_literal_falls_back_to_inner() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns")]);
    let literal = lit(ScalarValue::TimestampMicrosecond(Some(1), None));
    assert_falls_back(project_of(scan, vec![col("ns"), literal.alias("v")]));
}

#[test]
fn timestamp_reader_call_falls_back_to_inner() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns")]);
    let call = call_of(
        crate::instant_ts::to_timestamp_udf(),
        vec![lit("2026-01-02 03:04:05")],
    );
    assert_falls_back(project_of(scan, vec![col("ns"), call.alias("v")]));
}

fn assert_left_alone(plan: LogicalPlan) {
    let before = format!("{plan:?}");
    let before_fallback = fallback_count();
    let out = analyzed(plan);
    assert_eq!(format!("{out:?}"), before);
    assert_eq!(fallback_count() - before_fallback, 0);
}

#[test]
fn microsecond_sibling_leaves_the_site_alone() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns"), micro_field()]);
    let plan = project_of(scan, vec![coalesce_call(vec![col("ns"), col("ts")])]);
    assert_left_alone(plan);
}

#[test]
fn date_sibling_leaves_the_site_alone() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![
        ns_field("ns"),
        Field::new("d", DataType::Date32, true),
    ]);
    let plan = project_of(scan, vec![coalesce_call(vec![col("ns"), col("d")])]);
    assert_left_alone(plan);
}

#[test]
fn string_sibling_leaves_the_site_alone() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let valued = project_of(scan_of(vec![ns_field("v")]), vec![col("v")]);
    let strings = project_of(
        scan_of(vec![Field::new("s", DataType::Utf8, true)]),
        vec![col("s").alias("v")],
    );
    assert_left_alone(union_of(valued, strings));
}

#[test]
fn mixed_ns_and_tzns_siblings_leave_the_site_alone() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns"), tzns_field("tzns")]);
    let plan = project_of(scan, vec![coalesce_call(vec![col("ns"), col("tzns")])]);
    assert_left_alone(plan);
}

#[test]
fn nvl_with_untyped_null_is_not_rewritten_this_round() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns")]);
    let plan = project_of(scan, vec![nvl_call(vec![col("ns"), null_lit()])]);
    assert_left_alone(plan);
}

#[test]
fn comparison_against_null_is_not_a_site() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns")]);
    let predicate = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(col("ns")),
        Operator::Gt,
        Box::new(null_lit()),
    ));
    assert_left_alone(project_of(scan, vec![predicate.alias("v")]));
}

#[test]
fn in_list_with_null_is_not_a_site() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns")]);
    let predicate = Expr::InList(InList::new(Box::new(col("ns")), vec![null_lit()], false));
    assert_left_alone(project_of(scan, vec![predicate.alias("v")]));
}

#[test]
fn join_keys_are_not_sites() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let left = scan_named("l", vec![ns_field("ns")]);
    let right = scan_named("r", vec![ns_field("ns")]);
    let plan = LogicalPlanBuilder::from(left)
        .join(right, JoinType::Inner, (vec!["ns"], vec!["ns"]), None)
        .expect("join builds")
        .build()
        .expect("plan builds");
    assert_left_alone(plan);
}

fn recording_sink() -> (super::FallbackSink, Arc<Mutex<Vec<String>>>) {
    let lines: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let held = Arc::clone(&lines);
    let sink: super::FallbackSink = Arc::new(move |line: &str| {
        held.lock()
            .expect("test serial lock")
            .push(line.to_string());
    });
    (sink, lines)
}

#[test]
fn fallback_sink_receives_one_line_per_fallback() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let (sink, lines) = recording_sink();
    let wrapper = SparkTimestampNsCoercion::new(Arc::new(PassThroughRule)).with_fallback_sink(sink);
    let config = ConfigOptions::new();
    for _ in 0..2 {
        let scan = scan_of(vec![ns_field("ns")]);
        let literal = lit(ScalarValue::TimestampMicrosecond(Some(1), None));
        let plan = project_of(scan, vec![col("ns"), literal.alias("v")]);
        let _ = wrapper.analyze(plan, &config).expect("plan analyzes");
    }
    let lines = lines.lock().expect("test serial lock");
    assert_eq!(lines.len(), 2);
    assert!(lines.iter().all(|line| line.contains("fallback")));
}

#[test]
fn fallback_sink_stays_quiet_on_rewrite_and_closed_gate() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let (sink, lines) = recording_sink();
    let wrapper = SparkTimestampNsCoercion::new(Arc::new(PassThroughRule)).with_fallback_sink(sink);
    let config = ConfigOptions::new();
    let open = scan_of(vec![ns_field("ns")]);
    let rewritten = project_of(open, vec![coalesce_call(vec![col("ns"), null_lit()])]);
    let _ = wrapper
        .analyze(rewritten, &config)
        .expect("open plan analyzes");
    let closed = scan_of(vec![micro_field()]);
    let _ = wrapper
        .analyze(closed, &config)
        .expect("closed plan analyzes");
    assert!(lines.lock().expect("test serial lock").is_empty());
}

#[test]
fn wrapper_reports_whether_a_sink_is_set() {
    let bare = SparkTimestampNsCoercion::new(Arc::new(PassThroughRule));
    assert!(!bare.has_fallback_sink());
    let (sink, _) = recording_sink();
    let wired = SparkTimestampNsCoercion::new(Arc::new(PassThroughRule)).with_fallback_sink(sink);
    assert!(wired.has_fallback_sink());
}
