use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{Result, ScalarValue};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::{Cast, Expr, LogicalPlan, LogicalPlanBuilder, col, lit};
use datafusion::optimizer::AnalyzerRule;

use super::{
    SparkTimestampNsCoercion, fallback_count, fallback_log_line, gate_open_count, plan_opens_gate,
    reset_counts,
};
use crate::timestamp_ns_cast::timestamp_ns_cast_expr;

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
    let schema = Arc::new(Schema::new(fields));
    let table = MemTable::try_new(schema, vec![Vec::new()]).expect("memory table builds");
    let source = provider_as_source(Arc::new(table));
    LogicalPlanBuilder::scan("t", source, None)
        .expect("scan builds")
        .build()
        .expect("plan builds")
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
fn fallback_log_line_carries_the_reason() {
    let line = fallback_log_line("written cast");
    assert!(line.contains("written cast"), "unexpected line: {line}");
}
