use std::sync::{Arc, Mutex};

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, ScalarValue, TableReference};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    Case, DmlStatement, Expr, LogicalPlan, LogicalPlanBuilder, ScalarUDF, WriteOp, col, lit,
};
use datafusion::optimizer::AnalyzerRule;

use super::{SparkTimestampNsCoercion, fallback_count, gate_open_count};
use crate::timestamp_ns_cast::{TIMESTAMP_NS_CAST_NAME, TIMESTAMPTZ_NS_CAST_NAME};
use crate::timestamp_ns_coercion_feeds::{FeedOutcome, rewrite_feeds};

static SERIAL: Mutex<()> = Mutex::new(());

fn ns_field(name: &str) -> Field {
    Field::new(name, DataType::Timestamp(TimeUnit::Nanosecond, None), true)
}

fn micro_field() -> Field {
    Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true)
}

fn null_lit() -> Expr {
    lit(ScalarValue::Null)
}

fn scan_of(fields: Vec<Field>) -> LogicalPlan {
    let schema = Arc::new(Schema::new(fields));
    let mem = MemTable::try_new(schema, vec![Vec::new()]).expect("memory table builds");
    let source = provider_as_source(Arc::new(mem));
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

fn coalesce_call(args: Vec<Expr>) -> Expr {
    let udf = datafusion::functions::core::coalesce::CoalesceFunc::new();
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::from(udf)),
        args,
    ))
}

fn case_call(column: &str) -> Expr {
    Expr::Case(Case {
        expr: None,
        when_then_expr: vec![(Box::new(lit(true)), Box::new(col(column)))],
        else_expr: Some(Box::new(null_lit())),
    })
}

fn target_source(fields: Vec<Field>) -> Arc<dyn datafusion::logical_expr::TableSource> {
    let schema = Arc::new(Schema::new(fields));
    let mem = MemTable::try_new(schema, vec![Vec::new()]).expect("memory table builds");
    provider_as_source(Arc::new(mem))
}

fn dml_insert(target_fields: Vec<Field>, input: LogicalPlan) -> LogicalPlan {
    let target = target_source(target_fields);
    LogicalPlan::Dml(DmlStatement::new(
        TableReference::from("t"),
        target,
        WriteOp::Insert(InsertOp::Append),
        Arc::new(input),
    ))
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

#[derive(Debug, Default)]
struct PassThroughRule;

impl AnalyzerRule for PassThroughRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<LogicalPlan> {
        Ok(plan)
    }

    fn name(&self) -> &'static str {
        "pass_through"
    }
}

struct RecordingRule {
    want: Arc<dyn datafusion::logical_expr::TableSource>,
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

impl AnalyzerRule for RecordingRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &datafusion::common::config::ConfigOptions,
    ) -> Result<LogicalPlan> {
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
fn read_site_is_left_to_main() {
    let scan = scan_of(vec![ns_field("ns")]);
    let plan = project_of(scan, vec![coalesce_call(vec![col("ns"), null_lit()])]);
    let out = rewrite_feeds(plan).expect("feed walk runs");
    assert!(matches!(out, FeedOutcome::Unchanged));
}

#[test]
fn open_gate_without_a_feed_hands_the_inner_rule_the_plan_it_was_given() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let schema = Arc::new(Schema::new(vec![ns_field("ns")]));
    let table = MemTable::try_new(schema, vec![Vec::new()]).expect("memory table builds");
    let source = provider_as_source(Arc::new(table));
    let want = Arc::clone(&source);
    let plan = LogicalPlanBuilder::scan("t", source, None)
        .expect("scan builds")
        .build()
        .expect("plan builds");
    let before = format!("{plan:?}");
    let inner = Arc::new(RecordingRule {
        want,
        matched: Mutex::new(false),
    });
    let held = Arc::clone(&inner);
    let (sink, lines) = recording_sink();
    let wrapper = SparkTimestampNsCoercion::new(held).with_fallback_sink(sink);
    let config = datafusion::common::config::ConfigOptions::new();
    let before_open = gate_open_count();
    let before_fallback = fallback_count();
    let out = wrapper.analyze(plan, &config).expect("plan analyzes");
    assert!(inner.matched.lock().expect("test serial lock").clone());
    assert_eq!(format!("{out:?}"), before);
    assert_eq!(gate_open_count() - before_open, 0);
    assert_eq!(fallback_count() - before_fallback, 0);
    assert!(lines.lock().expect("test serial lock").is_empty());
}

#[test]
fn dml_position_feed_is_rewritten() {
    let scan = scan_of(vec![ns_field("ns")]);
    let input = project_of(scan, vec![coalesce_call(vec![col("ns"), null_lit()])]);
    let plan = dml_insert(vec![ns_field("v")], input);
    let out = rewrite_feeds(plan).expect("feed walk runs");
    let FeedOutcome::Rewritten(rewritten) = out else {
        panic!("expected a rewritten feed, got {out:?}");
    };
    assert!(plan_calls_kernel(&rewritten, TIMESTAMP_NS_CAST_NAME));
}

#[test]
fn kernel_argument_feed_is_rewritten() {
    let scan = scan_of(vec![ns_field("ns")]);
    let site = coalesce_call(vec![col("ns"), null_lit()]);
    let kernel = crate::timestamp_ns_cast::timestamp_ns_cast_expr(site, false);
    let plan = project_of(scan, vec![kernel.alias("v")]);
    let out = rewrite_feeds(plan).expect("feed walk runs");
    let FeedOutcome::Rewritten(rewritten) = out else {
        panic!("expected a rewritten feed, got {out:?}");
    };
    assert!(plan_calls_kernel(&rewritten, TIMESTAMP_NS_CAST_NAME));
}

#[test]
fn feed_behind_alias_subquery_and_bare_projection_is_rewritten() {
    let scan = scan_of(vec![ns_field("ns")]);
    let site = project_of(
        scan,
        vec![coalesce_call(vec![col("ns"), null_lit()]).alias("v")],
    );
    let aliased = LogicalPlanBuilder::from(site)
        .alias("s")
        .expect("alias builds")
        .build()
        .expect("plan builds");
    let input = project_of(aliased, vec![col("s.v")]);
    let plan = dml_insert(vec![ns_field("v")], input);
    let out = rewrite_feeds(plan).expect("feed walk runs");
    let FeedOutcome::Rewritten(rewritten) = out else {
        panic!("expected a rewritten feed, got {out:?}");
    };
    assert!(plan_calls_kernel(&rewritten, TIMESTAMP_NS_CAST_NAME));
}

#[test]
fn feed_behind_a_sort_and_filter_that_do_not_read_it_is_rewritten() {
    let scan = scan_of(vec![
        Field::new("id", DataType::Int64, true),
        ns_field("ns"),
    ]);
    let site = project_of(
        scan,
        vec![
            col("id"),
            coalesce_call(vec![col("ns"), null_lit()]).alias("v"),
        ],
    );
    let filtered = LogicalPlanBuilder::from(site)
        .filter(col("id").gt(lit(0)))
        .expect("filter builds")
        .build()
        .expect("plan builds");
    let sorted = LogicalPlanBuilder::from(filtered)
        .sort(vec![col("id").sort(true, true)])
        .expect("sort builds")
        .build()
        .expect("plan builds");
    let input = project_of(sorted, vec![col("v")]);
    let plan = dml_insert(vec![ns_field("v")], input);
    let out = rewrite_feeds(plan).expect("feed walk runs");
    let FeedOutcome::Rewritten(rewritten) = out else {
        panic!("expected a rewritten feed, got {out:?}");
    };
    assert!(plan_calls_kernel(&rewritten, TIMESTAMP_NS_CAST_NAME));
}

#[test]
fn filter_or_sort_that_reads_the_column_stops_the_walk() {
    let scan = scan_of(vec![
        Field::new("id", DataType::Int64, true),
        ns_field("ns"),
    ]);
    let site = project_of(
        scan.clone(),
        vec![
            col("id"),
            coalesce_call(vec![col("ns"), null_lit()]).alias("v"),
        ],
    );
    let filtered = LogicalPlanBuilder::from(site)
        .filter(col("v").is_not_null())
        .expect("filter builds")
        .build()
        .expect("plan builds");
    let input = project_of(filtered, vec![col("v")]);
    let plan = dml_insert(vec![ns_field("v")], input);
    let out = rewrite_feeds(plan).expect("feed walk runs");
    assert!(matches!(out, FeedOutcome::Unchanged));
    let site = project_of(
        scan,
        vec![
            col("id"),
            coalesce_call(vec![col("ns"), null_lit()]).alias("v"),
        ],
    );
    let sorted = LogicalPlanBuilder::from(site)
        .sort(vec![col("v").sort(true, true)])
        .expect("sort builds")
        .build()
        .expect("plan builds");
    let input = project_of(sorted, vec![col("v")]);
    let plan = dml_insert(vec![ns_field("v")], input);
    let out = rewrite_feeds(plan).expect("feed walk runs");
    assert!(matches!(out, FeedOutcome::Unchanged));
}

#[test]
fn column_read_twice_stops_the_walk() {
    let scan = scan_of(vec![ns_field("ns")]);
    let site = project_of(
        scan,
        vec![coalesce_call(vec![col("ns"), null_lit()]).alias("v")],
    );
    let input = project_of(site, vec![col("v"), col("v").alias("w")]);
    let plan = dml_insert(vec![ns_field("v"), ns_field("w")], input);
    let out = rewrite_feeds(plan).expect("feed walk runs");
    assert!(matches!(out, FeedOutcome::Unchanged));
}

#[test]
fn every_other_plan_node_stops_the_walk() {
    let left = project_of(scan_of(vec![ns_field("ns")]), vec![col("ns").alias("v")]);
    let right = project_of(scan_of(vec![ns_field("ns")]), vec![col("ns").alias("v")]);
    let joined = LogicalPlanBuilder::from(left)
        .join(
            right,
            datafusion::logical_expr::JoinType::Inner,
            (vec!["v"], vec!["v"]),
            None,
        )
        .expect("join builds")
        .build()
        .expect("plan builds");
    let input = project_of(joined, vec![col("v")]);
    let plan = dml_insert(vec![ns_field("v")], input);
    assert!(matches!(
        rewrite_feeds(plan).expect("feed walk runs"),
        FeedOutcome::Unchanged
    ));
    let scan = scan_of(vec![
        Field::new("id", DataType::Int64, true),
        ns_field("ns"),
    ]);
    let grouped = LogicalPlanBuilder::from(scan)
        .aggregate(vec![col("id")], Vec::<Expr>::new())
        .expect("aggregate builds")
        .build()
        .expect("plan builds");
    let input = project_of(grouped, vec![col("id").alias("v")]);
    let plan = dml_insert(vec![Field::new("v", DataType::Int64, true)], input);
    assert!(matches!(
        rewrite_feeds(plan).expect("feed walk runs"),
        FeedOutcome::Unchanged
    ));
    let valued = project_of(scan_of(vec![ns_field("v")]), vec![col("v")]);
    let nulls = project_of(scan_of(vec![micro_field()]), vec![null_lit().alias("v")]);
    let union = LogicalPlanBuilder::from(valued)
        .union(nulls)
        .expect("union builds")
        .build()
        .expect("plan builds");
    let input = project_of(union, vec![col("v")]);
    let plan = dml_insert(vec![ns_field("v")], input);
    assert!(matches!(
        rewrite_feeds(plan).expect("feed walk runs"),
        FeedOutcome::Unchanged
    ));
    let scan = scan_of(vec![ns_field("ns")]);
    let plan = dml_insert(vec![ns_field("v")], scan);
    assert!(matches!(
        rewrite_feeds(plan).expect("feed walk runs"),
        FeedOutcome::Unchanged
    ));
}

#[test]
fn consumer_at_the_feed_root_is_left_to_main() {
    let scan = scan_of(vec![ns_field("ns")]);
    let site = coalesce_call(vec![col("ns"), null_lit()]);
    let cast = Expr::Cast(datafusion::logical_expr::Cast::new(
        Box::new(site),
        DataType::Utf8,
    ));
    let input = project_of(scan.clone(), vec![cast.alias("v")]);
    let plan = dml_insert(vec![Field::new("v", DataType::Utf8, true)], input);
    assert!(matches!(
        rewrite_feeds(plan).expect("feed walk runs"),
        FeedOutcome::Unchanged
    ));
    let greater = Expr::BinaryExpr(datafusion::logical_expr::expr::BinaryExpr::new(
        Box::new(col("ns")),
        datafusion::logical_expr::Operator::Gt,
        Box::new(null_lit()),
    ));
    let input = project_of(scan, vec![greater.alias("v")]);
    let plan = dml_insert(vec![Field::new("v", DataType::Boolean, true)], input);
    assert!(matches!(
        rewrite_feeds(plan).expect("feed walk runs"),
        FeedOutcome::Unchanged
    ));
}

#[test]
fn site_left_inside_a_rewritten_root_restores_the_feed() {
    let scan = scan_of(vec![ns_field("ns")]);
    let inner = coalesce_call(vec![col("ns"), null_lit()]);
    let cast = Expr::Cast(datafusion::logical_expr::Cast::new(
        Box::new(inner),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ));
    let root = coalesce_call(vec![cast, null_lit()]);
    let input = project_of(scan, vec![root.alias("v")]);
    let plan = dml_insert(vec![ns_field("v")], input);
    let out = rewrite_feeds(plan).expect("feed walk runs");
    assert!(matches!(out, FeedOutcome::Refused(_)));
}

#[test]
fn microsecond_target_position_is_not_an_anchor() {
    let scan = scan_of(vec![ns_field("ns")]);
    let input = project_of(scan, vec![coalesce_call(vec![col("ns"), null_lit()])]);
    let plan = dml_insert(vec![micro_field()], input);
    let out = rewrite_feeds(plan).expect("feed walk runs");
    assert!(matches!(out, FeedOutcome::Unchanged));
}

#[test]
fn raw_planner_cast_position_is_not_an_anchor() {
    let scan = scan_of(vec![micro_field()]);
    let cast = Expr::Cast(datafusion::logical_expr::Cast::new(
        Box::new(col("ts")),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ));
    let input = project_of(scan, vec![cast.alias("v")]);
    let plan = dml_insert(vec![ns_field("v")], input);
    let out = rewrite_feeds(plan).expect("feed walk runs");
    assert!(matches!(out, FeedOutcome::Unchanged));
}

#[test]
fn refused_feed_counts_once_and_reanalysis_counts_nothing() {
    let _guard = SERIAL.lock().expect("test serial lock");
    let scan = scan_of(vec![ns_field("ns")]);
    let inner = coalesce_call(vec![col("ns"), null_lit()]);
    let cast = Expr::Cast(datafusion::logical_expr::Cast::new(
        Box::new(inner),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ));
    let root = coalesce_call(vec![cast, null_lit()]);
    let input = project_of(scan.clone(), vec![root.alias("v")]);
    let plan = dml_insert(vec![ns_field("v")], input);
    let (sink, lines) = recording_sink();
    let wrapper =
        SparkTimestampNsCoercion::new(Arc::new(PassThroughRule)).with_fallback_sink(sink.clone());
    let config = datafusion::common::config::ConfigOptions::new();
    let _ = wrapper.analyze(plan, &config).expect("plan analyzes");
    assert_eq!(lines.lock().expect("test serial lock").len(), 1);
    let clean = project_of(scan, vec![col("ns").alias("v")]);
    let clean = dml_insert(vec![ns_field("v")], clean);
    let _ = wrapper.analyze(clean, &config).expect("plan analyzes");
    assert_eq!(lines.lock().expect("test serial lock").len(), 1);
    let _ = TIMESTAMPTZ_NS_CAST_NAME.len() + case_call("ns").to_string().len();
}
