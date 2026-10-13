use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::common::TableReference;
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{
    DmlStatement, Expr, LogicalPlan, LogicalPlanBuilder, ScalarUDF, WriteOp, col, lit,
};

fn ns_field(name: &str) -> Field {
    Field::new(name, DataType::Timestamp(TimeUnit::Nanosecond, None), true)
}

fn micro_field() -> Field {
    Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true)
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

fn null_lit() -> Expr {
    lit(ScalarValue::Null)
}

fn coalesce_call(args: Vec<Expr>) -> Expr {
    let udf = datafusion::functions::core::coalesce::CoalesceFunc::new();
    Expr::ScalarFunction(ScalarFunction::new_udf(
        Arc::new(ScalarUDF::from(udf)),
        args,
    ))
}

pub(crate) fn dml_of(targets: Vec<Field>, exprs: Vec<Expr>, input: LogicalPlan) -> LogicalPlan {
    let schema = Arc::new(Schema::new(targets));
    let mem = MemTable::try_new(schema, vec![Vec::new()]).expect("memory table builds");
    let target = provider_as_source(Arc::new(mem));
    let projection = LogicalPlanBuilder::from(input)
        .project(exprs)
        .expect("project builds")
        .build()
        .expect("plan builds");
    LogicalPlan::Dml(DmlStatement::new(
        TableReference::from("t"),
        target,
        WriteOp::Insert(InsertOp::Append),
        Arc::new(projection),
    ))
}

pub(crate) fn dml_feed(expr: Expr, target: Field) -> LogicalPlan {
    let scan = scan_of(vec![
        ns_field("ns"),
        Field::new(
            "tzns",
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            true,
        ),
        micro_field(),
        Field::new("id", DataType::Int64, true),
    ]);
    dml_of(vec![target], vec![expr], scan)
}

pub(crate) fn dml_feed_with_trigger(trigger: Expr) -> LogicalPlan {
    let scan = scan_of(vec![ns_field("ns")]);
    dml_of(
        vec![ns_field("v"), micro_field()],
        vec![coalesce_call(vec![col("ns"), null_lit()]), trigger],
        scan,
    )
}

pub(crate) fn dml_input_of(plan: &LogicalPlan) -> &LogicalPlan {
    let LogicalPlan::Dml(dml) = plan else {
        panic!("expected a Dml, got {plan:?}");
    };
    dml.input.as_ref()
}
