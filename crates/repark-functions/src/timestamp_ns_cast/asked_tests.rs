use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::datasource::{MemTable, provider_as_source};
use datafusion::functions::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::functions_aggregate::min_max::max_udaf;
use datafusion::functions_nested::expr_fn::make_array;
use datafusion::logical_expr::expr::{GroupingSet, ScalarFunction, WindowFunction};
use datafusion::logical_expr::{
    Cast, EmptyRelation, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Partitioning,
    WindowFunctionDefinition, col, lit, when,
};

use super::asked::asked_nanosecond_columns;
use super::{CastOrigin, timestamp_ns_cast_expr, timestamp_ns_cast_udf};

fn nanos(zone: Option<&str>) -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, zone.map(Arc::from))
}

fn table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("ns", nanos(None), true),
        Field::new("tzns", nanos(Some("UTC")), true),
        Field::new("us", DataType::Timestamp(TimeUnit::Microsecond, None), true),
    ]))
}

fn table() -> LogicalPlanBuilder {
    let provider = MemTable::try_new(table_schema(), vec![vec![]]).unwrap();
    LogicalPlanBuilder::scan("t", provider_as_source(Arc::new(provider)), None).unwrap()
}

fn written(expr: Expr) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(
        timestamp_ns_cast_udf(false, CastOrigin::Written),
        vec![expr],
    ))
}

fn inserted(expr: Expr) -> Expr {
    timestamp_ns_cast_expr(expr, false)
}

fn projected(exprs: Vec<Expr>) -> LogicalPlan {
    table().project(exprs).unwrap().build().unwrap()
}

fn asked(plan: &LogicalPlan) -> Vec<bool> {
    asked_nanosecond_columns(plan).unwrap()
}

fn cast_column() -> LogicalPlanBuilder {
    table()
        .project(vec![col("id"), written(col("us")).alias("c")])
        .unwrap()
}

#[test]
fn a_written_cast_column_is_asked() {
    assert_eq!(asked(&cast_column().build().unwrap()), vec![false, true]);
    let over_nanoseconds = projected(vec![written(col("ns")).alias("c")]);
    assert_eq!(asked(&over_nanoseconds), vec![true]);
}

#[test]
fn a_table_column_is_not_asked() {
    assert_eq!(asked(&table().build().unwrap()), vec![false; 4]);
    let renamed = projected(vec![col("ns").alias("a"), col("tzns").alias("b")]);
    assert_eq!(asked(&renamed), vec![false, false]);
}

#[test]
fn an_inserted_kernel_over_a_table_column_is_not_asked() {
    assert_eq!(asked(&projected(vec![inserted(col("ns"))])), vec![false]);
    let beside_a_written_cast = coalesce(vec![written(col("us")), inserted(col("ns"))]);
    assert_eq!(asked(&projected(vec![beside_a_written_cast])), vec![false]);
}

#[test]
fn an_inserted_kernel_over_a_microsecond_value_is_no_origin() {
    assert_eq!(asked(&projected(vec![inserted(col("us"))])), vec![false]);
    let beside_a_written_cast = coalesce(vec![written(col("us")), inserted(col("us"))]);
    assert_eq!(asked(&projected(vec![beside_a_written_cast])), vec![true]);
}

#[test]
fn an_aggregate_over_a_written_cast_is_asked() {
    let aggregated = table()
        .aggregate(vec![col("id")], vec![max(written(col("us"))).alias("m")])
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(asked(&aggregated), vec![false, true]);
    let grouped = table()
        .aggregate(vec![written(col("us")).alias("g")], vec![max(col("ns"))])
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(asked(&grouped), vec![true, false]);
    let rollup = Expr::GroupingSet(GroupingSet::Rollup(vec![written(col("us"))]));
    let sets = table()
        .aggregate(vec![rollup], vec![max(written(col("us")))])
        .unwrap()
        .build()
        .unwrap();
    assert!(asked(&sets).iter().all(|flag| !flag));
}

#[test]
fn a_window_over_a_table_column_is_not_asked() {
    let over = |expr: Expr| {
        let function = WindowFunctionDefinition::AggregateUDF(max_udaf());
        Expr::WindowFunction(Box::new(WindowFunction::new(function, vec![expr])))
    };
    let windowed = cast_column()
        .window(vec![over(col("c")).alias("w")])
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(asked(&windowed), vec![false, true, true]);
    let over_the_table = table()
        .window(vec![over(col("ns")).alias("w")])
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(asked(&over_the_table), vec![false; 5]);
}

#[test]
fn a_values_column_with_a_written_cell_and_an_inserted_null_is_asked() {
    let rows = |second: Expr| {
        LogicalPlanBuilder::values(vec![
            vec![written(lit("2026-01-02 03:04:05.123456789"))],
            vec![second],
        ])
        .unwrap()
        .build()
        .unwrap()
    };
    assert_eq!(asked(&rows(inserted(lit(ScalarValue::Null)))), vec![true]);
    let made = lit(ScalarValue::TimestampNanosecond(Some(1), None));
    assert_eq!(asked(&rows(made)), vec![false]);
}

#[test]
fn a_union_with_one_table_column_branch_is_not_asked() {
    let table_branch = projected(vec![col("ns").alias("c")]);
    let cast_branch = projected(vec![written(col("us")).alias("c")]);
    for (first, second) in [
        (cast_branch.clone(), table_branch.clone()),
        (table_branch, cast_branch),
    ] {
        let union = LogicalPlanBuilder::from(first)
            .union(second)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(asked(&union), vec![false]);
    }
}

#[test]
fn a_union_of_written_casts_is_asked() {
    let branch = || projected(vec![written(col("us")).alias("c")]);
    let union = LogicalPlanBuilder::from(branch())
        .union(branch())
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(asked(&union), vec![true]);
}

#[test]
fn a_join_side_is_followed() {
    let right = || table().alias("r").unwrap().build().unwrap();
    let joined = cast_column()
        .join(right(), JoinType::Inner, (vec!["id"], vec!["r.id"]), None)
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(
        asked(&joined),
        vec![false, true, false, false, false, false]
    );
    let swapped = LogicalPlanBuilder::from(right())
        .join(
            cast_column().build().unwrap(),
            JoinType::Full,
            (vec!["r.id"], vec!["t.id"]),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(
        asked(&swapped),
        vec![false, false, false, false, false, true]
    );
    let semi = cast_column()
        .join(
            right(),
            JoinType::LeftSemi,
            (vec!["id"], vec!["r.id"]),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(asked(&semi), vec![false, false]);
}

#[test]
fn a_nested_type_holding_a_table_nanosecond_is_not_asked() {
    assert_eq!(
        asked(&projected(vec![make_array(vec![col("ns")])])),
        vec![false]
    );
    assert_eq!(
        asked(&projected(vec![make_array(vec![written(col("us"))])])),
        vec![true]
    );
}

#[test]
fn a_condition_over_nanoseconds_is_not_an_origin() {
    let chosen = when(col("ns").gt(col("tzns")), written(col("us")))
        .otherwise(written(col("us")))
        .unwrap();
    assert_eq!(asked(&projected(vec![chosen])), vec![true]);
    let over_the_table = when(col("id").gt(lit(0)), written(col("us")))
        .otherwise(col("ns"))
        .unwrap();
    assert_eq!(asked(&projected(vec![over_the_table])), vec![false]);
}

#[test]
fn an_unknown_plan_node_is_not_asked() {
    let schema = DFSchema::try_from(Schema::new(vec![Field::new("c", nanos(None), true)])).unwrap();
    let empty = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(schema),
    });
    assert_eq!(asked(&empty), vec![false]);
    let distinct_on = cast_column()
        .distinct_on(vec![col("id")], vec![col("c")], None)
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(asked(&distinct_on), vec![false]);
}

#[test]
fn a_column_with_no_nanosecond_type_is_false_and_never_refused() {
    let plan = projected(vec![col("us"), col("id"), lit("text")]);
    assert_eq!(asked(&plan), vec![false, false, false]);
}

#[test]
fn a_function_that_makes_nanoseconds_itself_is_not_asked() {
    let literal = lit(ScalarValue::TimestampNanosecond(Some(1), None));
    let arrow_cast = Expr::Cast(Cast::new(Box::new(col("us")), nanos(None)));
    assert_eq!(
        asked(&projected(vec![literal, arrow_cast])),
        vec![false, false]
    );
}

#[test]
fn nodes_that_keep_their_input_columns_keep_the_ask() {
    let plan = cast_column()
        .filter(col("id").gt(lit(0)))
        .unwrap()
        .sort(vec![col("id").sort(true, true)])
        .unwrap()
        .limit(0, Some(1))
        .unwrap()
        .distinct()
        .unwrap()
        .repartition(Partitioning::RoundRobinBatch(2))
        .unwrap()
        .alias("x")
        .unwrap()
        .project(vec![col("c"), col("id")])
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(asked(&plan), vec![true, false]);
}

#[test]
fn a_plan_deeper_than_the_walk_follows_is_not_asked() {
    let mut plan = cast_column().build().unwrap();
    for level in 0..1100 {
        plan = LogicalPlanBuilder::from(plan)
            .alias(format!("a{level}"))
            .unwrap()
            .build()
            .unwrap();
    }
    assert_eq!(asked(&plan), vec![false, false]);
}
