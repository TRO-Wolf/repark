use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::datasource::empty::EmptyTable;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions::datetime::expr_fn::date_trunc;
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::logical_expr::{
    Cast, ColumnarValue, Expr, JoinType, LogicalPlan, TableScan, Union, Volatility, col,
    create_udf, lit, when,
};
use datafusion::prelude::SessionContext;

use super::{
    NARROW_TIMESTAMP_NS_UDF_NAME, NARROWED_BESIDE_NULL_UDF_NAME, NARROWED_BESIDE_VALUE_UDF_NAME,
    TIMESTAMP_TO_DATE_UDF_NAME, column_narrowed, refuse_narrowed_ns_columns,
    refuse_narrowed_ns_inserts, refuse_narrowed_ns_writes,
};

fn instant() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

fn nanos() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

fn zoned_nanos() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")))
}

fn identity(name: &str, input: DataType, output: DataType, value: Expr) -> Expr {
    create_udf(
        name,
        vec![input],
        output,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
    )
    .call(vec![value])
}

fn cast(value: Expr, target: DataType) -> Expr {
    Expr::Cast(Cast::new(Box::new(value), target))
}

fn absent() -> Expr {
    cast(
        cast(Expr::Literal(ScalarValue::Null, None), nanos()),
        instant(),
    )
}

fn cut(column: &str, name: &str) -> Expr {
    let kind = if column == "z" {
        zoned_nanos()
    } else {
        nanos()
    };
    cast(identity(name, kind.clone(), kind, col(column)), instant())
}

fn narrowed(column: &str) -> Expr {
    coalesce(vec![cut(column, NARROWED_BESIDE_NULL_UDF_NAME), absent()])
}

fn session() -> SessionContext {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("m", nanos(), true),
        Field::new("z", zoned_nanos(), true),
        Field::new("u", instant(), true),
    ]);
    ctx.register_table("src", Arc::new(EmptyTable::new(Arc::new(schema))))
        .unwrap();
    ctx
}

async fn plan_of(ctx: &SessionContext, value: Expr) -> LogicalPlan {
    ctx.table("src")
        .await
        .unwrap()
        .select(vec![col("id"), value.alias("v")])
        .unwrap()
        .into_unoptimized_plan()
}

#[tokio::test]
async fn a_cast_over_a_marked_value_is_followed_through_every_carrying_position_and_no_other() {
    let ctx = session();
    let marked = narrowed("m");
    let carried = [
        marked.clone(),
        cast(marked.clone(), nanos()),
        when(col("id").gt(lit(0)), marked.clone())
            .otherwise(col("u"))
            .unwrap(),
        when(col("id").gt(lit(0)), col("u"))
            .otherwise(marked.clone())
            .unwrap(),
        coalesce(vec![
            identity(
                NARROW_TIMESTAMP_NS_UDF_NAME,
                nanos(),
                instant(),
                identity(NARROWED_BESIDE_VALUE_UDF_NAME, nanos(), nanos(), col("m")),
            ),
            col("u"),
        ]),
    ];
    for value in carried {
        let plan = plan_of(&ctx, value.clone()).await;
        assert_eq!(column_narrowed(&plan, 0, None), None, "{value}");
        let found = column_narrowed(&plan, 1, None).unwrap_or_else(|| panic!("{value}"));
        assert_eq!(found.value, "src.m", "{value}");
        assert!(!found.zoned, "{value}");
    }
    let unmarked = identity(NARROWED_BESIDE_NULL_UDF_NAME, nanos(), nanos(), col("m"));
    let passed_by = [
        col("u"),
        when(marked.clone().is_not_null(), col("u"))
            .otherwise(col("u"))
            .unwrap(),
        cast(cast(marked.clone(), DataType::Utf8), nanos()),
        coalesce(vec![cast(col("m"), instant()), absent()]),
        unmarked.clone(),
        cast(unmarked, DataType::Utf8),
    ];
    for value in passed_by {
        let plan = plan_of(&ctx, value.clone()).await;
        assert_eq!(column_narrowed(&plan, 1, None), None, "{value}");
    }
}

#[tokio::test]
async fn a_written_call_stores_only_where_it_equals_the_call_over_the_nanosecond_value() {
    let ctx = session();
    let to_date = |value: Expr| {
        identity(
            TIMESTAMP_TO_DATE_UDF_NAME,
            instant(),
            DataType::Date32,
            value,
        )
    };
    let written = |value: Expr| identity(NARROW_TIMESTAMP_NS_UDF_NAME, instant(), instant(), value);
    for (value, refuses) in [
        (written(narrowed("m")), false),
        (written(narrowed("z")), false),
        (cast(narrowed("m"), instant()), false),
        (date_trunc(lit("second"), narrowed("z")), false),
        (date_trunc(lit("second"), narrowed("m")), true),
        (to_date(narrowed("m")), false),
        (date_trunc(lit("second"), cast(col("m"), instant())), false),
        (to_date(narrowed("z")), true),
        (
            date_trunc(lit("day"), coalesce(vec![narrowed("z"), narrowed("m")])),
            true,
        ),
        (
            cast(
                narrowed("z"),
                DataType::Timestamp(TimeUnit::Microsecond, None),
            ),
            true,
        ),
    ] {
        let plan = plan_of(&ctx, value.clone()).await;
        assert_eq!(
            column_narrowed(&plan, 1, None).is_some(),
            refuses,
            "{value}"
        );
    }
}

#[tokio::test]
async fn a_narrowed_value_is_followed_through_the_plan_nodes_between_it_and_the_store() {
    let ctx = session();
    let src = ctx.table("src").await.unwrap();
    let marked = src
        .clone()
        .select(vec![col("id"), narrowed("m").alias("v")])
        .unwrap();
    let bare = src
        .clone()
        .select(vec![col("id"), col("u").alias("v")])
        .unwrap();
    let frames = [
        marked.clone().filter(col("id").gt(lit(0))).unwrap(),
        marked
            .clone()
            .sort(vec![col("id").sort(true, true)])
            .unwrap(),
        marked.clone().limit(0, Some(1)).unwrap(),
        marked.clone().distinct().unwrap(),
        marked.clone().alias("a").unwrap(),
        bare.clone().union(marked.clone()).unwrap(),
        marked
            .clone()
            .aggregate(vec![col("id")], vec![max(col("v")).alias("v")])
            .unwrap(),
        marked
            .clone()
            .alias("l")
            .unwrap()
            .join(
                bare.clone().alias("r").unwrap(),
                JoinType::Inner,
                &["id"],
                &["id"],
                None,
            )
            .unwrap()
            .select(vec![col("l.id"), col("l.v")])
            .unwrap(),
    ];
    for frame in frames {
        let plan = frame.into_unoptimized_plan();
        assert_eq!(column_narrowed(&plan, 0, None), None, "{plan}");
        assert!(column_narrowed(&plan, 1, None).is_some(), "{plan}");
    }
    let unmarked = bare.clone().union(bare).unwrap().into_unoptimized_plan();
    assert_eq!(column_narrowed(&unmarked, 1, None), None);
}

#[tokio::test]
async fn a_narrowed_value_is_followed_into_a_view_the_scan_holds_and_a_union_branch() {
    let ctx = session();
    let marked = plan_of(&ctx, narrowed("m")).await;
    let view = datafusion::datasource::ViewTable::new(marked.clone(), None);
    ctx.register_table("narrowed", Arc::new(view)).unwrap();
    let source =
        datafusion::datasource::provider_as_source(ctx.table_provider("narrowed").await.unwrap());
    let scan = TableScan::try_new("narrowed", source, Some(vec![1]), vec![], None).unwrap();
    assert!(column_narrowed(&LogicalPlan::TableScan(scan), 0, None).is_some());
    let source =
        datafusion::datasource::provider_as_source(ctx.table_provider("src").await.unwrap());
    let scan = TableScan::try_new("src", source, None, vec![], None).unwrap();
    assert_eq!(
        column_narrowed(&LogicalPlan::TableScan(scan), 1, None),
        None
    );
    let rows = plan_of(&ctx, cut("z", NARROWED_BESIDE_VALUE_UDF_NAME)).await;
    let other = plan_of(&ctx, col("u")).await;
    let schema = Arc::clone(rows.schema());
    let union = LogicalPlan::Union(Union {
        inputs: vec![Arc::new(other), Arc::new(rows)],
        schema,
    });
    let found = column_narrowed(&union, 1, None).unwrap();
    assert!(found.zoned && !found.beside_untyped_null);
    assert_eq!(column_narrowed(&union, 0, None), None);
    let beside_null = plan_of(&ctx, narrowed("m")).await;
    let LogicalPlan::Union(mut both) = union else {
        panic!("a union");
    };
    both.inputs.push(Arc::new(beside_null));
    let found = column_narrowed(&LogicalPlan::Union(both), 1, None).unwrap();
    assert!(found.beside_untyped_null && !found.zoned);
}

#[tokio::test]
async fn only_a_nanosecond_target_refuses_and_the_text_names_the_column_and_the_value() {
    let ctx = session();
    let beside_null = plan_of(&ctx, narrowed("m")).await;
    let beside_value = plan_of(
        &ctx,
        coalesce(vec![cut("m", NARROWED_BESIDE_VALUE_UDF_NAME), col("u")]),
    )
    .await;
    let id = DataType::Int32;
    let zoned = DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+00:00")));
    let targets = [("id", &id), ("v", &nanos())];
    let refused = refuse_narrowed_ns_writes(&ctx, "`t`", &beside_null, targets)
        .unwrap_err()
        .to_string();
    assert_eq!(
        refused,
        "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write \
         incompatible data for the table `t`: Cannot safely cast `v` \"TIMESTAMP\" to \
         \"TIMESTAMP_NS\". The value was narrowed from nanoseconds to microseconds before the \
         store; give the NULL beside it the type timestamp_ns. SQLSTATE: KD000"
    );
    let refused =
        refuse_narrowed_ns_columns("`t`", &beside_value, [("id", &id), ("v", &zoned)], None)
            .unwrap_err()
            .to_string();
    assert_eq!(
        refused,
        "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write \
         incompatible data for the table `t`: Cannot safely cast `v` \"TIMESTAMP\" to \
         \"TIMESTAMPTZ_NS\". The value src.m was narrowed from nanoseconds to microseconds \
         before the store, to match the microsecond value beside it; write CAST(src.m AS \
         TIMESTAMP) if microseconds are intended, or give the value beside it a nanosecond \
         type. SQLSTATE: KD000"
    );
    for kept in [instant(), DataType::Timestamp(TimeUnit::Microsecond, None)] {
        refuse_narrowed_ns_writes(&ctx, "`t`", &beside_null, [("id", &id), ("v", &kept)]).unwrap();
    }
    refuse_narrowed_ns_writes(&ctx, "`t`", &beside_null, [("v", &nanos())]).unwrap();
    let written = |value: Expr| identity(NARROW_TIMESTAMP_NS_UDF_NAME, instant(), instant(), value);
    for conform in [written(col("v")), cast(col("v"), nanos())] {
        let conforming = datafusion::logical_expr::LogicalPlanBuilder::from(beside_null.clone())
            .project(vec![col("id"), conform.alias("v")])
            .unwrap()
            .build()
            .unwrap();
        let pairs = [("id", &id), ("v", &nanos())];
        assert!(refuse_narrowed_ns_inserts("`t`", &conforming, pairs, None).is_err());
    }
    let conforming = datafusion::logical_expr::LogicalPlanBuilder::from(beside_null.clone())
        .project(vec![col("id"), written(col("v")).alias("v")])
        .unwrap()
        .build()
        .unwrap();
    refuse_narrowed_ns_columns("`t`", &conforming, [("id", &id), ("v", &nanos())], None).unwrap();
    refuse_narrowed_ns_writes(&ctx, "`t`", &beside_null, [("id", &nanos()), ("v", &id)]).unwrap();
}
