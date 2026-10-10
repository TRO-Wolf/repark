use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::datasource::empty::EmptyTable;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::logical_expr::{Cast, ColumnarValue, Expr, Volatility, col, create_udf, lit, when};
use datafusion::prelude::SessionContext;

use super::{
    NARROWED_BESIDE_NULL_UDF_NAME, column_narrowed, refuse_narrowed_ns_columns,
    refuse_narrowed_ns_writes,
};

fn instant() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

fn nanos() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

fn mark(node: Expr) -> Expr {
    create_udf(
        NARROWED_BESIDE_NULL_UDF_NAME,
        vec![instant()],
        instant(),
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
    )
    .call(vec![node])
}

fn absent() -> Expr {
    cast(
        cast(Expr::Literal(ScalarValue::Null, None), nanos()),
        instant(),
    )
}

fn cast(value: Expr, target: DataType) -> Expr {
    Expr::Cast(Cast::new(Box::new(value), target))
}

fn narrowed(value: Expr) -> Expr {
    mark(coalesce(vec![cast(value, instant()), absent()]))
}

fn session() -> SessionContext {
    let ctx = SessionContext::new();
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("m", nanos(), true),
        Field::new("u", instant(), true),
    ]);
    ctx.register_table("src", Arc::new(EmptyTable::new(Arc::new(schema))))
        .unwrap();
    ctx
}

#[tokio::test]
async fn a_marked_value_is_followed_through_every_carrying_position_and_no_other() {
    let ctx = session();
    let src = ctx.table("src").await.unwrap();
    let marked = narrowed(col("m"));
    let carried = [
        marked.clone().alias("v"),
        cast(marked.clone(), nanos()).alias("v"),
        when(col("id").gt(lit(0)), marked.clone())
            .otherwise(col("u"))
            .unwrap()
            .alias("v"),
        when(col("id").gt(lit(0)), col("u"))
            .otherwise(marked.clone())
            .unwrap()
            .alias("v"),
        datafusion::functions::datetime::expr_fn::date_trunc(lit("second"), marked.clone())
            .alias("v"),
    ];
    for value in carried {
        let plan = src
            .clone()
            .select(vec![col("id"), value.clone()])
            .unwrap()
            .into_unoptimized_plan();
        assert!(!column_narrowed(&plan, 0, None), "{value}");
        assert!(column_narrowed(&plan, 1, None), "{value}");
    }
    let passed_by = [
        col("u").alias("v"),
        when(marked.clone().is_not_null(), col("u"))
            .otherwise(col("u"))
            .unwrap()
            .alias("v"),
        cast(cast(marked.clone(), DataType::Utf8), nanos()).alias("v"),
        mark(coalesce(vec![col("u"), absent()])).alias("v"),
        mark(coalesce(vec![
            when(col("id").gt(lit(0)), cast(col("m"), instant()))
                .otherwise(col("u"))
                .unwrap(),
            absent(),
        ]))
        .alias("v"),
    ];
    for value in passed_by {
        let plan = src
            .clone()
            .select(vec![col("id"), value.clone()])
            .unwrap()
            .into_unoptimized_plan();
        assert!(!column_narrowed(&plan, 1, None), "{value}");
    }
}

#[tokio::test]
async fn a_marked_value_is_followed_through_the_plan_nodes_between_it_and_the_store() {
    let ctx = session();
    let src = ctx.table("src").await.unwrap();
    let marked = src
        .clone()
        .select(vec![col("id"), narrowed(col("m")).alias("v")])
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
                datafusion::logical_expr::JoinType::Inner,
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
        assert!(!column_narrowed(&plan, 0, None), "{plan}");
        assert!(column_narrowed(&plan, 1, None), "{plan}");
    }
    let unmarked = bare.clone().union(bare).unwrap().into_unoptimized_plan();
    assert!(!column_narrowed(&unmarked, 1, None));
}

#[tokio::test]
async fn only_a_nanosecond_target_refuses_and_the_text_names_the_column() {
    let ctx = session();
    let frame = ctx
        .table("src")
        .await
        .unwrap()
        .select(vec![col("id"), narrowed(col("m")).alias("v")])
        .unwrap();
    let plan = frame.logical_plan();
    let id = DataType::Int32;
    let zoned = DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+00:00")));
    let refused = refuse_narrowed_ns_writes(&ctx, "`t`", plan, [("id", &id), ("v", &nanos())])
        .unwrap_err()
        .to_string();
    assert_eq!(
        refused,
        "Error during planning: [INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write \
         incompatible data for the table `t`: Cannot safely cast `v` \"TIMESTAMP\" to \
         \"TIMESTAMP_NS\". The value was narrowed from nanoseconds to microseconds before the \
         store; give the NULL beside it the type timestamp_ns. SQLSTATE: KD000"
    );
    let refused = refuse_narrowed_ns_columns("`t`", plan, [("id", &id), ("v", &zoned)], None)
        .unwrap_err()
        .to_string();
    assert!(
        refused.contains("\"TIMESTAMPTZ_NS\"") && refused.contains("the type timestamptz_ns"),
        "{refused}"
    );
    for kept in [instant(), DataType::Timestamp(TimeUnit::Microsecond, None)] {
        refuse_narrowed_ns_writes(&ctx, "`t`", plan, [("id", &id), ("v", &kept)]).unwrap();
    }
    refuse_narrowed_ns_writes(&ctx, "`t`", plan, [("v", &nanos())]).unwrap();
    refuse_narrowed_ns_writes(&ctx, "`t`", plan, [("id", &nanos()), ("v", &id)]).unwrap();
}

#[tokio::test]
async fn a_marked_value_is_followed_into_a_view_the_scan_holds() {
    let ctx = session();
    let src = ctx.table("src").await.unwrap();
    let marked = src
        .clone()
        .select(vec![col("id"), narrowed(col("m")).alias("v")])
        .unwrap()
        .into_unoptimized_plan();
    let view = datafusion::datasource::ViewTable::new(marked, None);
    ctx.register_table("narrowed", Arc::new(view)).unwrap();
    let source =
        datafusion::datasource::provider_as_source(ctx.table_provider("narrowed").await.unwrap());
    let scan = datafusion::logical_expr::TableScan::try_new(
        "narrowed",
        source,
        Some(vec![1]),
        vec![],
        None,
    )
    .unwrap();
    let scanned = datafusion::logical_expr::LogicalPlan::TableScan(scan);
    assert!(column_narrowed(&scanned, 0, None), "{scanned}");
    let source =
        datafusion::datasource::provider_as_source(ctx.table_provider("src").await.unwrap());
    let scan =
        datafusion::logical_expr::TableScan::try_new("src", source, None, vec![], None).unwrap();
    let plain = datafusion::logical_expr::LogicalPlan::TableScan(scan);
    assert!(!column_narrowed(&plain, 1, None), "{plain}");
}

#[tokio::test]
async fn a_marked_null_branch_of_a_union_refuses_only_beside_a_narrowed_branch() {
    let ctx = session();
    let src = ctx.table("src").await.unwrap();
    let branch = |value: Expr| {
        Arc::new(
            src.clone()
                .select(vec![col("id"), value.alias("v")])
                .unwrap()
                .into_unoptimized_plan(),
        )
    };
    let union = |rows: Expr| {
        let rows = branch(rows);
        let schema = Arc::clone(rows.schema());
        datafusion::logical_expr::LogicalPlan::Union(datafusion::logical_expr::Union {
            inputs: vec![rows, branch(mark(absent()))],
            schema,
        })
    };
    assert!(column_narrowed(&union(cast(col("m"), instant())), 1, None));
    assert!(!column_narrowed(&union(col("u")), 1, None));
    assert!(!column_narrowed(&union(cast(col("m"), instant())), 0, None));
}
