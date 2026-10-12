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

pub(super) fn instant() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

pub(super) fn nanos() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

fn zoned_nanos() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")))
}

pub(super) fn identity(name: &str, input: DataType, output: DataType, value: Expr) -> Expr {
    create_udf(
        name,
        vec![input],
        output,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
    )
    .call(vec![value])
}

pub(super) fn cast(value: Expr, target: DataType) -> Expr {
    Expr::Cast(Cast::new(Box::new(value), target))
}

fn absent() -> Expr {
    cast(
        cast(Expr::Literal(ScalarValue::Null, None), nanos()),
        instant(),
    )
}

pub(super) fn cut(column: &str, name: &str) -> Expr {
    let kind = if column == "z" {
        zoned_nanos()
    } else {
        nanos()
    };
    cast(identity(name, kind.clone(), kind, col(column)), instant())
}

pub(super) fn narrowed(column: &str) -> Expr {
    coalesce(vec![cut(column, NARROWED_BESIDE_NULL_UDF_NAME), absent()])
}

pub(super) fn session() -> SessionContext {
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

pub(super) async fn plan_of(ctx: &SessionContext, value: Expr) -> LogicalPlan {
    ctx.table("src")
        .await
        .unwrap()
        .select(vec![col("id"), value.alias("v")])
        .unwrap()
        .into_unoptimized_plan()
}

pub(super) fn listed(value: Expr, kind: DataType) -> Expr {
    let list = |element: DataType| DataType::List(Arc::new(Field::new("item", element, true)));
    identity("make_array", kind.clone(), list(kind), value)
}

#[tokio::test]
async fn a_mark_refuses_through_every_argument_that_carries_the_value_and_no_other() {
    let ctx = session();
    let marked = narrowed("m");
    let bare = identity(NARROWED_BESIDE_VALUE_UDF_NAME, nanos(), nanos(), col("m"));
    let list = |element: DataType| DataType::List(Arc::new(Field::new("item", element, true)));
    let first = |value: Expr| {
        identity(
            "element_at",
            list(instant()),
            instant(),
            cast(value, list(instant())),
        )
    };
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
                bare.clone(),
            ),
            col("u"),
        ]),
        cast(cast(marked.clone(), DataType::Utf8), nanos()),
        cast(cast(marked.clone(), DataType::Int64), nanos()),
        bare.clone(),
        cast(cast(bare.clone(), DataType::Utf8), nanos()),
        first(identity(
            NARROWED_BESIDE_VALUE_UDF_NAME,
            list(nanos()),
            list(nanos()),
            listed(col("m"), nanos()),
        )),
        marked.clone() + lit(ScalarValue::new_interval_dt(0, 0)),
    ];
    for value in carried {
        let plan = plan_of(&ctx, value.clone()).await;
        assert_eq!(column_narrowed(&plan, 0, None), None, "{value}");
        let found = column_narrowed(&plan, 1, None).unwrap_or_else(|| panic!("{value}"));
        let named = found.value.as_deref();
        assert!(named.is_none_or(|named| named == "src.m"), "{value}");
        assert!(!found.zoned, "{value}");
    }
    let selector = Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
        Arc::new(create_udf(
            "element_at",
            vec![list(nanos()), instant()],
            nanos(),
            Volatility::Immutable,
            Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
        )),
        vec![listed(col("m"), nanos()), marked.clone()],
    ));
    let passed_by = [
        col("u"),
        col("m"),
        when(marked.clone().is_not_null(), col("m"))
            .otherwise(col("m"))
            .unwrap(),
        when(marked.clone().gt(col("u")), col("m"))
            .otherwise(col("m"))
            .unwrap(),
        coalesce(vec![cast(col("m"), instant()), absent()]),
        selector,
        when(
            identity("isnotnull", instant(), DataType::Boolean, marked.clone()),
            col("m"),
        )
        .otherwise(col("m"))
        .unwrap(),
        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            Arc::new(create_udf(
                "pick",
                vec![DataType::Boolean, nanos()],
                nanos(),
                Volatility::Immutable,
                Arc::new(|args: &[ColumnarValue]| Ok(args[1].clone())),
            )),
            vec![
                identity("isnotnull", instant(), DataType::Boolean, marked.clone()),
                col("m"),
            ],
        )),
    ];
    for value in passed_by {
        let plan = plan_of(&ctx, value.clone()).await;
        assert_eq!(column_narrowed(&plan, 1, None), None, "{value}");
    }
}

fn kernel(mark: &'static str, column: &str) -> Expr {
    let kind = if column == "z" {
        zoned_nanos()
    } else {
        nanos()
    };
    create_udf(
        NARROW_TIMESTAMP_NS_UDF_NAME,
        vec![kind],
        instant(),
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
    )
    .with_aliases([mark])
    .call(vec![col(column)])
}

#[tokio::test]
async fn the_narrowing_kernel_that_carries_a_mark_is_the_analyzers_cut() {
    let ctx = session();
    let id = DataType::Int32;
    for (mark, untyped_null) in [
        (NARROWED_BESIDE_NULL_UDF_NAME, true),
        (NARROWED_BESIDE_VALUE_UDF_NAME, false),
    ] {
        let narrowed = coalesce(vec![kernel(mark, "m"), col("u")]);
        let plan = plan_of(&ctx, narrowed.clone()).await;
        let found = column_narrowed(&plan, 1, None).expect("the kernel's mark");
        assert_eq!(found.value.as_deref(), Some("src.m"));
        let generated = ctx
            .table("src")
            .await
            .unwrap()
            .alias("__repark_selx_1")
            .unwrap()
            .select(vec![col("id"), coalesce(vec![kernel(mark, "m"), col("u")])])
            .unwrap()
            .into_unoptimized_plan();
        let found = column_narrowed(&generated, 1, None).expect("the kernel's mark");
        assert_eq!(found.value.as_deref(), Some("m"));
        assert_eq!(found.beside_untyped_null, untyped_null);
        let written = identity(NARROW_TIMESTAMP_NS_UDF_NAME, instant(), instant(), narrowed);
        let forgiven = plan_of(&ctx, written).await;
        assert_eq!(column_narrowed(&forgiven, 1, None), None);
        let trunc = plan_of(&ctx, date_trunc(lit("second"), kernel(mark, "m"))).await;
        assert!(column_narrowed(&trunc, 1, None).is_some());
        let zoned = plan_of(&ctx, date_trunc(lit("second"), kernel(mark, "z"))).await;
        assert_eq!(column_narrowed(&zoned, 1, None), None);
        let root =
            datafusion::logical_expr::LogicalPlanBuilder::from(plan_of(&ctx, col("m")).await)
                .project(vec![col("id"), kernel(mark, "v").alias("v")])
                .unwrap()
                .build()
                .unwrap();
        let pairs = [("id", &id), ("v", &nanos())];
        assert!(refuse_narrowed_ns_inserts("`t`", &root, pairs, None).is_err());
    }
}

#[tokio::test]
async fn a_plain_column_beside_a_narrowing_has_its_own_lineage_under_every_plan_node() {
    let ctx = session();
    let src = ctx.table("src").await.unwrap();
    let both = src
        .clone()
        .select(vec![
            col("id"),
            col("m").alias("plain"),
            narrowed("m").alias("cut"),
        ])
        .unwrap();
    let rollup = datafusion::logical_expr::Expr::GroupingSet(
        datafusion::logical_expr::GroupingSet::Rollup(vec![col("id")]),
    );
    let grouped = both
        .clone()
        .filter(col("cut").is_not_null())
        .unwrap()
        .aggregate(
            vec![rollup],
            vec![max(col("plain")).alias("p"), max(col("cut")).alias("c")],
        )
        .unwrap()
        .into_unoptimized_plan();
    assert_eq!(column_narrowed(&grouped, 0, None), None, "{grouped}");
    assert_eq!(column_narrowed(&grouped, 1, None), None, "{grouped}");
    assert!(column_narrowed(&grouped, 2, None).is_some(), "{grouped}");
    let LogicalPlan::Projection(above) = &grouped else {
        panic!("a projection over the aggregate: {grouped}");
    };
    let sets = above.input.as_ref();
    assert_eq!(sets.schema().fields().len(), 4, "{sets}");
    assert_eq!(column_narrowed(sets, 0, None), None, "{sets}");
    assert_eq!(column_narrowed(sets, 1, None), None, "{sets}");
    assert_eq!(column_narrowed(sets, 2, None), None, "{sets}");
    assert!(column_narrowed(sets, 3, None).is_some(), "{sets}");
    let outer = datafusion::logical_expr::LogicalPlanBuilder::from(both.into_unoptimized_plan())
        .alias("o")
        .unwrap()
        .build()
        .unwrap();
    let field = |name: &str| {
        let (_, field) = outer
            .schema()
            .qualified_field_with_unqualified_name(name)
            .unwrap();
        Arc::clone(field)
    };
    let reference = |name: &str| {
        Expr::OuterReferenceColumn(
            field(name),
            datafusion::common::Column::new(Some("o"), name),
        )
    };
    let inner = datafusion::logical_expr::LogicalPlanBuilder::empty(true)
        .project(vec![
            reference("plain").alias("p"),
            reference("cut").alias("c"),
            when(reference("cut").is_not_null(), reference("plain"))
                .otherwise(reference("plain"))
                .unwrap()
                .alias("k"),
        ])
        .unwrap()
        .build()
        .unwrap();
    let lateral = LogicalPlan::Subquery(datafusion::logical_expr::Subquery {
        subquery: Arc::new(inner),
        outer_ref_columns: vec![reference("plain"), reference("cut")],
        spans: datafusion::common::Spans::new(),
    });
    let joined = datafusion::logical_expr::LogicalPlanBuilder::from(outer)
        .cross_join(lateral)
        .unwrap()
        .build()
        .unwrap();
    let width = joined.schema().fields().len();
    assert_eq!(width, 6, "{joined}");
    assert_eq!(column_narrowed(&joined, 1, None), None, "{joined}");
    assert!(column_narrowed(&joined, 2, None).is_some(), "{joined}");
    assert_eq!(column_narrowed(&joined, 3, None), None, "{joined}");
    assert!(column_narrowed(&joined, 4, None).is_some(), "{joined}");
    assert_eq!(column_narrowed(&joined, 5, None), None, "{joined}");
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
    let naive = DataType::Timestamp(TimeUnit::Microsecond, None);
    let beside_naive = |column: &str| {
        let kind = if column == "z" {
            zoned_nanos()
        } else {
            nanos()
        };
        let marked = identity(
            NARROWED_BESIDE_VALUE_UDF_NAME,
            kind.clone(),
            kind,
            col(column),
        );
        coalesce(vec![
            cast(marked, naive.clone()),
            cast(col("u"), naive.clone()),
        ])
    };
    for (value, refuses) in [
        (written(narrowed("m")), false),
        (written(narrowed("z")), false),
        (cast(narrowed("m"), instant()), false),
        (date_trunc(lit("second"), narrowed("z")), false),
        (date_trunc(lit("second"), narrowed("m")), true),
        (to_date(narrowed("m")), false),
        (cast(narrowed("m"), DataType::Date32), false),
        (date_trunc(lit("second"), cast(col("m"), instant())), false),
        (to_date(narrowed("z")), true),
        (
            date_trunc(lit("day"), coalesce(vec![narrowed("z"), narrowed("m")])),
            true,
        ),
        (cast(narrowed("z"), naive.clone()), true),
        (written(beside_naive("m")), true),
        (to_date(beside_naive("m")), true),
        (written(date_trunc(lit("second"), narrowed("m"))), true),
        (to_date(date_trunc(lit("second"), narrowed("m"))), true),
        (date_trunc(lit("month"), to_date(narrowed("z"))), true),
        (written(date_trunc(lit("second"), narrowed("z"))), false),
        (date_trunc(lit("month"), to_date(narrowed("m"))), false),
        (
            written(narrowed("m") + lit(ScalarValue::new_interval_dt(0, 0))),
            true,
        ),
        (
            written(cast(cast(narrowed("m"), DataType::Utf8), nanos())),
            true,
        ),
        (
            written(identity("other", instant(), instant(), narrowed("m"))),
            true,
        ),
        (
            written(
                when(col("id").gt(lit(0)), narrowed("m"))
                    .otherwise(col("u"))
                    .unwrap(),
            ),
            false,
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
