use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::ScalarValue;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions_aggregate::expr_fn::{count, max};
use datafusion::logical_expr::{ColumnarValue, Expr, TryCast, Volatility, col, create_udf, lit};

use super::tests::{cast, cut, identity, instant, listed, nanos, narrowed, plan_of, session};
use super::{
    NARROW_TIMESTAMP_NS_UDF_NAME, NARROWED_BESIDE_VALUE_UDF_NAME, Narrowing, WRITTEN_CAST_UDF_NAME,
    column_narrowed, narrowed_refusal, refuse_narrowed_ns_columns, refuse_narrowed_ns_inserts,
    refuse_narrowed_ns_updates,
};

fn call(name: &str, inputs: Vec<DataType>, output: DataType, values: Vec<Expr>) -> Expr {
    create_udf(
        name,
        inputs,
        output,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
    )
    .call(values)
}

fn list_of(element: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", element, true)))
}

fn map_of(key: DataType, value: DataType) -> DataType {
    let entries = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("key", key, false),
                Field::new("value", value, true),
            ]
            .into(),
        ),
        false,
    );
    DataType::Map(Arc::new(entries), false)
}

fn map(key: Expr, value: Expr, kinds: (DataType, DataType)) -> Expr {
    let keys = call(
        "make_array",
        vec![kinds.0.clone()],
        list_of(kinds.0.clone()),
        vec![key],
    );
    let values = call(
        "make_array",
        vec![kinds.1.clone()],
        list_of(kinds.1.clone()),
        vec![value],
    );
    call(
        "map",
        vec![list_of(kinds.0.clone()), list_of(kinds.1.clone())],
        map_of(kinds.0, kinds.1),
        vec![keys, values],
    )
}

fn keyed(key: Expr, value: Expr, kinds: (DataType, DataType)) -> Expr {
    let held = map(key.clone(), value, kinds.clone());
    let kind = map_of(kinds.0.clone(), kinds.1.clone());
    let wrapped = identity("__repark_spark_nonnull__", kind.clone(), kind.clone(), held);
    call(
        "element_at",
        vec![kind, kinds.0],
        kinds.1,
        vec![wrapped, key],
    )
}

fn part(name: &str, held: Expr, kinds: (DataType, DataType), index: usize) -> Expr {
    let chosen = [kinds.0.clone(), kinds.1.clone()][index].clone();
    let listed = call(
        name,
        vec![map_of(kinds.0, kinds.1)],
        list_of(chosen.clone()),
        vec![held],
    );
    call(
        "array_element",
        vec![list_of(chosen.clone()), DataType::Int64],
        chosen,
        vec![listed, lit(0_i64)],
    )
}

fn micros_of(value: Expr) -> Expr {
    call("unix_micros", vec![instant()], DataType::Int64, vec![value])
}

fn narrowed_beside_a_value() -> Expr {
    coalesce(vec![cut("m", NARROWED_BESIDE_VALUE_UDF_NAME), col("u")])
}

fn stated(value: Expr) -> Expr {
    identity(WRITTEN_CAST_UDF_NAME, instant(), instant(), value)
}

fn element(listed: Expr) -> Expr {
    call(
        "__repark_array_get__",
        vec![list_of(instant()), DataType::Int64],
        instant(),
        vec![listed, lit(0_i64)],
    )
}

fn chosen(name: &str, value: Expr) -> Expr {
    call(
        name,
        vec![instant(), instant()],
        instant(),
        vec![value, col("u")],
    )
}

fn tried(value: Expr, target: DataType) -> Expr {
    Expr::TryCast(TryCast::new(Box::new(value), target))
}

#[tokio::test]
async fn a_plain_value_stores_beside_a_narrowing_it_does_not_pass_through() {
    let ctx = session();
    let pair = (instant(), nanos());
    let span = call(
        "make_dt_interval",
        vec![DataType::Int64],
        DataType::Interval(datafusion::arrow::datatypes::IntervalUnit::MonthDayNano),
        vec![micros_of(narrowed("m"))],
    );
    let passed_by = [
        keyed(narrowed("m"), col("m"), pair.clone()),
        part(
            "map_values",
            map(narrowed("m"), col("m"), pair.clone()),
            pair,
            1,
        ),
        col("m") + (narrowed("m") - narrowed("m")),
        col("m") + span,
        call(
            "shifted",
            vec![nanos(), DataType::Int64],
            nanos(),
            vec![col("m"), micros_of(narrowed("m"))],
        ),
    ];
    for value in passed_by {
        let plan = plan_of(&ctx, value.clone()).await;
        assert_eq!(column_narrowed(&plan, 1, None), None, "{value}");
    }
}

#[tokio::test]
async fn a_narrowed_value_refuses_through_the_arguments_it_passes_through() {
    let ctx = session();
    let pair = (instant(), nanos());
    let turned = (nanos(), instant());
    let carried = [
        keyed(col("m"), narrowed("m"), turned.clone()),
        part(
            "map_keys",
            map(narrowed("m"), col("m"), pair.clone()),
            pair.clone(),
            0,
        ),
        part(
            "map_values",
            map(col("m"), narrowed("m"), turned.clone()),
            turned,
            1,
        ),
        call(
            "merged",
            vec![map_of(instant(), nanos())],
            nanos(),
            vec![map(narrowed("m"), col("m"), pair)],
        ),
        narrowed("m") + (col("u") - col("u")),
        call(
            "timestamp_micros",
            vec![DataType::Int64],
            instant(),
            vec![micros_of(narrowed("m"))],
        ),
        call(
            "shifted",
            vec![instant(), DataType::Int64],
            instant(),
            vec![narrowed("m"), lit(0_i64)],
        ),
        call(
            "named",
            vec![DataType::Utf8, DataType::Int64],
            DataType::Utf8,
            vec![lit("a"), micros_of(narrowed("m"))],
        ),
    ];
    for value in carried {
        let plan = plan_of(&ctx, value.clone()).await;
        assert!(column_narrowed(&plan, 1, None).is_some(), "{value}");
    }
}

#[tokio::test]
async fn a_written_call_reaches_the_mark_through_what_hands_the_value_on_unchanged() {
    let ctx = session();
    let cut = narrowed_beside_a_value;
    let array = |value: Expr| {
        call(
            "make_array",
            vec![instant(), instant()],
            list_of(instant()),
            vec![value, col("u")],
        )
    };
    let idle = cast(
        identity(NARROWED_BESIDE_VALUE_UDF_NAME, instant(), instant(), cut()),
        instant(),
    );
    let handed_on = [
        cut(),
        chosen("greatest", cut()),
        chosen("least", cut()),
        call(
            "if",
            vec![DataType::Boolean, instant(), instant()],
            instant(),
            vec![col("id").gt(lit(0)), cut(), col("u")],
        ),
        call(
            "nvl2",
            vec![instant(), instant(), instant()],
            instant(),
            vec![col("u"), cut(), col("u")],
        ),
        chosen("nullif", cut()),
        element(array(cut())),
        element(cast(array(cut()), list_of(instant()))),
        idle,
    ];
    for value in handed_on {
        let bare = plan_of(&ctx, value.clone()).await;
        assert!(column_narrowed(&bare, 1, None).is_some(), "{value}");
        let forgiven = plan_of(&ctx, stated(value.clone())).await;
        assert_eq!(column_narrowed(&forgiven, 1, None), None, "{value}");
        let kernel = identity(NARROW_TIMESTAMP_NS_UDF_NAME, instant(), instant(), value);
        let forgiven = plan_of(&ctx, kernel.clone()).await;
        assert_eq!(column_narrowed(&forgiven, 1, None), None, "{kernel}");
    }
    let stated_twice = plan_of(&ctx, tried(cut(), instant())).await;
    assert_eq!(column_narrowed(&stated_twice, 1, None), None);
    let grouped = |value: Expr| {
        let ctx = &ctx;
        async move {
            ctx.table("src")
                .await
                .unwrap()
                .aggregate(vec![col("id")], vec![value.alias("v")])
                .unwrap()
                .select(vec![col("id"), stated(col("v")).alias("w")])
                .unwrap()
                .into_unoptimized_plan()
        }
    };
    let greatest = grouped(max(cut())).await;
    assert_eq!(column_narrowed(&greatest, 1, None), None, "{greatest}");
    let counted = grouped(cast(count(cut()), instant())).await;
    assert!(column_narrowed(&counted, 1, None).is_some(), "{counted}");
}

#[tokio::test]
async fn a_written_call_does_not_reach_the_mark_through_a_call_that_changes_the_value() {
    let ctx = session();
    let cut = narrowed_beside_a_value;
    let changed = [
        chosen("shifted", cut()),
        cut() + lit(ScalarValue::new_interval_dt(0, 0)),
        cast(cast(cut(), DataType::Utf8), instant()),
        cast(tried(cut(), DataType::Utf8), instant()),
    ];
    for value in changed {
        let refused = plan_of(&ctx, stated(value.clone())).await;
        assert!(column_narrowed(&refused, 1, None).is_some(), "{value}");
    }
    let compared = plan_of(&ctx, chosen("nullif", col("u")).eq(cut())).await;
    assert_eq!(column_narrowed(&compared, 1, None), None);
    let root = datafusion::logical_expr::LogicalPlanBuilder::from(plan_of(&ctx, cut()).await)
        .project(vec![col("id"), stated(col("v")).alias("v")])
        .unwrap()
        .build()
        .unwrap();
    let id = DataType::Int32;
    let pairs = [("id", &id), ("v", &nanos())];
    assert!(refuse_narrowed_ns_inserts("`t`", &root, pairs, None).is_err());
    assert!(refuse_narrowed_ns_columns("`t`", &root, pairs, None).is_ok());
}

#[tokio::test]
async fn a_mark_over_a_value_that_holds_no_nanoseconds_cuts_nothing() {
    let ctx = session();
    let made = datafusion::functions_nested::expr_fn::make_array;
    let marked = |held: Expr, kind: DataType| {
        let mark = identity(NARROWED_BESIDE_VALUE_UDF_NAME, kind.clone(), kind, held);
        element(cast(mark, list_of(instant())))
    };
    let settled = plan_of(&ctx, marked(made(vec![col("u")]), list_of(instant()))).await;
    assert_eq!(column_narrowed(&settled, 1, None), None, "{settled}");
    let inside = marked(made(vec![tried(col("m"), nanos())]), list_of(nanos()));
    let stated_inside = plan_of(&ctx, inside).await;
    assert_eq!(column_narrowed(&stated_inside, 1, None), None);
    let whole = plan_of(&ctx, marked(made(vec![col("m")]), list_of(nanos()))).await;
    assert!(column_narrowed(&whole, 1, None).is_some(), "{whole}");
    let fixed = identity(
        NARROWED_BESIDE_VALUE_UDF_NAME,
        list_of(nanos()),
        list_of(nanos()),
        listed(col("m"), nanos()),
    );
    let container = plan_of(&ctx, stated(element(cast(fixed, list_of(instant()))))).await;
    assert!(column_narrowed(&container, 1, None).is_some());
    let scalar = cast(
        identity(
            NARROWED_BESIDE_VALUE_UDF_NAME,
            instant(),
            instant(),
            col("u"),
        ),
        instant(),
    );
    let idle = plan_of(&ctx, scalar).await;
    assert_eq!(column_narrowed(&idle, 1, None), None);
    let over = |kind: DataType| {
        let inner = narrowed_beside_a_value();
        let mark = identity(NARROWED_BESIDE_VALUE_UDF_NAME, instant(), instant(), inner);
        stated(cast(cast(mark, kind), instant()))
    };
    let same_kind = plan_of(&ctx, over(instant())).await;
    assert_eq!(column_narrowed(&same_kind, 1, None), None);
    let naive = DataType::Timestamp(datafusion::arrow::datatypes::TimeUnit::Microsecond, None);
    let turned = plan_of(&ctx, over(naive)).await;
    assert!(column_narrowed(&turned, 1, None).is_some());
    let above = |held: Expr| {
        let ctx = &ctx;
        async move {
            let mark = identity(NARROWED_BESIDE_VALUE_UDF_NAME, nanos(), nanos(), col("w"));
            ctx.table("src")
                .await
                .unwrap()
                .select(vec![col("id"), held.alias("w")])
                .unwrap()
                .select(vec![col("id"), cast(mark, instant()).alias("v")])
                .unwrap()
                .into_unoptimized_plan()
        }
    };
    let stated_below = above(tried(col("m"), nanos())).await;
    assert_eq!(column_narrowed(&stated_below, 1, None), None);
    let plain_below = above(col("m")).await;
    assert!(column_narrowed(&plain_below, 1, None).is_some());
}

#[tokio::test]
async fn an_update_reads_the_cast_at_the_root_of_its_scalar_subquery_as_the_planners() {
    let ctx = session();
    let cut = narrowed_beside_a_value;
    let scalar = |held: Expr, value: Expr| {
        let ctx = &ctx;
        async move {
            let inner = ctx
                .table("src")
                .await
                .unwrap()
                .aggregate(vec![], vec![max(held).alias("w")])
                .unwrap()
                .select(vec![value.alias("w")])
                .unwrap()
                .into_unoptimized_plan();
            Expr::ScalarSubquery(datafusion::logical_expr::Subquery {
                subquery: Arc::new(inner),
                outer_ref_columns: Vec::new(),
                spans: datafusion::common::Spans::new(),
            })
        }
    };
    let id = DataType::Int32;
    let pairs = [("id", &id), ("v", &nanos())];
    let conformed = plan_of(&ctx, scalar(cut(), stated(col("w"))).await).await;
    assert!(refuse_narrowed_ns_updates("`t`", &conformed, pairs, None).is_err());
    assert!(refuse_narrowed_ns_columns("`t`", &conformed, pairs, None).is_ok());
    let outside = plan_of(&ctx, stated(scalar(cut(), col("w")).await)).await;
    assert!(refuse_narrowed_ns_updates("`t`", &outside, pairs, None).is_err());
    let twice = plan_of(&ctx, scalar(cut(), stated(stated(col("w")))).await).await;
    assert!(refuse_narrowed_ns_updates("`t`", &twice, pairs, None).is_ok());
    let whole = plan_of(&ctx, scalar(col("u"), stated(col("w"))).await).await;
    assert!(refuse_narrowed_ns_updates("`t`", &whole, pairs, None).is_ok());
    let written = plan_of(&ctx, stated(cut())).await;
    assert!(refuse_narrowed_ns_updates("`t`", &written, pairs, None).is_ok());
}

#[tokio::test]
async fn every_row_of_values_is_followed() {
    let rows = |cells: Vec<Expr>| {
        let rows = cells.into_iter().map(|cell| vec![lit(1), cell]).collect();
        datafusion::logical_expr::LogicalPlanBuilder::values(rows)
            .unwrap()
            .build()
            .unwrap()
    };
    let literal = || {
        lit(ScalarValue::TimestampMicrosecond(
            Some(1),
            Some(Arc::from("UTC")),
        ))
    };
    let marked = || {
        coalesce(vec![
            cast(
                identity(
                    NARROWED_BESIDE_VALUE_UDF_NAME,
                    nanos(),
                    nanos(),
                    lit(ScalarValue::TimestampNanosecond(Some(1), None)),
                ),
                instant(),
            ),
            literal(),
        ])
    };
    for position in 0..3 {
        let mut cells = vec![literal(), literal(), literal()];
        cells[position] = marked();
        let plan = rows(cells);
        assert_eq!(column_narrowed(&plan, 0, None), None, "{position}");
        assert!(column_narrowed(&plan, 1, None).is_some(), "{position}");
    }
    let plain = rows(vec![literal(), literal()]);
    assert_eq!(column_narrowed(&plain, 1, None), None);
}

#[test]
fn the_text_names_a_plain_column_and_no_generated_relation() {
    let narrowing = |value: Option<&str>| Narrowing {
        value: value.map(str::to_string),
        element_of: None,
        beside_untyped_null: false,
        zoned: false,
    };
    let named = narrowed_refusal("`ice.ns.t`", "v", false, &narrowing(Some("src.m"))).to_string();
    assert!(
        named.contains("the table `ice.ns.t`: Cannot safely cast `v`"),
        "{named}"
    );
    assert!(named.contains("The value src.m was narrowed"), "{named}");
    assert!(
        named.contains("write CAST(src.m AS TIMESTAMP) if"),
        "{named}"
    );
    let unnamed = narrowed_refusal("`ice.ns.t`", "v", true, &narrowing(None)).to_string();
    assert!(
        unnamed.contains("\"TIMESTAMP\" to \"TIMESTAMPTZ_NS\""),
        "{unnamed}"
    );
    assert!(
        unnamed.contains(
            "The value was narrowed from nanoseconds to microseconds before the store, to \
             match the microsecond value beside it; write a CAST to TIMESTAMP over the \
             nanosecond value if"
        ),
        "{unnamed}"
    );
    let column = |relation: Option<&str>, name: &str| {
        Expr::Column(datafusion::common::Column::new(relation, name))
    };
    assert_eq!(
        super::text::named(&column(Some("src"), "m")).as_deref(),
        Some("src.m")
    );
    assert_eq!(
        super::text::named(&column(None, "m").alias("v")).as_deref(),
        Some("m")
    );
    let generated = column(Some("__repark_selx_1"), "m");
    assert_eq!(super::text::named(&generated).as_deref(), Some("m"));
    for unnamed in [
        column(None, "max(src.m) FILTER (WHERE src.id > Int64(0))"),
        column(Some("a"), "__repark_l_1_m"),
        column(None, ""),
        lit(1),
        coalesce(vec![col("m"), col("u")]),
    ] {
        assert_eq!(super::text::named(&unnamed), None, "{unnamed}");
    }
    for generated in [
        "`datafusion.public.__repark_tt_1`",
        "`__repark_tt_7`",
        "`_repark_jl_ab12`",
    ] {
        let text = narrowed_refusal(generated, "v", false, &narrowing(None)).to_string();
        assert!(text.contains("the table ``: Cannot safely cast"), "{text}");
        assert!(!text.contains("repark"), "{text}");
    }
}
