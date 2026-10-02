use super::*;

#[test]
fn expr_sql_substr_zero_matches_spark() {
    let column = PyColumn::sql("substr('hello', 0, 3)", false).expect("parse");
    let context = datafusion::prelude::SessionContext::new();
    repark_functions::register_all(&context);
    for rule in repark_functions::analyzer_rules() {
        context.add_analyzer_rule(rule);
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let batches = runtime.block_on(async {
        let df = context.sql("SELECT 1 AS dummy").await.unwrap();
        let df = df.select(vec![column.expr().alias("s")]).unwrap();
        df.collect().await.unwrap()
    });
    let pretty = arrow::util::pretty::pretty_format_batches(&batches)
        .unwrap()
        .to_string();
    assert!(
        pretty.contains("hel"),
        "expected Spark substr pos0 → hel, got:\n{pretty}\nexpr={:?}",
        column.expr()
    );
}

#[test]
fn call_scalar_substr_zero_matches_spark() {
    use datafusion::arrow::array::StringArray;

    let string_col = PyColumn::from_expr(lit("hello"));
    let start = PyColumn::from_expr(lit(0_i64));
    let length = PyColumn::from_expr(lit(3_i64));
    let column = PyColumn::call_scalar("substr", vec![string_col, start, length])
        .expect("call_scalar substr");
    let context = datafusion::prelude::SessionContext::new();
    repark_functions::register_all(&context);
    for rule in repark_functions::analyzer_rules() {
        context.add_analyzer_rule(rule);
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let batches = runtime.block_on(async {
        let df = context.sql("SELECT 1 AS dummy").await.unwrap();
        let df = df.select(vec![column.expr().alias("s")]).unwrap();
        df.collect().await.unwrap()
    });
    let array = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8 column");
    assert_eq!(
        array.value(0),
        "hel",
        "call_scalar substr pos0 len3 must be Spark 'hel' (not DF 'he'); expr={:?}",
        column.expr()
    );
    let neg = PyColumn::call_scalar(
        "substr",
        vec![
            PyColumn::from_expr(lit("hello")),
            PyColumn::from_expr(lit(-3_i64)),
            PyColumn::from_expr(lit(2_i64)),
        ],
    )
    .expect("call_scalar substr neg");
    let batches_neg = runtime.block_on(async {
        let df = context.sql("SELECT 1 AS dummy").await.unwrap();
        let df = df.select(vec![neg.expr().alias("s")]).unwrap();
        df.collect().await.unwrap()
    });
    let array_neg = batches_neg[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("utf8 column");
    assert_eq!(array_neg.value(0), "ll");
}

#[test]
fn expr_sql_integer_division_hands_off_float64() {
    use datafusion::arrow::array::Float64Array;

    let column = PyColumn::sql("5/2", false).expect("parse");
    let context = datafusion::prelude::SessionContext::new();
    repark_functions::register_all(&context);
    for rule in repark_functions::analyzer_rules() {
        context.add_analyzer_rule(rule);
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (logical_type, batches) = runtime.block_on(async {
        let df = context.sql("SELECT 1 AS dummy").await.unwrap();
        let df = df.select(vec![column.expr().alias("x")]).unwrap();
        let logical_type = df.schema().field(0).data_type().clone();
        (logical_type, df.collect().await.unwrap())
    });
    assert_eq!(
        logical_type,
        DataType::Float64,
        "F.expr('5/2') must hand off Float64 — an Int64 label over Float64 buffers \
         bit-reinterprets at the Arrow boundary"
    );
    let executed_type = batches[0].schema().field(0).data_type().clone();
    assert_eq!(
        executed_type, logical_type,
        "logical (exported) schema and executed batch schema must agree"
    );
    let values = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("executed column must be Float64");
    assert!((values.value(0) - 2.5).abs() < f64::EPSILON);
}

fn assert_pure_levels(column: &PyColumn, label: &str) {
    let (expr, plan) = column.grown_read(crate::deep_stack::survey_expression);
    assert_eq!(
        column.expression_depth(),
        expr,
        "{label}: cached expr depth"
    );
    assert_eq!(
        column.df_depth(),
        expr,
        "{label}: pure shapes count every level"
    );
    assert_eq!(column.plan_depth(), plan, "{label}: cached plan depth");
}

#[test]
fn cached_column_levels_match_a_fresh_survey() {
    let left = PyColumn::column("a").expect("a column builds");
    let right = PyColumn::column("b").expect("a column builds");
    assert_pure_levels(&left, "column leaf");
    for (label, built) in [
        ("add", left.add(&right)),
        ("sub", left.sub(&right)),
        ("mul", left.mul(&right)),
        ("div", left.div(&right)),
        ("modulo", left.modulo(&right)),
        ("eq", left.eq(&right)),
        ("ne", left.ne(&right)),
        ("lt", left.lt(&right)),
        ("gt", left.gt(&right)),
        ("le", left.le(&right)),
        ("ge", left.ge(&right)),
        ("and", left.and_(&right)),
        ("or", left.or_(&right)),
    ] {
        assert_pure_levels(&built.expect("a binary op builds"), label);
    }
    let mut chained = PyColumn::column("a").expect("a column builds");
    for index in 0..25 {
        let term = PyColumn::column("a")
            .expect("a column builds")
            .eq(&PyColumn::column("b").expect("a column builds"))
            .expect("an eq builds");
        chained = chained.or_(&term).expect("an or builds");
        assert_pure_levels(&chained, &format!("or chain {index}"));
    }
}

#[test]
fn cached_column_op_levels_match_a_fresh_survey() {
    let left = PyColumn::column("a").expect("a column builds");
    let right = PyColumn::column("b").expect("a column builds");
    for (label, built) in [
        ("not", left.not_()),
        ("alias", left.alias("x")),
        ("cast", left.cast("int")),
        ("decimal cast", left.cast("decimal(10, 2)")),
        ("try cast", left.try_cast("string")),
        ("year", left.year()),
        ("month", left.month()),
        ("day", left.dayofmonth()),
        ("date format", left.date_format("yyyy")),
        ("trunc", left.trunc("month")),
        ("date trunc", left.date_trunc("month")),
        ("is null", left.is_null()),
        ("is not null", left.is_not_null()),
        ("add months", left.add_months(&right)),
        ("date add", left.date_add(&right)),
        (
            "coalesce",
            PyColumn::coalesce(vec![left.clone(), right.clone()]),
        ),
        (
            "concat",
            PyColumn::concat(vec![left.clone(), right.clone()]),
        ),
        ("upper", PyColumn::call_scalar("upper", vec![left.clone()])),
        ("sum", left.aggregate("sum", false)),
        ("collect list", left.aggregate("collect_list", false)),
        ("corr", left.aggregate_binary("corr", vec![right.clone()])),
        ("percentile", left.approx_percentile_cont(0.5, None)),
        (
            "count",
            PyColumn::count_aggregate(vec![left.clone()], false),
        ),
        (
            "count distinct",
            PyColumn::count_aggregate(vec![left.clone()], true),
        ),
        ("lag", PyColumn::lag(vec![left.clone()])),
        ("lead", PyColumn::lead(vec![left.clone()])),
        ("ntile", PyColumn::ntile(4)),
        (
            "struct",
            PyColumn::make_struct(vec![
                left.alias("f").expect("an alias builds"),
                right.clone(),
            ]),
        ),
        (
            "case when",
            PyColumn::case_when(vec![(left.clone(), right.clone())], Some(right.clone())),
        ),
        (
            "transform",
            PyColumn::call_higher_order(
                "transform",
                vec![left.clone()],
                vec![(vec!["x".to_string()], right.clone())],
            ),
        ),
        (
            "grouping id",
            super::expr_build::grouping_id_column(vec![left.clone(), right.clone()]),
        ),
    ] {
        assert_pure_levels(&built.expect("the op builds"), label);
    }
    let collapsed = left
        .alias("a")
        .expect("an alias builds")
        .collapse_identity_aliases()
        .expect("an identity alias collapses");
    assert_pure_levels(&collapsed, "collapsed alias");
    let windowed = PyColumn::row_number()
        .expect("row_number builds")
        .over(
            vec![],
            vec![left.clone()],
            vec![true],
            vec![true],
            None,
            None,
            None,
        )
        .expect("an ordered over builds");
    assert_pure_levels(&windowed, "over");
}

#[test]
fn sql_text_levels_count_no_df_depth() {
    let left = PyColumn::column("a").expect("a column builds");
    let right = PyColumn::column("b").expect("a column builds");
    let text = PyColumn::sql("a = 1 OR a = 2", false).expect("sql text parses");
    let (text_expr, text_plan) = text.grown_read(crate::deep_stack::survey_expression);
    assert_eq!(text.expression_depth(), text_expr);
    assert_eq!(text.df_depth(), 0, "sql text counts no df levels");
    assert_eq!(text.plan_depth(), text_plan);
    let mixed = text
        .or_(&left.eq(&right).expect("an eq builds"))
        .expect("a mixed or builds");
    let (mixed_expr, mixed_plan) = mixed.grown_read(crate::deep_stack::survey_expression);
    assert_eq!(mixed.expression_depth(), mixed_expr);
    assert_eq!(
        mixed.df_depth(),
        3,
        "one df level over sql text and a leaf pair"
    );
    assert_eq!(mixed.plan_depth(), mixed_plan);
    let wrapped =
        PyColumn::call_scalar("upper", vec![text.clone()]).expect("upper over sql builds");
    assert_eq!(wrapped.df_depth(), 1, "one df level over sql text");
    let structured = PyColumn::make_struct(vec![text.clone()]).expect("a struct over sql builds");
    assert_eq!(
        structured.df_depth(),
        1,
        "the struct wrapper adds one df level"
    );
    let merged =
        PyColumn::coalesce(vec![text.clone(), left.clone()]).expect("a coalesce over sql builds");
    assert_eq!(merged.df_depth(), 2, "the leaf pair sets the df depth");
    let summed = text
        .aggregate("sum", false)
        .expect("an aggregate over sql builds");
    assert_eq!(summed.df_depth(), 1, "one df level over sql text");
}
