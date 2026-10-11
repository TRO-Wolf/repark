use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::TreeNode;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::functions::datetime::expr_fn::date_trunc;
use datafusion::logical_expr::{col, lit, table_scan, when};

use super::*;

fn nanos() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

fn micros() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

fn arrow() -> Schema {
    Schema::new(vec![
        Field::new("c", nanos(), true),
        Field::new("m", micros(), true),
        Field::new("id", DataType::Int32, true),
    ])
}

fn schema() -> DFSchema {
    DFSchema::try_from(arrow()).unwrap()
}

fn untyped() -> Expr {
    Expr::Literal(ScalarValue::Null, None)
}

fn cast(expr: Expr, target: DataType) -> Expr {
    Expr::Cast(Cast::new(Box::new(expr), target))
}

fn widened(expr: Expr) -> Expr {
    expr.transform_up(|node| {
        Ok(
            if matches!(&node, Expr::Literal(value, _) if value.is_null()) {
                Transformed::yes(cast(cast(node, nanos()), micros()))
            } else {
                Transformed::no(node)
            },
        )
    })
    .unwrap()
    .data
}

fn carries_tag(expr: &Expr) -> bool {
    expr.exists(|node| {
        Ok(matches!(node, Expr::Literal(_, Some(metadata))
            if metadata.inner().contains_key(UNTYPED_NULL_TAG)))
    })
    .unwrap()
}

fn marks(expr: &Expr) -> Vec<String> {
    let mut found = Vec::new();
    expr.apply(|node| {
        if let Expr::ScalarFunction(function) = node
            && is_narrowing_mark(node)
        {
            found.push(format!("{}({})", function.func.name(), function.args[0]));
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    found
}

fn settled(expr: Expr, scoped: bool) -> Expr {
    let tagged = before_coercion(expr, &schema()).unwrap().data;
    settle_branches(widened(tagged), &schema(), scoped)
        .unwrap()
        .data
}

#[test]
fn an_untyped_null_beside_a_nanosecond_branch_is_tagged_and_no_other_null() {
    for (expr, tagged) in [
        (coalesce(vec![col("c"), untyped()]), true),
        (coalesce(vec![untyped(), col("c")]), true),
        (
            when(col("id").gt(col("id")), col("c"))
                .otherwise(untyped())
                .unwrap(),
            true,
        ),
        (coalesce(vec![col("m"), untyped()]), false),
        (coalesce(vec![col("id"), untyped()]), false),
        (coalesce(vec![col("c"), cast(untyped(), nanos())]), false),
        (coalesce(vec![col("c"), col("c")]), false),
    ] {
        let out = before_coercion(expr.clone(), &schema()).unwrap();
        assert_eq!(out.transformed, tagged, "{expr}");
        assert_eq!(carries_tag(&out.data), tagged, "{expr}");
    }
}

#[test]
fn the_nanosecond_branch_beside_a_widened_untyped_null_is_marked_as_beside_a_null() {
    for scoped in [true, false] {
        let out = settled(coalesce(vec![col("c"), untyped()]), scoped);
        assert_eq!(marks(&out), vec![format!("{NARROWED_BESIDE_NULL_NAME}(c)")]);
        assert!(!carries_tag(&out), "{out}");
    }
}

#[test]
fn the_nanosecond_branch_beside_a_typed_null_or_a_microsecond_value_is_marked_as_beside_a_value() {
    let typed = coalesce(vec![col("c"), cast(cast(untyped(), nanos()), micros())]);
    let value = coalesce(vec![col("c"), col("m")]);
    let both = when(col("id").gt(col("id")), col("c"))
        .otherwise(col("m"))
        .unwrap();
    for expr in [typed, value, both] {
        let out = settle_branches(expr.clone(), &schema(), true).unwrap().data;
        assert_eq!(
            marks(&out),
            vec![format!("{NARROWED_BESIDE_VALUE_NAME}(c)")],
            "{expr}"
        );
        let unasked = settle_branches(expr.clone(), &schema(), false).unwrap();
        assert!(!unasked.transformed, "{expr}");
        let again = settle_branches(out.clone(), &schema(), true).unwrap().data;
        assert_eq!(again, out, "{expr}");
    }
}

#[test]
fn a_branch_the_statement_narrowed_and_a_node_with_one_kind_are_not_marked() {
    let written = coalesce(vec![cast(col("c"), micros()), col("m")]);
    let nanoseconds = coalesce(vec![col("c"), col("c")]);
    let microseconds = coalesce(vec![col("m"), col("m")]);
    for expr in [written, nanoseconds, microseconds] {
        let out = settle_branches(expr.clone(), &schema(), true).unwrap();
        assert!(!out.transformed, "{expr}");
    }
    let tagged = before_coercion(coalesce(vec![col("c"), untyped()]), &schema())
        .unwrap()
        .data;
    let unwidened = settle_branches(tagged, &schema(), true).unwrap().data;
    assert_eq!(unwidened, coalesce(vec![col("c"), untyped()]));
}

#[test]
fn a_cast_from_nanoseconds_to_an_instant_before_coercion_is_the_statements() {
    let written = before_coercion(cast(col("c"), micros()), &schema())
        .unwrap()
        .data;
    assert_eq!(
        written,
        crate::timestamp_ns_cast::narrow_timestamp_ns_expr(col("c"))
    );
    assert!(marks(&written).is_empty());
    let kept = cast(col("m"), micros());
    assert!(!before_coercion(kept, &schema()).unwrap().transformed);
}

#[test]
fn a_cast_coercion_left_over_a_nanosecond_value_is_marked() {
    let naive = DataType::Timestamp(TimeUnit::Microsecond, None);
    for target in [micros(), naive] {
        let out = mark_coerced_narrowing(cast(col("c"), target.clone()), &schema(), true);
        assert!(out.transformed, "{target}");
        assert_eq!(
            marks(&out.data),
            vec![format!("{NARROWED_BESIDE_VALUE_NAME}(c)")]
        );
        let again = mark_coerced_narrowing(out.data.clone(), &schema(), true);
        assert!(!again.transformed, "{target}");
        let narrowed = cast(col("c"), target.clone());
        let named = mark_coerced_narrowing(narrowed.clone().alias("v"), &schema(), true);
        assert_eq!(named.data, out.data.clone().alias("v"));
        let unified = [
            coalesce(vec![narrowed.clone(), col("m")]),
            coalesce(vec![
                narrowed.clone(),
                cast(cast(untyped(), nanos()), micros()),
            ]),
            when(col("id").gt(col("id")), narrowed.clone())
                .otherwise(col("m"))
                .unwrap(),
        ];
        for expr in unified {
            let out = mark_coerced_branches(expr.clone(), &schema()).unwrap();
            assert_eq!(
                marks(&out.data),
                vec![format!("{NARROWED_BESIDE_VALUE_NAME}(c)")],
                "{expr}"
            );
        }
        let fitted = [
            date_trunc(lit("second"), narrowed.clone()),
            coalesce(vec![narrowed.clone(), narrowed.clone()]),
            narrowed.clone().is_null(),
        ];
        for expr in fitted {
            let out = mark_coerced_branches(expr.clone(), &schema()).unwrap();
            assert!(!out.transformed, "{expr}");
        }
    }
    for kept in [
        cast(col("c"), nanos()),
        cast(col("m"), micros()),
        cast(cast(untyped(), nanos()), micros()),
        cast(col("c"), DataType::Utf8),
    ] {
        assert!(
            !mark_coerced_narrowing(kept.clone(), &schema(), true).transformed,
            "{kept}"
        );
    }
}

#[test]
fn the_session_rule_marks_a_cast_coercion_left_at_the_root_of_a_projection() {
    let narrowed = table_scan(Some("t"), &arrow(), None)
        .unwrap()
        .project(vec![col("id"), cast(col("c"), micros()).alias("v")])
        .unwrap()
        .build()
        .unwrap();
    let analyzed = crate::instant_ts::ltz_timestamp_cast_rule()
        .analyze(narrowed, &ConfigOptions::new())
        .unwrap();
    let LogicalPlan::Projection(projection) = analyzed else {
        panic!("a projection");
    };
    assert_eq!(
        marks(&projection.expr[1]),
        vec![format!("{NARROWED_BESIDE_VALUE_NAME}(t.c)")]
    );
    assert!(marks(&projection.expr[0]).is_empty());
}

#[test]
fn the_marker_is_its_argument_and_the_optimizer_drops_it() {
    for untyped_null in [true, false] {
        let udf = narrowed_beside_udf(untyped_null);
        let argument = Arc::new(Field::new("x", micros(), false));
        let returned = udf
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&argument)],
                scalar_arguments: &[None],
            })
            .unwrap();
        assert_eq!(returned.data_type(), &micros());
        assert!(!returned.is_nullable());
        let simplified = udf
            .simplify(vec![col("m")], &SimplifyContext::default())
            .unwrap();
        assert!(matches!(simplified, ExprSimplifyResult::Simplified(expr) if expr == col("m")));
    }
}

fn union_of(value: Expr, absent: Expr) -> LogicalPlan {
    let scan = || table_scan(Some("t"), &arrow(), None).unwrap();
    let rows = scan()
        .project(vec![col("id"), value])
        .unwrap()
        .build()
        .unwrap();
    let nulls = scan()
        .project(vec![col("id"), absent])
        .unwrap()
        .build()
        .unwrap();
    let schema = Arc::clone(rows.schema());
    LogicalPlan::Union(Union {
        inputs: vec![Arc::new(rows), Arc::new(nulls)],
        schema,
    })
}

fn branch(plan: &LogicalPlan, input: usize) -> Expr {
    let LogicalPlan::Union(union) = plan else {
        unreachable!()
    };
    let LogicalPlan::Projection(projection) = union.inputs[input].as_ref() else {
        unreachable!()
    };
    projection.expr[1].clone()
}

fn widened_branch(plan: LogicalPlan) -> LogicalPlan {
    let LogicalPlan::Union(union) = plan else {
        unreachable!()
    };
    let LogicalPlan::Projection(projection) = union.inputs[1].as_ref() else {
        unreachable!()
    };
    let exprs = projection.expr.iter().cloned().map(widened).collect();
    let rebuilt = Projection::try_new(exprs, Arc::clone(&projection.input)).unwrap();
    LogicalPlan::Union(Union {
        inputs: vec![
            Arc::clone(&union.inputs[0]),
            Arc::new(LogicalPlan::Projection(rebuilt)),
        ],
        schema: union.schema,
    })
}

#[test]
fn the_nanosecond_branch_of_a_union_beside_an_untyped_null_branch_is_marked() {
    for absent in [untyped().alias("v"), untyped()] {
        let tagged = tag_union_nulls(union_of(col("c").alias("v"), absent.clone())).unwrap();
        assert!(tagged.transformed, "{absent}");
        assert!(carries_tag(&branch(&tagged.data, 1)), "{absent}");
        let settled = settle_union_branches(widened_branch(tagged.data)).unwrap();
        assert!(!carries_tag(&branch(&settled.data, 1)));
        let rows = branch(&settled.data, 0);
        assert_eq!(
            marks(&rows),
            vec![format!("{NARROWED_BESIDE_NULL_NAME}(t.c)")]
        );
        assert_eq!(rows.schema_name().to_string(), "v");
        assert_eq!(
            branch(&settled.data, 1).schema_name().to_string(),
            absent.schema_name().to_string()
        );
    }
}

#[test]
fn the_nanosecond_branch_of_a_union_beside_a_microsecond_branch_is_marked() {
    let plan = union_of(col("c").alias("v"), col("m").alias("v"));
    assert!(!tag_union_nulls(plan.clone()).unwrap().transformed);
    let settled = settle_union_branches(plan).unwrap().data;
    assert_eq!(
        marks(&branch(&settled, 0)),
        vec![format!("{NARROWED_BESIDE_VALUE_NAME}(t.c)")]
    );
    assert!(marks(&branch(&settled, 1)).is_empty());
    let again = settle_union_branches(settled.clone()).unwrap();
    assert!(!again.transformed);
}

#[test]
fn a_union_of_one_kind_is_left_alone() {
    for (value, absent) in [
        (col("m").alias("v"), untyped().alias("v")),
        (col("c").alias("v"), cast(untyped(), nanos()).alias("v")),
        (col("c").alias("v"), col("c").alias("v")),
        (col("m").alias("v"), col("m").alias("v")),
    ] {
        let plan = union_of(value, absent);
        assert!(!tag_union_nulls(plan.clone()).unwrap().transformed);
        assert!(!settle_union_branches(plan).unwrap().transformed);
    }
}

fn zoned() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC")))
}

fn list(element: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", element, true)))
}

fn wide() -> DFSchema {
    DFSchema::try_from(Schema::new(vec![
        Field::new("c", nanos(), true),
        Field::new("m", micros(), true),
        Field::new("z", zoned(), true),
        Field::new("a", list(nanos()), true),
        Field::new("b", list(micros()), true),
        Field::new("d", DataType::Date32, true),
        Field::new("id", DataType::Int32, true),
    ]))
    .unwrap()
}

fn call(name: &str, arguments: Vec<(Expr, DataType)>, returned: DataType) -> Expr {
    let (values, kinds): (Vec<Expr>, Vec<DataType>) = arguments.into_iter().unzip();
    datafusion::logical_expr::create_udf(
        name,
        kinds,
        returned,
        Volatility::Immutable,
        Arc::new(|args: &[ColumnarValue]| Ok(args[0].clone())),
    )
    .call(values)
}

fn written(value: Expr) -> Expr {
    crate::timestamp_ns_cast::narrow_timestamp_ns_expr(value)
}

fn session_rule(plan: LogicalPlan) -> LogicalPlan {
    crate::instant_ts::ltz_timestamp_cast_rule()
        .analyze(plan, &ConfigOptions::new())
        .unwrap()
}

fn all_marks(plan: &LogicalPlan) -> Vec<String> {
    let mut found = Vec::new();
    plan.apply(|node| {
        node.apply_expressions(|expr| {
            found.extend(marks(expr));
            Ok(TreeNodeRecursion::Continue)
        })
    })
    .unwrap();
    found
}

#[test]
fn a_cast_to_the_sql_timestamp_type_before_coercion_is_the_statements() {
    for source in ["c", "z"] {
        let out = before_coercion(cast(col(source), nanos()), &wide()).unwrap();
        assert_eq!(out.data, written(col(source)), "{source}");
    }
    for kept in [cast(col("m"), nanos()), cast(col("d"), nanos())] {
        assert!(
            !before_coercion(kept.clone(), &wide()).unwrap().transformed,
            "{kept}"
        );
    }
    let beside = coalesce(vec![cast(written(col("c")), micros()), col("m")]);
    assert!(!mark_coerced_branches(beside, &wide()).unwrap().transformed);
    let rows = datafusion::logical_expr::LogicalPlanBuilder::values(vec![vec![
        written(lit(ScalarValue::TimestampNanosecond(Some(1), None))),
        coalesce(vec![
            written(lit(ScalarValue::TimestampNanosecond(Some(1), None))),
            lit(ScalarValue::TimestampMicrosecond(
                Some(1),
                Some(Arc::from("UTC")),
            )),
        ]),
    ]])
    .unwrap()
    .build()
    .unwrap();
    let LogicalPlan::Values(kept) = keep_values_roots(rows).unwrap().data else {
        panic!("a values node");
    };
    assert!(
        matches!(&kept.values[0][0], Expr::Cast(cast) if sql_timestamp(cast.field.data_type())),
        "{}",
        kept.values[0][0]
    );
    assert!(
        kept.values[0][1]
            .to_string()
            .contains(crate::timestamp_ns_cast::NARROW_TIMESTAMP_NS_NAME),
        "{}",
        kept.values[0][1]
    );
}

#[test]
fn a_cast_coercion_left_inside_a_container_or_to_the_wire_type_is_marked() {
    let schema = wide();
    let lists = coalesce(vec![cast(col("a"), list(micros())), col("b")]);
    let out = mark_coerced_branches(lists, &schema).unwrap().data;
    assert_eq!(
        marks(&out),
        vec![format!("{NARROWED_BESIDE_VALUE_NAME}(a)")]
    );
    let wire = coalesce(vec![cast(col("z"), nanos()), cast(col("d"), nanos())]);
    let out = mark_coerced_branches(wire, &schema).unwrap().data;
    assert_eq!(
        marks(&out),
        vec![format!("{NARROWED_BESIDE_VALUE_NAME}(z)")]
    );
    let root = cast(col("z"), nanos());
    assert!(mark_coerced_narrowing(root.clone(), &schema, true).transformed);
    assert!(!mark_coerced_narrowing(root, &schema, false).transformed);
    for kept in [
        cast(col("c"), nanos()),
        cast(col("a"), list(nanos())),
        cast(col("b"), list(micros())),
        cast(col("a"), DataType::Utf8),
    ] {
        assert!(
            !mark_coerced_narrowing(kept.clone(), &schema, true).transformed,
            "{kept}"
        );
    }
    let fitted = call(
        "one_list",
        vec![(cast(col("a"), list(micros())), list(micros()))],
        list(micros()),
    );
    assert!(!mark_coerced_branches(fitted, &schema).unwrap().transformed);
}

#[test]
fn a_branch_is_marked_only_when_the_node_loses_the_nanoseconds() {
    let schema = wide();
    let unifies = call(
        "unify",
        vec![(col("a"), list(nanos())), (col("m"), micros())],
        list(micros()),
    );
    let out = settle_branches(unifies, &schema, true).unwrap().data;
    assert_eq!(
        marks(&out),
        vec![format!("{NARROWED_BESIDE_VALUE_NAME}(a)")]
    );
    let keeps = call(
        "keep",
        vec![(col("a"), list(nanos())), (col("m"), micros())],
        list(nanos()),
    );
    let both = datafusion::functions::core::expr_fn::named_struct(vec![
        lit("f"),
        col("c"),
        lit("g"),
        col("m"),
    ]);
    let counts = call(
        "count_both",
        vec![(col("c"), nanos()), (col("m"), micros())],
        DataType::Int64,
    );
    for kept in [keeps, both, counts] {
        assert!(
            !settle_branches(kept.clone(), &schema, true)
                .unwrap()
                .transformed,
            "{kept}"
        );
    }
}

#[test]
fn only_a_node_that_produces_a_column_is_marked_and_a_producer_is_found_without_a_scan() {
    let scan = || table_scan(Some("t"), &arrow(), None).unwrap();
    let narrowed = coalesce(vec![cast(col("c"), micros()), col("m")]);
    let filtered = scan()
        .filter(narrowed.clone().is_not_null())
        .unwrap()
        .sort(vec![narrowed.clone().sort(true, true)])
        .unwrap()
        .build()
        .unwrap();
    assert!(all_marks(&session_rule(filtered)).is_empty());
    let projected = scan()
        .project(vec![narrowed.alias("v")])
        .unwrap()
        .build()
        .unwrap();
    assert_eq!(all_marks(&session_rule(projected)).len(), 1);
    let instant = || {
        lit(ScalarValue::TimestampMicrosecond(
            Some(1),
            Some(Arc::from("UTC")),
        ))
    };
    let outer = Expr::OuterReferenceColumn(
        Arc::new(Field::new("c", nanos(), true)),
        datafusion::common::Column::new(Some("o"), "c"),
    );
    let literal = lit(ScalarValue::TimestampNanosecond(
        Some(1),
        Some(Arc::from("UTC")),
    ));
    let variable = Expr::LambdaVariable(datafusion::logical_expr::expr::LambdaVariable::new(
        "x".to_string(),
        Some(Arc::new(Field::new("x", nanos(), true))),
    ));
    for producer in [outer, variable] {
        assert!(produces_nanoseconds(&producer), "{producer}");
        let alone = datafusion::logical_expr::LogicalPlanBuilder::empty(true)
            .project(vec![coalesce(vec![producer.clone(), instant()]).alias("v")])
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(all_marks(&session_rule(alone)).len(), 1, "{producer}");
    }
    let wire = lit(ScalarValue::TimestampNanosecond(Some(1), None));
    for plain in [col("c"), instant(), cast(col("m"), nanos()), wire, literal] {
        assert!(!produces_nanoseconds(&plain), "{plain}");
    }
}

#[test]
fn a_union_schema_loses_the_tag_and_keeps_its_other_metadata() {
    let plan = union_of(col("c").alias("v"), untyped().alias("v"));
    let tagged = tag_union_nulls(plan).unwrap().data;
    let LogicalPlan::Union(mut union) = widened_branch(tagged) else {
        panic!("a union");
    };
    let fields = union
        .schema
        .iter()
        .map(|(qualifier, field)| {
            let metadata = std::collections::HashMap::from([
                (UNTYPED_NULL_TAG.to_string(), String::new()),
                ("PARQUET:field_id".to_string(), "2".to_string()),
            ]);
            let field = field.as_ref().clone().with_metadata(metadata);
            (qualifier.cloned(), Arc::new(field))
        })
        .collect();
    union.schema =
        Arc::new(DFSchema::new_with_metadata(fields, std::collections::HashMap::new()).unwrap());
    let settled = settle_union_branches(LogicalPlan::Union(union))
        .unwrap()
        .data;
    for field in settled.schema().fields() {
        let kept: Vec<&String> = field.metadata().keys().collect();
        assert_eq!(kept, vec!["PARQUET:field_id"], "{field}");
    }
}

#[test]
fn a_written_cast_over_a_microsecond_column_is_kept_only_above_a_mark() {
    let scan = || table_scan(Some("t"), &arrow(), None).unwrap();
    let above = |inner: Expr| {
        let plan = scan()
            .project(vec![inner.alias("v")])
            .unwrap()
            .project(vec![cast(col("v"), nanos()).alias("w")])
            .unwrap()
            .build()
            .unwrap();
        let LogicalPlan::Projection(outer) = session_rule(plan) else {
            panic!("a projection");
        };
        outer.expr[0].clone()
    };
    let kept = above(coalesce(vec![cast(col("c"), micros()), col("m")]));
    assert_eq!(kept, written(col("v")).alias("w"));
    let dropped = above(col("m"));
    assert_eq!(dropped, col("v").alias("w"));
}

#[test]
fn the_mark_returns_the_field_of_its_argument() {
    let metadata =
        std::collections::HashMap::from([("PARQUET:field_id".to_string(), "2".to_string())]);
    let argument = Arc::new(Field::new("x", nanos(), false).with_metadata(metadata));
    for untyped_null in [true, false] {
        let returned = narrowed_beside_udf(untyped_null)
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&argument)],
                scalar_arguments: &[None],
            })
            .unwrap();
        assert_eq!(returned, argument);
    }
}
