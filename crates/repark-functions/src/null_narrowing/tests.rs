use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::TreeNode;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::logical_expr::{col, table_scan, when};

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
        let out = mark_coerced_narrowing(cast(col("c"), target.clone()), &schema());
        assert!(out.transformed, "{target}");
        assert_eq!(
            marks(&out.data),
            vec![format!("{NARROWED_BESIDE_VALUE_NAME}(c)")]
        );
        let again = mark_coerced_narrowing(out.data.clone(), &schema());
        assert!(!again.transformed, "{target}");
    }
    for kept in [
        cast(col("c"), nanos()),
        cast(col("m"), micros()),
        cast(cast(untyped(), nanos()), micros()),
        cast(col("c"), DataType::Utf8),
    ] {
        assert!(
            !mark_coerced_narrowing(kept.clone(), &schema()).transformed,
            "{kept}"
        );
    }
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
