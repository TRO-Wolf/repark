use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::TreeNode;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::logical_expr::{Cast, col, when};

use super::*;

fn schema() -> DFSchema {
    let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
    DFSchema::try_from(Schema::new(vec![
        Field::new("c", nanos, true),
        Field::new("m", instant(), true),
        Field::new("id", DataType::Int32, true),
    ]))
    .unwrap()
}

fn instant() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
}

fn untyped() -> Expr {
    Expr::Literal(ScalarValue::Null, None)
}

fn cast(expr: Expr, target: DataType) -> Expr {
    Expr::Cast(Cast::new(Box::new(expr), target))
}

fn widened(expr: Expr) -> Expr {
    expr.transform_up(|node| {
        Ok(if is_tagged_null(&node) {
            let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
            Transformed::yes(cast(cast(node, nanos), instant()))
        } else {
            Transformed::no(node)
        })
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

fn settled(expr: Expr) -> Expr {
    let tagged = tag_untyped_nulls(expr, &schema()).unwrap().data;
    settle_tagged_nulls(widened(tagged), &schema())
        .unwrap()
        .data
}

#[test]
fn an_untyped_null_beside_a_nanosecond_branch_is_tagged_and_no_other_null() {
    let schema = schema();
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
        (
            coalesce(vec![
                col("c"),
                cast(untyped(), DataType::Timestamp(TimeUnit::Nanosecond, None)),
            ]),
            false,
        ),
        (coalesce(vec![col("c"), col("c")]), false),
    ] {
        let out = tag_untyped_nulls(expr.clone(), &schema).unwrap();
        assert_eq!(out.transformed, tagged, "{expr}");
        assert_eq!(carries_tag(&out.data), tagged, "{expr}");
    }
}

#[test]
fn a_widened_null_beside_a_nanosecond_branch_is_marked_and_the_tag_is_gone() {
    let marked = settled(coalesce(vec![col("c"), untyped()]));
    assert!(is_narrowed_beside_null(&marked), "{marked}");
    assert!(!carries_tag(&marked), "{marked}");
    let Expr::ScalarFunction(call) = &marked else {
        unreachable!()
    };
    assert_eq!(
        call.args,
        vec![widened_plain(coalesce(vec![col("c"), untyped()]))]
    );
}

fn widened_plain(expr: Expr) -> Expr {
    expr.transform_up(|node| {
        Ok(if is_untyped_null(&node) {
            let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
            Transformed::yes(cast(cast(node, nanos), instant()))
        } else {
            Transformed::no(node)
        })
    })
    .unwrap()
    .data
}

#[test]
fn a_branch_narrowed_before_the_null_is_read_is_not_marked() {
    let schema = schema();
    let tagged = tag_untyped_nulls(coalesce(vec![col("c"), untyped()]), &schema)
        .unwrap()
        .data;
    let written = widened(tagged)
        .transform_up(|node| {
            Ok(if node == col("c") {
                Transformed::yes(cast(node, instant()))
            } else {
                Transformed::no(node)
            })
        })
        .unwrap()
        .data;
    let out = settle_tagged_nulls(written, &schema).unwrap().data;
    assert!(!is_narrowed_beside_null(&out), "{out}");
    assert!(!carries_tag(&out), "{out}");
}

#[test]
fn a_null_that_no_coercion_widened_is_untagged_and_not_marked() {
    let schema = schema();
    let tagged = tag_untyped_nulls(coalesce(vec![col("c"), untyped()]), &schema)
        .unwrap()
        .data;
    assert!(carries_tag(&tagged));
    let out = settle_tagged_nulls(tagged, &schema).unwrap().data;
    assert_eq!(out, coalesce(vec![col("c"), untyped()]));
}

#[test]
fn the_marker_is_its_argument_and_the_optimizer_drops_it() {
    let udf = narrowed_beside_null_udf();
    let argument = Arc::new(Field::new("x", instant(), false));
    let returned = udf
        .return_field_from_args(ReturnFieldArgs {
            arg_fields: &[Arc::clone(&argument)],
            scalar_arguments: &[None],
        })
        .unwrap();
    assert_eq!(returned.data_type(), &instant());
    assert!(!returned.is_nullable());
    let simplified = udf
        .simplify(vec![col("m")], &SimplifyContext::default())
        .unwrap();
    assert!(matches!(simplified, ExprSimplifyResult::Simplified(expr) if expr == col("m")));
}

fn union_of(value: Expr, absent: Expr) -> LogicalPlan {
    let arrow = Schema::new(vec![
        Field::new("c", DataType::Timestamp(TimeUnit::Nanosecond, None), true),
        Field::new("m", instant(), true),
        Field::new("id", DataType::Int32, true),
    ]);
    let scan = || datafusion::logical_expr::table_scan(Some("t"), &arrow, None).unwrap();
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

fn null_branch(plan: &LogicalPlan) -> Expr {
    let LogicalPlan::Union(union) = plan else {
        unreachable!()
    };
    let LogicalPlan::Projection(projection) = union.inputs[1].as_ref() else {
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
fn an_untyped_null_branch_of_a_union_beside_nanoseconds_is_tagged_then_marked() {
    for absent in [untyped().alias("v"), untyped()] {
        let tagged = tag_union_nulls(union_of(col("c").alias("v"), absent.clone())).unwrap();
        assert!(tagged.transformed, "{absent}");
        assert!(carries_tag(&null_branch(&tagged.data)), "{absent}");
        let settled = settle_union_nulls(widened_branch(tagged.data)).unwrap();
        let branch = null_branch(&settled.data);
        assert!(!carries_tag(&branch), "{branch}");
        assert!(is_narrowed_beside_null(peeled(&branch)), "{branch}");
        assert_eq!(
            branch.schema_name().to_string(),
            absent.schema_name().to_string()
        );
    }
}

#[test]
fn a_null_column_that_is_not_a_projection_is_tagged_in_one_above_it() {
    let LogicalPlan::Union(direct) = union_of(col("c").alias("v"), untyped().alias("v")) else {
        unreachable!()
    };
    let nulls = datafusion::logical_expr::LogicalPlanBuilder::from(Arc::unwrap_or_clone(
        Arc::clone(&direct.inputs[1]),
    ))
    .limit(0, Some(1))
    .unwrap()
    .build()
    .unwrap();
    let limited = LogicalPlan::Union(Union {
        inputs: vec![Arc::clone(&direct.inputs[0]), Arc::new(nulls)],
        schema: direct.schema,
    });
    let tagged = tag_union_nulls(limited).unwrap();
    assert!(tagged.transformed);
    let branch = null_branch(&tagged.data);
    assert!(carries_tag(&branch), "{branch}");
    assert_eq!(branch.schema_name().to_string(), "v");
}

#[test]
fn a_null_branch_of_a_union_beside_microseconds_is_left_alone() {
    let plain = union_of(col("m").alias("v"), untyped().alias("v"));
    assert!(!tag_union_nulls(plain).unwrap().transformed);
    let typed = cast(untyped(), DataType::Timestamp(TimeUnit::Nanosecond, None));
    let written = union_of(col("c").alias("v"), typed.alias("v"));
    assert!(!tag_union_nulls(written).unwrap().transformed);
    let tagged = tag_union_nulls(union_of(col("c").alias("v"), untyped().alias("v")))
        .unwrap()
        .data;
    let LogicalPlan::Union(union) = widened_branch(tagged) else {
        unreachable!()
    };
    let narrowed = union_of(cast(col("c"), instant()).alias("v"), untyped().alias("v"));
    let LogicalPlan::Union(first) = narrowed else {
        unreachable!()
    };
    let already = LogicalPlan::Union(Union {
        inputs: vec![Arc::clone(&first.inputs[0]), Arc::clone(&union.inputs[1])],
        schema: union.schema,
    });
    let settled = settle_union_nulls(already).unwrap().data;
    let branch = null_branch(&settled);
    assert!(!carries_tag(&branch), "{branch}");
    assert!(!is_narrowed_beside_null(peeled(&branch)), "{branch}");
}
