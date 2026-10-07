use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::JoinType;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::{Column, Spans, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::functions_aggregate::expr_fn::max;
use datafusion::logical_expr::expr::{Exists, InSubquery};
use datafusion::logical_expr::{Expr, LogicalPlan, Subquery, col, lit};
use datafusion::prelude::SessionContext;

use crate::frame_names::{
    NameRule::{Exact, IgnoreCase},
    Resolution, SortShape, bind_free_names, grandchild_key, sort_shape, stamp,
};

const KEY: &str = "repark.attr";

fn tag(id: &str) -> FieldMetadata {
    FieldMetadata::from(HashMap::from([(KEY.to_string(), id.to_string())]))
}

fn source(context: &SessionContext) -> DataFrame {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
        Field::new("w", DataType::Utf8, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(Int64Array::from(vec![10, 20])),
        Arc::new(StringArray::from(vec!["a", "b"])),
    ];
    context
        .read_batch(RecordBatch::try_new(schema, columns).unwrap())
        .unwrap()
}

fn tagged_as(context: &SessionContext, ids: [&str; 3]) -> DataFrame {
    source(context)
        .select(vec![
            col("id").alias_with_metadata("id", Some(tag(ids[0]))),
            col("v").alias_with_metadata("v", Some(tag(ids[1]))),
            col("w").alias_with_metadata("w", Some(tag(ids[2]))),
        ])
        .unwrap()
}

fn tagged_joined(context: &SessionContext) -> DataFrame {
    let left = tagged_as(context, ["k1", "l2", "l3"]).alias("l").unwrap();
    let right = tagged_as(context, ["k1", "r2", "r3"]).alias("r").unwrap();
    left.join(right, JoinType::Inner, &["l.id"], &["r.id"], None)
        .unwrap()
}

fn dup_schema() -> (SessionContext, DataFrame) {
    let context = SessionContext::new();
    let frame = source(&context)
        .select(vec![
            col("v").alias_with_metadata("e1", Some(tag("a2"))),
            col("w").alias_with_metadata("e2", Some(tag("a1"))),
        ])
        .unwrap();
    (context, frame)
}

fn bound_name(bound: Expr) -> String {
    match bound {
        Expr::Column(column) => column.name,
        other => panic!("expected a bound column, got {other:?}"),
    }
}

#[test]
fn sort_shape_select_through_filter_is_project() {
    let context = SessionContext::new();
    let frame = tagged_as(&context, ["a1", "a2", "a3"])
        .filter(col("v").gt(lit(1i64)))
        .unwrap()
        .select_columns(&["id", "v"])
        .unwrap();
    assert_eq!(sort_shape(frame.logical_plan()), SortShape::Project);
}

#[test]
fn sort_shape_join_is_other() {
    let context = SessionContext::new();
    assert_eq!(
        sort_shape(tagged_joined(&context).logical_plan()),
        SortShape::Other
    );
}

#[test]
fn sort_shape_aggregate_is_aggregate() {
    let context = SessionContext::new();
    let frame = source(&context)
        .aggregate(vec![col("w")], vec![max(col("v")).alias("m")])
        .unwrap();
    assert_eq!(sort_shape(frame.logical_plan()), SortShape::Aggregate);
}

#[test]
fn sort_shape_stamp_passthrough_over_aggregate_is_aggregate() {
    let context = SessionContext::new();
    let frame = source(&context)
        .aggregate(vec![col("w")], vec![max(col("v")).alias("m")])
        .unwrap();
    let stamped = stamp(frame.logical_plan().clone()).unwrap();
    assert!(matches!(stamped, LogicalPlan::Projection(_)));
    assert_eq!(sort_shape(&stamped), SortShape::Aggregate);
}

#[test]
fn sort_shape_union_is_other() {
    let context = SessionContext::new();
    let frame = source(&context).union(source(&context)).unwrap();
    assert_eq!(sort_shape(frame.logical_plan()), SortShape::Other);
}

#[test]
fn sort_shape_join_select_skips_to_join() {
    let context = SessionContext::new();
    let left = tagged_as(&context, ["l1", "l2", "l3"])
        .alias("_repark_l")
        .unwrap();
    let right = tagged_as(&context, ["r1", "r2", "r3"])
        .alias("_repark_r")
        .unwrap();
    let joined = left
        .join(
            right,
            JoinType::Inner,
            &["_repark_l.id"],
            &["_repark_r.id"],
            None,
        )
        .unwrap();
    let plan = joined.select(vec![col("_repark_l.id")]).unwrap();
    assert_eq!(sort_shape(plan.logical_plan()), SortShape::Other);
}

#[test]
fn grandchild_key_binds_join_output_under_select() {
    let context = SessionContext::new();
    let narrowed = tagged_joined(&context).select(vec![col("l.id")]).unwrap();
    let plan = narrowed.logical_plan().clone();
    let LogicalPlan::Projection(narrow) = &plan else {
        panic!("expected the narrowed select, got {plan:?}");
    };
    let joined = narrow.input.schema();
    match grandchild_key(&plan, "id", Exact) {
        Ok(Some(Resolution::Bound(hits))) => {
            assert_eq!(hits.len(), 2);
            for position in hits {
                let field = &joined.fields()[position];
                assert_eq!(field.metadata().get(KEY).map(String::as_str), Some("k1"));
            }
        }
        other => panic!("expected the shared key bound, got {other:?}"),
    }
    assert!(matches!(
        grandchild_key(&plan, "v", IgnoreCase),
        Ok(Some(Resolution::Ambiguous(_)))
    ));
    assert!(matches!(
        grandchild_key(&plan, "nope", Exact),
        Ok(Some(Resolution::Missing))
    ));
}

#[test]
fn grandchild_key_rejects_non_project_roots() {
    let context = SessionContext::new();
    assert!(matches!(
        grandchild_key(tagged_joined(&context).logical_plan(), "v", Exact),
        Ok(None)
    ));
    let frame = source(&context).select(vec![col("id")]).unwrap();
    assert!(matches!(
        grandchild_key(frame.logical_plan(), "v", Exact),
        Ok(None)
    ));
}

#[test]
fn bind_free_names_rewrites_display_to_engine() {
    let (_context, frame) = dup_schema();
    let plan = frame.logical_plan();
    let bound = bind_free_names(col("v"), plan, Exact, &["v".into(), "w".into()], false);
    assert_eq!(bound_name(bound.unwrap()), "e1");
}

#[test]
fn bind_free_names_filter_raises_ambiguous() {
    let (_context, frame) = dup_schema();
    let plan = frame.logical_plan();
    let err = bind_free_names(col("v"), plan, IgnoreCase, &["v".into(), "v".into()], false);
    let message = format!("{:?}", err.unwrap_err());
    assert!(message.contains("AMBIGUOUS_REFERENCE"), "{message}");
}

#[test]
fn bind_free_names_filter_refuses_exact_among_folded_rivals() {
    let (_context, frame) = dup_schema();
    let plan = frame.logical_plan();
    let written = Expr::Column(Column::from_name("V"));
    let err = bind_free_names(written, plan, IgnoreCase, &["V".into(), "v".into()], false);
    let message = format!("{:?}", err.unwrap_err());
    assert!(message.contains("AMBIGUOUS_REFERENCE"), "{message}");
}

#[test]
fn bind_free_names_sort_routes_project_dup_through_input() {
    let (_context, frame) = dup_schema();
    let plan = frame.logical_plan();
    assert_eq!(sort_shape(plan), SortShape::Project);
    let bound = bind_free_names(col("v"), plan, IgnoreCase, &["v".into(), "v".into()], true);
    assert_eq!(bound_name(bound.unwrap()), "v");
}

#[test]
fn bind_free_names_sort_respells_case_mismatched_key_to_input() {
    let (_context, frame) = dup_schema();
    let plan = frame.logical_plan();
    let written = Expr::Column(Column::from_name("V"));
    let bound = bind_free_names(written, plan, IgnoreCase, &["v".into(), "v".into()], true);
    assert_eq!(bound_name(bound.unwrap()), "v");
}

#[test]
fn bind_free_names_sort_keeps_written_spelling_when_engines_are_twins() {
    let context = SessionContext::new();
    let inner = source(&context)
        .select(vec![
            col("id").alias_with_metadata("ID", Some(tag("a8"))),
            col("v").alias_with_metadata("v", Some(tag("a9"))),
        ])
        .unwrap();
    let outer = inner
        .select(vec![
            col("ID").alias_with_metadata("ID", Some(tag("b8"))),
            col("ID").alias_with_metadata("id", Some(tag("b9"))),
        ])
        .unwrap();
    let plan = outer.logical_plan();
    assert_eq!(sort_shape(plan), SortShape::Project);
    let written = Expr::Column(Column::from_name("id"));
    let bound = bind_free_names(written, plan, IgnoreCase, &["ID".into(), "id".into()], true);
    assert_eq!(bound_name(bound.unwrap()), "id");
}

#[test]
fn bind_free_names_sort_passes_through_when_input_is_not_unique() {
    let context = SessionContext::new();
    let inner = source(&context)
        .select(vec![
            (col("v") + lit(1)).alias_with_metadata("e1", Some(tag("a4"))),
            (col("v") + lit(2)).alias_with_metadata("e2", Some(tag("a5"))),
        ])
        .unwrap();
    let doubled = inner
        .select(vec![
            col("e1").alias_with_metadata("o1", Some(tag("a6"))),
            col("e2").alias_with_metadata("o2", Some(tag("a7"))),
        ])
        .unwrap();
    let plan = doubled.logical_plan();
    assert_eq!(sort_shape(plan), SortShape::Project);
    let bound = bind_free_names(col("v"), plan, IgnoreCase, &["v".into(), "v".into()], true);
    assert_eq!(bound_name(bound.unwrap()), "v");
}

#[test]
fn bind_free_names_sort_is_unresolved_on_join() {
    let context = SessionContext::new();
    let plan = tagged_joined(&context).logical_plan().clone();
    let displays = ["id", "v", "w", "id", "v", "w"]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
    let err = bind_free_names(col("v"), &plan, IgnoreCase, &displays, true);
    let message = format!("{:?}", err.unwrap_err());
    assert!(message.contains("UNRESOLVED_COLUMN"), "{message}");
}

#[test]
fn bind_free_names_sort_binds_sourced_twin_over_reminted_input() {
    let context = SessionContext::new();
    let computed = source(&context)
        .select(vec![
            col("id").alias_with_metadata("id", Some(tag("a0"))),
            col("v").alias_with_metadata("e1", Some(tag("a2"))),
            (col("v") - lit(15)).alias_with_metadata("e2", Some(tag("a3"))),
        ])
        .unwrap();
    let outer = computed
        .select(vec![
            col("id"),
            col("e1").alias_with_metadata("o1", Some(tag("a2"))),
            col("e2").alias_with_metadata("o2", Some(tag("a3"))),
            lit(1).alias("w"),
        ])
        .unwrap();
    let plan = outer.logical_plan().clone();
    assert_eq!(sort_shape(&plan), SortShape::Project);
    let bound = bind_free_names(
        col("v"),
        &plan,
        IgnoreCase,
        &["id".into(), "v".into(), "v".into(), "w".into()],
        true,
    );
    assert_eq!(bound_name(bound.unwrap()), "o1");
}

#[test]
fn bind_free_names_sort_leaves_key_unbound_when_lineage_misses_nearest_visible() {
    let context = SessionContext::new();
    let base = source(&context)
        .select(vec![
            col("w").alias_with_metadata("v", Some(tag("b1"))),
            col("v").alias_with_metadata("y", Some(tag("b2"))),
            col("id").alias_with_metadata("id", Some(tag("b0"))),
        ])
        .unwrap();
    let mid = base.select(vec![col("y"), col("id")]).unwrap();
    let outer = mid
        .select(vec![
            col("y").alias_with_metadata("o1", Some(tag("c1"))),
            (col("y") * lit(-1)).alias_with_metadata("o2", Some(tag("c2"))),
            col("id"),
        ])
        .unwrap();
    let plan = outer.logical_plan().clone();
    assert_eq!(sort_shape(&plan), SortShape::Project);
    let bound = bind_free_names(
        col("v"),
        &plan,
        IgnoreCase,
        &["v".into(), "v".into(), "id".into()],
        true,
    );
    assert_eq!(bound_name(bound.unwrap()), "v");
}

#[test]
fn bind_free_names_sort_is_unresolved_when_join_input_carries_visible_twice() {
    let context = SessionContext::new();
    let renamed = tagged_joined(&context)
        .select(vec![
            col("l.v").alias_with_metadata("__repark_l_abcdef012345_1_v", Some(tag("l2"))),
            col("r.v").alias_with_metadata("__repark_r_abcdef012345_4_v", Some(tag("r2"))),
            col("l.id").alias_with_metadata("id", Some(tag("k1"))),
        ])
        .unwrap();
    let picked = renamed
        .select(vec![
            col("__repark_l_abcdef012345_1_v").alias_with_metadata("o1", Some(tag("l2"))),
            (col("__repark_r_abcdef012345_4_v") * lit(-1))
                .alias_with_metadata("o2", Some(tag("n9"))),
            col("id"),
        ])
        .unwrap();
    let plan = picked.logical_plan().clone();
    assert_eq!(sort_shape(&plan), SortShape::Project);
    let err = bind_free_names(
        col("v"),
        &plan,
        IgnoreCase,
        &["v".into(), "v".into(), "id".into()],
        true,
    );
    let message = format!("{:?}", err.unwrap_err());
    assert!(message.contains("UNRESOLVED_COLUMN"), "{message}");
}

#[test]
fn bind_free_names_sort_is_unresolved_when_twins_meet_at_join() {
    let context = SessionContext::new();
    let picked = tagged_joined(&context)
        .select(vec![
            col("l.id").alias_with_metadata("id", Some(tag("k1"))),
            col("l.v").alias_with_metadata("e1", Some(tag("l2"))),
            col("r.v").alias_with_metadata("e2", Some(tag("r2"))),
        ])
        .unwrap();
    let plan = picked.logical_plan().clone();
    assert_eq!(sort_shape(&plan), SortShape::Project);
    let err = bind_free_names(
        col("v"),
        &plan,
        IgnoreCase,
        &["id".into(), "v".into(), "v".into()],
        true,
    );
    let message = format!("{:?}", err.unwrap_err());
    assert!(message.contains("UNRESOLVED_COLUMN"), "{message}");
}

#[test]
fn bind_free_names_leaves_missing_and_qualified() {
    let (_context, frame) = dup_schema();
    let plan = frame.logical_plan();
    let missing = bind_free_names(col("nope"), plan, Exact, &["e1".into(), "e2".into()], false);
    assert_eq!(bound_name(missing.unwrap()), "nope");
    let qualified = Expr::Column(Column::new(Some(TableReference::bare("L")), "v"));
    let kept = bind_free_names(qualified, plan, Exact, &["e1".into(), "e2".into()], false);
    match kept.unwrap() {
        Expr::Column(column) => assert_eq!(column.relation, Some(TableReference::bare("L"))),
        other => panic!("expected the qualified column untouched, got {other:?}"),
    }
}

#[test]
fn bind_free_names_binds_outer_but_not_subquery_plans() {
    let (_context, frame) = dup_schema();
    let plan = frame.logical_plan().clone();
    let inner = dup_schema().1.logical_plan().clone();
    let exists = Expr::Exists(Exists::new(
        Subquery {
            outer_ref_columns: vec![],
            subquery: Arc::new(inner.clone()),
            spans: Spans::new(),
        },
        false,
    ));
    let kept = bind_free_names(exists, &plan, Exact, &["v".into(), "w".into()], false);
    assert!(matches!(kept.unwrap(), Expr::Exists(_)));
    let subquery = Expr::InSubquery(InSubquery::new(
        Box::new(col("v")),
        Subquery {
            outer_ref_columns: vec![],
            subquery: Arc::new(inner),
            spans: Spans::new(),
        },
        false,
    ));
    let rebound = bind_free_names(subquery, &plan, Exact, &["v".into(), "w".into()], false);
    match rebound.unwrap() {
        Expr::InSubquery(found) => assert_eq!(bound_name(*found.expr), "e1"),
        other => panic!("expected the outer name bound, got {other:?}"),
    }
}
