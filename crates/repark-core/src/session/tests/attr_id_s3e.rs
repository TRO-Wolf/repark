use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::JoinType;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::{Column, TableReference};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Expr, col};
use datafusion::prelude::SessionContext;

use crate::frame_names::NameRule::{Exact, IgnoreCase};
use crate::frame_names::{
    FrameNode, Resolution, bind_qualified_free_refs, grandchild_qualified_key, join_on_named_keys,
    join_output_sources, qualifier_star_positions, resolve,
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

fn strings(names: &[&str]) -> Vec<String> {
    names.iter().map(ToString::to_string).collect()
}

fn quals(entries: &[(&str, &[&str])]) -> BTreeMap<String, Vec<String>> {
    entries
        .iter()
        .map(|(id, names)| {
            (
                (*id).to_string(),
                names.iter().map(ToString::to_string).collect(),
            )
        })
        .collect()
}

fn qualified(qualifier: &str, name: &str) -> Expr {
    Expr::Column(Column::new(
        Some(TableReference::bare(qualifier)),
        name.to_string(),
    ))
}

fn bound_column(bound: Expr) -> Column {
    match bound {
        Expr::Column(column) => column,
        other => panic!("expected a bound column, got {other:?}"),
    }
}

#[test]
fn resolve_prefers_facade_qualifiers_over_plan_relations() {
    let context = SessionContext::new();
    let frame = tagged_as(&context, ["a1", "a2", "a3"]).alias("T").unwrap();
    let map = quals(&[("a1", &["T"]), ("a2", &["T"]), ("a3", &["T"])]);
    let displays = strings(&["id", "v", "w"]);
    assert_eq!(
        resolve(frame.schema(), "v", Some("T"), Exact, &displays, Some(&map)).unwrap(),
        Resolution::Bound(vec![1])
    );
    assert_eq!(
        resolve(frame.schema(), "v", Some("t"), Exact, &displays, Some(&map)).unwrap(),
        Resolution::Missing
    );
}

#[test]
fn resolve_matches_either_side_of_a_using_key_union() {
    let context = SessionContext::new();
    let frame = tagged_as(&context, ["a1", "a2", "a3"]);
    let map = quals(&[("a1", &["a", "b"])]);
    let displays = strings(&["id", "v", "w"]);
    assert_eq!(
        resolve(
            frame.schema(),
            "id",
            Some("a"),
            Exact,
            &displays,
            Some(&map)
        )
        .unwrap(),
        Resolution::Bound(vec![0])
    );
    assert_eq!(
        resolve(
            frame.schema(),
            "id",
            Some("b"),
            Exact,
            &displays,
            Some(&map)
        )
        .unwrap(),
        Resolution::Bound(vec![0])
    );
    assert_eq!(
        resolve(
            frame.schema(),
            "id",
            Some("c"),
            Exact,
            &displays,
            Some(&map)
        )
        .unwrap(),
        Resolution::Missing
    );
}

#[test]
fn resolve_falls_back_to_plan_relations_without_a_facade_entry() {
    let context = SessionContext::new();
    let frame = tagged_as(&context, ["a1", "a2", "a3"]).alias("l").unwrap();
    let map = quals(&[("zz", &["q"])]);
    let displays = strings(&["id", "v", "w"]);
    assert_eq!(
        resolve(frame.schema(), "v", Some("l"), Exact, &displays, Some(&map)).unwrap(),
        Resolution::Bound(vec![1])
    );
    assert_eq!(
        resolve(frame.schema(), "v", Some("q"), Exact, &displays, Some(&map)).unwrap(),
        Resolution::Missing
    );
}

#[test]
fn resolve_ignores_facade_qualifiers_for_unqualified_names() {
    let context = SessionContext::new();
    let frame = tagged_as(&context, ["a1", "a2", "a3"]);
    let map = quals(&[("a2", &["q"])]);
    let displays = strings(&["id", "v", "w"]);
    assert_eq!(
        resolve(frame.schema(), "v", None, Exact, &displays, Some(&map)).unwrap(),
        Resolution::Bound(vec![1])
    );
}

#[test]
fn rewriter_binds_a_facade_qualified_reference_to_the_plan_engine() {
    let context = SessionContext::new();
    let frame = tagged_as(&context, ["a1", "a2", "a3"]).alias("t").unwrap();
    let plan = frame.logical_plan().clone();
    let map = quals(&[("a1", &["T"]), ("a2", &["T"]), ("a3", &["T"])]);
    let bound = bind_qualified_free_refs(
        qualified("T", "v"),
        &plan,
        Exact,
        &strings(&["id", "v", "w"]),
        Some(&map),
        false,
    )
    .unwrap();
    let column = bound_column(bound);
    assert_eq!(column.name, "v");
    assert_eq!(
        column.relation.map(|held| held.to_string()),
        Some("t".into())
    );
}

#[test]
fn rewriter_leaves_missing_unqualified_and_marked_references() {
    let context = SessionContext::new();
    let frame = tagged_as(&context, ["a1", "a2", "a3"]);
    let plan = frame.logical_plan().clone();
    let displays = strings(&["id", "v", "w"]);
    let missing =
        bind_qualified_free_refs(qualified("q", "zz"), &plan, Exact, &displays, None, false)
            .unwrap();
    assert_eq!(bound_column(missing).name, "zz");
    let bare = bind_qualified_free_refs(col("v"), &plan, Exact, &displays, None, false).unwrap();
    assert_eq!(bound_name(&bare), "v");
}

fn bound_name(bound: &Expr) -> String {
    match bound {
        Expr::Column(column) => column.name.clone(),
        other => panic!("expected a column, got {other:?}"),
    }
}

#[test]
fn rewriter_refuses_ambiguous_qualified_references_per_door() {
    let context = SessionContext::new();
    let frame = source(&context)
        .select(vec![
            col("v").alias_with_metadata("e1", Some(tag("a2"))),
            col("v").alias_with_metadata("e2", Some(tag("a9"))),
        ])
        .unwrap()
        .alias("q")
        .unwrap();
    let plan = frame.logical_plan().clone();
    let displays = strings(&["v", "v"]);
    let err = bind_qualified_free_refs(qualified("q", "v"), &plan, Exact, &displays, None, false);
    let message = format!("{:?}", err.unwrap_err());
    assert!(message.contains("AMBIGUOUS_REFERENCE"), "{message}");
    let err = bind_qualified_free_refs(qualified("q", "v"), &plan, Exact, &displays, None, true);
    let message = format!("{:?}", err.unwrap_err());
    assert!(message.contains("UNRESOLVED_COLUMN"), "{message}");
}

#[test]
fn grandchild_qualified_key_binds_through_a_select() {
    let context = SessionContext::new();
    let left = tagged_as(&context, ["k1", "l2", "l3"]).alias("l").unwrap();
    let right = tagged_as(&context, ["k1", "r2", "r3"]).alias("r").unwrap();
    let joined = left
        .join(right, JoinType::Inner, &["l.id"], &["r.id"], None)
        .unwrap();
    let plan = joined
        .select(vec![col("l.id")])
        .unwrap()
        .logical_plan()
        .clone();
    let found = grandchild_qualified_key(&plan, "v", "l", Exact, None).unwrap();
    let Some((resolution, render)) = found else {
        panic!("expected a grandchild hit");
    };
    assert_eq!(resolution, Resolution::Bound(vec![1]));
    assert_eq!(render, Some((vec!["l".to_string()], "v".to_string())));
}

#[test]
fn grandchild_qualified_key_rejects_a_non_project_root() {
    let context = SessionContext::new();
    let frame = tagged_as(&context, ["a1", "a2", "a3"]);
    let plan = frame.logical_plan().clone();
    assert!(
        grandchild_qualified_key(&plan, "v", "l", Exact, None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn join_output_sources_pairs_using_keys() {
    let context = SessionContext::new();
    let left = tagged_as(&context, ["k1", "l2", "l3"]).alias("l").unwrap();
    let right = tagged_as(&context, ["k1", "r2", "r3"]).alias("r").unwrap();
    let left_node = FrameNode::root(left.schema()).unwrap();
    let right_node = FrameNode::root(right.schema()).unwrap();
    let (joined, _) = join_on_named_keys(
        left,
        right,
        &["id".to_string()],
        JoinType::Inner,
        IgnoreCase,
        left_node,
        right_node,
    )
    .unwrap();
    assert_eq!(
        join_output_sources(joined.logical_plan()),
        vec![
            vec![(false, 0), (true, 0)],
            vec![(false, 1)],
            vec![(false, 2)],
            vec![(true, 1)],
            vec![(true, 2)],
        ]
    );
}

#[test]
fn join_output_sources_keeps_condition_sides_separate() {
    let context = SessionContext::new();
    let left = tagged_as(&context, ["k1", "l2", "l3"]).alias("l").unwrap();
    let right = tagged_as(&context, ["k1", "r2", "r3"]).alias("r").unwrap();
    let joined = left
        .join(right, JoinType::Inner, &["l.id"], &["r.id"], None)
        .unwrap();
    assert_eq!(
        join_output_sources(joined.logical_plan()),
        vec![
            vec![(false, 0)],
            vec![(false, 1)],
            vec![(false, 2)],
            vec![(true, 0)],
            vec![(true, 1)],
            vec![(true, 2)],
        ]
    );
}

#[test]
fn join_output_sources_emits_left_positions_only_for_semi() {
    let context = SessionContext::new();
    let left = tagged_as(&context, ["k1", "l2", "l3"]).alias("l").unwrap();
    let right = tagged_as(&context, ["k1", "r2", "r3"]).alias("r").unwrap();
    let left_node = FrameNode::root(left.schema()).unwrap();
    let right_node = FrameNode::root(right.schema()).unwrap();
    let (joined, _) = join_on_named_keys(
        left,
        right,
        &["id".to_string()],
        JoinType::LeftSemi,
        IgnoreCase,
        left_node,
        right_node,
    )
    .unwrap();
    assert_eq!(
        join_output_sources(joined.logical_plan()),
        vec![vec![(false, 0)], vec![(false, 1)], vec![(false, 2)],]
    );
}

#[test]
fn qualifier_star_positions_lists_plan_and_facade_positions() {
    let context = SessionContext::new();
    let frame = tagged_as(&context, ["a1", "a2", "a3"]).alias("l").unwrap();
    let plan = frame.logical_plan().clone();
    let displays = strings(&["id", "v", "w"]);
    assert_eq!(
        qualifier_star_positions(&plan, "l", Exact, &displays, None),
        Some(vec![
            (0, vec!["l".to_string()]),
            (1, vec!["l".to_string()]),
            (2, vec!["l".to_string()]),
        ])
    );
    assert_eq!(
        qualifier_star_positions(&plan, "zz", Exact, &displays, None),
        None
    );
    let map = quals(&[("a2", &["q"])]);
    assert_eq!(
        qualifier_star_positions(&plan, "q", Exact, &displays, Some(&map)),
        Some(vec![(1, vec!["l".to_string()])])
    );
}

#[test]
fn join_output_sources_is_empty_away_from_a_join() {
    let context = SessionContext::new();
    let frame = tagged_as(&context, ["a1", "a2", "a3"]);
    assert_eq!(
        join_output_sources(frame.logical_plan()),
        vec![Vec::new(), Vec::new(), Vec::new()]
    );
}
