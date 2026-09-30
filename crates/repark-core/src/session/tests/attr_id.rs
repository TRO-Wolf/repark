use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::JoinType;
use datafusion::common::metadata::FieldMetadata;
use datafusion::dataframe::DataFrame;
use datafusion::functions::expr_fn::{abs, coalesce, upper};
use datafusion::functions_aggregate::expr_fn::{count, max};
use datafusion::functions_window::expr_fn::row_number;
use datafusion::logical_expr::{Expr, cast, col, lit, when};
use datafusion::prelude::SessionContext;

use crate::ReparkSession;

const KEY: &str = "repark.attr";

fn tag(id: &str) -> Option<FieldMetadata> {
    Some(FieldMetadata::from(HashMap::from([(
        KEY.to_string(),
        id.to_string(),
    )])))
}

fn source(context: &SessionContext) -> DataFrame {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("data", DataType::Utf8, false),
        Field::new("s", DataType::Utf8, false),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(StringArray::from(vec!["a", "b"])),
        Arc::new(StringArray::from(vec!["x", "y"])),
    ];
    context
        .read_batch(RecordBatch::try_new(schema, columns).unwrap())
        .unwrap()
}

fn tagged_as(context: &SessionContext, ids: [&str; 3]) -> DataFrame {
    source(context)
        .select(vec![
            col("id").alias_with_metadata("id", tag(ids[0])),
            col("data").alias_with_metadata("data", tag(ids[1])),
            col("s").alias_with_metadata("s", tag(ids[2])),
        ])
        .unwrap()
}

fn tagged(context: &SessionContext) -> DataFrame {
    tagged_as(context, ["a1", "a2", "a3"])
}

fn ids(frame: &DataFrame) -> Vec<Option<String>> {
    frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.metadata().get(KEY).cloned())
        .collect()
}

fn named(values: &[&str]) -> Vec<Option<String>> {
    values
        .iter()
        .map(|value| (!value.is_empty()).then(|| (*value).to_string()))
        .collect()
}

#[test]
fn column_and_alias_of_a_column_inherit_the_id() {
    let context = SessionContext::new();
    let frame = tagged(&context)
        .select(vec![
            col("id"),
            col("data").alias("Data"),
            col("id").alias("id2").alias_with_metadata("id3", None),
        ])
        .unwrap();
    assert_eq!(ids(&frame), named(&["a1", "a2", "a1"]));
}

#[test]
fn cast_copies_the_source_id_so_the_stamp_must_override_it() {
    let context = SessionContext::new();
    let frame = tagged(&context)
        .select(vec![
            cast(col("id"), DataType::Utf8).alias("id"),
            cast(col("data"), DataType::Utf8),
        ])
        .unwrap();
    assert_eq!(ids(&frame), named(&["a1", "a2"]));
}

#[test]
fn arithmetic_literal_function_and_case_carry_no_id() {
    let context = SessionContext::new();
    let frame = tagged(&context)
        .select(vec![
            (col("id") + lit(1i64)).alias("id"),
            lit(5i64).alias("k"),
            abs(col("id")),
            upper(col("data")),
            coalesce(vec![col("data")]).alias("data"),
            when(col("id").gt(lit(0i64)), col("data"))
                .otherwise(col("s"))
                .unwrap()
                .alias("c"),
        ])
        .unwrap();
    assert_eq!(ids(&frame), named(&["", "", "", "", "", ""]));
}

#[test]
fn filter_limit_sort_distinct_and_subquery_alias_keep_the_ids() {
    let context = SessionContext::new();
    let base = tagged(&context);
    let filtered = base.filter(col("id").gt(lit(0i64))).unwrap();
    assert_eq!(ids(&filtered), named(&["a1", "a2", "a3"]));
    let limited = filtered.limit(0, Some(1)).unwrap();
    assert_eq!(ids(&limited), named(&["a1", "a2", "a3"]));
    let sorted = limited.sort(vec![col("data").sort(true, false)]).unwrap();
    assert_eq!(ids(&sorted), named(&["a1", "a2", "a3"]));
    let distinct = sorted.distinct().unwrap();
    assert_eq!(ids(&distinct), named(&["a1", "a2", "a3"]));
    let aliased = distinct.alias("q").unwrap();
    assert_eq!(ids(&aliased), named(&["a1", "a2", "a3"]));
    let qualifiers = aliased
        .schema()
        .iter()
        .map(|(qualifier, _)| qualifier.map(ToString::to_string))
        .collect::<Vec<_>>();
    assert_eq!(qualifiers, vec![Some("q".to_string()); 3]);
}

#[test]
fn a_self_join_carries_the_same_ids_on_both_sides() {
    let context = SessionContext::new();
    let left = tagged(&context).alias("l").unwrap();
    let right = tagged(&context).alias("r").unwrap();
    let joined = left
        .join(right, JoinType::Inner, &["l.id"], &["r.id"], None)
        .unwrap();
    assert_eq!(ids(&joined), named(&["a1", "a2", "a3", "a1", "a2", "a3"]));
}

#[test]
fn union_keeps_an_id_only_where_every_input_agrees() {
    let context = SessionContext::new();
    let first = tagged(&context);
    let second = tagged_as(&context, ["b1", "a2", "b3"]);
    let unioned = first.clone().union(second.clone()).unwrap();
    assert_eq!(ids(&unioned), named(&["", "a2", ""]));
    let by_name = first.clone().union_by_name(second).unwrap();
    assert_eq!(ids(&by_name), named(&["", "a2", ""]));
    let self_union = first.clone().union(first).unwrap();
    assert_eq!(ids(&self_union), named(&["a1", "a2", "a3"]));
}

#[test]
fn aggregate_keys_keep_their_ids_and_values_carry_none() {
    let context = SessionContext::new();
    let frame = tagged(&context)
        .aggregate(vec![col("data")], vec![max(col("id")), count(col("s"))])
        .unwrap();
    assert_eq!(ids(&frame), named(&["a2", "", ""]));
}

#[test]
fn window_passes_its_input_through_and_its_value_carries_none() {
    let context = SessionContext::new();
    let frame = tagged(&context)
        .window(vec![row_number().alias("rn")])
        .unwrap();
    assert_eq!(ids(&frame), named(&["a1", "a2", "a3", ""]));
}

#[tokio::test]
async fn a_temp_view_round_trip_keeps_the_ids() {
    let session = ReparkSession::new().unwrap();
    let frame = tagged(session.context());
    session
        .create_or_replace_temp_view_from("attr_view", &frame)
        .unwrap();
    let everything = session.sql("SELECT * FROM attr_view").await.unwrap();
    assert_eq!(ids(&everything), named(&["a1", "a2", "a3"]));
    let renamed = session
        .sql("SELECT s, id AS __repark_sel_0 FROM attr_view v WHERE v.id > 0")
        .await
        .unwrap();
    assert_eq!(ids(&renamed), named(&["a3", "a1"]));
}

#[tokio::test]
async fn the_facade_join_shape_over_two_views_of_one_frame_repeats_the_ids() {
    let session = ReparkSession::new().unwrap();
    let frame = tagged(session.context());
    session
        .create_or_replace_temp_view_from("attr_left", &frame)
        .unwrap();
    session
        .create_or_replace_temp_view_from("attr_right", &frame)
        .unwrap();
    let joined = session
        .sql(
            "SELECT attr_left.id AS l_id, attr_left.data AS l_data, attr_right.id AS r_id \
             FROM attr_left INNER JOIN attr_right ON attr_left.id = attr_right.id",
        )
        .await
        .unwrap();
    assert_eq!(ids(&joined), named(&["a1", "a2", "a1"]));
}
