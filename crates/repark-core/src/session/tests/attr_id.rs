use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::JoinType;
use datafusion::common::metadata::FieldMetadata;
use datafusion::common::{DFSchema, DFSchemaRef};
use datafusion::dataframe::DataFrame;
use datafusion::functions::expr_fn::{abs, coalesce, upper};
use datafusion::functions_aggregate::expr_fn::{count, max};
use datafusion::functions_window::expr_fn::{lag, row_number};
use datafusion::logical_expr::{EmptyRelation, LogicalPlan, cast, col, lit, when};
use datafusion::prelude::SessionContext;

use crate::ReparkSession;
use crate::frame_names::{
    AttrId, NameRule::IgnoreCase, Resolution, alias_with_fresh_id, attribute_ids,
    join_on_named_keys, plan_is_relation, plan_is_stamped, projection_source_ids,
    remint_join_collisions, requalify_join_sides, resolve, stamp, strip, union_by_folded_name,
};

const KEY: &str = "repark.attr";

fn tag(id: &str) -> FieldMetadata {
    FieldMetadata::from(HashMap::from([(KEY.to_string(), id.to_string())]))
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
            col("id").alias_with_metadata("id", Some(tag(ids[0]))),
            col("data").alias_with_metadata("data", Some(tag(ids[1]))),
            col("s").alias_with_metadata("s", Some(tag(ids[2]))),
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
fn union_keeps_an_id_only_where_every_input_carrying_the_column_agrees() {
    let context = SessionContext::new();
    let first = tagged(&context);
    let second = tagged_as(&context, ["b1", "a2", "b3"]);
    let unioned = first.clone().union(second.clone()).unwrap();
    assert_eq!(ids(&unioned), named(&["", "a2", ""]));
    let by_name = first.clone().union_by_name(second).unwrap();
    assert_eq!(ids(&by_name), named(&["", "a2", ""]));
    let self_union = first.clone().union(first.clone()).unwrap();
    assert_eq!(ids(&self_union), named(&["a1", "a2", "a3"]));
    let narrow = first.select(vec![col("id"), col("data")]).unwrap();
    let padded = narrow
        .union_by_name(tagged_as(&context, ["b1", "b2", "b3"]))
        .unwrap();
    assert_eq!(ids(&padded), named(&["", "", "b3"]));
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

fn stamped(frame: DataFrame) -> DataFrame {
    let (state, plan) = frame.into_parts();
    DataFrame::new(state, stamp(plan).unwrap())
}

fn distinct_count(values: &[Option<String>]) -> usize {
    values.iter().collect::<HashSet<_>>().len()
}

#[tokio::test]
async fn stamp_mints_a_fresh_id_per_source_field_and_is_idempotent() {
    let session = ReparkSession::new().unwrap();
    let frame = stamped(source(session.context()));
    let held = ids(&frame);
    assert!(held.iter().all(Option::is_some));
    assert_eq!(distinct_count(&held), 3);
    let again = stamp(frame.logical_plan().clone()).unwrap();
    assert_eq!(&again, frame.logical_plan());
    let projected = stamped(frame.clone().select(vec![col("s"), col("id")]).unwrap());
    let restamped = stamp(projected.logical_plan().clone()).unwrap();
    assert_eq!(&restamped, projected.logical_plan());
    session
        .create_or_replace_temp_view_from("attr_stamped", &frame)
        .unwrap();
    let read_back = session.sql("SELECT * FROM attr_stamped").await.unwrap();
    assert_eq!(ids(&read_back), held);
    assert_eq!(
        attribute_ids(read_back.schema())
            .into_iter()
            .map(|id| id.map(|id| id.as_str().to_string()))
            .collect::<Vec<_>>(),
        held
    );
}

#[test]
fn stamp_keeps_column_ids_and_mints_for_cast_arithmetic_and_literals() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let held = ids(&frame);
    let projected = stamped(
        frame
            .select(vec![
                col("id"),
                col("id").alias("id_again"),
                cast(col("id"), DataType::Utf8).alias("id_text"),
                cast(col("data"), DataType::Utf8),
                col("id") + lit(1i64),
                lit(5i64).alias("k"),
            ])
            .unwrap(),
    );
    let after = ids(&projected);
    assert_eq!(after[0], held[0]);
    assert_eq!(after[1], held[0]);
    assert!(after[2..].iter().all(Option::is_some));
    assert!(after[2..].iter().all(|id| !held.contains(id)));
    assert_eq!(distinct_count(&after[2..]), 4);
}

#[test]
fn stamp_leaves_pass_through_nodes_unchanged() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let held = ids(&frame);
    let chained = frame
        .filter(col("id").gt(lit(0i64)))
        .unwrap()
        .limit(0, Some(1))
        .unwrap()
        .sort(vec![col("data").sort(true, false)])
        .unwrap()
        .distinct()
        .unwrap()
        .alias("q")
        .unwrap();
    let again = stamp(chained.logical_plan().clone()).unwrap();
    assert_eq!(&again, chained.logical_plan());
    assert_eq!(ids(&chained), held);
}

#[test]
fn stamp_keeps_aggregate_keys_and_mints_values_and_window_outputs() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let held = ids(&frame);
    let grouped = stamped(
        frame
            .clone()
            .aggregate(vec![col("data")], vec![max(col("id")), count(col("s"))])
            .unwrap(),
    );
    let after = ids(&grouped);
    assert_eq!(after[0], held[1]);
    assert!(
        after[1..]
            .iter()
            .all(|id| id.is_some() && !held.contains(id))
    );
    assert_eq!(distinct_count(&after), 3);
    let windowed = stamped(frame.window(vec![row_number().alias("rn")]).unwrap());
    let after = ids(&windowed);
    assert_eq!(after[..3], held[..]);
    assert!(after[3].is_some() && !held.contains(&after[3]));
}

#[test]
fn stamp_gives_a_union_the_first_input_ids() {
    let context = SessionContext::new();
    let first = stamped(source(&context));
    let second = stamped(source(&context));
    let held = ids(&first);
    assert!(ids(&second).iter().all(|id| !held.contains(id)));
    let unioned = stamped(first.clone().union(second.clone()).unwrap());
    assert_eq!(ids(&unioned), held);
    let by_name = stamped(
        first
            .clone()
            .union_by_name(
                second
                    .clone()
                    .select(vec![col("s"), col("data"), col("id")])
                    .unwrap(),
            )
            .unwrap(),
    );
    assert_eq!(ids(&by_name), held);
    let respelled = second
        .select(vec![col("id").alias("ID"), col("data"), col("s")])
        .unwrap();
    let folded = stamped(union_by_folded_name(first, respelled, false, IgnoreCase).unwrap());
    assert_eq!(ids(&folded), held);
}

#[test]
fn stamp_mints_where_the_first_union_input_has_no_id_to_give() {
    let context = SessionContext::new();
    let raw = stamped(source(&context).union(source(&context)).unwrap());
    let minted = ids(&raw);
    assert!(minted.iter().all(Option::is_some));
    assert_eq!(distinct_count(&minted), 3);
    let full = stamped(source(&context));
    let narrow = stamped(source(&context))
        .select(vec![col("id"), col("data")])
        .unwrap();
    let held = ids(&narrow);
    let padded = stamped(narrow.union_by_name(full.clone()).unwrap());
    let after = ids(&padded);
    assert_eq!(after[..2], held[..]);
    assert!(after[2].is_some() && !ids(&full).contains(&after[2]));
}

#[test]
fn union_reads_inner_alias_ids_through_plain_machinery_aliases() {
    let context = SessionContext::new();
    let twins = stamped(source(&context))
        .select(vec![
            col("id"),
            alias_with_fresh_id(col("data"), "data").alias("__sel_1"),
            alias_with_fresh_id(col("data"), "data").alias("__sel_2"),
        ])
        .unwrap();
    let held = ids(&twins);
    assert_eq!(distinct_count(&held), 3);
    let unioned = stamped(twins.clone().union(twins).unwrap());
    assert_eq!(ids(&unioned), held);
}

fn joined_over_views(session: &ReparkSession, left: &DataFrame, right: &DataFrame) -> String {
    session
        .create_or_replace_temp_view_from("attr_jl", left)
        .unwrap();
    session
        .create_or_replace_temp_view_from("attr_jr", right)
        .unwrap();
    let left_parts = left
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(position, field)| format!("attr_jl.{} AS l_{position}", field.name()));
    let right_parts = right
        .schema()
        .fields()
        .iter()
        .enumerate()
        .map(|(position, field)| format!("attr_jr.{} AS r_{position}", field.name()));
    format!(
        "SELECT {} FROM attr_jl INNER JOIN attr_jr ON attr_jl.id = attr_jr.id",
        left_parts.chain(right_parts).collect::<Vec<_>>().join(", ")
    )
}

#[tokio::test]
async fn the_join_re_mint_keeps_the_left_and_renames_each_colliding_right_id_once() {
    let session = ReparkSession::new().unwrap();
    let frame = stamped(source(session.context()));
    let held = ids(&frame);
    let twins = frame
        .clone()
        .select(vec![col("id"), col("id").alias("id_twin"), col("s")])
        .unwrap();
    let sql = joined_over_views(&session, &frame, &twins);
    let joined = session.sql(&sql).await.unwrap();
    assert_eq!(
        ids(&joined)[3..],
        [held[0].clone(), held[0].clone(), held[2].clone()]
    );
    let requalified = requalify_join_sides(joined, &[frame.schema(), twins.schema()]).unwrap();
    let (state, plan) = requalified.into_parts();
    let reminted = DataFrame::new(state, remint_join_collisions(plan, 3).unwrap());
    let after = ids(&reminted);
    assert_eq!(after[..3], held[..]);
    assert!(
        after[3..]
            .iter()
            .all(|id| id.is_some() && !held.contains(id))
    );
    assert_eq!(after[3], after[4]);
    assert_ne!(after[3], after[5]);
    let displays = strings(&["id", "data", "s", "id", "id", "s"]);
    assert_eq!(
        resolve(reminted.schema(), "id", None, IgnoreCase, &displays, None).unwrap(),
        Resolution::Ambiguous(vec![0, 3, 4])
    );
    assert_eq!(
        resolve(reminted.schema(), "data", None, IgnoreCase, &displays, None).unwrap(),
        Resolution::Bound(vec![1])
    );
}

#[tokio::test]
async fn the_join_re_mint_leaves_a_join_of_distinct_attributes_unchanged() {
    let session = ReparkSession::new().unwrap();
    let left = stamped(source(session.context()));
    let right = stamped(source(session.context()));
    let sql = joined_over_views(&session, &left, &right);
    let joined = session.sql(&sql).await.unwrap();
    assert!(ids(&joined).iter().all(Option::is_some));
    assert_eq!(distinct_count(&ids(&joined)), 6);
    let plan = joined.logical_plan().clone();
    assert_eq!(remint_join_collisions(plan.clone(), 3).unwrap(), plan);
    assert!(remint_join_collisions(plan, 7).is_err());
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

#[tokio::test]
async fn using_join_re_mints_colliding_right_ids_of_a_self_join() {
    let session = ReparkSession::new().unwrap();
    let frame = stamped(source(session.context()));
    let held = ids(&frame);
    session
        .create_or_replace_temp_view_from("attr_l", &frame)
        .unwrap();
    session
        .create_or_replace_temp_view_from("attr_r", &frame)
        .unwrap();
    let left = session.sql("SELECT * FROM attr_l").await.unwrap();
    let right = session.sql("SELECT * FROM attr_r").await.unwrap();
    let joined = join_on_named_keys(
        left,
        right,
        &["id".to_string()],
        JoinType::Inner,
        IgnoreCase,
    )
    .unwrap();
    let after = ids(&joined);
    assert_eq!(after.len(), 5);
    assert_eq!(after[0], held[0]);
    assert_eq!(&after[1..3], &held[1..]);
    assert!(
        after[3..]
            .iter()
            .all(|id| id.is_some() && !held.contains(id))
    );
    assert_ne!(after[3], after[4]);
}

#[test]
fn strip_removes_every_id_and_keeps_names_types_and_qualifiers() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let (state, plan) = frame.clone().into_parts();
    let stripped = DataFrame::new(state, strip(plan).unwrap());
    assert_eq!(ids(&stripped), named(&["", "", ""]));
    for ((before_qualifier, before), (after_qualifier, after)) in
        frame.schema().iter().zip(stripped.schema().iter())
    {
        assert_eq!(before_qualifier, after_qualifier);
        assert_eq!(before.name(), after.name());
        assert_eq!(before.data_type(), after.data_type());
        assert_eq!(before.is_nullable(), after.is_nullable());
    }
    let bare = source(&context);
    let (_, plan) = bare.clone().into_parts();
    assert_eq!(&strip(plan).unwrap(), bare.logical_plan());
}

#[tokio::test]
async fn stamp_leaves_statement_roots_unchanged() {
    let context = SessionContext::new();
    let probe = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    context
        .register_table(
            "probe_stmt",
            Arc::new(
                datafusion::datasource::memory::MemTable::try_new(
                    Arc::clone(&probe),
                    vec![Vec::new()],
                )
                .unwrap(),
            ),
        )
        .unwrap();
    let query = context.sql("SELECT id FROM probe_stmt").await.unwrap();
    assert!(plan_is_relation(query.logical_plan()));
    for query in [
        "EXPLAIN SELECT id FROM probe_stmt",
        "EXPLAIN ANALYZE SELECT id FROM probe_stmt",
        "DESCRIBE probe_stmt",
        "INSERT INTO probe_stmt VALUES (1)",
        "COPY (SELECT id FROM probe_stmt) TO '/tmp/attr_probe_out' STORED AS PARQUET",
        "DROP TABLE probe_stmt",
    ] {
        let planned = context.sql(query).await.expect(query);
        let (_, plan) = planned.clone().into_parts();
        assert!(!plan_is_relation(&plan), "{query} is a statement root");
        assert_eq!(&stamp(plan.clone()).unwrap(), &plan);
    }
}

#[tokio::test]
async fn strip_survives_optimization_and_execution() {
    let session = ReparkSession::new().unwrap();
    let deep = stamped(
        stamped(source(session.context()))
            .select(vec![col("id"), col("data"), col("s")])
            .unwrap(),
    );
    let deep = stamped(
        deep.select(vec![col("id"), col("data")])
            .unwrap()
            .filter(col("id").gt(lit(0)))
            .unwrap(),
    );
    let (state, plan) = deep.clone().into_parts();
    let stripped = DataFrame::new(state, strip(plan).unwrap());
    for batch in stripped.collect().await.unwrap() {
        for field in batch.schema().fields() {
            assert!(AttrId::of(field).is_none());
        }
    }
    session
        .create_or_replace_temp_view_from("strip_u_l", &deep)
        .unwrap();
    session
        .create_or_replace_temp_view_from("strip_u_r", &deep)
        .unwrap();
    let left = session.sql("SELECT * FROM strip_u_l").await.unwrap();
    let right = session.sql("SELECT * FROM strip_u_r").await.unwrap();
    let joined = join_on_named_keys(
        left,
        right,
        &["id".to_string()],
        JoinType::Inner,
        IgnoreCase,
    )
    .unwrap();
    let (state, plan) = joined.into_parts();
    let stripped = DataFrame::new(state, strip(plan).unwrap());
    assert_eq!(stripped.clone().count().await.unwrap(), 2);
    for batch in stripped.collect().await.unwrap() {
        for field in batch.schema().fields() {
            assert!(AttrId::of(field).is_none());
        }
    }
}

#[tokio::test]
async fn strip_rebuilds_strict_and_by_name_unions() {
    let context = SessionContext::new();
    let left = stamped(source(&context));
    let right = stamped(source(&context));
    for unioned in [
        left.clone().union(right.clone()).unwrap(),
        left.clone().union_by_name(right.clone()).unwrap(),
    ] {
        let (state, plan) = unioned.into_parts();
        let stripped = DataFrame::new(state, strip(plan).unwrap());
        assert_eq!(stripped.clone().count().await.unwrap(), 4);
        let names = ["id", "data", "s"].map(str::to_string);
        assert_eq!(
            stripped
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>(),
            names
        );
        for batch in stripped.collect().await.unwrap() {
            for field in batch.schema().fields() {
                assert!(AttrId::of(field).is_none());
            }
        }
    }
}

#[tokio::test]
async fn strip_refuses_a_keyed_table_scan() {
    let context = SessionContext::new();
    let field =
        Field::new("id", DataType::Int64, false).with_metadata(std::collections::HashMap::from([
            ("repark.attr".to_string(), "a1".to_string()),
        ]));
    let schema = Arc::new(Schema::new(vec![field]));
    let table =
        datafusion::datasource::memory::MemTable::try_new(Arc::clone(&schema), vec![Vec::new()])
            .unwrap();
    context
        .register_table("keyed_probe", Arc::new(table))
        .unwrap();
    let scan = context.table("keyed_probe").await.unwrap();
    let (_, plan) = scan.into_parts();
    assert!(strip(plan).is_err());
}

#[test]
fn strip_record_batches_cleans_schema_and_batches() {
    use datafusion::arrow::array::{Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::SchemaRef;

    let field =
        Field::new("id", DataType::Int64, false).with_metadata(std::collections::HashMap::from([
            ("repark.attr".to_string(), "a1".to_string()),
        ]));
    let schema: SchemaRef = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let (cleaned, batches) =
        crate::session::df_guards::attr_id::strip_record_batches(schema, vec![batch]).unwrap();
    assert!(AttrId::of(&cleaned.fields()[0]).is_none());
    assert_eq!(batches.len(), 1);
    assert!(AttrId::of(&batches[0].schema().fields()[0]).is_none());
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 2]
    );
    let plain: SchemaRef = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&plain),
        vec![Arc::new(Int64Array::from(vec![1]))],
    )
    .unwrap();
    let (same, _) =
        crate::session::df_guards::attr_id::strip_record_batches(Arc::clone(&plain), vec![batch])
            .unwrap();
    assert!(Arc::ptr_eq(&same, &plain));
}

#[test]
fn plan_is_relation_splits_empty_relations_by_their_fields() {
    let bare: DFSchemaRef = Arc::new(DFSchema::empty());
    assert!(!plan_is_relation(&LogicalPlan::EmptyRelation(
        EmptyRelation {
            produce_one_row: false,
            schema: bare,
        }
    )));
    let with_field: DFSchemaRef = Arc::new(
        DFSchema::try_from(Schema::new(vec![Field::new("id", DataType::Int64, false)])).unwrap(),
    );
    assert!(plan_is_relation(&LogicalPlan::EmptyRelation(
        EmptyRelation {
            produce_one_row: false,
            schema: with_field,
        }
    )));
}

#[test]
fn strip_clears_a_non_projection_root() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let aggregated = stamped(
        frame
            .aggregate(vec![col("s")], vec![max(col("id")).alias("top")])
            .unwrap(),
    );
    assert!(ids(&aggregated).iter().all(Option::is_some));
    let (state, plan) = aggregated.into_parts();
    let stripped = DataFrame::new(state, strip(plan).unwrap());
    assert_eq!(ids(&stripped), named(&["", ""]));
    assert_eq!(
        stripped
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>(),
        vec!["s".to_string(), "top".to_string()]
    );
}

fn assert_mirror(frame: &DataFrame, stamped_already: bool) {
    assert_eq!(plan_is_stamped(frame.logical_plan()), stamped_already);
    let restamped = stamp(frame.logical_plan().clone()).unwrap();
    assert_eq!(&restamped == frame.logical_plan(), stamped_already);
}

#[test]
fn plan_is_stamped_matches_what_stamp_would_change() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    assert_mirror(&frame, true);
    assert_mirror(&source(&context), false);
    let pure = frame.clone().select(vec![col("s"), col("id")]).unwrap();
    assert_mirror(&pure, true);
    let computed = frame
        .clone()
        .select(vec![
            col("id"),
            cast(col("data"), DataType::Int64),
            lit(1i64),
        ])
        .unwrap();
    assert_mirror(&computed, false);
    assert_mirror(&stamped(computed), true);
    let chained = frame
        .clone()
        .filter(lit(true))
        .unwrap()
        .limit(0, Some(1))
        .unwrap()
        .sort(vec![col("data").sort(true, false)])
        .unwrap()
        .distinct()
        .unwrap()
        .alias("q")
        .unwrap();
    assert_mirror(&chained, true);
    let grouped = frame
        .clone()
        .aggregate(vec![col("data")], vec![max(col("id"))])
        .unwrap();
    assert_mirror(&grouped, false);
    assert_mirror(&stamped(grouped), true);
    let windowed = frame
        .clone()
        .window(vec![lag(col("id"), Some(1), None).alias("prev")])
        .unwrap();
    assert!(ids(&windowed).iter().all(Option::is_some));
    assert_mirror(&windowed, false);
    assert_mirror(&stamped(windowed), true);
    let having = frame
        .clone()
        .aggregate(vec![col("data")], vec![max(col("id"))])
        .unwrap()
        .filter(lit(true))
        .unwrap();
    assert_mirror(&having, false);
    assert_mirror(&stamped(having), true);
    let self_union = frame.clone().union(frame.clone()).unwrap();
    assert_mirror(&self_union, true);
    let other = stamped(source(&context));
    let mixed = frame.clone().union(other).unwrap();
    assert_mirror(&mixed, false);
    assert_mirror(&stamped(mixed), true);
    let bare: DFSchemaRef = Arc::new(DFSchema::empty());
    assert!(plan_is_stamped(&LogicalPlan::EmptyRelation(
        EmptyRelation {
            produce_one_row: false,
            schema: bare,
        }
    )));
}

fn source_strs(sources: &[Option<AttrId>]) -> Vec<Option<String>> {
    sources
        .iter()
        .map(|id| id.as_ref().map(|id| id.as_str().to_string()))
        .collect()
}

#[test]
fn projection_source_ids_reads_alias_coalesce_and_single_column_case() {
    let context = SessionContext::new();
    let minted = [AttrId::mint(), AttrId::mint(), AttrId::mint()];
    let tags = [minted[0].as_str(), minted[1].as_str(), minted[2].as_str()];
    let frame = tagged_as(&context, tags);
    let aliased = frame
        .clone()
        .select(vec![col("id").alias("renamed"), col("data"), col("s")])
        .unwrap();
    assert_eq!(
        source_strs(&projection_source_ids(aliased.logical_plan())),
        vec![
            Some(tags[0].to_string()),
            Some(tags[1].to_string()),
            Some(tags[2].to_string())
        ]
    );
    let filled = frame
        .clone()
        .select(vec![
            coalesce(vec![col("id"), lit(0)]).alias("id"),
            col("data"),
            col("s"),
        ])
        .unwrap();
    assert_eq!(
        source_strs(&projection_source_ids(filled.logical_plan()))[0],
        Some(tags[0].to_string())
    );
    let fill_case = frame
        .clone()
        .select(vec![
            when(col("data").is_not_null(), col("data"))
                .otherwise(lit("z"))
                .unwrap()
                .alias("data"),
            col("id"),
            col("s"),
        ])
        .unwrap();
    assert_eq!(
        source_strs(&projection_source_ids(fill_case.logical_plan()))[0],
        Some(tags[1].to_string())
    );
    let replaced = frame
        .clone()
        .select(vec![
            when(col("id").eq(lit(2)), lit(200))
                .otherwise(col("id"))
                .unwrap()
                .alias("id"),
            col("data"),
            col("s"),
        ])
        .unwrap();
    assert_eq!(
        source_strs(&projection_source_ids(replaced.logical_plan()))[0],
        Some(tags[0].to_string())
    );
}

#[test]
fn projection_source_ids_refuses_multi_column_and_computed_outputs() {
    let context = SessionContext::new();
    let minted = [AttrId::mint(), AttrId::mint(), AttrId::mint()];
    let tags = [minted[0].as_str(), minted[1].as_str(), minted[2].as_str()];
    let frame = tagged_as(&context, tags);
    let mixed = frame
        .clone()
        .select(vec![
            coalesce(vec![col("id"), col("data")]).alias("id"),
            (col("id") + lit(1)).alias("data"),
            upper(col("s")).alias("s"),
        ])
        .unwrap();
    assert_eq!(
        source_strs(&projection_source_ids(mixed.logical_plan())),
        vec![None, None, None]
    );
}
