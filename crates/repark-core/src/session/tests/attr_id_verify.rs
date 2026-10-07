use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::JoinType;
use datafusion::common::metadata::FieldMetadata;
use datafusion::dataframe::DataFrame;
use datafusion::functions_aggregate::expr_fn::{count, first_value, last_value};
use datafusion::functions_window::expr_fn::{lag, lead};
use datafusion::logical_expr::{cast, col};
use datafusion::prelude::SessionContext;

use crate::ReparkSession;
use crate::frame_names::{
    NameRule::{Exact, IgnoreCase},
    Resolution, join_collisions, remint_shared, resolve, stamp,
};

const KEY: &str = "repark.attr";

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

fn stamped(frame: DataFrame) -> DataFrame {
    let (state, plan) = frame.into_parts();
    DataFrame::new(state, stamp(plan).unwrap())
}

fn reminted(frame: DataFrame, left_width: usize) -> DataFrame {
    let (state, plan) = frame.into_parts();
    let shared = join_collisions(&plan, left_width).unwrap();
    DataFrame::new(state, remint_shared(plan, left_width, &shared).unwrap().0)
}

fn ids(frame: &DataFrame) -> Vec<Option<String>> {
    frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.metadata().get(KEY).cloned())
        .collect()
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

#[test]
fn va_first_and_last_value_aggregates_get_fresh_ids() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let held = ids(&frame);
    let grouped = stamped(
        frame
            .aggregate(
                vec![col("data")],
                vec![
                    first_value(col("id"), vec![]).alias("f"),
                    last_value(col("s"), vec![]).alias("l"),
                ],
            )
            .unwrap(),
    );
    let after = ids(&grouped);
    assert_eq!(after[0], held[1]);
    assert!(!held.contains(&after[1]), "first_value kept {:?}", after[1]);
    assert!(!held.contains(&after[2]), "last_value kept {:?}", after[2]);
}

#[test]
fn va_cast_and_negative_group_keys_get_fresh_ids() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let held = ids(&frame);
    let grouped = stamped(
        frame
            .aggregate(
                vec![
                    cast(col("id"), DataType::Utf8).alias("k"),
                    (-col("id")).alias("n"),
                ],
                vec![count(col("s"))],
            )
            .unwrap(),
    );
    let after = ids(&grouped);
    assert!(!held.contains(&after[0]), "cast key kept {:?}", after[0]);
    assert!(
        !held.contains(&after[1]),
        "negative key kept {:?}",
        after[1]
    );
}

#[test]
fn va_lag_and_lead_window_roots_get_fresh_ids() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let held = ids(&frame);
    let windowed = stamped(
        frame
            .window(vec![
                lag(col("id"), Some(1), None).alias("prev"),
                lead(col("data"), Some(1), None).alias("next"),
            ])
            .unwrap(),
    );
    let after = ids(&windowed);
    assert_eq!(after[..3], held[..]);
    assert!(!held.contains(&after[3]), "lag kept {:?}", after[3]);
    assert!(!held.contains(&after[4]), "lead kept {:?}", after[4]);
}

#[test]
fn va_lag_through_select_is_ambiguous_with_its_source() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let twin = stamped(
        frame
            .select(vec![
                col("id"),
                lag(col("id"), Some(1), None).alias("__repark_sel_0"),
            ])
            .unwrap(),
    );
    assert_eq!(
        resolve(
            twin.schema(),
            "id",
            None,
            IgnoreCase,
            &strings(&["id", "id"]),
            None,
        )
        .unwrap(),
        Resolution::Ambiguous(vec![0, 1])
    );
}

#[tokio::test]
async fn va_first_value_through_sql_over_a_view_gets_a_fresh_id() {
    let session = ReparkSession::new().unwrap();
    let frame = stamped(source(session.context()));
    let held = ids(&frame);
    session
        .create_or_replace_temp_view_from("va_view", &frame)
        .unwrap();
    let grouped = stamped(
        session
            .sql("SELECT data, first_value(id) AS f FROM va_view GROUP BY data")
            .await
            .unwrap(),
    );
    let after = ids(&grouped);
    assert_eq!(after[0], held[1]);
    assert!(
        !held.contains(&after[1]),
        "sql first_value kept {:?}",
        after[1]
    );
}

#[test]
fn va_union_of_unions_takes_the_outermost_first_input() {
    let context = SessionContext::new();
    let a = stamped(source(&context));
    let b = stamped(source(&context));
    let c = stamped(source(&context));
    let inner = stamped(a.clone().union(b).unwrap());
    assert_eq!(ids(&inner), ids(&a));
    let outer = stamped(inner.clone().union(c.clone()).unwrap());
    assert_eq!(ids(&outer), ids(&a));
    let flipped = stamped(c.clone().union(inner).unwrap());
    assert_eq!(ids(&flipped), ids(&c));
    let again = stamp(outer.logical_plan().clone()).unwrap();
    assert_eq!(&again, outer.logical_plan());
}

fn renamed(frame: DataFrame, left: &str, right: &str) -> DataFrame {
    frame
        .select(vec![
            col(format!("{left}.id")).alias("id"),
            col(format!("{left}.data")).alias("data"),
            col(format!("{left}.s")).alias("s"),
            col(format!("{right}.id")).alias("id2"),
            col(format!("{right}.data")).alias("data2"),
            col(format!("{right}.s")).alias("s2"),
        ])
        .unwrap()
}

#[test]
fn va_three_way_and_nested_self_joins_never_share_an_id_across_sides() {
    let context = SessionContext::new();
    let a = stamped(source(&context));
    let held = ids(&a);
    let j1 = reminted(
        a.clone()
            .alias("l")
            .unwrap()
            .join(
                a.clone().alias("r").unwrap(),
                JoinType::Inner,
                &["l.id"],
                &["r.id"],
                None,
            )
            .unwrap(),
        3,
    );
    let first = ids(&j1);
    assert_eq!(first[..3], held[..]);
    assert!(
        first[3..]
            .iter()
            .all(|id| id.is_some() && !held.contains(id))
    );
    let j1 = stamped(renamed(j1, "l", "r"));
    assert_eq!(ids(&j1), first);
    let j2 = reminted(
        j1.clone()
            .alias("x")
            .unwrap()
            .join(
                a.clone().alias("y").unwrap(),
                JoinType::Inner,
                &["x.id"],
                &["y.id"],
                None,
            )
            .unwrap(),
        6,
    );
    let second = ids(&j2);
    assert_eq!(second[..6], first[..]);
    assert!(
        second[6..]
            .iter()
            .all(|id| id.is_some() && !first.contains(id))
    );
    assert_eq!(second.iter().collect::<HashSet<_>>().len(), 9);
    let j3 = reminted(
        a.clone()
            .alias("p")
            .unwrap()
            .join(
                j1.clone().alias("q").unwrap(),
                JoinType::Inner,
                &["p.id"],
                &["q.id"],
                None,
            )
            .unwrap(),
        3,
    );
    let third = ids(&j3);
    assert_eq!(third[..3], held[..]);
    assert!(!held.contains(&third[3]) && third[3] != first[3]);
    assert_eq!(third[6..], first[3..]);
    let j4 = reminted(
        j1.clone()
            .alias("m")
            .unwrap()
            .join(
                j1.clone().alias("n").unwrap(),
                JoinType::Inner,
                &["m.id"],
                &["n.id"],
                None,
            )
            .unwrap(),
        6,
    );
    let fourth = ids(&j4);
    assert_eq!(fourth[..6], first[..]);
    assert_eq!(fourth.iter().collect::<HashSet<_>>().len(), 12);
}

#[test]
fn va_a_semi_join_is_a_no_op_and_a_qualified_name_binds_one_side() {
    let context = SessionContext::new();
    let a = stamped(source(&context));
    let semi = a
        .clone()
        .alias("l")
        .unwrap()
        .join(
            a.clone().alias("r").unwrap(),
            JoinType::LeftSemi,
            &["l.id"],
            &["r.id"],
            None,
        )
        .unwrap();
    let plan = semi.logical_plan().clone();
    let shared = join_collisions(&plan, 3).unwrap();
    assert_eq!(remint_shared(plan.clone(), 3, &shared).unwrap().0, plan);
    let displays = strings(&["id", "data", "s", "id", "data", "s"]);
    let joined = reminted(
        a.clone()
            .alias("l")
            .unwrap()
            .join(
                a.alias("r").unwrap(),
                JoinType::Inner,
                &["l.id"],
                &["r.id"],
                None,
            )
            .unwrap(),
        3,
    );
    assert_eq!(
        resolve(
            joined.schema(),
            "ID",
            Some("R"),
            IgnoreCase,
            &displays,
            None
        )
        .unwrap(),
        Resolution::Bound(vec![3])
    );
    assert_eq!(
        resolve(joined.schema(), "id", Some("l"), Exact, &displays, None).unwrap(),
        Resolution::Bound(vec![0])
    );
    assert_eq!(
        resolve(joined.schema(), "id", None, Exact, &displays, None).unwrap(),
        Resolution::Ambiguous(vec![0, 3])
    );
}

#[test]
fn va_alias_over_an_aliased_cast_that_carries_an_id_is_re_minted_once() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let held = ids(&frame);
    let tagged = FieldMetadata::from(HashMap::from([(KEY.to_string(), "z9".to_string())]));
    let projected = stamped(
        frame
            .select(vec![
                cast(col("id"), DataType::Utf8)
                    .alias_with_metadata("x", Some(tagged))
                    .alias("y"),
                col("data").alias("d").alias("e"),
            ])
            .unwrap(),
    );
    let after = ids(&projected);
    assert!(after[0].is_some() && after[0] != Some("z9".to_string()) && !held.contains(&after[0]));
    assert_eq!(after[1], held[1]);
    let again = stamp(projected.logical_plan().clone()).unwrap();
    assert_eq!(&again, projected.logical_plan());
}
