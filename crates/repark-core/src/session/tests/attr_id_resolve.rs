use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{cast, col, lit};
use datafusion::prelude::SessionContext;

use crate::frame_names::{
    NameRule,
    NameRule::{Exact, IgnoreCase},
    Resolution, resolve, stamp,
};

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

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}

fn resolve_frame(context: &SessionContext) -> DataFrame {
    stamped(
        stamped(source(context))
            .select(vec![
                col("id"),
                col("data").alias("Data"),
                col("s"),
                col("data").alias("__repark_sel_0"),
                lit("z").alias("__repark_sel_1"),
            ])
            .unwrap(),
    )
}

fn resolved(
    frame: &DataFrame,
    written: &str,
    qualifier: Option<&str>,
    rule: NameRule,
    displays: &[&str],
) -> Resolution {
    resolve(
        frame.schema(),
        written,
        qualifier,
        rule,
        &strings(displays),
        None,
    )
    .unwrap()
}

#[test]
fn resolve_binds_a_single_hit_under_each_rule() {
    let context = SessionContext::new();
    let frame = resolve_frame(&context);
    let displays = ["id", "Data", "s", "a", "b"];
    assert_eq!(
        resolved(&frame, "data", None, IgnoreCase, &displays),
        Resolution::Bound(vec![1])
    );
    assert_eq!(
        resolved(&frame, "Data", None, Exact, &displays),
        Resolution::Bound(vec![1])
    );
    assert_eq!(
        resolved(&frame, "data", None, Exact, &displays),
        Resolution::Missing
    );
    assert_eq!(
        resolved(&frame, "nope", None, IgnoreCase, &displays),
        Resolution::Missing
    );
}

#[test]
fn resolve_binds_every_twin_of_one_attribute() {
    let context = SessionContext::new();
    let frame = resolve_frame(&context);
    let same = ["id", "Data", "s", "Data", "b"];
    assert_eq!(
        resolved(&frame, "data", None, IgnoreCase, &same),
        Resolution::Bound(vec![1, 3])
    );
    assert_eq!(
        resolved(&frame, "Data", None, Exact, &same),
        Resolution::Bound(vec![1, 3])
    );
    let folded = ["id", "Data", "s", "DATA", "b"];
    assert_eq!(
        resolved(&frame, "data", None, IgnoreCase, &folded),
        Resolution::Bound(vec![1, 3])
    );
    assert_eq!(
        resolved(&frame, "DATA", None, Exact, &folded),
        Resolution::Bound(vec![3])
    );
}

#[test]
fn resolve_refuses_two_attributes_under_one_name() {
    let context = SessionContext::new();
    let frame = resolve_frame(&context);
    let clash = ["id", "Data", "s", "a", "Data"];
    assert_eq!(
        resolved(&frame, "Data", None, IgnoreCase, &clash),
        Resolution::Ambiguous(vec![1, 4])
    );
    assert_eq!(
        resolved(&frame, "Data", None, Exact, &clash),
        Resolution::Ambiguous(vec![1, 4])
    );
    let folded = ["id", "Data", "s", "a", "DATA"];
    assert_eq!(
        resolved(&frame, "data", None, IgnoreCase, &folded),
        Resolution::Ambiguous(vec![1, 4])
    );
    assert_eq!(
        resolved(&frame, "DATA", None, IgnoreCase, &folded),
        Resolution::Ambiguous(vec![1, 4])
    );
    assert_eq!(
        resolved(&frame, "Data", None, Exact, &folded),
        Resolution::Bound(vec![1])
    );
    assert_eq!(
        resolved(&frame, "DATA", None, Exact, &folded),
        Resolution::Bound(vec![4])
    );
    assert_eq!(
        resolved(&frame, "data", None, Exact, &folded),
        Resolution::Missing
    );
}

#[test]
fn resolve_narrows_by_a_written_qualifier() {
    let context = SessionContext::new();
    let frame = resolve_frame(&context).alias("t").unwrap();
    let displays = ["id", "Data", "s", "a", "b"];
    assert_eq!(
        resolved(&frame, "data", Some("T"), IgnoreCase, &displays),
        Resolution::Bound(vec![1])
    );
    assert_eq!(
        resolved(&frame, "Data", Some("t"), Exact, &displays),
        Resolution::Bound(vec![1])
    );
    assert_eq!(
        resolved(&frame, "Data", Some("T"), Exact, &displays),
        Resolution::Missing
    );
    assert_eq!(
        resolved(&frame, "Data", Some("u"), IgnoreCase, &displays),
        Resolution::Missing
    );
    let bare = resolve_frame(&context);
    assert_eq!(
        resolved(&bare, "Data", Some("t"), IgnoreCase, &displays),
        Resolution::Missing
    );
}

#[test]
fn resolve_refuses_a_cast_twin_that_reuses_the_name() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let twin = stamped(
        frame
            .select(vec![
                col("id"),
                cast(col("id"), DataType::Utf8).alias("__repark_sel_0"),
            ])
            .unwrap(),
    );
    assert_eq!(
        resolved(&twin, "id", None, IgnoreCase, &["id", "id"]),
        Resolution::Ambiguous(vec![0, 1])
    );
}

#[test]
fn resolve_treats_a_missing_id_as_a_bug_and_checks_the_display_count() {
    let context = SessionContext::new();
    let unstamped = source(&context);
    assert!(
        resolve(
            unstamped.schema(),
            "id",
            None,
            IgnoreCase,
            &strings(&["id", "data", "s"]),
            None,
        )
        .is_err()
    );
    assert_eq!(
        resolve(
            unstamped.schema(),
            "zz",
            None,
            IgnoreCase,
            &strings(&["id", "data", "s"]),
            None,
        )
        .unwrap(),
        Resolution::Missing
    );
    let frame = stamped(source(&context));
    assert!(
        resolve(
            frame.schema(),
            "id",
            None,
            IgnoreCase,
            &strings(&["id"]),
            None
        )
        .is_err()
    );
}
