use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::metadata::FieldMetadata;
use datafusion::dataframe::DataFrame;
use datafusion::functions_aggregate::expr_fn::{
    array_agg, avg, count, first_value, last_value, max, min, nth_value, sum,
};
use datafusion::functions_aggregate::first_last::{first_value_udaf, last_value_udaf};
use datafusion::functions_aggregate::min_max::max_udaf;
use datafusion::functions_window::expr_fn::{lag, lead, nth_value as window_nth_value};
use datafusion::logical_expr::expr::{NullTreatment, WindowFunction};
use datafusion::logical_expr::{AggregateUDF, Expr, ExprFunctionExt, LogicalPlan, cast, col, lit};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::{ParquetReadOptions, SessionContext};

use crate::ReparkSession;
use crate::frame_names::{AttrId, stamp};

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

fn tagged(context: &SessionContext) -> DataFrame {
    let tag = |id: &str| FieldMetadata::from(HashMap::from([(KEY.to_string(), id.to_string())]));
    source(context)
        .select(vec![
            col("id").alias_with_metadata("id", Some(tag("t1"))),
            col("data").alias_with_metadata("data", Some(tag("t2"))),
            col("s").alias_with_metadata("s", Some(tag("t3"))),
        ])
        .unwrap()
}

fn stamped(frame: DataFrame) -> DataFrame {
    let (state, plan) = frame.into_parts();
    DataFrame::new(state, stamp(plan).unwrap())
}

fn ids(frame: &DataFrame) -> Vec<Option<String>> {
    frame
        .schema()
        .fields()
        .iter()
        .map(|field| field.metadata().get(KEY).cloned())
        .collect()
}

fn assert_idempotent(frame: &DataFrame) {
    let again = stamp(frame.logical_plan().clone()).unwrap();
    assert_eq!(&again, frame.logical_plan());
}

fn assert_fresh(after: &[Option<String>], held: &[Option<String>], positions: &[usize]) {
    for position in positions {
        let id = &after[*position];
        assert!(id.is_some(), "position {position} has no id");
        assert!(!held.contains(id), "position {position} kept {id:?}");
        let twins = after.iter().filter(|other| *other == id).count();
        assert_eq!(twins, 1, "position {position} shares {id:?}");
    }
}

#[test]
fn datafusion_copies_the_argument_id_onto_these_computed_outputs() {
    let context = SessionContext::new();
    let aggregated = tagged(&context)
        .aggregate(
            vec![(-col("id")).alias("n")],
            vec![
                first_value(col("data"), vec![]).alias("f"),
                last_value(col("s"), vec![]).alias("l"),
            ],
        )
        .unwrap();
    assert_eq!(
        ids(&aggregated),
        vec![
            Some("t1".to_string()),
            Some("t2".to_string()),
            Some("t3".to_string())
        ]
    );
    let windowed = tagged(&context)
        .window(vec![
            lag(col("id"), Some(1), None).alias("prev"),
            lead(col("data"), Some(1), None).alias("next"),
            window_nth_value(col("s"), 1).alias("nth"),
        ])
        .unwrap();
    assert_eq!(
        ids(&windowed)[3..],
        [
            Some("t1".to_string()),
            Some("t2".to_string()),
            Some("t3".to_string())
        ]
    );
}

fn aggregate_values() -> Vec<Expr> {
    vec![
        first_value(col("id"), vec![]).alias("first"),
        last_value(col("id"), vec![]).alias("last"),
        first_value(col("id"), vec![])
            .null_treatment(NullTreatment::IgnoreNulls)
            .build()
            .unwrap()
            .alias("any"),
        min(col("id")).alias("min"),
        max(col("id")).alias("max"),
        sum(col("id")).alias("sum"),
        avg(col("id")).alias("avg"),
        count(col("id")).alias("count"),
        array_agg(col("id")).alias("list"),
        array_agg(col("id"))
            .distinct()
            .build()
            .unwrap()
            .alias("set"),
        nth_value(col("id"), 1, vec![]).alias("nth"),
    ]
}

#[test]
fn every_aggregate_value_the_facade_emits_gets_a_fresh_id() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let held = ids(&frame);
    let values = aggregate_values();
    let width = values.len();
    let grouped = stamped(frame.aggregate(vec![col("data")], values).unwrap());
    let after = ids(&grouped);
    assert_eq!(after[0], held[1]);
    assert_fresh(&after, &held, &(1..=width).collect::<Vec<_>>());
    assert_idempotent(&grouped);
}

#[test]
fn cast_negated_and_arithmetic_group_keys_get_fresh_ids() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let held = ids(&frame);
    let grouped = stamped(
        frame
            .aggregate(
                vec![
                    cast(col("id"), DataType::Utf8).alias("k"),
                    (-col("id")).alias("n"),
                    (col("id") + lit(1)).alias("p"),
                    col("s").alias("t"),
                ],
                vec![count(col("data"))],
            )
            .unwrap(),
    );
    let after = ids(&grouped);
    assert_fresh(&after, &held, &[0, 1, 2, 4]);
    assert_eq!(after[3], held[2]);
    assert_idempotent(&grouped);
}

fn over(function: Arc<AggregateUDF>, name: &str) -> Expr {
    Expr::from(WindowFunction::new(function, vec![col("id")])).alias(name)
}

#[test]
fn every_window_output_gets_a_fresh_id_and_the_input_passes_through() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let held = ids(&frame);
    let windowed = stamped(
        frame
            .window(vec![
                lag(col("id"), Some(1), None).alias("prev"),
                lead(col("data"), Some(1), None).alias("next"),
                window_nth_value(col("s"), 1).alias("nth"),
                over(first_value_udaf(), "first"),
                over(last_value_udaf(), "last"),
                over(max_udaf(), "max"),
            ])
            .unwrap(),
    );
    let after = ids(&windowed);
    assert_eq!(after[..3], held[..]);
    assert_fresh(&after, &held, &[3, 4, 5, 6, 7, 8]);
    assert_idempotent(&windowed);
}

#[test]
fn a_projection_over_a_same_op_window_or_aggregate_mints_what_it_computes() {
    let context = SessionContext::new();
    let frame = stamped(source(&context));
    let held = ids(&frame);
    let selected = stamped(
        frame
            .clone()
            .select(vec![
                col("id"),
                lag(col("id"), Some(1), None).alias("lagged"),
                col("s"),
                lead(col("s"), Some(1), None).alias("led"),
            ])
            .unwrap(),
    );
    let after = ids(&selected);
    assert_eq!(after[0], held[0]);
    assert_eq!(after[2], held[2]);
    assert_fresh(&after, &held, &[1, 3]);
    assert_idempotent(&selected);
    let grouped = stamped(
        frame
            .aggregate(
                vec![col("data")],
                vec![
                    max(col("id")).alias("m"),
                    first_value(col("s"), vec![]).alias("f"),
                ],
            )
            .unwrap(),
    );
    let reselected = stamped(grouped.clone().select(vec![col("f"), col("data")]).unwrap());
    assert_eq!(
        ids(&reselected),
        vec![ids(&grouped)[2].clone(), held[1].clone()]
    );
    assert_idempotent(&reselected);
}

#[tokio::test]
async fn sql_group_by_and_having_over_a_view_mint_the_computed_outputs() {
    let session = ReparkSession::new().unwrap();
    let frame = stamped(source(session.context()));
    let held = ids(&frame);
    session
        .create_or_replace_temp_view_from("fresh_view", &frame)
        .unwrap();
    for sql in [
        "SELECT data, first_value(id) AS f, -id AS n FROM fresh_view GROUP BY data, -id",
        "SELECT data, first_value(id) AS f, max(s) AS n FROM fresh_view GROUP BY data \
         HAVING count(*) > 0",
    ] {
        let grouped = stamped(session.sql(sql).await.unwrap());
        let after = ids(&grouped);
        assert_eq!(after[0], held[1], "{sql}");
        assert_fresh(&after, &held, &[1, 2]);
        assert_idempotent(&grouped);
    }
}

fn foreign_parquet(directory: &std::path::Path) -> String {
    let tag = |name: &str, data_type: DataType, id: &str| {
        Field::new(name, data_type, false)
            .with_metadata(HashMap::from([(KEY.to_string(), id.to_string())]))
    };
    let schema = Arc::new(Schema::new(vec![
        tag("id", DataType::Int64, "a000000000001"),
        tag("data", DataType::Utf8, "a000000000002"),
    ]));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1, 2])),
        Arc::new(StringArray::from(vec!["a", "b"])),
    ];
    let path = directory.join("foreign.parquet");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();
    writer
        .write(&RecordBatch::try_new(schema, columns).unwrap())
        .unwrap();
    writer.close().unwrap();
    path.to_str().unwrap().to_string()
}

#[tokio::test]
async fn a_source_id_minted_by_another_process_is_re_minted_once() {
    let directory = tempfile::tempdir().unwrap();
    let path = foreign_parquet(directory.path());
    let session = ReparkSession::new().unwrap();
    let context = session.context();
    let by_default = context
        .read_parquet(path.as_str(), ParquetReadOptions::default())
        .await
        .unwrap();
    assert_eq!(ids(&by_default), vec![None, None]);
    let read = || async {
        context
            .read_parquet(
                path.as_str(),
                ParquetReadOptions::default().skip_metadata(false),
            )
            .await
            .unwrap()
    };
    let raw = read().await;
    assert!(matches!(raw.logical_plan(), LogicalPlan::TableScan(_)));
    let foreign = ids(&raw);
    assert_eq!(
        foreign,
        vec![
            Some("a000000000001".to_string()),
            Some("a000000000002".to_string())
        ]
    );
    let first = stamped(raw);
    let second = stamped(read().await);
    let first_ids = ids(&first);
    assert_fresh(&first_ids, &foreign, &[0, 1]);
    assert!(first_ids.iter().all(|id| !ids(&second).contains(id)));
    assert_idempotent(&first);
    let reselected = stamped(first.clone().select(vec![col("id"), col("data")]).unwrap());
    assert_eq!(ids(&reselected), first_ids);
    let rows = first
        .clone()
        .aggregate(vec![col("data")], vec![count(col("id"))])
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(rows.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
    assert!(
        first
            .schema()
            .fields()
            .iter()
            .all(|field| AttrId::of(field).is_some_and(|id| id.is_native()))
    );
}
