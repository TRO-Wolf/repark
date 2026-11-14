use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Float32Array, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::SessionContext;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use super::FloatBoundsInexactFormat;
use crate::session::ReparkSession;

#[derive(Clone, Copy)]
struct Floats {
    wide: [f64; 4],
    narrow: [f32; 4],
}

const CONSTANT: Floats = Floats {
    wide: [f64::NAN, 1.0, f64::NAN, 1.0],
    narrow: [f32::NAN, 1.0, f32::NAN, 1.0],
};
const SPREAD: Floats = Floats {
    wide: [f64::NAN, 0.5, f64::NAN, 1.0],
    narrow: [f32::NAN, 0.5, f32::NAN, 1.0],
};

fn float_batch(values: Floats) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("d", DataType::Float64, true),
        Field::new("f", DataType::Float32, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![20, 21, 22, 23])),
            Arc::new(Float64Array::from(values.wide.to_vec())),
            Arc::new(Float32Array::from(values.narrow.to_vec())),
            Arc::new(StringArray::from(vec!["a", "a", "a", "a"])),
        ],
    )
    .expect("the float batch is well formed")
}

fn optional_batch(wide: [Option<f64>; 4], narrow: [Option<f32>; 4]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("d", DataType::Float64, true),
        Field::new("f", DataType::Float32, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            Arc::new(Float64Array::from(wide.to_vec())),
            Arc::new(Float32Array::from(narrow.to_vec())),
        ],
    )
    .expect("the optional float batch is well formed")
}

const NAN_GROUP: ([Option<f64>; 4], [Option<f32>; 4]) = (
    [Some(f64::NAN), Some(f64::NAN), Some(1.0), Some(2.0)],
    [Some(f32::NAN), Some(f32::NAN), Some(1.0), Some(2.0)],
);
const NULL_NAN_GROUP: ([Option<f64>; 4], [Option<f32>; 4]) = (
    [None, Some(f64::NAN), Some(1.0), Some(2.0)],
    [None, Some(f32::NAN), Some(1.0), Some(2.0)],
);
const NULL_GROUP: ([Option<f64>; 4], [Option<f32>; 4]) = (
    [None, None, Some(1.0), Some(2.0)],
    [None, None, Some(1.0), Some(2.0)],
);

fn write_float_file(dir: &Path, values: Floats, row_group_rows: usize) -> std::path::PathBuf {
    write_batch_file(dir, &float_batch(values), row_group_rows)
}

fn write_batch_file(dir: &Path, batch: &RecordBatch, row_group_rows: usize) -> std::path::PathBuf {
    let path = dir.join("part-0.parquet");
    let file = std::fs::File::create(&path).expect("create the parquet file");
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(row_group_rows))
        .build();
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))
        .expect("open the parquet writer");
    writer.write(batch).expect("write the batch");
    writer.close().expect("close the parquet writer");
    path
}

async fn file_meta(store: &Arc<dyn ObjectStore>, path: &Path) -> ObjectMeta {
    let location = ObjectPath::from_filesystem_path(path).expect("a local object path");
    store.head(&location).await.expect("head the parquet file")
}

async fn stats_pair(values: Floats, row_group_rows: usize) -> (Statistics, Statistics, Statistics) {
    stats_of(&float_batch(values), row_group_rows).await
}

async fn stats_of(
    batch: &RecordBatch,
    row_group_rows: usize,
) -> (Statistics, Statistics, Statistics) {
    let dir = tempfile::tempdir().expect("a temp dir");
    let path = write_batch_file(dir.path(), batch, row_group_rows);
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new());
    let meta = file_meta(&store, &path).await;
    let context = SessionContext::new();
    let state = context.state();
    let inner: Arc<dyn FileFormat> = Arc::new(ParquetFormat::default());
    let schema = inner
        .infer_schema(&state, &store, std::slice::from_ref(&meta))
        .await
        .expect("infer the schema");
    let wrapped = FloatBoundsInexactFormat::new(Arc::clone(&inner));
    let plain = inner
        .infer_stats(&state, &store, Arc::clone(&schema), &meta)
        .await
        .expect("plain statistics");
    let demoted = wrapped
        .infer_stats(&state, &store, Arc::clone(&schema), &meta)
        .await
        .expect("wrapped statistics");
    let combined = wrapped
        .infer_stats_and_ordering(&state, &store, schema, &meta)
        .await
        .expect("wrapped statistics and ordering")
        .statistics;
    (plain, demoted, combined)
}

fn assert_only_float_bounds_inexact(plain: &Statistics, wrapped: &Statistics) {
    assert_eq!(wrapped.num_rows, plain.num_rows);
    assert_eq!(wrapped.num_rows, Precision::Exact(4));
    assert_eq!(wrapped.total_byte_size, plain.total_byte_size);
    assert_eq!(wrapped.column_statistics.len(), 4);
    for (index, (before, after)) in plain
        .column_statistics
        .iter()
        .zip(&wrapped.column_statistics)
        .enumerate()
    {
        assert_eq!(after.null_count, before.null_count, "column {index}");
        assert_eq!(
            after.distinct_count, before.distinct_count,
            "column {index}"
        );
        assert_eq!(after.sum_value, before.sum_value, "column {index}");
        if index == 1 || index == 2 {
            assert!(
                matches!(before.min_value, Precision::Exact(_)),
                "column {index}"
            );
            assert_eq!(after.min_value, before.min_value.clone().to_inexact());
            assert_eq!(after.max_value, before.max_value.clone().to_inexact());
            assert!(
                matches!(after.max_value, Precision::Inexact(_)),
                "column {index}"
            );
        } else {
            assert_eq!(after.min_value, before.min_value, "column {index}");
            assert_eq!(after.max_value, before.max_value, "column {index}");
        }
    }
    assert!(matches!(
        wrapped.column_statistics[0].max_value,
        Precision::Exact(_)
    ));
}

#[tokio::test]
async fn constant_float_bounds_turn_inexact_and_every_other_statistic_is_unchanged() {
    for row_group_rows in [4, 2] {
        let (plain, demoted, combined) = stats_pair(CONSTANT, row_group_rows).await;
        assert_only_float_bounds_inexact(&plain, &demoted);
        assert_only_float_bounds_inexact(&plain, &combined);
    }
}

#[tokio::test]
async fn spread_float_bounds_stay_exact_for_file_pruning() {
    for row_group_rows in [4, 2] {
        let (plain, demoted, combined) = stats_pair(SPREAD, row_group_rows).await;
        assert_eq!(demoted, plain);
        assert_eq!(combined, plain);
        assert!(matches!(
            plain.column_statistics[1].max_value,
            Precision::Exact(_)
        ));
    }
}

async fn first_column_floats(session: &ReparkSession, query: &str) -> Vec<f64> {
    let batches = session
        .sql(query)
        .await
        .expect("plan the query")
        .collect()
        .await
        .expect("run the query");
    let mut out = Vec::new();
    for batch in batches {
        let column = batch.column(0);
        if let Some(values) = column.as_any().downcast_ref::<Float64Array>() {
            out.extend(values.iter().map(|v| v.unwrap_or(f64::INFINITY)));
        } else if let Some(values) = column.as_any().downcast_ref::<Float32Array>() {
            out.extend(values.iter().map(|v| f64::from(v.unwrap_or(f32::INFINITY))));
        } else if let Some(values) = column.as_any().downcast_ref::<Int64Array>() {
            out.extend(values.iter().map(|v| {
                v.and_then(|n| i32::try_from(n).ok())
                    .map_or(f64::INFINITY, f64::from)
            }));
        }
    }
    out
}

fn same_floats(left: &[f64], right: &[f64]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(a, b)| (a.is_nan() && b.is_nan()) || (a - b).abs() < f64::EPSILON)
}

async fn assert_view_keeps_nan(session: &ReparkSession, view: &str, values: Floats) {
    let lowest = values.wide[1].min(values.wide[3]);
    for column in ["d", "f"] {
        let rows =
            first_column_floats(session, &format!("SELECT {column} FROM {view} ORDER BY id")).await;
        assert!(
            same_floats(&rows, &values.wide),
            "{view}.{column} rows: {rows:?}"
        );
        let top = first_column_floats(session, &format!("SELECT max({column}) FROM {view}")).await;
        assert!(
            top.len() == 1 && top[0].is_nan(),
            "{view} max({column}): {top:?}"
        );
        let bottom =
            first_column_floats(session, &format!("SELECT min({column}) FROM {view}")).await;
        assert!(
            same_floats(&bottom, &[lowest]),
            "{view} min({column}): {bottom:?}"
        );
        let nans = first_column_floats(
            session,
            &format!("SELECT count(*) FROM {view} WHERE isnan({column})"),
        )
        .await;
        assert_eq!(nans, vec![2.0], "{view} isnan({column})");
    }
}

#[tokio::test]
async fn read_parquet_keeps_every_nan_and_reads_max_from_the_column() {
    for (values, row_group_rows) in [(CONSTANT, 4), (CONSTANT, 2), (SPREAD, 4), (SPREAD, 2)] {
        let dir = tempfile::tempdir().expect("a temp dir");
        write_float_file(dir.path(), values, row_group_rows);
        let session = ReparkSession::new().expect("a session");
        let frame = session
            .read_parquet(dir.path().to_str().expect("a utf-8 path"))
            .await
            .expect("read the float file");
        session
            .context()
            .register_table("float_read", frame.into_view())
            .expect("register the view");
        assert_view_keeps_nan(&session, "float_read", values).await;
    }
}

#[tokio::test]
async fn create_external_table_keeps_every_nan_and_reads_max_from_the_column() {
    for values in [CONSTANT, SPREAD] {
        let dir = tempfile::tempdir().expect("a temp dir");
        write_float_file(dir.path(), values, 4);
        let session = ReparkSession::new().expect("a session");
        let location = format!("{}/", dir.path().to_str().expect("a utf-8 path"));
        session
            .sql(&format!(
                "CREATE EXTERNAL TABLE float_ext STORED AS PARQUET LOCATION '{location}'"
            ))
            .await
            .expect("plan the external table")
            .collect()
            .await
            .expect("create the external table");
        assert_view_keeps_nan(&session, "float_ext", values).await;
    }
}

fn assert_float_bounds_only_inexact(plain: &Statistics, wrapped: &Statistics) {
    assert_eq!(wrapped.num_rows, plain.num_rows);
    for (index, (before, after)) in plain
        .column_statistics
        .iter()
        .zip(&wrapped.column_statistics)
        .enumerate()
    {
        assert_eq!(after.null_count, before.null_count, "column {index}");
        if index == 0 {
            assert_eq!(after, before, "column {index}");
        } else {
            assert!(
                matches!(before.max_value, Precision::Exact(_)),
                "column {index}: {before:?}"
            );
            assert_eq!(after.min_value, before.min_value.clone().to_inexact());
            assert_eq!(after.max_value, before.max_value.clone().to_inexact());
        }
    }
}

#[tokio::test]
async fn a_row_group_without_float_bounds_turns_the_file_bounds_inexact() {
    for (wide, narrow) in [NAN_GROUP, NULL_NAN_GROUP] {
        let (plain, demoted, combined) = stats_of(&optional_batch(wide, narrow), 2).await;
        assert_float_bounds_only_inexact(&plain, &demoted);
        assert_float_bounds_only_inexact(&plain, &combined);
    }
}

#[tokio::test]
async fn an_all_null_row_group_keeps_the_file_bounds_exact() {
    let (wide, narrow) = NULL_GROUP;
    let (plain, demoted, combined) = stats_of(&optional_batch(wide, narrow), 2).await;
    assert_eq!(demoted, plain);
    assert_eq!(combined, plain);
    assert!(matches!(
        plain.column_statistics[1].max_value,
        Precision::Exact(_)
    ));
}

#[tokio::test]
async fn read_parquet_keeps_a_nan_row_group_under_a_range_and_a_nan_filter() {
    for ((wide, narrow), nans) in [(NAN_GROUP, 2.0), (NULL_NAN_GROUP, 1.0)] {
        let dir = tempfile::tempdir().expect("a temp dir");
        write_batch_file(dir.path(), &optional_batch(wide, narrow), 2);
        let session = ReparkSession::new().expect("a session");
        let frame = session
            .read_parquet(dir.path().to_str().expect("a utf-8 path"))
            .await
            .expect("read the nan row group file");
        session
            .context()
            .register_table("nan_group", frame.into_view())
            .expect("register the view");
        for column in ["d", "f"] {
            for filter in [
                format!("{column} > 5.0"),
                format!("{column} = CAST('NaN' AS DOUBLE)"),
            ] {
                let count = first_column_floats(
                    &session,
                    &format!("SELECT count(*) FROM nan_group WHERE {filter}"),
                )
                .await;
                assert_eq!(count, vec![nans], "{filter}");
            }
        }
    }
}

const NULL_PARTITION_FILES: [&[Option<f64>]; 3] = [
    &[Some(3.0), Some(4.0)],
    &[None, None, Some(1.0), Some(2.0)],
    &[Some(5.0), Some(6.0)],
];
const UMBRELLA_KEY: &str = "datafusion.optimizer.enable_dynamic_filter_pushdown";
const AGGREGATE_KEY: &str = "datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown";

fn write_null_partition_files(dir: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("d", DataType::Float64, true),
    ]));
    let mut first_id = 1;
    for (index, values) in NULL_PARTITION_FILES.into_iter().enumerate() {
        let ids: Vec<i64> = (first_id..).take(values.len()).collect();
        first_id = ids.last().map_or(first_id, |last| last + 1);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .expect("the id and d batch is well formed");
        let file = std::fs::File::create(dir.join(format!("part-{index}.parquet")))
            .expect("create the parquet file");
        let properties = WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), Some(properties))
            .expect("open the parquet writer");
        writer.write(&batch).expect("write the batch");
        writer.close().expect("close the parquet writer");
    }
}

async fn first_row(session: &ReparkSession, query: &str) -> Vec<String> {
    let batches = session
        .sql(query)
        .await
        .expect("plan the query")
        .collect()
        .await
        .expect("run the query");
    let batch = batches
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .expect("one row");
    batch
        .columns()
        .iter()
        .map(|column| {
            arrow::util::display::array_value_to_string(column, 0).expect("display the value")
        })
        .collect()
}

fn aggregate_filter_on(session: &ReparkSession) -> bool {
    session
        .context()
        .state()
        .config()
        .options()
        .optimizer
        .enable_aggregate_dynamic_filter_pushdown
}

#[tokio::test]
async fn an_all_null_row_group_cannot_narrow_an_ungrouped_min() {
    let dir = tempfile::tempdir().expect("a temp dir");
    write_null_partition_files(dir.path());
    let session = ReparkSession::new().expect("a session");
    let frame = session
        .read_parquet(dir.path().to_str().expect("a utf-8 path"))
        .await
        .expect("read the three files");
    session
        .context()
        .register_table("null_partition", frame.into_view())
        .expect("register the view");
    for _ in 0..10 {
        for (query, expected) in [
            ("SELECT max(d), min(d) FROM null_partition", ["6.0", "1.0"]),
            ("SELECT min(d), max(id) FROM null_partition", ["1.0", "8"]),
        ] {
            assert_eq!(first_row(&session, query).await, expected, "{query}");
        }
    }
}

#[tokio::test]
async fn the_dynamic_filter_umbrella_key_cannot_turn_the_aggregate_filter_on() {
    let session = ReparkSession::new().expect("a session");
    assert!(!aggregate_filter_on(&session));
    for statement in [
        format!("SET {UMBRELLA_KEY} = true"),
        format!("SET {UMBRELLA_KEY} = 'true'"),
        format!("SET {UMBRELLA_KEY} TO true"),
        format!("SET VARIABLE {UMBRELLA_KEY} = TRUE"),
    ] {
        let message = match session.sql(&statement).await {
            Ok(_) => panic!("{statement} was accepted"),
            Err(error) => error.to_string(),
        };
        assert!(message.contains(AGGREGATE_KEY), "{statement}: {message}");
        assert!(!aggregate_filter_on(&session), "{statement}");
    }
    assert!(
        ReparkSession::builder()
            .config(UMBRELLA_KEY, "true")
            .build()
            .is_err()
    );
    session
        .sql(&format!("SET {UMBRELLA_KEY} = false"))
        .await
        .expect("the umbrella key turns every dynamic filter off");
    assert!(!aggregate_filter_on(&session));
    session
        .sql(&format!("SET {AGGREGATE_KEY} = true"))
        .await
        .expect("the specific key opts in");
    assert!(aggregate_filter_on(&session));
    let opted = ReparkSession::builder()
        .config(AGGREGATE_KEY, "true")
        .build()
        .expect("the specific key opts in at build");
    assert!(aggregate_filter_on(&opted));
}
